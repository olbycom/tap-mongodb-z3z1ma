"""MongoDB single log-based stream for change stream handling."""

from __future__ import annotations

import functools
import json
import os
import sys
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Generator

from bson import json_util
from dateutil import parser
from nekt_singer_sdk import Stream, metrics
from nekt_singer_sdk.custom_logger import internal_logger, user_logger
from nekt_singer_sdk.helpers._state import increment_state
from nekt_singer_sdk.helpers._typing import TypeConformanceLevel
from pymongo.mongo_client import MongoClient

if TYPE_CHECKING:
    from collections.abc import Iterable

    from nekt_singer_sdk.helpers import types
    from nekt_singer_sdk.tap_base import Tap

    from tap_mongodb.streams import MongoDBLogBasedStream


class MongoDBSingleLogBasedStream(Stream):
    """Stream class for MongoDB change streams (log-based replication)."""

    replication_key = "_sdc_lsn"
    log_based_streams: list["MongoDBLogBasedStream"] = []

    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    def __init__(
        self,
        tap: "Tap",
        log_based_streams: list["MongoDBLogBasedStream"] = [],
        mongo_client: MongoClient | None = None,
    ):
        super().__init__(
            tap=tap,
            schema={},
            name="single_log_based",
        )
        self.log_based_streams = log_based_streams
        self.mongo_client = mongo_client

    @functools.cached_property
    def schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "name": {"type": "string"},
            },
            "required": ["name"],
        }

    @property
    def tap_stream_id(self) -> str:
        return "single_log_based"

    @property
    def selected(self) -> bool:
        return True

    def write_all_schema_messages(self) -> None:
        for stream in self.log_based_streams:
            if stream.selected:
                stream._write_schema_message()

    def write_all_replication_key_signposts(self, context: types.Context | None = None) -> None:
        for stream in self.log_based_streams:
            signpost = stream.get_replication_key_signpost(context)
            if signpost:
                stream._write_replication_key_signpost(context, signpost)

    def handle_record(
        self,
        record: dict,
        stream_name: str,
        current_context: types.Context | None = None,
        record_index: int = 0,
        write_messages: bool = True,
        record_counter: metrics.RecordCounter | None = None,
    ) -> Generator[dict]:
        stream = [stream for stream in self.log_based_streams if stream.name == stream_name][0]
        if stream.selected:
            if write_messages:
                stream._write_record_message(record)

            self._increment_stream_state(record, context=current_context)
            if (record_index + 1) % self.STATE_MSG_FREQUENCY == 0 and write_messages:
                self._write_state_message()

            record_counter.increment()
            yield record

    def sync(self, context: types.Context | None = None) -> None:
        """Sync this stream.

        This method is internal to the SDK and should not need to be overridden.

        Args:
            context: Stream partition or context dictionary.
        """
        msg = f"Beginning LOG_BASED syncs for {len(self.log_based_streams)} streams"
        if context:
            msg += f" with context: {context}"
        internal_logger.info("%s...", msg)
        self.context = MappingProxyType(context) if context else None

        # Use a replication signpost, if available
        self.write_all_replication_key_signposts(context)

        # Send a SCHEMA message to the downstream target:
        self.write_all_schema_messages()

        try:
            # Sync the records themselves:
            for _ in self._sync_records(context=context):
                pass
        except Exception:
            user_logger.exception("An unhandled error occurred while syncing log-based streams")
            sys.exit(1)

    def fast_forward_to_latest_change_stream(self):
        """Fast-forward the stream state to the latest change stream position."""
        internal_logger.info("Fast-forwarding stream state to the latest change stream position.")

        # If no log_based_streams, we can't get a meaningful token
        if not self.log_based_streams:
            internal_logger.info("No LOG_BASED streams configured, skipping fast-forward.")
            return

        current_context = None
        state = self.get_context_state(current_context)
        self._get_state_partition_context(current_context)
        self._write_starting_replication_value(current_context)

        latest_token = None
        try:
            # Get the latest resume token from a change stream
            # We'll open a change stream briefly to get the current token
            for db_name, collection_name in self._get_watched_collections():
                collection = self.mongo_client[db_name][collection_name]
                with collection.watch(full_document="updateLookup") as stream:
                    # Get the current resume token without waiting for changes
                    latest_token = stream.resume_token
                    if latest_token:
                        break
        except Exception:
            internal_logger.warning("Unable to get latest change stream token.")
            raise

        if latest_token:
            identifier = json_util.dumps(latest_token)
            fake_record = {self.replication_key: identifier}

            treat_as_sorted = self.is_sorted

            increment_state(
                state,
                replication_key=self.replication_key,
                latest_record=fake_record,
                is_sorted=treat_as_sorted,
                check_sorted=self.check_sorted,
            )

            self._finalize_state(state)
            self._write_state_message()
            user_logger.info(f"State fast-forwarded to latest change stream position.")

    def _sync_records(
        self,
        context: types.Context | None = None,
        *,
        write_messages: bool = True,
    ) -> Generator[dict, Any, Any]:
        # Initialize metrics
        record_counter = metrics.record_counter(self.name)
        timer = metrics.sync_timer(self.name)

        # Reset the last resume token tracker
        self._last_resume_token = None

        record_index = 0
        context_element: types.Context | None
        context_list: list[types.Context] | list[dict] | None = None

        with record_counter, timer:
            for context_element in context_list or [{}]:
                record_counter.context = context_element
                timer.context = context_element

                current_context = context_element or None
                state = self.get_context_state(current_context)
                state_partition_context = self._get_state_partition_context(
                    current_context,
                )
                self._write_starting_replication_value(current_context)

                for _, record_result in enumerate(self.get_records(current_context)):
                    record, stream_name = record_result
                    yield from self.handle_record(
                        record, stream_name, current_context, record_index, write_messages, record_counter
                    )
                    record_index += 1

                # If no records were processed but we have a resume token, update state
                if record_index == 0 and hasattr(self, '_last_resume_token') and self._last_resume_token:
                    identifier = json_util.dumps(self._last_resume_token)
                    fake_record = {self.replication_key: identifier}
                    increment_state(
                        state,
                        replication_key=self.replication_key,
                        latest_record=fake_record,
                        is_sorted=self.is_sorted,
                        check_sorted=self.check_sorted,
                    )
                    internal_logger.info("Updated state with current change stream position (no new records).")

                if current_context == state_partition_context:
                    # Finalize per-partition state only if 1:1 with context
                    self._finalize_state(state)

        if not context:
            # Finalize total stream only if we have the full context.
            # Otherwise will be finalized by tap at end of sync.
            self._finalize_state(self.stream_state)

        if write_messages:
            # Write final state message if we haven't already
            self._write_state_message()

    def _get_watched_collections(self) -> list[tuple[str, str]]:
        """Get list of (database, collection) tuples to watch."""
        collections = []
        for stream in self.log_based_streams:
            # Use the database and table stored on the stream
            db_name = stream.database
            collection_name = stream.table
            collections.append((db_name, collection_name))
        return collections

    def load_resume_token(self) -> dict | None:
        """Loads a BSON-safe resume token from state."""
        start_lsn = self.get_starting_replication_key_value(context=None)
        if start_lsn:
            try:
                return json_util.loads(start_lsn)
            except Exception as e:
                internal_logger.warning(f"Error loading resume token from state: {e}. Starting fresh.")
        return None

    def create_change_stream(self, resume_token: dict | None = None):
        """Create a change stream for all watched collections.

        MongoDB change streams can watch entire databases or the entire deployment.
        We'll watch at the database level for each unique database.
        """
        # Group collections by database
        db_collections = {}
        for db_name, collection_name in self._get_watched_collections():
            if db_name not in db_collections:
                db_collections[db_name] = []
            db_collections[db_name].append(collection_name)

        # For now, we'll watch each database separately
        # In the future, we could optimize by watching the entire deployment
        # if we have access to multiple databases

        # Start with the first database (can be extended to watch multiple)
        if not db_collections:
            user_logger.error("No collections to watch for change streams")
            sys.exit(1)

        # For simplicity, watch at deployment level with filters
        pipeline = []

        # Add filters for specific databases and collections
        match_conditions = []
        for db_name, collections in db_collections.items():
            for collection in collections:
                match_conditions.append({
                    "ns.db": db_name,
                    "ns.coll": collection
                })

        if match_conditions:
            pipeline.append({
                "$match": {
                    "$or": match_conditions
                }
            })
            internal_logger.debug(f"Change stream pipeline filter: {match_conditions}")

        kwargs = {
            "pipeline": pipeline,
            "full_document": "updateLookup",
            # max_await_time_ms: Maximum time in milliseconds for the server to wait
            # for new changes before returning an empty batch. This prevents the
            # change stream from blocking indefinitely. 1000ms = 1 second.
            "max_await_time_ms": 1000,
        }

        if resume_token:
            kwargs["resume_after"] = resume_token

        # Watch at the client level to capture all databases
        return self.mongo_client.watch(**kwargs)

    def handle_insert(
        self,
        change: dict,
        stream_name: str,
    ) -> dict[str, Any]:
        """Handle insert operation from change stream."""
        document = change["fullDocument"]
        resume_token = change["_id"]
        cluster_time = change.get("clusterTime")

        # Get the stream to process the document
        stream = [s for s in self.log_based_streams if s.name == stream_name][0]

        # Add _sdc columns (aligned with tap-mysql CDC columns)
        sdc_lsn = json_util.dumps(resume_token)

        # Create the record similar to CollectionStream
        record = {
            "_id": str(document["_id"]),
            "document": json.dumps(document, default=self._handle_unusual_types),
        }

        # Add replication key if it exists and is not _id
        if stream.replication_key and stream.replication_key != "_id":
            if stream.replication_key in document:
                record[stream.replication_key] = document[stream.replication_key]

        record["_sdc_lsn"] = sdc_lsn
        record["_sdc_operation"] = "INSERT"
        record["_sdc_event_timestamp"] = cluster_time.as_datetime().isoformat() if cluster_time else None
        record["_sdc_deleted_at"] = None

        return record

    def handle_update(
        self,
        change: dict,
        stream_name: str,
    ) -> dict[str, Any]:
        """Handle update operation from change stream."""
        document = change["fullDocument"]
        resume_token = change["_id"]
        cluster_time = change.get("clusterTime")

        # Get the stream to process the document
        stream = [s for s in self.log_based_streams if s.name == stream_name][0]

        # Add _sdc columns (aligned with tap-mysql CDC columns)
        sdc_lsn = json_util.dumps(resume_token)

        # Create the record similar to CollectionStream
        record = {
            "_id": str(document["_id"]),
            "document": json.dumps(document, default=self._handle_unusual_types),
        }

        # Add replication key if it exists and is not _id
        if stream.replication_key and stream.replication_key != "_id":
            if stream.replication_key in document:
                record[stream.replication_key] = document[stream.replication_key]

        record["_sdc_lsn"] = sdc_lsn
        record["_sdc_operation"] = "UPDATE"
        record["_sdc_event_timestamp"] = cluster_time.as_datetime().isoformat() if cluster_time else None
        record["_sdc_deleted_at"] = None

        return record

    def handle_delete(
        self,
        change: dict,
        stream_name: str,
    ) -> dict[str, Any]:
        """Handle delete operation from change stream."""
        document_key = change["documentKey"]
        resume_token = change["_id"]
        cluster_time = change.get("clusterTime")

        # Add _sdc columns (aligned with tap-mysql CDC columns)
        sdc_lsn = json_util.dumps(resume_token)
        event_timestamp = cluster_time.as_datetime().isoformat() if cluster_time else None

        # For deletes, we only have the _id
        record = {
            "_id": str(document_key["_id"]),
            "document": json.dumps({"_id": document_key["_id"]}, default=self._handle_unusual_types),
        }

        record["_sdc_lsn"] = sdc_lsn
        record["_sdc_operation"] = "DELETE"
        record["_sdc_event_timestamp"] = event_timestamp
        record["_sdc_deleted_at"] = event_timestamp

        return record

    def handle_replace(
        self,
        change: dict,
        stream_name: str,
    ) -> dict[str, Any]:
        """Handle replace operation from change stream."""
        # Replace is similar to update
        return self.handle_update(change, stream_name)

    def _handle_unusual_types(self, obj):
        """Handle unusual MongoDB types for JSON serialization."""
        import datetime
        from bson import ObjectId, Timestamp
        from bson.datetime_ms import DatetimeMS

        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, DatetimeMS):
            return None
        elif isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, Timestamp):
            return str(obj)
        else:
            return str(obj)

    def get_records(self, context: dict | None) -> Iterable[tuple[dict[str, Any], str]]:
        """Get records from MongoDB change stream."""
        resume_token = self.load_resume_token()

        if resume_token:
            user_logger.info(f"Resuming change stream from saved position.")
        else:
            user_logger.info(f"Starting new change stream (no resume token found).")

        change_stream = self.create_change_stream(resume_token=resume_token)

        # Track consecutive empty iterations to know when we've caught up
        empty_iterations = 0
        max_empty_iterations = 3  # Exit after 3 consecutive empty polls (3 seconds with 1s timeout)
        records_processed = 0

        try:
            while True:
                # try_next() returns the next change or None if no change is available
                # within max_await_time_ms. This is non-blocking with our timeout setting.
                change = change_stream.try_next()

                if change is None:
                    # No change available within the timeout period
                    empty_iterations += 1
                    if empty_iterations >= max_empty_iterations:
                        user_logger.info(
                            f"No new changes detected after {max_empty_iterations} seconds. "
                            "Change stream sync complete."
                        )
                        # If we didn't process any records, we still need to save the current
                        # resume token so we don't re-read the same position next time
                        if records_processed == 0:
                            current_token = change_stream.resume_token
                            if current_token:
                                # Yield a "marker" record with just the resume token
                                # This ensures state gets updated even with no changes
                                marker_record = {
                                    self.replication_key: json_util.dumps(current_token),
                                }
                                # Store the token for state update but don't yield as a real record
                                self._last_resume_token = current_token
                        break
                    continue

                # Reset counter when we get a change
                empty_iterations = 0
                records_processed += 1

                operation = change["operationType"]
                ns = change["ns"]
                db_name = ns["db"]
                collection_name = ns["coll"]

                internal_logger.debug(
                    f"Change stream event: operation={operation}, db={db_name}, collection={collection_name}"
                )

                # Find the matching stream
                stream_name = None
                for log_stream in self.log_based_streams:
                    if (log_stream.database == db_name and
                        log_stream.table == collection_name):
                        stream_name = log_stream.name
                        break

                if not stream_name:
                    continue

                # Process the change based on operation type
                record = None
                if operation == "insert":
                    record = self.handle_insert(change, stream_name)
                elif operation == "update":
                    record = self.handle_update(change, stream_name)
                elif operation == "delete":
                    record = self.handle_delete(change, stream_name)
                elif operation == "replace":
                    record = self.handle_replace(change, stream_name)
                else:
                    internal_logger.warning(f"Unsupported operation type: {operation}")
                    continue

                if record:
                    # Transform and yield the record
                    transformed_record = self.post_process(record)
                    yield transformed_record, stream_name
        finally:
            change_stream.close()

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        """Post-process records to ensure _sdc_lsn is a string."""
        # _sdc_lsn is already a JSON string from json_util.dumps
        return row

    @property
    def is_sorted(self) -> bool:
        return True

    def _increment_stream_state(
        self,
        latest_record: types.Record,
        *,
        context: types.Context | None = None,
    ) -> None:
        # This also creates a state entry if one does not yet exist:
        state_dict = self.get_context_state(context)

        # Advance state bookmark values if applicable
        if latest_record:
            if not self.replication_key:
                msg = f"Could not detect replication key for '{self.name}' stream(replication method={self.replication_method})"
                raise ValueError(msg)

            # The _sdc_lsn is already a string from json_util.dumps, no conversion needed
            treat_as_sorted = self.is_sorted
            if not treat_as_sorted and self.state_partitioning_keys is not None:
                # Streams with custom state partitioning are not resumable.
                treat_as_sorted = False
            increment_state(
                state_dict,
                replication_key=self.replication_key,
                latest_record=latest_record,
                is_sorted=treat_as_sorted,
                check_sorted=self.check_sorted,
            )
