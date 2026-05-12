"""MongoDB tap class."""

from __future__ import annotations

import datetime
import sys
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, Any

import nekt_singer_sdk.singerlib.messages
import orjson
import yaml
from bson import Timestamp
from bson.codec_options import DatetimeConversion
from nekt_singer_sdk import Stream, Tap
from nekt_singer_sdk import typing as th
from nekt_singer_sdk.singerlib import Metadata, MetadataMapping, Schema, StateMessage
from nekt_singer_sdk.singerlib.catalog import Catalog, CatalogEntry
from nekt_singer_sdk.streams.core import REPLICATION_FULL_TABLE
from pymongo.mongo_client import MongoClient

from tap_mongodb.collection import CollectionStream
from tap_mongodb.streams import MongoDBLogBasedStream, MongoDBSingleLogBasedStream

if TYPE_CHECKING:
    from collections.abc import Sequence

_BLANK = ""
"""A sentinel value to represent a blank value in the config."""

# Monkey patch the singer lib to use orjson
nekt_singer_sdk.singerlib.messages.format_message = lambda message: orjson.dumps(
    message.to_dict(), default=lambda o: str(o), option=orjson.OPT_OMIT_MICROSECONDS
).decode("utf-8")


class TapMongoDB(Tap):
    """MongoDB tap class."""

    name = "tap-mongodb"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "mongo",
            th.ObjectType(),
            description=(
                "These props are passed directly to pymongo MongoClient allowing the "
                "tap user full flexibility not provided in other Mongo taps since every kwarg "
                "can be tuned."
            ),
            required=True,
        ),
        th.Property(
            "mongo_file_location",
            th.StringType,
            description=("Optional file path, useful if reading mongo configuration from a file."),
            default=_BLANK,
        ),
        th.Property(
            "stream_prefix",
            th.StringType,
            description=(
                "Optionally add a prefix for all streams, useful if ingesting from multiple"
                " shards/clusters via independent tap-mongodb configs. This is applied during"
                " catalog generation. Regenerate the catalog to apply a new stream prefix."
            ),
            default=_BLANK,
        ),
        th.Property(
            "optional_replication_key",
            th.BooleanType,
            description=(
                "This setting allows the tap to continue processing if a document is"
                " missing the replication key. Useful if a very small percentage of documents"
                " are missing the property."
            ),
            default=True,
        ),
        th.Property(
            "database_includes",
            th.ArrayType(th.StringType),
            description=("A list of databases to include. If this list is empty, all databases will be included."),
        ),
        th.Property(
            "database_excludes",
            th.ArrayType(th.StringType),
            description=("A list of databases to exclude. If this list is empty, no databases will be excluded."),
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            description="The number of documents to fetch in a single batch.",
        ),
        th.Property(
            "cursor_timeout",
            th.IntegerType,
            description="Server-side cursor timeout in minutes. Set to 0 to disable timeout (prevents CursorNotFound errors on long-running syncs). 0 is not supported on Atlas free/shared tiers. Defaults to MongoDB's server default (10 minutes) when not set.",
        ),
        th.Property(
            "cdc_image_mode",
            th.StringType,
            description=(
                "LOG_BASED (CDC) replication requires changeStreamPreAndPostImages to "
                "be enabled on every source collection — without it, update events "
                "silently misattribute state and SCD2 history is incorrect. Enable on "
                "each collection with: "
                "db.runCommand({collMod: '<coll>', changeStreamPreAndPostImages: {enabled: true}}). "
                "This setting only controls WHERE enforcement happens; the pre-flight "
                "audit always skips misconfigured streams and fails the run. "
                "'whenAvailable' (default, migration-friendly): MongoDB returns the "
                "post-image when the collection has it enabled, and null otherwise; "
                "the tap crashes loudly on null. Correctly-configured streams still "
                "sync while misconfigured ones are skipped. "
                "'required': MongoDB itself refuses to open the change stream if any "
                "watched collection is not configured. Use once every source collection "
                "is confirmed set up."
            ),
            default="whenAvailable",
            allowed_values=["whenAvailable", "required"],
        ),
        th.Property(
            "max_parallel_streams",
            th.IntegerType,
            description="The number of streams to sync in parallel. Defaults to 1 (sequential).",
            default=1,
        ),
        th.Property("stream_maps", th.ObjectType()),
        th.Property("stream_map_config", th.ObjectType()),
        th.Property("batch_config", th.ObjectType()),
    ).to_dict()

    def __init__(self, *args, **kwargs):
        self._catalog_dict: dict[str, list[dict]] | None = None
        self._streams_missing_replication_key: list[str] = []
        self._streams_with_no_records: list[str] = []
        super().__init__(*args, **kwargs)

    def get_mongo_config(self) -> dict[str, Any]:
        mongo_file_location = self.config.get("mongo_file_location", _BLANK)

        if mongo_file_location != _BLANK:
            if Path(mongo_file_location).is_file():
                try:
                    with open(mongo_file_location) as f:
                        return yaml.safe_load(f)
                except ValueError:
                    self.internal_logger.critical(f"The YAML mongo_file_location '{mongo_file_location}' has errors")
                    sys.exit(1)

        return self.config["mongo"]

    @property
    def mongo_client(self) -> MongoClient:
        """Get the MongoDB client."""
        if not hasattr(self, "_mongo_client"):
            mongo_config = self.get_mongo_config()
            # Set datetime_conversion to handle out-of-range dates
            if "datetime_conversion" not in mongo_config:
                mongo_config["datetime_conversion"] = DatetimeConversion.DATETIME_AUTO
            self._mongo_client = MongoClient(**mongo_config)
        return self._mongo_client

    def discover_collections(
        self,
        # tap_metadata: dict,
    ) -> list[CatalogEntry]:
        db_includes = self.config.get("database_includes", [])
        db_excludes = self.config.get("database_excludes", [])

        catalog_entries = []
        self.user_discovery_logger.info("Discovering databases...")
        databases = self.mongo_client.list_database_names()
        if databases:
            db_list = "\n\t- " + "\n\t- ".join(databases)
            self.user_discovery_logger.info(f"Discovered databases ({len(databases)}): {db_list}")
        else:
            self.user_discovery_logger.error("No databases discovered, please check your configurations.")
            return []

        for db_name in databases:
            if db_includes and db_name not in db_includes:
                self.user_discovery_logger.info(f"Skipping database '{db_name}' (database not in database_includes config).")
                continue
            if db_excludes and db_name in db_excludes:
                self.user_discovery_logger.info(f"Skipping database '{db_name}' (database in database_excludes config).")
                continue
            try:
                self.user_discovery_logger.info(f"Discovering collections for database '{db_name}'...")
                collections = self.mongo_client[db_name].list_collection_names()
            except Exception:
                self.user_discovery_logger.warning(f"Skipping database '{db_name}', authenticated user does not have permission to access.")
                continue

            if collections:
                collection_list = "\n\t- " + "\n\t- ".join(collections)
                self.user_discovery_logger.info(f"Discovered {len(collections)} collections for database '{db_name}': {collection_list}")
            else:
                self.user_discovery_logger.warning(f"No collections discovered for database '{db_name}'.")
                continue

            for collection in collections:
                stream_prefix = self.config.get("stream_prefix", _BLANK)
                stream_prefix += db_name.replace("-", "_").replace(".", "_")
                stream_name = f"{stream_prefix}_{collection}"

                schema = th.PropertiesList(
                    th.Property("_id", th.StringType),
                    th.Property("document", th.StringType),
                )

                # This is to avoid breaking on discovery
                replication_key: str | None = None
                replication_method: str = REPLICATION_FULL_TABLE

                try:
                    replication_key: str | None = self.input_catalog.get(stream_name).replication_key
                    replication_method: str = self.input_catalog.get(stream_name).replication_method

                    # For LOG_BASED replication with _sdc_lsn, skip replication key lookup since
                    # _sdc_lsn is a synthetic column added by the CDC process, not a document field
                    if replication_key and replication_key != "_id" and replication_key != "_sdc_lsn":
                        collection_obj = self.mongo_client[db_name][collection]
                        sample_document = collection_obj.find_one({replication_key: {"$ne": None}})
                        if sample_document is None:
                            # Distinguish empty collection from one whose docs lack the key
                            if collection_obj.find_one({}) is None:
                                self._streams_with_no_records.append(stream_name)
                            else:
                                self._streams_missing_replication_key.append(stream_name)
                        else:
                            replication_key_type = self.get_replication_key_schema_type(
                                sample_document,
                                stream_name,
                                replication_key,
                            )
                            if replication_key_type is not None:
                                schema.append(th.Property(replication_key, replication_key_type))
                except Exception:
                    pass

                metadata = MetadataMapping.get_standard_metadata(
                    schema=schema.to_dict(),
                    replication_method=replication_method,
                    selected_by_default=True,
                )

                if replication_key:
                    metadata[()].replication_key = replication_key

                catalog_entry = CatalogEntry(
                    tap_stream_id=stream_name,
                    stream=stream_name,
                    metadata=metadata,
                    key_properties=["_id"],
                    schema=Schema.from_dict(schema.to_dict()),
                    database=db_name,
                    table=collection,
                    replication_method=replication_method,
                    replication_key=replication_key,
                )

                catalog_entries.append(catalog_entry.to_dict())

        return catalog_entries

    @property
    def mongo_catalog_entries(self) -> MongoClient:
        """Get the MongoDB client."""
        if not hasattr(self, "_mongo_catalog_entries"):
            catalog_entries = self.discover_collections()
            self._mongo_catalog_entries = {catalog_entry["tap_stream_id"]: catalog_entry for catalog_entry in catalog_entries}
        return self._mongo_catalog_entries

    def retrieve_collection(self, catalog_entry: CatalogEntry) -> Any:
        """Retrieve a single collection from CatalogEntry."""
        entry = self.mongo_catalog_entries[catalog_entry["tap_stream_id"]]
        return self.mongo_client[entry["database_name"]][entry["table_name"]]

    @property
    def catalog_dict(self) -> dict:
        if self._catalog_dict:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        result: dict[str, list[dict]] = {"streams": []}
        result["streams"].extend(self.discover_collections())

        self._catalog_dict: dict = result
        return self._catalog_dict

    @cached_property
    def catalog(self) -> Catalog:
        """Get the tap's working catalog.

        Override to do LOG_BASED modifications.

        Returns:
            A Singer catalog object.
        """
        base_catalog = super().catalog
        modified_count = 0
        for stream in base_catalog.streams:
            modified = False

            # If incremental stream
            if stream.replication_method == "INCREMENTAL" and stream.schema.properties:
                if stream.replication_key not in stream.schema.properties:
                    # Add replication key to schema if missing. The discovered entry
                    # may not carry the property when no sample document was available
                    # (empty collection or replication key missing on every document).
                    entry = self.mongo_catalog_entries[stream.tap_stream_id]
                    discovered_property = entry.get("schema").get("properties").get(stream.replication_key)
                    if discovered_property is not None:
                        modified = True
                        stream.schema.properties.update({stream.replication_key: Schema(**discovered_property)})
                        stream.metadata.update({("properties", stream.replication_key): Metadata(Metadata.InclusionType.AVAILABLE, True, None)})

            # If LOG_BASED, apply nullability and _sdc column logic
            if stream.replication_method == "LOG_BASED" and stream.schema.properties:
                for property in stream.schema.properties.values():
                    if "null" not in property.type:
                        if isinstance(property.type, list):
                            property.type.append("null")
                        else:
                            property.type = [property.type, "null"]

                if stream.schema.required:
                    modified = True
                    stream.schema.required = None

                # Add _sdc columns (aligned with tap-mysql CDC columns)
                if "_sdc_deleted_at" not in stream.schema.properties:
                    modified = True
                    stream.schema.properties.update({"_sdc_deleted_at": Schema(type=["string", "null"], format="date-time")})
                    stream.metadata.update({("properties", "_sdc_deleted_at"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)})

                if "_sdc_operation" not in stream.schema.properties:
                    modified = True
                    stream.schema.properties.update({"_sdc_operation": Schema(type=["string", "null"])})
                    stream.metadata.update({("properties", "_sdc_operation"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)})

                if "_sdc_event_timestamp" not in stream.schema.properties:
                    modified = True
                    stream.schema.properties.update({"_sdc_event_timestamp": Schema(type=["string", "null"], format="date-time")})
                    stream.metadata.update({("properties", "_sdc_event_timestamp"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)})

                if "_sdc_lsn" not in stream.schema.properties:
                    modified = True
                    stream.schema.properties.update({"_sdc_lsn": Schema(type=["string", "null"])})
                    stream.metadata.update({("properties", "_sdc_lsn"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)})

            if modified:
                modified_count += 1

        if modified_count:
            self.internal_logger.info(
                f"{modified_count} LOG_BASED catalog entries were modified to allow nullability and include _sdc columns. See README for further information."
            )
        return base_catalog

    @property
    def streams(self) -> dict[str, Stream]:
        if self._streams is None:
            self._streams = {}

            for stream in self.load_streams():
                if self.catalog is not None:
                    stream.apply_catalog(self.catalog)
                self._streams[stream.name] = stream
        return self._streams

    def discover_streams(self) -> Sequence[Stream]:
        streams: list[Stream] = []
        for catalog_entry in self.catalog_dict["streams"]:
            if catalog_entry["replication_method"] == "LOG_BASED":
                streams.append(MongoDBLogBasedStream(self, catalog_entry))
            else:
                streams.append(
                    CollectionStream(
                        self,
                        name=catalog_entry.get("tap_stream_id"),
                        schema=catalog_entry.get("schema"),
                        collection=self.retrieve_collection(catalog_entry),
                    )
                )

        if streams:
            if len(streams) <= 20:
                stream_list = "\n\t- " + "\n\t- ".join([stream.name for stream in streams])
                self.user_discovery_logger.info(f"Discovered streams ({len(streams)}): {stream_list}")
            else:
                preview = "\n\t- " + "\n\t- ".join([stream.name for stream in streams[:10]])
                self.user_discovery_logger.info(f"Discovered streams ({len(streams)}, showing first 10): {preview}\n\t...")
        else:
            self.user_discovery_logger.error("No streams discovered, please check your configurations.")

        return streams

    def _fast_forward_change_stream_position(self) -> None:
        """Capture current change stream position before FULL_TABLE sync.

        This ensures that when the user later switches to LOG_BASED replication,
        the tap will resume from this position and not miss any changes that
        occurred during or after the FULL_TABLE sync.
        """
        from bson import json_util

        self.user_logger.info("Capturing change stream position before full sync...")

        try:
            # Get any collection to open a change stream
            # We just need the current cluster time, any collection will do
            for entry in self.catalog.streams:
                if entry.database and entry.table:
                    collection = self.mongo_client[entry.database][entry.table]
                    with collection.watch(max_await_time_ms=1000) as stream:
                        # Get the current resume token
                        resume_token = stream.resume_token
                        if resume_token:
                            # Store in state under the single_log_based stream key
                            identifier = json_util.dumps(resume_token)
                            if "bookmarks" not in self.state:
                                self.state["bookmarks"] = {}
                            self.state["bookmarks"]["single_log_based"] = {
                                "replication_key": "_sdc_lsn",
                                "replication_key_value": identifier,
                            }
                            self.write_message(StateMessage(value=self.state))
                            self.user_logger.info("Change stream position captured. When you switch to LOG_BASED, sync will resume from this point.")
                            return
        except Exception as e:
            self.internal_logger.warning(f"Could not capture change stream position: {e}")
            # Non-fatal - continue with the sync

    def _install_threadsafe_write(self) -> None:
        """Wrap sys.stdout.write + flush in a lock for thread-safe Singer messages."""
        if hasattr(self, "_stdout_lock"):
            return
        self._stdout_lock = threading.Lock()
        _original_write = sys.stdout.write
        _original_flush = sys.stdout.flush

        def locked_write(msg: str) -> int:
            with self._stdout_lock:
                result = _original_write(msg)
                _original_flush()
                return result

        def locked_flush() -> None:
            with self._stdout_lock:
                _original_flush()

        sys.stdout.write = locked_write  # type: ignore[assignment]
        sys.stdout.flush = locked_flush  # type: ignore[assignment]

    def _sync_streams_parallel(self, streams: list, max_parallel: int) -> list[tuple[str, str]]:
        """Sync streams using a thread pool."""
        self._install_threadsafe_write()
        failed_streams: list[tuple[str, str]] = []
        failed_lock = threading.Lock()

        def sync_one(stream):
            try:
                stream.sync()
                stream.finalize_state_progress_markers()
            except Exception as e:
                self.user_logger.exception(f"Stream '{stream.name}' failed, continuing with remaining streams.")
                with failed_lock:
                    failed_streams.append((stream.name, f"{type(e).__name__}: {e}"))

        self.user_logger.info(f"Syncing {len(streams)} streams with max_parallel_streams={max_parallel}")
        with ThreadPoolExecutor(max_workers=max_parallel) as executor:
            futures = [executor.submit(sync_one, stream) for stream in streams]
            for future in as_completed(futures):
                future.result()  # surfaces unexpected exceptions

        return failed_streams

    def _partition_log_based_by_image_support(
        self,
        log_based_streams: list,
    ) -> tuple[list, list[str]]:
        """Split LOG_BASED streams by whether their collection has
        changeStreamPreAndPostImages enabled. Ineligible streams are excluded
        from the log-based sync — LOG_BASED replication without pre-/post-images
        silently misattributes state to earlier events, breaking SCD2.

        Returns (eligible_streams, skipped_stream_names).

        One listCollections call per distinct database, not per collection.
        """
        by_db: dict[str, dict[str, Any]] = {}
        for s in log_based_streams:
            by_db.setdefault(s.database, {})[s.table] = s

        eligible: list = []
        skipped_names: list[str] = []
        missing_refs: list[str] = []

        for db_name, table_to_stream in by_db.items():
            enabled_tables: set[str] = set()
            try:
                for col in self.mongo_client[db_name].list_collections(filter={"name": {"$in": list(table_to_stream.keys())}}):
                    opt = (col.get("options") or {}).get("changeStreamPreAndPostImages", {})
                    if opt.get("enabled") is True:
                        enabled_tables.add(col["name"])
            except Exception as e:
                self.internal_logger.warning(
                    f"Could not inspect collection options for '{db_name}': {e}. Treating all LOG_BASED streams in this database as ineligible."
                )

            for table, stream in table_to_stream.items():
                if table in enabled_tables:
                    eligible.append(stream)
                else:
                    ref = f"{db_name}.{table}"
                    missing_refs.append(ref)
                    skipped_names.append(stream.name)
                    self.user_logger.error(
                        f"Stream '{stream.name}' ({ref}) SKIPPED from LOG_BASED "
                        f"sync: changeStreamPreAndPostImages is not enabled on "
                        f"the collection. LOG_BASED replication cannot faithfully "
                        f"represent history (SCD2) without it. Enable on the "
                        f"source MongoDB with:\n"
                        f'  db.getSiblingDB("{db_name}").runCommand({{collMod: "{table}", '
                        f"changeStreamPreAndPostImages: {{enabled: true}}}})"
                    )

        if missing_refs:
            # Emit every affected collection — no truncation. Operators need
            # the complete list to fix everything in a single pass; otherwise
            # the next pipeline run would surface the same problem again for
            # collections that were hidden behind a "... N more" placeholder.
            commands = "\n".join(
                f'  db.getSiblingDB("{ref.split(".", 1)[0]}").runCommand('
                f'{{collMod: "{ref.split(".", 1)[1]}", '
                f"changeStreamPreAndPostImages: {{enabled: true}}}})"
                for ref in missing_refs
            )
            self.user_logger.error(
                f"LOG_BASED PRE-FLIGHT SUMMARY: {len(missing_refs)} collection(s) "
                f"skipped because changeStreamPreAndPostImages is not enabled. "
                f"Run each of the following commands on the source MongoDB "
                f"(copy-paste ready, targets the correct database explicitly):\n"
                f"{commands}\n"
                f"After enabling, re-run the pipeline. Until then, these streams "
                f"will not be replicated."
            )

        return eligible, skipped_names

    def sync_all(self) -> None:
        """Sync all streams."""
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        if self.state:
            self.write_message(StateMessage(value=self.state))

        log_based_streams = [
            stream
            for stream in self.streams.values()
            if stream.replication_method == "LOG_BASED" and stream.selected and isinstance(stream, MongoDBLogBasedStream)
        ]
        other_streams = [stream for stream in self.streams.values() if stream.replication_method != "LOG_BASED" and stream.selected]

        skipped_log_based: list[str] = []
        if log_based_streams:
            log_based_streams, skipped_log_based = self._partition_log_based_by_image_support(log_based_streams)

        if log_based_streams:
            log_based_stream = MongoDBSingleLogBasedStream(
                tap=self,
                log_based_streams=log_based_streams,
                mongo_client=self.mongo_client,
            )
            log_based_stream.sync()
            log_based_stream.finalize_state_progress_markers()

        # If running FULL_TABLE streams but no LOG_BASED streams are configured,
        # fast-forward the change stream position BEFORE the full sync.
        # This captures the current position so that when the user later switches
        # to LOG_BASED, it will resume from this point and catch any changes
        # that happened during or after the FULL_TABLE sync.
        if other_streams and not log_based_streams:
            self._fast_forward_change_stream_position()

        streams_to_sync = []
        for stream in other_streams:
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.info("Skipping deselected stream '%s'.", stream.name)
                continue

            if stream.parent_stream_type:
                self.logger.debug(
                    "Child stream '%s' is expected to be called by parent stream '%s'. Skipping direct invocation.",
                    type(stream).__name__,
                    stream.parent_stream_type.__name__,
                )
                continue

            if stream.name in self._streams_missing_replication_key or stream.name in self._streams_with_no_records:
                continue

            streams_to_sync.append(stream)

        max_parallel = self.config.get("max_parallel_streams", 1)
        failed_streams: list[tuple[str, str]] = []

        if max_parallel > 1 and len(streams_to_sync) > 1:
            failed_streams = self._sync_streams_parallel(streams_to_sync, max_parallel)
        else:
            for stream in streams_to_sync:
                try:
                    stream.sync()
                    stream.finalize_state_progress_markers()
                except Exception as e:
                    self.user_logger.exception(f"Stream '{stream.name}' failed, continuing with remaining streams.")
                    failed_streams.append((stream.name, f"{type(e).__name__}: {e}"))

        # this second loop is needed for all streams to print out their costs
        # including child streams which are otherwise skipped in the loop above
        for stream in self.streams.values():
            stream.log_sync_costs()

        if skipped_log_based:
            self.user_logger.error(
                f"{len(skipped_log_based)} LOG_BASED stream(s) skipped due to "
                f"missing changeStreamPreAndPostImages (see pre-flight summary "
                f"above): {', '.join(skipped_log_based)}"
            )
        if self._streams_missing_replication_key:
            self.user_logger.error(
                f"{len(self._streams_missing_replication_key)} stream(s) had no documents containing the configured replication key: "
                f"{', '.join(self._streams_missing_replication_key)}"
            )
        if self._streams_with_no_records:
            self.user_logger.error(
                f"{len(self._streams_with_no_records)} stream(s) had no records in the source collection: "
                f"{', '.join(self._streams_with_no_records)}"
            )
        if failed_streams:
            failure_details = "\n".join(f"\t- {name}: {reason}" for name, reason in failed_streams)
            self.user_logger.error(
                f"{len(failed_streams)} stream(s) failed during sync:\n{failure_details}"
            )

        # Only fail the pipeline if every attempted stream failed. Streams skipped
        # for missing replication key, empty collections, or missing pre/post images
        # are reported above but do not, on their own, fail the run.
        if streams_to_sync and len(failed_streams) == len(streams_to_sync):
            sys.exit(1)

    def get_replication_key_schema_type(self, sample_document: dict | None, stream_name: str, replication_key: str) -> th.AnyType | None:
        if sample_document:
            if isinstance(sample_document.get(replication_key), int):
                return th.IntegerType
            elif isinstance(sample_document.get(replication_key), datetime.datetime):
                return th.DateTimeType
            elif isinstance(sample_document.get(replication_key), Timestamp):
                return th.IntegerType
            elif isinstance(sample_document.get(replication_key), str):
                return th.StringType
            else:
                self.user_logger.error(
                    f"Invalid replication key type for stream `{stream_name}`: {type(sample_document.get(replication_key))}. Allowed types are: int32, int64, date and timestamp."
                )
                return None

        self.user_logger.error(
            f"Replication key not found on documents for stream `{stream_name}`. Please choose a key that exists on documents with type int32, int64, date or timestamp."
        )
        return None


# Use this to run the tap locally
if __name__ == "__main__":
    TapMongoDB.cli()
