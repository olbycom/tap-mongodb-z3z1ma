"""MongoDB tap class."""

from __future__ import annotations

import datetime
import json
import os
import sys
from functools import cached_property
from typing import Any, Generator, Iterable

import nekt_singer_sdk.singerlib as singer
from bson.datetime_ms import DatetimeMS
from bson.objectid import ObjectId
from bson.timestamp import Timestamp
from nekt_singer_sdk import Stream
from nekt_singer_sdk.custom_logger import user_logger
from nekt_singer_sdk.helpers._state import increment_state
from nekt_singer_sdk.helpers._util import utc_now
from nekt_singer_sdk.plugin_base import PluginBase as TapBaseClass
from pymongo.collection import Collection
from singer_sdk.streams.core import (
    REPLICATION_INCREMENTAL,
    REPLICATION_LOG_BASED,
    TypeConformanceLevel,
)


class CollectionStream(Stream):
    """Collection stream class.

    This stream is used to represent a collection in a database. It is a generic
    stream that can be used to represent any collection in a database."""

    # The output stream will always have _id as the primary key
    primary_keys = ["_id"]

    # No conformance level is set by default since this is a generic stream
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.NONE

    def __init__(
        self,
        tap: TapBaseClass,
        schema: str | os.PathLike | dict[str, Any] | singer.Schema | None = None,
        name: str | None = None,
        *,
        collection: Collection,
    ) -> None:
        """Initialize the stream."""
        super().__init__(tap=tap, schema=schema, name=name)
        self._collection = collection

    @cached_property
    def replication_key_mongo_type(self) -> str:
        doc = self._collection.find_one({self.replication_key: {"$ne": None}})

        if not doc:
            user_logger.error(f"Replication key not found on documents for collection `{self.name}`. Please choose a different key and try again.")
            sys.exit(1)

        if isinstance(doc.get(self.replication_key), int):
            return "integer"
        elif isinstance(doc.get(self.replication_key), str):
            return "string"
        elif isinstance(doc.get(self.replication_key), datetime.datetime):
            return "datetime"
        elif isinstance(doc.get(self.replication_key), Timestamp):
            return "timestamp"
        elif isinstance(doc.get(self.replication_key), ObjectId):
            return "objectid"
        else:
            user_logger.error(
                f"Type not supported for replication key `{self.replication_key}` for collection `{self.name}`. Please choose an integer, date or timestamp field."
            )
            sys.exit(1)

    def get_records(self, context: dict | None) -> Iterable[dict]:
        cursor_timeout = self.config.get("cursor_timeout")
        no_timeout = cursor_timeout == 0
        bookmark = self._get_mongo_compatible_replication_key(context, self._collection)
        if bookmark:
            cursor = self._collection.find(
                {self.replication_key: {"$gt": bookmark}},
                no_cursor_timeout=no_timeout,
            ).sort(self.replication_key, -1)
        else:
            cursor = self._collection.find(no_cursor_timeout=no_timeout)

        batch_size = self.config.get("batch_size")
        if batch_size and batch_size > 0:
            cursor = cursor.batch_size(batch_size)

        try:
            for record in cursor:
                processed_record = {
                    "_id": record["_id"],
                    "document": json.dumps(record, default=self._handle_unusual_types),
                }
                if self.replication_key and self.replication_key != "_id":
                    replication_value = record.get(self.replication_key)
                    if replication_value is not None:
                        processed_record[self.replication_key] = self._process_replication_key_value(replication_value)
                transformed_record = self.post_process(processed_record, context)
                yield transformed_record
        finally:
            cursor.close()

    def _handle_unusual_types(self, obj):
        if isinstance(obj, datetime.datetime):
            return obj.isoformat()
        elif isinstance(obj, DatetimeMS):
            # Handle out-of-range dates from DATETIME_AUTO conversion
            # Return None (null in JSON) for out-of-range dates that can't be
            # represented as valid datetime strings
            return None
        elif isinstance(obj, ObjectId):
            return str(obj)
        elif isinstance(obj, Timestamp):
            return str(obj)
        else:
            return str(obj)

    def _process_replication_key_value(self, value: Any) -> Any:
        if self.replication_key_mongo_type == "timestamp":
            return self._from_timestamp_to_int(value)
        else:
            return value

    def _from_timestamp_to_int(self, timestamp: Timestamp) -> int:
        return int((timestamp.time << 32) | timestamp.inc)

    def _from_int_to_timestamp(self, timestamp: int) -> Timestamp:
        return Timestamp(timestamp >> 32, timestamp & 0xFFFFFFFF)

    def _get_mongo_compatible_replication_key(self, context: dict | None, collection: Collection) -> Any | None:
        bookmark = self.get_starting_replication_key_value(context)
        if not bookmark:
            return None

        if self.replication_key_mongo_type == "integer" or self.replication_key_mongo_type == "string":
            return bookmark
        elif self.replication_key_mongo_type == "datetime":
            return datetime.datetime.fromisoformat(bookmark)
        elif self.replication_key_mongo_type == "timestamp":
            return self._from_int_to_timestamp(bookmark)
        elif self.replication_key_mongo_type == "objectid":
            return ObjectId(bookmark)
        else:
            user_logger.error(
                f"Type not supported for replication key `{self.replication_key}` for collection `{self.name}`. Please choose an integer, date or timestamp field."
            )
            sys.exit(1)

    def _generate_record_messages(
        self,
        record: dict,
    ) -> Generator[singer.RecordMessage, None, None]:
        for stream_map in self.stream_maps:
            mapped_record = stream_map.transform(record)
            if mapped_record is not None:
                record_message = singer.RecordMessage(
                    stream=stream_map.stream_alias,
                    record=mapped_record,
                    version=None,
                    time_extracted=utc_now(),
                )
                yield record_message

    def _increment_stream_state(self, latest_record: dict[str, Any], *, context: dict | None = None) -> None:
        """This override adds error handling for replication key incrementing.

        This is useful since a single bad document could otherwise break the stream."""
        state_dict = self.get_context_state(context)
        if latest_record:
            if self.replication_method in [REPLICATION_INCREMENTAL, REPLICATION_LOG_BASED]:
                if not self.replication_key:
                    raise ValueError(f"Could not detect replication key for '{self.name}' stream(replication method={self.replication_method})")
                treat_as_sorted = self.is_sorted
                if not treat_as_sorted and self.state_partitioning_keys is not None:
                    # Streams with custom state partitioning are not resumable.
                    treat_as_sorted = False
                try:
                    increment_state(
                        state_dict,
                        replication_key=self.replication_key,
                        latest_record=latest_record,
                        is_sorted=treat_as_sorted,
                        check_sorted=self.check_sorted,
                    )
                except Exception as e:
                    # Handle the case where the replication key is not in the latest record
                    # since this is a valid case for Mongo
                    if self.config.get("optional_replication_key", False):
                        self.logger.warn("Failed to increment state. Ignoring...")
                        return
                    raise RuntimeError("Failed to increment state. Got record %s", latest_record) from e


class MockCollection:
    """Mock collection class.

    This class is used to mock a collection in the unit tests."""

    def __init__(self, name: str, schema: dict[str, Any]) -> None:
        self.name = name
        self.schema = schema

    def find(self, query: dict[str, Any]) -> list[dict[str, Any]]:
        """Mock find method."""
        return [{"_id": "1", "name": "test"}]

    def aggregate(self, pipeline: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Mock aggregate method."""
        return [{"_id": "1", "name": "test"}]

    def distinct(self, key: str) -> list[str]:
        """Mock distinct method."""
        return ["test"]

    def count_documents(self, query: dict[str, Any]) -> int:
        """Mock count_documents method."""
        return 1

    def drop(self) -> None:
        """Mock drop method."""
        pass
