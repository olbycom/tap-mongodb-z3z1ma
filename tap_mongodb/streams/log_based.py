"""MongoDB log-based stream class."""

from __future__ import annotations

import functools
import os
from typing import TYPE_CHECKING, Any, Iterable, cast

from nekt_singer_sdk import Stream
from nekt_singer_sdk.helpers._typing import TypeConformanceLevel

if TYPE_CHECKING:
    import nekt_singer_sdk.singerlib as singer
    from nekt_singer_sdk.plugin_base import PluginBase as TapBaseClass


class MongoDBLogBasedStream(Stream):
    """Stream class for MongoDB log-based (change stream) replication."""

    replication_key = "_sdc_lsn"

    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    def __init__(
        self,
        tap: TapBaseClass,
        catalog_entry: Any,
        schema: str | os.PathLike | dict[str, Any] | singer.Schema | None = None,
        name: str | None = None,
    ) -> None:
        """Initialize the stream."""
        # Store catalog entry BEFORE calling super().__init__ because
        # it may access the schema property which uses _catalog_entry
        self._catalog_entry = catalog_entry
        # Store database and table from catalog entry for change stream filtering
        self.database = catalog_entry.database
        self.table = catalog_entry.table
        super().__init__(tap=tap, schema=schema, name=name)

    @functools.cached_property
    def schema(self) -> dict:
        """Override schema for log-based replication adding _sdc columns."""
        # Use the stored catalog entry's schema to avoid circular references
        schema_dict = cast(dict, self._catalog_entry.schema.to_dict())

        # Ensure all properties are nullable for log-based replication
        for property in schema_dict["properties"].values():
            if isinstance(property["type"], list):
                if "null" not in property["type"]:
                    property["type"].append("null")
            else:
                property["type"] = [property["type"], "null"]

        # Remove required fields
        if "required" in schema_dict:
            schema_dict.pop("required")

        # Add _sdc columns (aligned with tap-mysql CDC columns)
        schema_dict["properties"].update({
            "_sdc_deleted_at": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "_sdc_operation": {
                "type": ["string", "null"]
            },
            "_sdc_event_timestamp": {
                "type": ["string", "null"],
                "format": "date-time"
            },
            "_sdc_lsn": {
                "type": ["string", "null"]
            },
        })

        return schema_dict

    def get_records(self, context: dict | None) -> Iterable[dict]:
        """This method is not used - records are fetched via MongoDBSingleLogBasedStream."""
        # This stream doesn't fetch records directly - the MongoDBSingleLogBasedStream
        # handles all the actual CDC work and yields records for this stream
        return iter([])  # Return empty iterator
