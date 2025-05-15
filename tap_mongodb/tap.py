"""MongoDB tap class."""

from __future__ import annotations

import datetime
import json
import os
import sys
from functools import cached_property
from pathlib import Path
from typing import Any

import orjson
import singer_sdk._singerlib.messages
import singer_sdk.helpers._typing
import yaml
from bson import Timestamp
from custom_logger import internal_logger, user_logger
from pymongo.mongo_client import MongoClient
from pymongo.synchronous.cursor import Cursor
from singer_sdk import Stream, Tap
from singer_sdk import typing as th
from singer_sdk._singerlib import Catalog, CatalogEntry, MetadataMapping, Schema
from singer_sdk._singerlib.catalog import Catalog, CatalogEntry
from singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL

from tap_mongodb.collection import CollectionStream

_BLANK = ""
"""A sentinel value to represent a blank value in the config."""

# Monkey patch the singer lib to use orjson
singer_sdk._singerlib.messages.format_message = lambda message: orjson.dumps(
    message.to_dict(), default=lambda o: str(o), option=orjson.OPT_OMIT_MICROSECONDS
).decode("utf-8")


def noop(*args, **kwargs) -> None:
    """No-op function to silence the warning about unmapped properties."""
    pass


# Monkey patch the singer lib to silence the warning about unmapped properties
singer_sdk.helpers._typing._warn_unmapped_properties = noop


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
            description=("A list of databases to include. If this list is empty, all databases" " will be included."),
        ),
        th.Property(
            "database_excludes",
            th.ArrayType(th.StringType),
            description=("A list of databases to exclude. If this list is empty, no databases" " will be excluded."),
        ),
        th.Property(
            "batch_size",
            th.IntegerType,
            description="The number of documents to fetch in a single batch.",
        ),
        th.Property("stream_maps", th.ObjectType()),
        th.Property("stream_map_config", th.ObjectType()),
        th.Property("batch_config", th.ObjectType()),
    ).to_dict()

    def get_mongo_config(self) -> dict[str, Any]:
        mongo_file_location = self.config.get("mongo_file_location", _BLANK)

        if mongo_file_location != _BLANK:
            if Path(mongo_file_location).is_file():
                try:
                    with open(mongo_file_location) as f:
                        return yaml.safe_load(f)
                except ValueError:
                    internal_logger.critical(f"The YAML mongo_file_location '{mongo_file_location}' has errors")
                    sys.exit(1)

        return self.config["mongo"]

    def discover_collections(
        self,
        tap_metadata: dict,
    ) -> list[CatalogEntry]:
        client = MongoClient(**self.get_mongo_config())

        try:
            client.server_info()
        except Exception as exc:
            user_logger.error(f"Could not connect to MongoDB to generate catalog: {exc}")
            sys.exit(1)

        db_includes = self.config.get("database_includes", [])
        db_excludes = self.config.get("database_excludes", [])

        catalog_entries = []
        for db_name in client.list_database_names():
            if db_includes and db_name not in db_includes:
                continue
            if db_excludes and db_name in db_excludes:
                continue
            try:
                collections = client[db_name].list_collection_names()
            except Exception:
                user_logger.warning(
                    f"Skipping database {db_name}, authenticated user does not have permission to access"
                )
                continue
            for collection in collections:
                try:
                    client[db_name][collection].find_one()
                except Exception:
                    user_logger.warning(
                        f"Skipping collection {collection}, authenticated user does not have permission to access",
                    )
                    continue

                user_logger.info(f"Discovered collection {db_name}.{collection}")
                stream_prefix = self.config.get("stream_prefix", _BLANK)
                stream_prefix += db_name.replace("-", "_").replace(".", "_")
                stream_name = f"{stream_prefix}_{collection}"

                stream_metadata = tap_metadata.get(stream_name, {})
                replication_key: str | None = stream_metadata.get("replication-key")
                replication_method: str = stream_metadata.get("replication-method", REPLICATION_FULL_TABLE)

                schema = th.PropertiesList(
                    th.Property("_id", th.StringType),
                    th.Property("document", th.StringType),
                )

                if replication_key:
                    replication_key_type = self.get_replication_key_schema_type(
                        client[db_name][collection].find_one({replication_key: {"$ne": None}}),
                        stream_name,
                        replication_key,
                    )
                    schema.append(th.Property(replication_key, replication_key_type))

                metadata = MetadataMapping.get_standard_metadata(
                    schema=schema.to_dict(),
                    replication_method=replication_method,
                    selected_by_default=True,
                )

                catalog_entry = CatalogEntry(
                    tap_stream_id=stream_name,
                    stream=stream_name,
                    metadata=metadata,
                    key_properties=["_id"],
                    schema=Schema.from_dict(schema.to_dict()),
                    database=db_name,
                    table=collection,
                )

                catalog_entries.append(catalog_entry)

        return catalog_entries

    @cached_property
    def catalog(self) -> Catalog:
        """Get the tap's working catalog.

        Returns:
            A Singer catalog object.
        """
        tap_metadata = json.loads(os.environ[f"{self._env_var_prefix}_METADATA"])
        catalog: Catalog = Catalog()
        catalog_entries: list[CatalogEntry] = []
        catalog_entries.extend(self.discover_collections(tap_metadata))
        for entry in catalog_entries:
            catalog.add_stream(entry=entry)
        return catalog

    def discover_streams(self) -> list[Stream]:  # type: ignore
        """Return a list of discovered streams."""
        client = MongoClient(**self.get_mongo_config())
        try:
            client.server_info()
        except Exception as e:
            raise RuntimeError("Could not connect to MongoDB") from e
        db_includes = self.config.get("database_includes", [])
        db_excludes = self.config.get("database_excludes", [])
        for entry in self.catalog.streams:
            if entry.database in db_excludes:
                continue
            if db_includes and entry.database not in db_includes:
                continue
            stream = CollectionStream(
                tap=self,
                name=entry.tap_stream_id,
                schema=entry.schema,
                collection=client[entry.database][entry.table],
            )
            stream.apply_catalog(self.catalog)
            yield stream

    def get_replication_key_schema_type(
        self, sample_document: dict | None, stream_name: str, replication_key: str
    ) -> th.AnyType | None:
        if sample_document:
            if isinstance(sample_document.get(replication_key), int):
                return th.IntegerType
            elif isinstance(sample_document.get(replication_key), datetime.datetime):
                return th.DateTimeType
            elif isinstance(sample_document.get(replication_key), Timestamp):
                return th.IntegerType
            else:
                self.logger.error(
                    f"Invalid replication key type for stream `{stream_name}`: {type(sample_document.get(replication_key))}. Please choose a different key with type integer or datetime."
                )
                sys.exit(1)

        self.logger.error(
            f"Replication key not found on stream `{stream_name}`. Please choose a different key with type integer or datetime."
        )
        sys.exit(1)


# Use this to run the tap locally
if __name__ == "__main__":
    TapMongoDB.cli()
