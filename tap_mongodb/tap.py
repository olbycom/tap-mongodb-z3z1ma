"""MongoDB tap class."""

from __future__ import annotations

import copy
import datetime
import json
import os
import sys
from functools import cached_property
from pathlib import Path
from typing import Any

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
                    self.internal_logger.critical(f"The YAML mongo_file_location '{mongo_file_location}' has errors")
                    sys.exit(1)

        return self.config["mongo"]

    def discover_collections(
        self,
        tap_metadata: dict,
    ) -> list[CatalogEntry]:
        mongo_config = self.get_mongo_config()
        # Set datetime_conversion to handle out-of-range dates
        if 'datetime_conversion' not in mongo_config:
            mongo_config['datetime_conversion'] = DatetimeConversion.DATETIME_AUTO
        client = MongoClient(**mongo_config)

        db_includes = self.config.get("database_includes", [])
        db_excludes = self.config.get("database_excludes", [])

        catalog_entries = []
        self.user_discovery_logger.info("Discovering databases...")
        databases = client.list_database_names()
        if databases:
            db_list = "\n\t- " + "\n\t- ".join(databases)
            self.user_discovery_logger.info(f"Discovered databases ({len(databases)}): {db_list}")
        else:
            self.user_discovery_logger.error("No databases discovered, please check your configurations.")
            return []

        for db_name in databases:
            if db_includes and db_name not in db_includes:
                self.user_discovery_logger.info(
                    f"Skipping database '{db_name}' (database not in database_includes config)."
                )
                continue
            if db_excludes and db_name in db_excludes:
                self.user_discovery_logger.info(
                    f"Skipping database '{db_name}' (database in database_excludes config)."
                )
                continue
            try:
                self.user_discovery_logger.info(f"Discovering collections for database '{db_name}'...")
                collections = client[db_name].list_collection_names()
            except Exception:
                self.user_discovery_logger.warning(
                    f"Skipping database '{db_name}', authenticated user does not have permission to access."
                )
                continue

            if collections:
                collection_list = "\n\t- " + "\n\t- ".join(collections)
                self.user_discovery_logger.info(
                    f"Discovered {len(collections)} collections for database '{db_name}': {collection_list}"
                )
            else:
                self.user_discovery_logger.warning(f"No collections discovered for database '{db_name}'.")
                continue

            for collection in collections:
                try:
                    client[db_name][collection].find_one()
                except Exception:
                    self.user_discovery_logger.warning(
                        f"Skipping collection '{collection}', authenticated user does not have permission to access.",
                    )
                    continue

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

                if replication_key and replication_key != "_id":  # in case it's _id, we already have it in the schema
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
                    replication_method=replication_method,
                )

                catalog_entries.append(catalog_entry)

        return catalog_entries

    @cached_property
    def catalog(self) -> Catalog:
        """Get the tap's working catalog.

        Override to do LOG_BASED modifications.

        Returns:
            A Singer catalog object.
        """
        tap_metadata = json.loads(os.environ.get(f"{self._env_var_prefix}_METADATA", "{}"))
        catalog: Catalog = Catalog()
        catalog_entries: list[CatalogEntry] = []
        catalog_entries.extend(self.discover_collections(tap_metadata))

        modified_streams: list = []
        for entry in catalog_entries:
            new_entry = copy.deepcopy(entry)
            stream_modified = False

            # If LOG_BASED, apply nullability and _sdc column logic
            if new_entry.replication_method == "LOG_BASED" and new_entry.schema.properties:
                for property in new_entry.schema.properties.values():
                    if "null" not in property.type:
                        if isinstance(property.type, list):
                            property.type.append("null")
                        else:
                            property.type = [property.type, "null"]

                if new_entry.schema.required:
                    stream_modified = True
                    new_entry.schema.required = None

                # Add _sdc columns (aligned with tap-mysql CDC columns)
                if "_sdc_deleted_at" not in new_entry.schema.properties:
                    stream_modified = True
                    new_entry.schema.properties.update({
                        "_sdc_deleted_at": Schema(type=["string", "null"], format="date-time")
                    })
                    new_entry.metadata.update({
                        ("properties", "_sdc_deleted_at"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)
                    })

                if "_sdc_operation" not in new_entry.schema.properties:
                    stream_modified = True
                    new_entry.schema.properties.update({
                        "_sdc_operation": Schema(type=["string", "null"])
                    })
                    new_entry.metadata.update({
                        ("properties", "_sdc_operation"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)
                    })

                if "_sdc_event_timestamp" not in new_entry.schema.properties:
                    stream_modified = True
                    new_entry.schema.properties.update({
                        "_sdc_event_timestamp": Schema(type=["string", "null"], format="date-time")
                    })
                    new_entry.metadata.update({
                        ("properties", "_sdc_event_timestamp"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)
                    })

                if "_sdc_lsn" not in new_entry.schema.properties:
                    stream_modified = True
                    new_entry.schema.properties.update({
                        "_sdc_lsn": Schema(type=["string", "null"])
                    })
                    new_entry.metadata.update({
                        ("properties", "_sdc_lsn"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)
                    })

            if stream_modified:
                modified_streams.append(new_entry.tap_stream_id)

            catalog.add_stream(new_entry)

        if modified_streams:
            self.internal_logger.info(
                "One or more LOG_BASED catalog entries were modified "
                f"({modified_streams=}) to allow nullability and include _sdc columns. "
                "See README for further information."
            )

        return catalog

    def discover_streams(self) -> list[Stream]:  # type: ignore
        """Return a list of discovered streams."""
        self.user_discovery_logger.info("Discovering streams...")

        mongo_config = self.get_mongo_config()
        # Set datetime_conversion to handle out-of-range dates
        if 'datetime_conversion' not in mongo_config:
            mongo_config['datetime_conversion'] = DatetimeConversion.DATETIME_AUTO
        client = MongoClient(**mongo_config)

        try:
            self.user_logger.info("Connecting to MongoDB...")
            info = client.server_info()
            self.user_logger.info(
                "Connected to MongoDB" + (f" (v{info.get('version')})." if info.get("version") else ".")
            )
        except Exception as exc:
            self.user_logger.error(f"Could not connect to MongoDB: {exc}")
            sys.exit(1)

        # Store the client for use in sync operations
        self.mongo_client = client

        # Build a lookup of replication methods from the input catalog (meltano-provided)
        # This is necessary because self.catalog is the discovered catalog, not the user's config
        input_replication_methods = {}
        if self.input_catalog:
            for input_entry in self.input_catalog.streams:
                input_replication_methods[input_entry.tap_stream_id] = input_entry.replication_method

        streams: list[Stream] = []
        for entry in self.catalog.streams:
            # Check the input catalog for the replication method, fallback to discovered entry
            replication_method = input_replication_methods.get(entry.tap_stream_id, entry.replication_method)
            if replication_method == "LOG_BASED":
                stream = MongoDBLogBasedStream(
                    tap=self,
                    catalog_entry=entry,
                    name=entry.tap_stream_id,
                    # Don't pass schema here - let MongoDBLogBasedStream.schema property
                    # handle it so _sdc columns are added dynamically
                )
            else:
                stream = CollectionStream(
                    tap=self,
                    name=entry.tap_stream_id,
                    schema=entry.schema,
                    collection=client[entry.database][entry.table],
                )
            stream.apply_catalog(self.catalog)
            streams.append(stream)

        if streams:
            stream_list = "\n\t- " + "\n\t- ".join([stream.name for stream in streams])
            self.user_discovery_logger.info(f"Discovered streams ({len(streams)}): {stream_list}")
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
                            self.user_logger.info(
                                "Change stream position captured. "
                                "When you switch to LOG_BASED, sync will resume from this point."
                            )
                            return
        except Exception as e:
            self.internal_logger.warning(f"Could not capture change stream position: {e}")
            # Non-fatal - continue with the sync

    def sync_all(self) -> None:
        """Sync all streams."""
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        if self.state:
            self.write_message(StateMessage(value=self.state))

        log_based_streams = [
            stream for stream in self.streams.values()
            if stream.replication_method == "LOG_BASED"
            and stream.selected
            and isinstance(stream, MongoDBLogBasedStream)
        ]
        other_streams = [
            stream for stream in self.streams.values()
            if stream.replication_method != "LOG_BASED" and stream.selected
        ]

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

            stream.sync()
            stream.finalize_state_progress_markers()

        # this second loop is needed for all streams to print out their costs
        # including child streams which are otherwise skipped in the loop above
        for stream in self.streams.values():
            stream.log_sync_costs()

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
                    f"Invalid replication key type for stream `{stream_name}`: {type(sample_document.get(replication_key))}. Allowed types are: int32, int64, date and timestamp."
                )
                sys.exit(1)

        self.logger.error(
            f"Replication key not found on documents for stream `{stream_name}`. Please choose a key that exists on documents with type int32, int64, date or timestamp."
        )
        sys.exit(1)


# Use this to run the tap locally
if __name__ == "__main__":
    TapMongoDB.cli()
