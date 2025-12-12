"""MongoDB stream classes."""

from __future__ import annotations

from tap_mongodb.streams.log_based import MongoDBLogBasedStream
from tap_mongodb.streams.single_log_based import MongoDBSingleLogBasedStream

__all__ = ["MongoDBLogBasedStream", "MongoDBSingleLogBasedStream"]
