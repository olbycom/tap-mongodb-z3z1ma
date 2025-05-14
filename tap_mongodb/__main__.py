"""MongoDB entry point."""

from __future__ import annotations

from tap_mongodb.tap import TapMongoDB

TapMongoDB.cli()
