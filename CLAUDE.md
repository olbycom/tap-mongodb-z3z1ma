# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
tap-mongodb is a Singer tap that extracts data from MongoDB databases using the Meltano Singer SDK. It outputs data in Singer specification format with three strategies: raw (flexible schema), envelope (wrapped with fixed schema), and infer (strongly-typed from samples).

## Development Commands

### Setup
```bash
poetry install
```

### Run Tests
```bash
poetry run pytest                     # Run all tests
poetry run pytest tests/test_core.py  # Run specific test file
```

### Code Quality
```bash
# Format code
poetry run black tap_mongodb/
poetry run isort tap_mongodb/

# Check linting
poetry run black --check tap_mongodb/
poetry run flake8 tap_mongodb
poetry run mypy tap_mongodb --exclude='tap_mongodb/tests'

# Run all checks via tox
tox -e lint
```

### Run the Tap
```bash
# Direct execution
poetry run tap-mongodb --config config.json --discover > catalog.json
poetry run tap-mongodb --config config.json --catalog catalog.json

# With Meltano
meltano invoke tap-mongodb --version
meltano elt tap-mongodb target-jsonl
```

## Architecture

### Core Components
- **tap.py**: Main TapMongoDB class handles MongoDB connection, database/collection discovery, and configuration validation. All pymongo MongoClient kwargs are passed through via the `mongo` config object.
- **collection.py**: CollectionStream implements data extraction with incremental sync support using replication keys (int, datetime, timestamp, ObjectId types).

### Key Implementation Details
- The tap monkey-patches Singer SDK to use orjson for performance and to silence unmapped property warnings
- Replication keys support multiple types with special handling for MongoDB-specific types
- Document transformation handles unusual types (ObjectId, Timestamp) by converting to strings/ISO format
- Three output strategies controlled by `strategy` config option

### Configuration Pattern
Configuration accepts a `mongo` object that passes all kwargs directly to pymongo MongoClient, providing maximum flexibility. Example:
```json
{
  "mongo": {
    "host": "mongodb://localhost:27017",
    "database": "mydb"
  },
  "strategy": "raw"
}
```

## Testing Approach
Uses pytest with Singer SDK standard tests. The test suite inherits from `sdk.testing.SuiteConfig` for comprehensive tap validation. Run specific tests with `poetry run pytest -k test_name`.

## Dependencies
Built on `nekt-singer-sdk` (custom fork) with pymongo for MongoDB connectivity and orjson for JSON performance. Python >=3.11 required per pyproject.toml.