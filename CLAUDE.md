# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`tap-mysql` is a Singer tap for MySQL databases built with the Meltano SDK. It supports:
- Standard MySQL databases
- Vitess/PlanetScale databases
- SSH tunnel connections (bastion hosts)
- Log-based replication streams

## Development Commands

### Setup
```bash
# Install dependencies
poetry install

# Activate virtual environment
poetry shell
```

### Testing
```bash
# Run all tests
poetry run pytest

# Run specific test file
poetry run pytest tests/test_core.py

# Run with verbose output
poetry run pytest -v
```

### Linting
```bash
# Run ruff linter
poetry run ruff check .

# Fix linting issues
poetry run ruff check --fix .
```

### Running the Tap
```bash
# Show help
poetry run tap-mysql --help

# Discover catalog
poetry run tap-mysql --config config.json --discover > catalog.json

# Run extraction
poetry run tap-mysql --config config.json --catalog catalog.json
```

## Architecture

### Core Components

1. **TapMySQL** (`tap_mysql/tap.py`): Main tap class that orchestrates discovery and stream creation. Handles SSH tunneling setup and determines which stream type to use (standard or log-based replication).

2. **MySQLConnector** (`tap_mysql/connector.py`): SQLAlchemy-based database connector that handles connection pooling, type mapping, and schema discovery. Includes special handling for Vitess databases.

3. **Stream Types** (`tap_mysql/streams/`):
   - `MySQLStream`: Standard full-table and incremental replication
   - `MySQLLogBasedStream`: Binary log replication for multiple tables
   - `MySQLSingleLogBasedStream`: Binary log replication for single table

4. **SSH Tunnel** (`tap_mysql/ssh_tunnel.py`): Manages SSH tunneling for accessing databases through bastion hosts using paramiko.

### Key Design Patterns

- Uses Nekt's fork of Singer SDK (`nekt-singer-sdk`) with msgspec for JSON serialization
- Implements custom date/datetime handling to preserve types without conversion
- Supports both SQLAlchemy URL and individual connection parameters
- Automatic Vitess detection with fallback schema discovery for views

### Configuration Priority

1. `sqlalchemy_url` (if provided, overrides individual connection settings)
2. Individual connection parameters (host, port, user, password, database)
3. SSH tunnel configuration (if enabled)
4. Stream-specific settings (filter_schemas, stream_maps, etc.)

## Important Notes

- Line length limit is 160 characters (configured in ruff)
- Python 3.11+ required
- Tests use pytest with fixtures defined in `tests/conftest.py`
- Binary log replication requires appropriate MySQL permissions
- Vitess/PlanetScale requires SSL configuration in sqlalchemy_options