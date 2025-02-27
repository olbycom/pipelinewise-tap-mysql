"""MySQL entry point."""

from __future__ import annotations

from tap_mysql.tap import TapMySQL

TapMySQL.cli()
