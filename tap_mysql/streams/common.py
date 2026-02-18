"""SQL client handling."""

from __future__ import annotations

import sys
import time
from typing import TYPE_CHECKING, Any

import pymysql.err
import sqlalchemy as sa
from nekt_singer_sdk import SQLStream
from nekt_singer_sdk.custom_logger import internal_logger, user_logger
from nekt_singer_sdk.helpers._typing import TypeConformanceLevel
from sqlalchemy.exc import OperationalError

from tap_mysql.connector import MySQLConnector

if TYPE_CHECKING:
    from collections.abc import Iterable


class MySQLStream(SQLStream):
    """Stream class for MySQL streams."""

    connector_class = MySQLConnector

    # JSONB Objects won't be selected without type_confomance_level to ROOT_ONLY
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    @property
    def is_sorted(self) -> bool:
        if self.config.get("use_batch_query", False):
            # Batch mode orders by PK, not replication_key
            return False
        if self.replication_key:
            # Streaming with replication_key: we ORDER BY replication_key
            # so records are sorted and state is resumable if interrupted
            return True
        # FULL_TABLE without replication_key: no ordering
        return False

    def _get_primary_key_columns(self) -> list[str] | None:
        """Get the primary key column names from stream metadata or database schema.

        Returns:
            List of primary key column names, or None if no PK exists.
        """
        if hasattr(self, "primary_keys") and self.primary_keys:
            return list(self.primary_keys)

        try:
            from sqlalchemy import inspect

            parts = self.fully_qualified_name.split("-", 1)
            if len(parts) == 2:
                schema_name, table_name = parts
            else:
                schema_name = self.config.get("database")
                table_name = self.fully_qualified_name

            inspector = inspect(self.connector._engine)  # noqa: SLF001
            pk_constraint = inspector.get_pk_constraint(table_name, schema=schema_name)
            if pk_constraint and pk_constraint.get("constrained_columns"):
                pk_columns = list(pk_constraint["constrained_columns"])
                user_logger.info(f"[{self.name}] Found primary key from database schema: {pk_columns}")
                return pk_columns
        except Exception as e:
            internal_logger.warning(f"[{self.name}] Failed to get primary key from database: {e}")

        return None

    def _get_records_streaming(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Standard streaming implementation."""
        selected_column_names = list(self.get_selected_schema()["properties"])
        table = self.connector.get_table(
            self.fully_qualified_name,
            column_names=selected_column_names,
        )

        query = table.select()
        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            query = query.order_by(replication_key_col)

            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                query = query.where(replication_key_col >= start_val)

        with self.connector._connect() as conn:  # noqa: SLF001
            user_logger.info(f"Getting records for query: '{query}'")
            if self.connector.is_vitess:  # type: ignore[attr-defined]
                conn.exec_driver_sql("set workload=olap")

            result = conn.execution_options(stream_results=True).execute(query)
            if self.config.get("chunk_size", 0) > 0:
                result = result.yield_per(self.config["chunk_size"])

            for record in result.mappings():
                # TODO: Standardize record mapping type
                # https://github.com/meltano/sdk/issues/2096
                transformed_record = self.post_process(dict(record))
                if transformed_record is None:
                    # Record filtered out during post_process()
                    continue
                yield transformed_record

    def _get_records_batched(self, context: dict | None, pk_columns: list[str]) -> Iterable[dict[str, Any]]:
        """Batch extraction with keyset pagination.

        Breaks queries into smaller batches to avoid connection timeouts on large tables.
        Each batch uses a fresh connection. Orders by primary key(s) for keyset pagination.
        Supports composite primary keys using tuple comparison.
        For INCREMENTAL streams, also applies the replication_key filter.

        Args:
            context: Stream context (must be None for this stream).
            pk_columns: List of primary key column names for keyset pagination.
        """
        selected_column_names = list(self.get_selected_schema()["properties"])
        table = self.connector.get_table(
            self.fully_qualified_name,
            column_names=selected_column_names,
        )

        batch_size = self.config.get("chunk_size", 20000) or 20000
        max_retries = self.config.get("batch_retry_max", 3)
        base_delay = self.config.get("batch_retry_delay", 5)

        total_rows = 0
        batch_num = 0
        last_pk_values = None

        pk_cols = [table.columns[name] for name in pk_columns]
        pk_names = ", ".join(pk_columns)

        replication_key_filter = None
        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                replication_key_filter = replication_key_col >= start_val
                user_logger.info(
                    f"[{self.name}] Starting batched extraction with keyset pagination "
                    f"(order_by=({pk_names}), filter={self.replication_key} >= {start_val}, batch_size={batch_size})"
                )
            else:
                user_logger.info(
                    f"[{self.name}] Starting batched extraction with keyset pagination "
                    f"(order_by=({pk_names}), batch_size={batch_size})"
                )
        else:
            user_logger.info(
                f"[{self.name}] Starting batched extraction with keyset pagination "
                f"(order_by=({pk_names}), batch_size={batch_size})"
            )

        while True:
            batch_num += 1
            for attempt in range(max_retries):
                try:
                    query = table.select()

                    if replication_key_filter is not None:
                        query = query.where(replication_key_filter)

                    if last_pk_values is not None:
                        query = query.where(sa.tuple_(*pk_cols) > sa.tuple_(*last_pk_values))

                    query = query.order_by(*[col.asc() for col in pk_cols]).limit(batch_size)

                    with self.connector._connect() as conn:  # noqa: SLF001
                        if self.connector.is_vitess:  # type: ignore[attr-defined]
                            conn.exec_driver_sql("set workload=olap")

                        user_logger.info(f"[{self.name}] Batch {batch_num}: after ({pk_names})={last_pk_values}")
                        result = conn.execute(query)

                        batch_rows = 0
                        for record in result.mappings():
                            last_pk_values = tuple(record[name] for name in pk_columns)
                            transformed_record = self.post_process(dict(record))
                            if transformed_record is not None:
                                yield transformed_record
                            batch_rows += 1

                        total_rows += batch_rows
                        user_logger.info(
                            f"[{self.name}] Batch {batch_num}: fetched {batch_rows} rows (total: {total_rows})"
                        )

                        if batch_rows < batch_size:
                            user_logger.info(
                                f"[{self.name}] Completed: {total_rows} total rows in {batch_num} batches"
                            )
                            return

                        break  # Success, exit retry loop

                except (OperationalError, pymysql.err.OperationalError) as e:
                    if attempt < max_retries - 1:
                        sleep_time = base_delay * (2**attempt)
                        user_logger.warning(
                            f"[{self.name}] Batch {batch_num} failed (attempt {attempt + 1}/{max_retries}), "
                            f"retrying in {sleep_time}s: {e}"
                        )
                        time.sleep(sleep_time)
                    else:
                        user_logger.error(f"[{self.name}] Batch {batch_num} failed after {max_retries} attempts")
                        raise

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Get records from stream - dispatches to batched or streaming based on config."""
        if context:
            msg = f"Stream '{self.name}' does not support partitioning."
            self._tap.user_logger.error(msg)
            sys.exit(1)

        if self.config.get("use_batch_query", False):
            pk_columns = self._get_primary_key_columns()
            if pk_columns:
                yield from self._get_records_batched(context, pk_columns)
            else:
                user_logger.warning(
                    f"[{self.name}] No primary key found, falling back to streaming mode"
                )
                yield from self._get_records_streaming(context)
        else:
            yield from self._get_records_streaming(context)
