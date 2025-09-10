"""SQL client handling."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from nekt_singer_sdk import SQLStream
from nekt_singer_sdk.custom_logger import user_logger
from nekt_singer_sdk.helpers._typing import TypeConformanceLevel

from tap_mysql.connector import MySQLConnector

if TYPE_CHECKING:
    from collections.abc import Iterable


class MySQLStream(SQLStream):
    """Stream class for MySQL streams."""

    connector_class = MySQLConnector

    # JSONB Objects won't be selected without type_confomance_level to ROOT_ONLY
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    is_sorted = False

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        if context:
            msg = f"Stream '{self.name}' does not support partitioning."
            self._tap.user_logger.error(msg)
            sys.exit(1)

        # pulling rows with only selected columns from stream
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
                conn.exec_driver_sql("set workload=olap")  # See https://github.com/planetscale/discussion/discussions/190

            # Check if streaming should be disabled for this specific table
            disable_streaming_config = self.config.get("disable_stream_results_for", {})
            disable_streaming = disable_streaming_config.get(self.fully_qualified_name, False)
            
            if disable_streaming:
                user_logger.info(f"Streaming disabled for {self.fully_qualified_name}, fetching all results into memory")
                result = conn.execute(query)
            else:
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
