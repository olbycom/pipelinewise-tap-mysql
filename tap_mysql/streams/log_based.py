"""SQL client handling."""

from __future__ import annotations

import functools
import random
import re
import sys
from typing import TYPE_CHECKING, Any, cast

from dateutil import parser
from nekt_singer_sdk import SQLStream
from nekt_singer_sdk.custom_logger import internal_logger, user_logger
from nekt_singer_sdk.helpers._state import increment_state
from nekt_singer_sdk.helpers._typing import TypeConformanceLevel
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from sqlalchemy import text
from sqlalchemy.engine.url import make_url

from tap_mysql.connector import MySQLConnector

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers import types


class MySQLLogBasedStream(SQLStream):
    """Stream class for MySQL streams."""

    connector_class = MySQLConnector
    replication_key = "_sdc_lsn"

    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    @functools.cached_property
    def schema(self) -> dict:
        """Override schema for log-based replication adding _sdc columns."""
        schema_dict = cast(dict, self._singer_catalog_entry.schema.to_dict())
        for property in schema_dict["properties"].values():
            if isinstance(property["type"], list):
                property["type"].append("null")
            else:
                property["type"] = [property["type"], "null"]
        if "required" in schema_dict:
            schema_dict.pop("required")
        schema_dict["properties"].update({"_sdc_deleted_at": {"type": ["string"], "format": "date-time"}})
        schema_dict["properties"].update({"_sdc_lsn": {"type": ["string"]}})
        return schema_dict

    def get_min_server_log_file_and_pos(self) -> tuple[str, str]:
        try:
            with self.connector._connect() as conn:
                binary_logs = conn.execute(text("SHOW BINARY LOGS"))

                if binary_logs:
                    initial_log = binary_logs.first()
                    return initial_log[0], 0
        except Exception:
            user_logger.error("Unable to replicate binlog stream because no binary logs exist on the server.")
            internal_logger.error(
                "Unable to replicate binlog stream because no binary logs exist on the server.",
                exc_info=True,
            )
            sys.exit(1)

    def get_log_file_from_lsn(self, lsn: int | str) -> tuple[str, int]:
        """Parse the lsn to get the file name and position."""
        # Decompose the LSN by reversing the bit-shifting
        lsn = int(lsn)
        file_number = lsn >> 48
        position = (lsn >> 16) & 0xFFFFFFFF  # Extract 32 bits for position

        with self.connector._connect() as conn:
            binary_logs = conn.execute(text("SHOW BINARY LOGS"))
            for log in binary_logs:
                log_name = log[0]
                match = re.search(r"\.(\d+)$", log_name)
                if match and int(match.group(1)) == file_number:
                    return log_name, position

        # Fallback or error
        msg = f"Could not find binlog file for number {file_number}"
        raise RuntimeError(msg)

    def create_binlog_stream_reader(
        self,
        *,
        log_file: str | None = None,
        log_pos: str | None = None,
    ) -> BinLogStreamReader:
        server_id = random.randint(1, 2**32 - 1)  # generate random server id for this slave
        internal_logger.info("Using randomly generated server_id=%s", server_id)

        url_obj = make_url(self.connector.sqlalchemy_url)
        schema, table_name = self.fully_qualified_name.split(".")
        kwargs = {
            "connection_settings": {
                "host": url_obj.host,
                "port": url_obj.port,
                "user": url_obj.username,
                "passwd": url_obj.password,
                "database": url_obj.database,
            },
            "is_mariadb": False,  # TODO: Later check to add this as a config
            "server_id": server_id,  # slave server ID
            "report_slave": "nekt",
            "only_events": [WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
            "only_tables": [table_name],
            "only_schemas": [schema],
            "log_file": log_file,
            "log_pos": log_pos,
            "resume_stream": True if log_pos else False,
        }

        return BinLogStreamReader(**kwargs)

    def create_unique_identifier(self, file_name: str, position: int, row_index: int = 0) -> int:
        """Create a unique 64-bit integer to represent the LSN.

        This is composed of:
        - binlog file number (top 16 bits)
        - binlog position (middle 32 bits)
        - row index within the event (bottom 16 bits)
        """
        match = re.search(r"\d+$", file_name)
        if not match:
            msg = f"Could not extract file number from binlog file name: {file_name}"
            raise ValueError(msg)

        file_number = int(match.group())

        if file_number >= (1 << 16):
            self.logger.warning("Binlog file number %s exceeds the 16-bit allocation.", file_number)
        if position >= (1 << 32):
            self.logger.warning("Binlog position %s exceeds the 32-bit allocation.", position)
        if row_index >= (1 << 16):
            self.logger.warning(
                "Row index %s exceeds the 16-bit allocation. LSN may not be unique for this event.",
                row_index,
            )

        # Compose the LSN by bit-shifting the components
        file_part = file_number << 48
        pos_part = position << 16
        row_part = row_index

        return file_part + pos_part + row_part

    def handle_write_row(
        self,
        event: WriteRowsEvent,
        row: dict,
        selected_columns,
        cur_log_file: str,
        cur_log_pos: int,
        row_index: int,
    ) -> dict[str, Any]:
        values = row.get("values")
        filtered_row = {col: values[col] for col in selected_columns if col in values}

        if not filtered_row:
            return

        filtered_row["_sdc_lsn"] = self.create_unique_identifier(cur_log_file, cur_log_pos, row_index)
        filtered_row["_sdc_deleted_at"] = None

        return filtered_row

    def handle_update_row(
        self,
        event: UpdateRowsEvent,
        row: dict,
        selected_columns,
        cur_log_file: str,
        cur_log_pos: int,
        row_index: int,
    ) -> dict[str, Any]:
        values = row.get("after_values")
        filtered_row = {col: values[col] for col in selected_columns if col in values}

        if not filtered_row:
            return

        filtered_row["_sdc_lsn"] = self.create_unique_identifier(cur_log_file, cur_log_pos, row_index)
        filtered_row["_sdc_deleted_at"] = None

        return filtered_row

    def handle_delete_row(
        self,
        event: DeleteRowsEvent,
        row: dict,
        selected_columns,
        cur_log_file: str,
        cur_log_pos: int,
        row_index: int,
    ) -> dict[str, Any]:
        values = row.get("values")
        filtered_row = {col: values[col] if col == event.primary_key else None for col in selected_columns if col in values}

        if not filtered_row:
            return

        filtered_row["_sdc_lsn"] = self.create_unique_identifier(cur_log_file, cur_log_pos, row_index)
        filtered_row["_sdc_deleted_at"] = parser.parse(event.formatted_timestamp)
        return filtered_row

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        start_lsn = self.get_starting_replication_key_value(context=context)
        if start_lsn:
            log_file, log_pos = self.get_log_file_from_lsn(start_lsn)
        else:
            log_file, log_pos = self.get_min_server_log_file_and_pos()

        reader = self.create_binlog_stream_reader(log_file=log_file, log_pos=log_pos)
        selected_columns = self.get_selected_schema()["properties"].keys()
        for binlog_event in reader:
            cur_log_file = reader.log_file
            cur_log_pos = reader.log_pos

            match binlog_event.__class__:
                case _ if isinstance(binlog_event, WriteRowsEvent):
                    for i, row in enumerate(binlog_event.rows):
                        row = self.handle_write_row(
                            binlog_event,
                            row,
                            selected_columns,
                            cur_log_file,
                            cur_log_pos,
                            i,
                        )
                        if row:
                            transformed_record = self.post_process(row)
                            yield transformed_record
                case _ if isinstance(binlog_event, UpdateRowsEvent):
                    for i, row in enumerate(binlog_event.rows):
                        row = self.handle_update_row(
                            binlog_event,
                            row,
                            selected_columns,
                            cur_log_file,
                            cur_log_pos,
                            i,
                        )
                        if row:
                            transformed_record = self.post_process(row)
                            yield transformed_record
                case _ if isinstance(binlog_event, DeleteRowsEvent):
                    for i, row in enumerate(binlog_event.rows):
                        row = self.handle_delete_row(
                            binlog_event,
                            row,
                            selected_columns,
                            cur_log_file,
                            cur_log_pos,
                            i,
                        )
                        if row:
                            transformed_record = self.post_process(row)
                            yield transformed_record
                case _:
                    user_logger.error(f"Unsupported binlog event: {binlog_event}")
                    internal_logger.error(f"Unsupported binlog event: {binlog_event}")
                    sys.exit(1)

    def post_process(self, row: dict, context: dict | None = None) -> dict | None:
        if "_sdc_lsn" in row:
            row["_sdc_lsn"] = str(row["_sdc_lsn"])
        return row

    @property
    def is_sorted(self) -> bool:
        return True

    def _increment_stream_state(
        self,
        latest_record: types.Record,
        *,
        context: types.Context | None = None,
    ) -> None:
        # This also creates a state entry if one does not yet exist:
        state_dict = self.get_context_state(context)

        # Advance state bookmark values if applicable
        if latest_record:
            if not self.replication_key:
                msg = f"Could not detect replication key for '{self.name}' stream(replication method={self.replication_method})"
                raise ValueError(msg)

            if self.replication_key in latest_record and isinstance(latest_record[self.replication_key], str):
                latest_record[self.replication_key] = int(latest_record[self.replication_key])

            treat_as_sorted = self.is_sorted
            if not treat_as_sorted and self.state_partitioning_keys is not None:
                # Streams with custom state partitioning are not resumable.
                treat_as_sorted = False
            increment_state(
                state_dict,
                replication_key=self.replication_key,
                latest_record=latest_record,
                is_sorted=treat_as_sorted,
                check_sorted=self.check_sorted,
            )
