"""SQL client handling."""

from __future__ import annotations

import datetime
import functools
import random
import re
import sys
from typing import TYPE_CHECKING, Any, cast

import singer_sdk.helpers._typing
import sqlalchemy as sa
import sqlalchemy.types
from dateutil import parser
from nekt_singer_sdk import SQLConnector, SQLStream
from nekt_singer_sdk import typing as th
from nekt_singer_sdk.custom_logger import internal_logger, user_logger
from nekt_singer_sdk.helpers._state import increment_state
from nekt_singer_sdk.helpers._typing import TypeConformanceLevel
from nekt_singer_sdk.singerlib import CatalogEntry, MetadataMapping, Schema
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from sqlalchemy import text
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import QueuePool

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers import types
    from sqlalchemy.engine import Engine, reflection
    from sqlalchemy.engine.reflection import Inspector

unpatched_conform = singer_sdk.helpers._typing._conform_primitive_property  # noqa: SLF001


def patched_conform(
    elem: Any,  # noqa: ANN401
    property_schema: dict,
) -> Any:  # noqa: ANN401
    """Override type conformance to prevent dates turning into datetimes.

    Converts a primitive to a json compatible type.

    Returns:
        The appropriate json compatible type.
    """
    if isinstance(elem, datetime.date):
        return elem.isoformat()
    return unpatched_conform(elem=elem, property_schema=property_schema)


singer_sdk.helpers._typing._conform_primitive_property = patched_conform  # noqa: SLF001


class MySQLConnector(SQLConnector):
    """Connects to the MySQL SQL source."""

    def __init__(
        self,
        is_running_discovery: bool,  # noqa: FBT001
        config: dict | None = None,
        sqlalchemy_url: str | None = None,
    ) -> None:
        self.pool_size = config.get("streams_in_parallel", 20) * 2
        self.is_vitess = config.get("is_vitess")
        super().__init__(
            is_running_discovery=is_running_discovery,
            config=config,
            sqlalchemy_url=sqlalchemy_url,
        )

    @staticmethod
    def to_jsonschema_type(
        sql_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine] | Any,  # noqa: ANN401
    ) -> dict:
        """Return a JSON Schema representation of the provided type.

        Overridden from SQLConnector to correctly handle JSONB and Arrays.

        By default will call `typing.to_jsonschema_type()` for strings and
        SQLAlchemy types.

        Args:
            sql_type: The string representation of the SQL type, a SQLAlchemy
                TypeEngine class or object, or a custom-specified object.

        Raises:
            ValueError: If the type received could not be translated to
            jsonschema.

        Returns:
            The JSON Schema representation of the provided type.

        """
        type_name = None
        if isinstance(sql_type, str):
            type_name = sql_type
        elif isinstance(sql_type, sqlalchemy.types.TypeEngine):
            type_name = type(sql_type).__name__

        if type_name is not None and type_name in ("JSONB", "JSON"):
            return th.ObjectType().type_dict

        # if (
        #     type_name is not None
        #     and isinstance(sql_type, sqlalchemy.dialects.mysql)
        #     and type_name == "ARRAY"
        # ):
        return MySQLConnector.sdk_typing_object(sql_type).type_dict

    @staticmethod
    def sdk_typing_object(
        from_type: str | sqlalchemy.types.TypeEngine | type[sqlalchemy.types.TypeEngine],
    ) -> th.DateTimeType | th.NumberType | th.IntegerType | th.DateType | th.StringType | th.BooleanType:
        """Return the JSON Schema dict that describes the sql type.

        Args:
            from_type: The SQL type as a string or as a TypeEngine. If a TypeEngine is
                provided, it may be provided as a class or a specific object instance.

        Raises:
            ValueError: If the `from_type` value is not of type `str` or `TypeEngine`.

        Returns:
            A compatible JSON Schema type definition.

        """
        sqltype_lookup: dict[
            str,
            th.DateTimeType | th.NumberType | th.IntegerType | th.DateType | th.StringType | th.BooleanType,
        ] = {
            # NOTE: This is an ordered mapping, with earlier mappings taking
            # precedence. If the SQL-provided type contains the type name on
            #  the left, the mapping will return the respective singer type.
            "timestamp": th.DateTimeType(),
            "datetime": th.DateTimeType(),
            "date": th.DateType(),
            "int": th.IntegerType(),
            "numeric": th.NumberType(),
            "decimal": th.NumberType(),
            "double": th.NumberType(),
            "float": th.NumberType(),
            "string": th.StringType(),
            "text": th.StringType(),
            "char": th.StringType(),
            "bool": th.BooleanType(),
            "variant": th.StringType(),
            "bit": th.IntegerType(),
        }
        if isinstance(from_type, str):
            type_name = from_type
        elif isinstance(from_type, sqlalchemy.types.TypeEngine):
            type_name = type(from_type).__name__
        elif isinstance(from_type, type) and issubclass(
            from_type,
            sqlalchemy.types.TypeEngine,
        ):
            type_name = from_type.__name__
        else:
            msg = "Expected `str` or a SQLAlchemy `TypeEngine` object or type."
            raise TypeError(
                msg,
            )

        # Look for the type name within the known SQL type names:
        for sqltype, jsonschema_type in sqltype_lookup.items():
            if sqltype.lower() in type_name.lower():
                return jsonschema_type

        return sqltype_lookup["string"]  # safe failover to str

    def get_schema_names(self, engine: Engine, inspected: Inspector) -> list[str]:
        if "filter_schemas" in self.config and len(self.config["filter_schemas"]) != 0:
            return self.config["filter_schemas"]
        schemas = super().get_schema_names(engine, inspected)
        exclude_schemas = ["information_schema", "mysql", "performance_schema", "sys"]
        if self.config.get("show_mysql_schema_on_discovery"):
            exclude_schemas.remove("mysql")
        if self.config.get("show_performance_schema_schema_on_discovery"):
            exclude_schemas.remove("performance_schema")
        if self.config.get("show_sys_schema_on_discovery"):
            exclude_schemas.remove("sys")
        return [schema for schema in schemas if schema not in exclude_schemas]

    def discover_catalog_entry(
        self,
        engine: Engine,  # noqa: ARG002
        inspected: Inspector,  # noqa: ARG002
        schema_name: str | None,
        table_name: str,
        is_view: bool,  # noqa: FBT001
        *,
        reflected_columns: list[reflection.ReflectedColumn] | None = None,
        reflected_pk: reflection.ReflectedPrimaryKeyConstraint | None = None,
        reflected_indices: list[reflection.ReflectedIndex] | None = None,
    ) -> CatalogEntry:
        """Overrode to support Vitess as DESCRIBE is not supported for views.

        Create `CatalogEntry` object for the given table or a view.

        Args:
            engine: SQLAlchemy engine
            inspected: SQLAlchemy inspector instance for engine
            schema_name: Schema name to inspect
            table_name: Name of the table or a view
            is_view: Flag whether this object is a view, returned by `get_object_names`

        Returns:
            `CatalogEntry` object for the given table or a view
        """
        if not self.is_vitess or not is_view:
            return super().discover_catalog_entry(
                engine,
                inspected,
                schema_name,
                table_name,
                is_view,
                reflected_columns=reflected_columns,
                reflected_pk=reflected_pk,
                reflected_indices=reflected_indices,
            )
        # For vitess views, we can't use DESCRIBE as it's not supported for
        # views so we do the below.
        unique_stream_id = self.get_fully_qualified_name(
            db_name=None,
            schema_name=schema_name,
            table_name=table_name,
            delimiter="-",
        )

        # Initialize columns list
        table_schema = th.PropertiesList()
        with self._connect() as conn:
            columns = conn.execute(f"SHOW columns from `{schema_name}`.`{table_name}`")
            for column in columns:
                column_name = column["Field"]
                is_nullable = column["Null"] == "YES"
                jsonschema_type: dict = self.to_jsonschema_type(column["Type"])
                table_schema.append(
                    th.Property(
                        name=column_name,
                        wrapped=th.CustomType(jsonschema_type),
                        required=not is_nullable,
                    ),
                )
        schema = table_schema.to_dict()

        # Initialize available replication methods
        addl_replication_methods: list[str] = [""]  # By default an empty list.
        # Notes regarding replication methods:
        # - 'INCREMENTAL' replication must be enabled by the user by specifying
        #   a replication_key value.
        # - 'LOG_BASED' replication must be enabled by the developer, according
        #   to source-specific implementation capabilities.
        replication_method = next(reversed(["FULL_TABLE", *addl_replication_methods]))

        # Create the catalog entry object
        return CatalogEntry(
            tap_stream_id=unique_stream_id,
            stream=unique_stream_id,
            table=table_name,
            key_properties=None,
            schema=Schema.from_dict(schema),
            is_view=is_view,
            replication_method=replication_method,
            metadata=MetadataMapping.get_standard_metadata(
                schema_name=schema_name,
                schema=schema,
                replication_method=replication_method,
                key_properties=None,
                valid_replication_keys=None,  # Must be defined by user
            ),
            database=None,  # Expects single-database context
            row_count=None,
            stream_alias=None,
            replication_key=None,  # Must be defined by user
        )

    def get_sqlalchemy_type(self, col_meta_type: str) -> sa.Column:
        """Return a SQLAlchemy type object for the given SQL type.

        Used ischema_names so we don't have to manually map all types.
        """
        dialect = sa.dialects.mysql.base.dialect()  # type: ignore[attr-defined]
        ischema_names = dialect.ischema_names
        # Example varchar(97)
        type_info = col_meta_type.split("(")
        base_type_name = type_info[0].split(" ")[0]  # bigint unsigned
        type_args = type_info[1].split(" ")[0].rstrip(")") if len(type_info) > 1 else None  # decimal(25,4) unsigned should work

        if base_type_name in {"enum", "set"}:
            self.logger.warning(
                "Enum and Set types not supported for col_meta_type=%s. Using varchar instead.",
                col_meta_type,
            )
            base_type_name = "varchar"
            type_args = None

        type_class = ischema_names.get(base_type_name.lower())

        try:
            # Create an instance of the type class with parameters if they exist
            if type_args:
                return type_class(*map(int, type_args.split(",")))  # Want to create a varchar(97) if asked for
            return type_class()
        except Exception:
            self.logger.exception("Error creating sqlalchemy type for col_meta_type=%s", col_meta_type)
            raise

    def get_table_columns(
        self,
        full_table_name: str,
        column_names: list[str] | None = None,
    ) -> dict[str, sa.Column]:
        """Overrode to support Vitess as DESCRIBE is not supported for views.

        Return a list of table columns.

        Args:
            full_table_name: Fully qualified table name.
            column_names: A list of column names to filter to.

        Returns:
            An ordered list of column objects.
        """
        if not self.is_vitess:
            return super().get_table_columns(full_table_name, column_names)
        # If Vitess Instance then we can't use DESCRIBE as it's not supported
        # for views so we do below
        if full_table_name not in self._table_cols_cache:
            _, schema_name, table_name = self.parse_full_table_name(full_table_name)
            with self._connect() as conn:
                columns = conn.execute(f"SHOW columns from `{schema_name}`.`{table_name}`")
                self._table_cols_cache[full_table_name] = {
                    col_meta["Field"]: sa.Column(
                        col_meta["Field"],
                        self.get_sqlalchemy_type(col_meta["Type"]),
                        nullable=col_meta["Null"] == "YES",
                    )
                    for col_meta in columns
                    if not column_names or col_meta["Field"].casefold() in {col.casefold() for col in column_names}
                }

        return self._table_cols_cache[full_table_name]

    def create_engine(self) -> Engine:
        try:
            connect_args = {
                "max_allowed_packet": 268_435_456,  # 256MB
                "connect_timeout": 3600,
                "read_timeout": 3600,
            }
            if session_variables := self.config.get("session_variables"):
                init_sql = ", ".join(f"@@session.{k}={v}" for k, v in session_variables.items())
                connect_args["init_command"] = f"SET {init_sql}"
            return sa.create_engine(
                self.sqlalchemy_url,
                echo=False,
                json_serializer=self.serialize_json,
                json_deserializer=self.deserialize_json,
                poolclass=QueuePool,
                pool_size=self.pool_size,
                max_overflow=self.pool_size * 2,
                pool_recycle=300,
                pool_pre_ping=True,
                connect_args=connect_args,
            )
        except TypeError:
            internal_logger.exception(
                "Retrying engine creation with fewer arguments due to TypeError.",
            )
            return sa.create_engine(
                self.sqlalchemy_url,
                echo=False,
            )


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

        # Add limits and offsets for chunking
        chunk_size = self.config["chunk_size"]
        if chunk_size:
            offset = 0
            more_records = True

            while more_records:
                query = table.select().limit(chunk_size).offset(offset)
                if self.replication_key:
                    replication_key_col = table.columns[self.replication_key]
                    query = query.order_by(replication_key_col)

                    start_val = self.get_starting_replication_key_value(context)
                    if start_val:
                        query = query.where(replication_key_col >= start_val)

                with self.connector._connect() as conn:
                    user_logger.info(f"Getting records for query: '{query}' - offset: {offset}")
                    if self.connector.is_vitess:
                        conn.exec_driver_sql("set workload=olap")

                    # Don't materialize the entire result set as a list
                    result_proxy = conn.execute(query)
                    record_count = 0

                    # Process each record one at a time
                    for record in result_proxy.mappings():
                        record_count += 1
                        transformed_record = self.post_process(dict(record))
                        if transformed_record is None:
                            continue
                        yield transformed_record

                    # Check if we need to fetch another chunk
                    more_records = record_count == chunk_size
                    offset += record_count

                    # Close the cursor explicitly
                    result_proxy.close()
        else:
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

                for record in conn.execute(query).mappings():
                    # TODO: Standardize record mapping type
                    # https://github.com/meltano/sdk/issues/2096
                    transformed_record = self.post_process(dict(record))
                    if transformed_record is None:
                        # Record filtered out during post_process()
                        continue
                    yield transformed_record


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

            # Create a copy to avoid modifying the original record, which is already
            # queued for output.
            record_for_state = latest_record.copy()

            # Ensure the replication key is an integer for state comparison.
            if self.replication_key in record_for_state and isinstance(record_for_state[self.replication_key], str):
                try:
                    record_for_state[self.replication_key] = int(record_for_state[self.replication_key])
                except ValueError:
                    self.logger.warning(
                        "Could not convert replication key '%s' to integer for state tracking.",
                        self.replication_key,
                    )

            treat_as_sorted = self.is_sorted
            if not treat_as_sorted and self.state_partitioning_keys is not None:
                # Streams with custom state partitioning are not resumable.
                treat_as_sorted = False
            increment_state(
                state_dict,
                replication_key=self.replication_key,
                latest_record=record_for_state,
                is_sorted=treat_as_sorted,
                check_sorted=self.check_sorted,
            )
