"""SQL client handling."""

from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any

import singer_sdk.helpers._typing
import sqlalchemy as sa
import sqlalchemy.types
from nekt_singer_sdk import SQLConnector
from nekt_singer_sdk import typing as th
from nekt_singer_sdk.custom_logger import internal_logger
from nekt_singer_sdk.singerlib import CatalogEntry, MetadataMapping, Schema
from sqlalchemy.pool import QueuePool

if TYPE_CHECKING:
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
