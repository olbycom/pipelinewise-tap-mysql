"""mysql tap class."""

from __future__ import annotations

import atexit
import copy
import io
import os
import signal
import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

import paramiko
from nekt_singer_sdk import SQLStream, SQLTap, Stream
from nekt_singer_sdk import typing as th  # JSON schema typing helpers
from nekt_singer_sdk.contrib.msgspec import MsgSpecWriter
from nekt_singer_sdk.singerlib import Catalog, Metadata, Schema, StateMessage
from sqlalchemy.engine import URL
from sqlalchemy.engine.url import make_url

from tap_mysql.connector import MySQLConnector
from tap_mysql.ssh_tunnel import SSHTunnelForwarder
from tap_mysql.streams import (
    MySQLLogBasedStream,
    MySQLSingleLogBasedStream,
    MySQLStream,
)

if TYPE_CHECKING:
    from collections.abc import Mapping, Sequence


class TapMySQL(SQLTap):
    name = "tap-mysql"
    default_stream_class = MySQLStream
    earliest_lsn_file_name: str | None = None
    latest_lsn_file_name: str | None = None
    message_writer_class = MsgSpecWriter

    def __init__(
        self,
        *args: tuple,
        **kwargs: dict,
    ) -> None:
        """Construct a MySQL tap.

        Should use JSON Schema instead
        See https://github.com/meltano/sdk/pull/1525
        """
        super().__init__(*args, **kwargs)
        sql_alchemy_url_exists = self.config.get("sqlalchemy_url") is not None
        individual_url_params_exist = all(
            [
                self.config.get("host") is not None,
                self.config.get("port") is not None,
                self.config.get("user") is not None,
                self.config.get("password") is not None,
            ]
        )
        if not (sql_alchemy_url_exists or individual_url_params_exist):
            msg = "Need either the sqlalchemy_url to be set or host, port, user, and password to be set"
            self.user_logger.error(msg)
            sys.exit(1)

    config_jsonschema = th.PropertiesList(
        th.Property(
            "host",
            th.StringType,
            description=("Hostname for mysql instance. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "port",
            th.IntegerType,
            default=3306,
            description=(
                "The port on which mysql is awaiting connection. Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "user",
            th.StringType,
            description=("User name used to authenticate. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "password",
            th.StringType,
            secret=True,
            description=("Password used to authenticate. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "database",
            th.StringType,
            description=("Database name. Note if sqlalchemy_url is set this will be ignored."),
        ),
        th.Property(
            "streams_in_parallel",
            th.IntegerType,
            default=1,
            description="Optional. Maximum number of streams in parallel.",
        ),
        th.Property(
            "sqlalchemy_url",
            th.StringType,
            secret=True,
            description=(
                "Example pymysql://[username]:[password]@localhost:3306/[db_name][?options] "  # noqa: E501
                "see https://docs.sqlalchemy.org/en/20/dialects/mysql.html#module-sqlalchemy.dialects.mysql.pymysql "  # noqa: E501
                "for more information"
            ),
        ),
        th.Property(
            "filter_schemas",
            th.ArrayType(th.StringType),
            description=(
                "If an array of schema names is provided, the tap will only process "
                "the specified MySQL schemas and ignore others. If left blank, the "
                "tap automatically determines ALL available MySQL schemas."
            ),
        ),
        th.Property(
            "show_mysql_schema_on_discovery",
            th.BooleanType,
            default=False,
            description=(
                "If set to true, the tap will return the default MySQL schema on discovery. "
                "If set to false, the tap will return the schemas specified in the "
                "filter_schemas property."
            ),
        ),
        th.Property(
            "show_sys_schema_on_discovery",
            th.BooleanType,
            default=False,
            description=(
                "If set to true, the tap will return the sys schema on discovery. "
                "If set to false, the tap will return the schemas specified in the "
                "filter_schemas property."
            ),
        ),
        th.Property(
            "show_performance_schema_schema_on_discovery",
            th.BooleanType,
            default=False,
            description=(
                "If set to true, the tap will return the performance_schema schema on discovery. "
                "If set to false, the tap will return the schemas specified in the "
                "filter_schemas property."
            ),
        ),
        th.Property(
            "is_vitess",
            th.BooleanType,
            default=None,
            description=(
                "By default we'll check if the database is a Vitess instance. "
                "If you would rather not automatically check, set this to "
                "`False`. See Vitess/PlanetScale documentation below for more "
                "information."
            ),
        ),
        th.Property(
            "ssh_tunnel",
            th.ObjectType(
                th.Property(
                    "enable",
                    th.BooleanType,
                    required=False,
                    default=False,
                    description=(
                        "Enable an ssh tunnel (also known as bastion server), see the other ssh_tunnel.* properties for more details"
                    ),
                ),
                th.Property(
                    "host",
                    th.StringType,
                    required=False,
                    description="Host of the bastion server, this is the host we'll connect to via ssh",
                ),
                th.Property(
                    "username",
                    th.StringType,
                    required=False,
                    description="Username to connect to bastion server",
                ),
                th.Property(
                    "port",
                    th.IntegerType,
                    required=False,
                    default=22,
                    description="Port to connect to bastion server",
                ),
                th.Property(
                    "password",
                    th.StringType,
                    required=False,
                    secret=True,
                    description="Password for authentication to the bastion server",
                ),
                th.Property(
                    "private_key",
                    th.StringType,
                    required=False,
                    secret=True,
                    description="Private Key for authentication to the bastion server",
                ),
                th.Property(
                    "private_key_password",
                    th.StringType,
                    required=False,
                    secret=True,
                    default=None,
                    description="Private Key Password, leave None if no password is set",
                ),
                th.Property(
                    "run_tunnel_auth_interactive_dumb",
                    th.BooleanType,
                    required=False,
                    default=False,
                    description=("Enable dumb interaction on auth for ssh tunnel"),
                ),
            ),
            required=False,
            description="SSH Tunnel Configuration, this is a json object",
        ),
        th.Property(
            "ssl_enable",
            th.BooleanType,
            default=False,
            description=(
                "Whether or not to use ssl to verify the server's identity. Use"
                + " ssl_certificate_authority and ssl_mode for further customization."
                + " To use a client certificate to authenticate yourself to the server,"
                + " use ssl_client_certificate_enable instead."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_client_certificate_enable",
            th.BooleanType,
            default=False,
            description=(
                "Whether or not to provide client-side certificates as a method of"
                + " authentication to the server. Use ssl_client_certificate and"
                + " ssl_client_private_key for further customization. To use SSL to"
                + " verify the server's identity, use ssl_enable instead."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_mode",
            th.StringType,
            default="verify-full",
            description=(
                "SSL Protection method, see [postgres documentation](https://www.postgresql.org/docs/current/libpq-ssl.html#LIBPQ-SSL-PROTECTION)"
                + " for more information. Must be one of disable, allow, prefer,"
                + " require, verify-ca, or verify-full."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_certificate_authority",
            th.StringType,
            default="~/.postgresql/root.crl",
            description=(
                "The certificate authority that should be used to verify the server's"
                + " identity. Can be provided either as the certificate itself (in"
                + " .env) or as a filepath to the certificate."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_client_certificate",
            th.StringType,
            default="~/.postgresql/postgresql.crt",
            description=(
                "The certificate that should be used to verify your identity to the"
                + " server. Can be provided either as the certificate itself (in .env)"
                + " or as a filepath to the certificate."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_client_private_key",
            th.StringType,
            default="~/.postgresql/postgresql.key",
            description=(
                "The private key for the certificate you provided. Can be provided"
                + " either as the certificate itself (in .env) or as a filepath to the"
                + " certificate."
                + " Note if sqlalchemy_url is set this will be ignored."
            ),
        ),
        th.Property(
            "ssl_storage_directory",
            th.StringType,
            default=".secrets",
            description=(
                "The folder in which to store SSL certificates provided as raw values."
                + " When a certificate/key is provided as a raw value instead of as a"
                + " filepath, it must be written to a file before it can be used. This"
                + " configuration option determines where that file is created."
            ),
        ),
        th.Property(
            "chunk_size",
            th.IntegerType,
            default=0,
            description=(
                "The number of rows to fetch at a time. If set to 0, the tap will fetch all rows at once (no chunking)."
            ),
        ),
        th.Property(
            "convert_dates_to_string",
            th.BooleanType,
            default=False,
            description=(
                "If true, all date, datetime and time columns will be exported as strings rather than date/time types."
            ),
        ),
        th.Property(
            "use_pagination_for",
            th.ObjectType(),
            default={},
            description=(
                "Dictionary of stream names to use pagination for instead of streaming. Key is stream name, value is boolean. If true, pagination is used for that table."
            ),
        ),
        th.Property(
            "pagination_page_size",
            th.IntegerType,
            default=50000,
            description=("Page size for pagination queries. Default is 50000 records per page."),
        ),
    ).to_dict()

    def get_sqlalchemy_url(self, config: Mapping[str, Any]) -> str:
        """Generate a SQLAlchemy URL.

        Args:
            config: The configuration for the connector.
        """
        if config.get("sqlalchemy_url"):
            return cast(str, config["sqlalchemy_url"])

        sqlalchemy_url = URL.create(
            drivername="mysql+pymysql",
            username=config["user"],
            password=config["password"],
            host=config["host"],
            port=config["port"],
            database=config["database"],
            query=self.get_sqlalchemy_query(config=config),
        )
        return cast(str, sqlalchemy_url)

    def get_sqlalchemy_query(self, config: Mapping[str, Any]) -> dict:
        query = {}

        # ssl_enable is for verifying the server's identity to the client.
        if config["ssl_enable"]:
            ssl_mode = config["ssl_mode"]
            query.update({"sslmode": ssl_mode})
            query["sslrootcert"] = self.filepath_or_certificate(
                value=config["ssl_certificate_authority"],
                alternative_name=config["ssl_storage_directory"] + "/root.crt",
            )

        # ssl_client_certificate_enable is for verifying the client's identity to the
        # server.
        if config["ssl_client_certificate_enable"]:
            query["sslcert"] = self.filepath_or_certificate(
                value=config["ssl_client_certificate"],
                alternative_name=config["ssl_storage_directory"] + "/cert.crt",
            )
            query["sslkey"] = self.filepath_or_certificate(
                value=config["ssl_client_private_key"],
                alternative_name=config["ssl_storage_directory"] + "/pkey.key",
                restrict_permissions=True,
            )
        return query

    def filepath_or_certificate(
        self,
        value: str,
        alternative_name: str,
        restrict_permissions: bool = False,
    ) -> str:
        if os.path.isfile(value):
            return value

        with open(alternative_name, "wb") as alternative_file:
            alternative_file.write(
                value.replace("\\n", "\n")
                .replace(" ", "")
                .replace("-----BEGINCERTIFICATE-----", "-----BEGIN CERTIFICATE-----")
                .replace("-----ENDCERTIFICATE-----", "-----END CERTIFICATE-----")
            )
        if restrict_permissions:
            os.chmod(alternative_name, 0o600)

        return alternative_name

    @cached_property
    def connector(self) -> MySQLConnector:
        url = make_url(self.get_sqlalchemy_url(config=self.config))
        ssh_config = self.config.get("ssh_tunnel", {})

        if ssh_config.get("enable", False):
            # Return a new URL with SSH tunnel parameters
            url = self.ssh_tunnel_connect(ssh_config=ssh_config, url=url)

        return MySQLConnector(
            is_running_discovery=self.is_running_discovery,
            config=dict(self.config),
            sqlalchemy_url=url.render_as_string(hide_password=False),
        )

    def guess_key_type(self, key_data: str) -> paramiko.PKey:
        for key_class in (
            paramiko.RSAKey,
            paramiko.DSSKey,
            paramiko.ECDSAKey,
            paramiko.Ed25519Key,
        ):
            try:
                key = key_class.from_private_key(io.StringIO(key_data))  # type: ignore[attr-defined]
            except paramiko.SSHException:  # noqa: PERF203
                continue
            else:
                return key

        errmsg = "Could not determine the key type."
        raise ValueError(errmsg)

    def ssh_tunnel_connect(self, *, ssh_config: dict[str, Any], url: URL) -> URL:
        """Connect to the SSH Tunnel and swap the URL to use the tunnel.

        Args:
            ssh_config: The SSH Tunnel configuration
            url: The original URL to connect to.

        Returns:
            The new URL to connect to, using the tunnel.
        """
        if ssh_config.get("password"):
            credentials = {
                "ssh_password": ssh_config.get("password"),
            }
        else:
            credentials = {
                "ssh_private_key": self.guess_key_type(ssh_config["private_key"]),
                "ssh_private_key_password": ssh_config.get("private_key_password"),
            }

        self.ssh_tunnel: SSHTunnelForwarder = SSHTunnelForwarder(
            ssh_address_or_host=(ssh_config["host"], ssh_config["port"]),
            ssh_username=ssh_config["username"],
            remote_bind_address=(url.host, url.port),
            run_tunnel_auth_interactive_dumb=ssh_config.get("run_tunnel_auth_interactive_dumb", False),
            **credentials,
        )
        self.ssh_tunnel.start()
        self.internal_logger.info("SSH Tunnel started")
        # On program exit clean up, want to also catch signals
        atexit.register(self.clean_up)
        signal.signal(signal.SIGTERM, self.catch_signal)
        # Probably overkill to catch SIGINT, but needed for SIGTERM
        signal.signal(signal.SIGINT, self.catch_signal)

        # Swap the URL to use the tunnel
        return url.set(
            host=self.ssh_tunnel.local_bind_host,
            port=self.ssh_tunnel.local_bind_port,
        )

    def clean_up(self) -> None:
        self.internal_logger.info("Shutting down SSH Tunnel")
        self.ssh_tunnel.stop()

    def catch_signal(self, signum, frame) -> None:  # noqa: ANN001 ARG002
        sys.exit(1)  # Calling this to be sure atexit is called, so clean_up gets called

    @property
    def catalog_dict(self) -> dict:
        if self._catalog_dict:
            return self._catalog_dict

        if self.input_catalog:
            return self.input_catalog.to_dict()

        result: dict[str, list[dict]] = {"streams": []}
        result["streams"].extend(self.connector.discover_catalog_entries())

        self._catalog_dict: dict = result
        return self._catalog_dict

    @cached_property
    def catalog(self) -> Catalog:
        """Get the tap's working catalog.

        Override to do LOG_BASED modifications.

        Returns:
            A Singer catalog object.
        """
        new_catalog: Catalog = Catalog()
        modified_streams: list = []
        for stream in super().catalog.streams:
            stream_modified = False
            new_stream = copy.deepcopy(stream)
            # If dates are converted to strings, strip the JSON-Schema "format" attribute from every property.
            if (
                getattr(self.connector, "convert_dates_to_string", False)
                and new_stream.schema
                and new_stream.schema.properties
            ):
                for prop in new_stream.schema.properties.values():
                    if hasattr(prop, "format") and prop.format is not None:
                        prop.format = None
                        stream_modified = True
            # If LOG_BASED, apply existing nullability and _sdc column logic
            if new_stream.replication_method == "LOG_BASED" and new_stream.schema.properties:
                for property in new_stream.schema.properties.values():
                    if "null" not in property.type:
                        if isinstance(property.type, list):
                            property.type.append("null")
                        else:
                            property.type = [property.type, "null"]
                if new_stream.schema.required:
                    stream_modified = True
                    new_stream.schema.required = None
                if "_sdc_deleted_at" not in new_stream.schema.properties:
                    stream_modified = True
                    if getattr(self.connector, "convert_dates_to_string", False):
                        new_stream.schema.properties.update({"_sdc_deleted_at": Schema(type=["string", "null"])})
                    else:
                        new_stream.schema.properties.update(
                            {"_sdc_deleted_at": Schema(type=["string", "null"], format="date-time")}
                        )
                    new_stream.metadata.update(
                        {("properties", "_sdc_deleted_at"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)}
                    )
                if "_sdc_lsn" not in new_stream.schema.properties:
                    stream_modified = True
                    new_stream.schema.properties.update({"_sdc_lsn": Schema(type=["string", "null"])})
                    new_stream.metadata.update(
                        {("properties", "_sdc_lsn"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)}
                    )
                if "_sdc_operation" not in new_stream.schema.properties:
                    stream_modified = True
                    new_stream.schema.properties.update({"_sdc_operation": Schema(type=["string", "null"])})
                    new_stream.metadata.update(
                        {("properties", "_sdc_operation"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)}
                    )
                if "_sdc_event_timestamp" not in new_stream.schema.properties:
                    stream_modified = True
                    if getattr(self.connector, "convert_dates_to_string", False):
                        new_stream.schema.properties.update({"_sdc_event_timestamp": Schema(type=["string", "null"])})
                    else:
                        new_stream.schema.properties.update(
                            {"_sdc_event_timestamp": Schema(type=["string", "null"], format="date-time")}
                        )
                    new_stream.metadata.update(
                        {("properties", "_sdc_event_timestamp"): Metadata(Metadata.InclusionType.AVAILABLE, True, None)}
                    )
            if stream_modified:
                modified_streams.append(new_stream.tap_stream_id)
            new_catalog.add_stream(new_stream)
        if modified_streams:
            self.internal_logger.info(
                "One or more LOG_BASED catalog entries were modified "
                f"({modified_streams=}) to allow nullability and include _sdc columns. "
                "See README for further information."
            )
        return new_catalog

    @property
    def streams(self) -> dict[str, Stream]:
        if self._streams is None:
            self._streams = {}

            for stream in self.load_streams():
                if self.catalog is not None:
                    stream.apply_catalog(self.catalog)
                self._streams[stream.name] = stream
        return self._streams

    def discover_streams(self) -> Sequence[Stream]:
        streams: list[SQLStream] = []
        for catalog_entry in self.catalog_dict["streams"]:
            if catalog_entry["replication_method"] == "LOG_BASED":
                streams.append(MySQLLogBasedStream(self, catalog_entry, connector=self.connector))
            else:
                streams.append(MySQLStream(self, catalog_entry, connector=self.connector))
        return streams

    def sync_all(self) -> None:
        """Sync all streams."""
        self._reset_state_progress_markers()
        self._set_compatible_replication_methods()
        if self.state:
            self.write_message(StateMessage(value=self.state))

        log_based_streams = [
            stream for stream in self.streams.values() if stream.replication_method == "LOG_BASED" and stream.selected
        ]
        other_streams = [
            stream for stream in self.streams.values() if stream.replication_method != "LOG_BASED" and stream.selected
        ]

        if log_based_streams:
            log_based_stream = MySQLSingleLogBasedStream(
                tap=self,
                connector=self.connector,
                log_based_streams=log_based_streams,
            )
            log_based_stream.sync()
            log_based_stream.finalize_state_progress_markers()
        else:
            try:
                log_based_stream = MySQLSingleLogBasedStream(
                    tap=self,
                    connector=self.connector,
                    log_based_streams=[],
                )
                log_based_stream.fast_forward_to_latest_binlog()
            except Exception:
                pass

        for stream in other_streams:
            if not stream.selected and not stream.has_selected_descendents:
                self.logger.info("Skipping deselected stream '%s'.", stream.name)
                continue

            if stream.parent_stream_type:
                self.logger.debug(
                    "Child stream '%s' is expected to be called by parent stream '%s'. Skipping direct invocation.",
                    type(stream).__name__,
                    stream.parent_stream_type.__name__,
                )
                continue

            stream.sync()
            stream.finalize_state_progress_markers()

        # this second loop is needed for all streams to print out their costs
        # including child streams which are otherwise skipped in the loop above
        for stream in self.streams.values():
            stream.log_sync_costs()


if __name__ == "__main__":
    TapMySQL.cli()
