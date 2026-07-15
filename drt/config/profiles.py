"""Source profile dataclasses for drt (#721, phase 2).

The 13 typed ``*Profile`` dataclasses (one per source connector) plus the
:data:`ProfileConfig` union, extracted verbatim from ``credentials.py`` so the
credential/secret-loading logic and the profile *shapes* live apart. Pure data:
these depend on nothing else in ``drt.config``. ``credentials.py`` re-exports
every name here, so ``from drt.config.credentials import XProfile`` is unchanged.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class BigQueryProfile:
    type: Literal["bigquery"]
    project: str
    dataset: str
    method: Literal["application_default", "keyfile"] = "application_default"
    keyfile: str | None = None
    location: str = "US"  # e.g. "US", "EU", "asia-northeast1"

    def describe(self) -> str:
        return f"{self.type} ({self.project}.{self.dataset})"


@dataclass
class DuckDBProfile:
    type: Literal["duckdb"]
    database: str = ":memory:"  # path or :memory:

    def describe(self) -> str:
        return f"{self.type} ({self.database})"


@dataclass
class SQLiteProfile:
    type: Literal["sqlite"]
    database: str = ":memory:"  # path or :memory:

    def describe(self) -> str:
        return f"{self.type} ({self.database})"


@dataclass
class PostgresProfile:
    type: Literal["postgres"]
    host: str = "localhost"
    port: int = 5432
    dbname: str = ""
    user: str = ""
    password_env: str | None = None  # env var name
    password: str | None = None  # explicit (non-recommended)

    def describe(self) -> str:
        return f"{self.type} ({self.host}:{self.port}/{self.dbname})"


@dataclass
class RedshiftProfile:
    """Amazon Redshift profile — PostgreSQL-compatible with schema support.

    Example ~/.drt/profiles.yml:
        redshift_prod:
          type: redshift
          host: my-cluster.xxx.us-east-1.redshift.amazonaws.com
          port: 5439
          dbname: analytics
          user: analyst
          password_env: REDSHIFT_PASSWORD
          schema: public
    """

    type: Literal["redshift"]
    host: str = ""
    port: int = 5439  # Redshift default port
    dbname: str = ""
    user: str = ""
    password_env: str | None = None  # env var name
    password: str | None = None  # explicit (non-recommended)
    schema: str = "public"  # Redshift schema

    def describe(self) -> str:
        return f"{self.type} ({self.host}:{self.port}/{self.dbname})"


@dataclass
class ClickHouseProfile:
    """ClickHouse profile via HTTP/s using clickhouse-connect."""

    type: Literal["clickhouse"]
    host: str = "localhost"
    port: int = 8123
    database: str = "default"
    user: str = "default"
    password_env: str | None = None  # env var name
    password: str | None = None  # explicit (non-recommended)

    def describe(self) -> str:
        return f"{self.type} ({self.host}:{self.port}/{self.database})"


@dataclass
class MySQLProfile:
    """MySQL profile for extracting data from MySQL databases.

    Example ~/.drt/profiles.yml:
        mysql:
          type: mysql
          host: localhost
          port: 3306
          dbname: analytics
          user: analyst
          password_env: MYSQL_PASSWORD
    """

    type: Literal["mysql"]
    host: str = "localhost"
    port: int = 3306
    dbname: str = ""
    user: str = ""
    password_env: str | None = None
    password: str | None = None

    def describe(self) -> str:
        return f"{self.type} ({self.host}:{self.port}/{self.dbname})"


@dataclass
class SnowflakeProfile:
    """Snowflake profile using snowflake-connector-python."""

    type: Literal["snowflake"]
    account: str = ""
    user: str = ""
    password_env: str | None = None
    password: str | None = None
    # Key-pair auth (#737) for TYPE = SERVICE users — new Snowflake accounts
    # enforce MFA on password sign-ins, so programmatic access should use a
    # SERVICE user with an RSA key pair. The env var holds the PEM private
    # key *contents* (PKCS#8). Takes precedence over password when set.
    private_key_env: str | None = None
    private_key_passphrase_env: str | None = None
    database: str = ""
    schema: str = "PUBLIC"
    warehouse: str = ""
    role: str | None = None

    def describe(self) -> str:
        return f"{self.type} ({self.account}/{self.database}.{self.schema})"


@dataclass
class SQLServerProfile:
    """SQL Server profile using pymssql."""

    type: Literal["sqlserver"]
    host: str = ""
    port: int = 1433
    database: str = ""
    user: str = ""
    password_env: str | None = None
    password: str | None = None
    schema: str = "dbo"

    def describe(self) -> str:
        return f"{self.type} ({self.host}/{self.database}.{self.schema})"


@dataclass
class DatabricksProfile:
    """Databricks SQL Warehouse profile using databricks-sql-connector."""

    type: Literal["databricks"]
    server_hostname: str = ""  # e.g. "dbc-abc.cloud.databricks.com"
    http_path: str = ""  # e.g. "/sql/1.0/warehouses/xxxxxx"
    access_token_env: str | None = None
    access_token: str | None = None
    catalog: str | None = None  # Unity Catalog (optional)
    schema: str = "default"

    def describe(self) -> str:
        path = f"{self.catalog}.{self.schema}" if self.catalog else self.schema
        return f"{self.type} ({self.server_hostname}/{path})"


@dataclass
class RestApiProfile:
    """REST API source profile."""

    type: Literal["rest_api"]
    url: str
    auth: dict[str, Any] | None = None
    pagination: dict[str, Any] | None = None
    result_path: str | None = None
    # Incremental extraction (#767): {"start_param": "updated_since"} — the
    # request query parameter that receives the sync's last watermark value
    # when mode=incremental. Validated as RestIncrementalConfig at extract
    # time (same late-validation pattern as auth / pagination).
    incremental: dict[str, Any] | None = None

    def describe(self) -> str:
        return f"{self.type} ({self.url})"


@dataclass
class DeltaLakeProfile:
    """Delta Lake source — read Delta tables from local / S3 / GCS via delta-rs."""

    type: Literal["deltalake"]
    location: str = ""  # local path, s3://bucket/table, gs://bucket/table
    table: str | None = None  # SQL name to query it as (default: last path segment)
    storage_options: dict[str, str] = field(default_factory=dict)  # cloud auth; *_ENV resolved

    def describe(self) -> str:
        return f"{self.type} ({self.location})"


@dataclass
class IcebergProfile:
    """Apache Iceberg source — read Iceberg tables via pyiceberg."""

    type: Literal["iceberg"]
    table: str = ""  # "namespace.table"
    catalog_uri: str | None = None  # REST catalog URI
    warehouse: str | None = None  # s3://... warehouse root
    catalog_name: str = "default"
    properties: dict[str, str] = field(default_factory=dict)  # extra catalog props; *_ENV resolved

    def describe(self) -> str:
        return f"{self.type} ({self.table})"


ProfileConfig = (
    BigQueryProfile
    | DuckDBProfile
    | SQLiteProfile
    | PostgresProfile
    | RedshiftProfile
    | ClickHouseProfile
    | MySQLProfile
    | SnowflakeProfile
    | DatabricksProfile
    | SQLServerProfile
    | RestApiProfile
    | DeltaLakeProfile
    | IcebergProfile
)
