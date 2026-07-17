"""SQL / warehouse destination configs (#721 split from models.py).

Postgres / MySQL / ClickHouse share :class:`BaseSqlDestinationConfig`;
Snowflake / Databricks / BigQuery / Elasticsearch stand alone. All are members
of the :data:`~drt.config.sync_options.DestinationConfig` union.
"""

from __future__ import annotations

from typing import Literal

from pydantic import Field, model_validator

from drt.config.base import DescribableConfig, LookupConfig, RetryConfig, SslConfig


class SnowflakeDestinationConfig(DescribableConfig):
    _detail_is_public = True  # object identity only (#696) — safe for hosted docs
    type: Literal["snowflake"]

    account_env: str
    user_env: str
    # Auth: either a password or an RSA private key (#737). Key-pair is the
    # path for ``TYPE = SERVICE`` users — new Snowflake accounts enforce MFA
    # on password sign-ins, so programmatic identities should migrate to a
    # SERVICE user + key pair. ``private_key_env`` names an env var holding
    # the PEM (PKCS#8) private key *contents*; it takes precedence over
    # ``password_env`` when both are set.
    password_env: str | None = None
    private_key_env: str | None = None
    private_key_passphrase_env: str | None = None

    database: str
    # Use alias because BaseModel.schema() shadows a plain `schema` attribute
    # under mypy strict mode; YAML key stays `schema:`.
    schema_: str = Field(alias="schema")
    table: str

    warehouse: str

    mode: Literal["insert", "merge"] = "insert"

    upsert_key: list[str] | None = None

    # FK resolution against the destination (#316/#354 lookup pattern,
    # extended to Snowflake in #468). Also makes Snowflake queryable, so
    # `drt run --dry-run --diff` produces a true diff and `drt test`
    # validation queries run against the target table.
    lookups: dict[str, LookupConfig] | None = None

    # Layer 3 (#317): introspect INFORMATION_SCHEMA once per sync to wrap
    # VARIANT/OBJECT/ARRAY columns with PARSE_JSON (so dict/list values land as
    # proper semi-structured data, not a stringified repr). On by default; set
    # false for roles without read access to information_schema.
    introspect_schema: bool = True

    @model_validator(mode="after")
    def _check_auth(self) -> SnowflakeDestinationConfig:
        if not self.password_env and not self.private_key_env:
            raise ValueError(
                "snowflake destination needs private_key_env (key-pair auth, "
                "preferred) or password_env."
            )
        return self

    def _describe_detail(self) -> str:
        return f"{self.database}.{self.schema_}.{self.table}"


class BigQueryDestinationConfig(DescribableConfig):
    """BigQuery destination — write data back to BigQuery tables.

    Auth mirrors the BigQuery source: Application Default Credentials by
    default, or a service-account ``keyfile``. The principal needs
    ``bigquery.tables.updateData`` on the target (plus ``bigquery.tables.create``
    / ``bigquery.jobs.create`` for the merge-path temp table).
    """

    _detail_is_public = True  # object identity only (#696) — safe for hosted docs

    type: Literal["bigquery"]

    project: str
    dataset: str
    table: str
    location: str | None = None

    mode: Literal["insert", "merge"] = "insert"
    upsert_key: list[str] | None = None

    # Auth — same convention as the BigQuery source (sources/bigquery.py).
    method: Literal["application_default", "keyfile"] = "application_default"
    keyfile: str | None = None

    def _describe_detail(self) -> str:
        return f"{self.project}.{self.dataset}.{self.table}"


class DatabricksDestinationConfig(DescribableConfig):
    """Databricks Delta Lake destination — write data back to Databricks tables.

    Auth via the Databricks SQL Connector: a SQL warehouse HTTP path
    plus a personal access token (PAT). The token-bearing user needs
    USAGE on the catalog + schema and INSERT/MODIFY on the target
    table.
    """

    _detail_is_public = True  # object identity only (#696) — safe for hosted docs

    type: Literal["databricks"]

    # Workspace hostname (env-var resolved), e.g.
    # ``dbc-abc12345-1234.cloud.databricks.com``.
    host_env: str
    # SQL warehouse HTTP path (env-var resolved), e.g.
    # ``/sql/1.0/warehouses/abc123def456``.
    http_path_env: str
    # Databricks personal access token (env-var resolved). Starts with ``dapi``.
    token_env: str

    # Three-part name (Unity Catalog). For Hive Metastore deployments
    # use catalog="hive_metastore".
    catalog: str
    # Field alias because BaseModel.schema() shadows a plain `schema`
    # attribute under mypy strict mode; YAML key stays `schema:`.
    schema_: str = Field(alias="schema")
    table: str

    mode: Literal["insert", "merge"] = "insert"

    # Required for merge mode and for ``sync.mode: mirror``. Composite
    # keys supported (`upsert_key: [tenant_id, user_id]`).
    upsert_key: list[str] | None = None

    # Layer 3 (#317): introspect information_schema once per sync to wrap
    # STRUCT/MAP/ARRAY columns with from_json (and VARIANT with parse_json), so
    # dict/list values land as proper complex types, not a stringified repr. On
    # by default; set false for roles without read access to information_schema.
    introspect_schema: bool = True

    def _describe_detail(self) -> str:
        return f"{self.catalog}.{self.schema_}.{self.table}"


class ElasticsearchDestinationConfig(DescribableConfig):
    """Elasticsearch / OpenSearch destination — bulk-index records via the ``_bulk`` API."""

    _detail_is_public = True  # object identity only (#696) — safe for hosted docs

    type: Literal["elasticsearch"]
    url: str  # cluster base URL, e.g. https://localhost:9200
    index: str  # target index name
    # Row field whose value becomes the document _id. None → the cluster
    # auto-generates ids (only valid with op_type="index"/"create" where
    # create without an id always inserts).
    id_field: str | None = None
    # "index" upserts (replace-if-exists); "create" fails the row if the
    # _id already exists (409). OpenSearch shares the same API surface.
    op_type: Literal["index", "create"] = "index"
    # Auth — provide an API key (direct or env) OR HTTP Basic creds (env):
    api_key: str | None = None
    api_key_env: str | None = None
    username_env: str | None = None
    password_env: str | None = None
    # TLS verification. Set False for self-signed dev clusters (local
    # OpenSearch / Elasticsearch with the bundled cert).
    verify_tls: bool = True
    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def _describe_detail(self) -> str:
        return f"{self.index}"


class BaseSqlDestinationConfig(DescribableConfig):
    """Shared connection fields for host-based SQL destinations.

    Postgres, MySQL, and ClickHouse all expose the same connection
    surface (``host`` / ``host_env`` + credentials + a connection-string
    escape hatch) plus a lookups field for FK resolution. The
    destination-specific bits (``dbname`` vs ``database`` field name,
    ``ssl`` vs ``secure``, port defaults, ``json_columns`` applicability,
    ``upsert_key`` typing) stay on the concrete subclasses where they
    differ in name or semantics.

    Subclasses keep their own ``_check_connection`` model_validator
    because the database field name differs across dialects; sharing the
    validator would require a ClassVar lookup that obscures the simple
    "host + dbname required unless connection_string_env" rule.

    New host-based SQL destinations (Redshift, future Vertica/CockroachDB)
    should inherit from this class and override ``port`` for the default
    wire port; see ``MySQLDestinationConfig`` / ``ClickHouseDestinationConfig``
    for examples.
    """

    _detail_is_public = True  # object identity only (#696) — safe for hosted docs

    connection_string_env: str | None = None
    host: str | None = None
    host_env: str | None = None
    port: int = 5432  # Postgres default; subclasses override
    user: str | None = None
    user_env: str | None = None
    password: str | None = None
    password_env: str | None = None
    lookups: dict[str, LookupConfig] | None = None
    # Layer 3 (#317): introspect INFORMATION_SCHEMA once per sync to route
    # dict/list values by the column's real type (no json_columns needed).
    # On by default; set false for locked-down environments without read
    # access to information_schema. Explicit json_columns always takes priority.
    introspect_schema: bool = True


class PostgresDestinationConfig(BaseSqlDestinationConfig):
    type: Literal["postgres"]
    dbname: str | None = None
    dbname_env: str | None = None
    table: str  # e.g. "public.analytics_scores"
    upsert_key: list[str]  # columns for ON CONFLICT
    ssl: SslConfig | None = None
    json_columns: list[str] | None = None  # columns that hold JSON/JSONB data

    def _describe_detail(self) -> str:
        return f"{self.table}"

    @model_validator(mode="after")
    def _check_connection(self) -> PostgresDestinationConfig:
        if self.connection_string_env:
            return self  # connection string takes precedence
        if not self.host and not self.host_env:
            raise ValueError("Either host, host_env, or connection_string_env is required.")
        if not self.dbname and not self.dbname_env:
            raise ValueError("Either dbname, dbname_env, or connection_string_env is required.")
        return self


class MySQLDestinationConfig(BaseSqlDestinationConfig):
    type: Literal["mysql"]
    port: int = 3306  # MySQL default
    dbname: str | None = None
    dbname_env: str | None = None
    table: str  # e.g. "interviewer_learning_profiles"
    upsert_key: list[str]  # columns for ON DUPLICATE KEY
    ssl: SslConfig | None = None
    json_columns: list[str] | None = None  # columns that hold JSON data

    def _describe_detail(self) -> str:
        return f"{self.table}"

    @model_validator(mode="after")
    def _check_connection(self) -> MySQLDestinationConfig:
        if self.connection_string_env:
            return self  # connection string takes precedence
        if not self.host and not self.host_env:
            raise ValueError("Either host, host_env, or connection_string_env is required.")
        if not self.dbname and not self.dbname_env:
            raise ValueError("Either dbname, dbname_env, or connection_string_env is required.")
        return self


class ClickHouseDestinationConfig(BaseSqlDestinationConfig):
    type: Literal["clickhouse"]
    port: int = 8123  # ClickHouse HTTP interface; use 8443 for HTTPS
    database: str | None = None
    database_env: str | None = None
    table: str  # unqualified table name (database set via database/database_env)

    # Informational only for ClickHouse. drt does not enforce/create
    # ReplacingMergeTree tables or apply upsert semantics from this field.
    upsert_key: list[str] | None = None
    secure: bool = False  # use HTTPS/TLS; set port explicitly for your deployment (commonly 8443)

    def _describe_detail(self) -> str:
        return f"{self.table}"

    @model_validator(mode="after")
    def _check_connection(self) -> ClickHouseDestinationConfig:
        if self.connection_string_env:
            return self  # connection string takes precedence
        if not self.host and not self.host_env:
            raise ValueError("Either host, host_env, or connection_string_env is required.")
        if not self.database and not self.database_env:
            raise ValueError("Either database, database_env, or connection_string_env is required.")
        return self
