"""Pydantic models for drt project and sync configuration."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

# ---------------------------------------------------------------------------
# Auth (shared across destination types)
# ---------------------------------------------------------------------------


class BearerAuth(BaseModel):
    type: Literal["bearer"]
    token: str | None = None
    token_env: str | None = None


class ApiKeyAuth(BaseModel):
    type: Literal["api_key"]
    header: str = "X-API-Key"
    value: str | None = None
    value_env: str | None = None


class BasicAuth(BaseModel):
    type: Literal["basic"]
    username_env: str
    password_env: str


class OAuth2ClientCredentialsAuth(BaseModel):
    type: Literal["oauth2_client_credentials"]
    token_url: str
    client_id_env: str
    client_secret_env: str
    scope: str | None = None


AuthConfig = Annotated[
    BearerAuth | ApiKeyAuth | BasicAuth | OAuth2ClientCredentialsAuth,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Pagination (REST API destination)
# ---------------------------------------------------------------------------


class OffsetPaginationConfig(BaseModel):
    type: Literal["offset"]
    limit: int = 100
    offset_param: str = "offset"
    limit_param: str = "limit"
    max_pages: int = 100


class CursorPaginationConfig(BaseModel):
    type: Literal["cursor"]
    limit: int = 100
    cursor_param: str = "cursor"
    limit_param: str = "limit"
    cursor_field: str
    max_pages: int = 100


class LinkHeaderPaginationConfig(BaseModel):
    type: Literal["link_header"]
    max_pages: int = 100


PaginationConfig = Annotated[
    OffsetPaginationConfig | CursorPaginationConfig | LinkHeaderPaginationConfig,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Source config (inline — kept for backward compat; prefer profiles.yml)
# ---------------------------------------------------------------------------


class SourceConfig(BaseModel):
    type: Literal["bigquery", "snowflake", "postgres", "duckdb", "clickhouse"]
    project: str | None = None
    dataset: str | None = None
    credentials: str | None = None


# ---------------------------------------------------------------------------
# Project
# ---------------------------------------------------------------------------


class HistoryConfig(BaseModel):
    """Sync execution history retention (#276)."""

    enabled: bool = True
    retention_days: int = 30
    # Future: max_entries, storage backend (sqlite), s3 upload, etc.


class ProjectConfig(BaseModel):
    name: str
    version: str = "0.1"
    profile: str = "default"
    source: SourceConfig | None = None  # optional; profile is authoritative
    history: HistoryConfig = Field(default_factory=HistoryConfig)


# ---------------------------------------------------------------------------
# Destination configs — discriminated union
# ---------------------------------------------------------------------------


class RestApiDestinationConfig(BaseModel):
    type: Literal["rest_api"]
    url: str
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    body_template: str | None = None
    auth: AuthConfig | None = None
    pagination: PaginationConfig | None = None
    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def describe(self) -> str:
        return f"{self.type} ({self.url})"


class SlackDestinationConfig(BaseModel):
    type: Literal["slack"]
    webhook_url: str | None = None
    webhook_url_env: str | None = None
    # Jinja2 template for Slack message. Supports plain text or Block Kit JSON.
    # Plain text example: "New user: {{ row.name }} ({{ row.email }})"
    # Block Kit: full JSON payload as template string
    message_template: str = "{{ row }}"
    # If True, treat message_template as a Block Kit JSON payload
    block_kit: bool = False
    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def describe(self) -> str:
        return f"{self.type} (webhook)"


class TwilioDestinationConfig(BaseModel):
    type: Literal["twilio"]

    account_sid: str | None = None
    account_sid_env: str | None = None

    auth_token: str | None = None
    auth_token_env: str | None = None

    from_number: str  # Twilio phone number (E.164 format)

    # Jinja2 template → destination phone number
    to_template: str  # e.g. "{{ row.phone }}"

    # Jinja2 template → SMS body
    message_template: str
    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def describe(self) -> str:
        return f"{self.type} ({self.from_number})"

    @model_validator(mode="after")
    def _check_auth(self) -> TwilioDestinationConfig:
        if not (self.account_sid or self.account_sid_env):
            raise ValueError("account_sid or account_sid_env is required.")
        if not (self.auth_token or self.auth_token_env):
            raise ValueError("auth_token or auth_token_env is required.")
        return self


class DiscordDestinationConfig(BaseModel):
    type: Literal["discord"]
    webhook_url: str | None = None
    webhook_url_env: str | None = None
    # Jinja2 template for Discord message. Supports plain text or embeds JSON.
    # Plain text example: "New user: {{ row.name }} ({{ row.email }})"
    # Embeds: full JSON payload with "embeds" array
    message_template: str = "{{ row }}"
    # If True, treat message_template as a JSON payload with embeds
    embeds: bool = False
    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def describe(self) -> str:
        return f"{self.type} (webhook)"


class GitHubActionsDestinationConfig(BaseModel):
    type: Literal["github_actions"]
    owner: str
    repo: str
    workflow_id: str  # filename (e.g. "deploy.yml") or workflow ID
    ref: str = "main"  # branch/tag to run on
    # Jinja2 template → JSON object for workflow inputs
    # Example: '{"environment": "{{ row.env }}", "version": "{{ row.version }}"}'
    inputs_template: str | None = None
    auth: BearerAuth = Field(default_factory=lambda: BearerAuth(type="bearer"))
    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def describe(self) -> str:
        return f"{self.type} ({self.owner}/{self.repo})"


class GoogleSheetsDestinationConfig(BaseModel):
    type: Literal["google_sheets"]
    spreadsheet_id: str
    sheet: str = "Sheet1"
    mode: Literal["overwrite", "append"] = "overwrite"
    credentials_path: str | None = None
    credentials_env: str | None = None

    def describe(self) -> str:
        return f"{self.type} ({self.sheet})"


class HubSpotDestinationConfig(BaseModel):
    type: Literal["hubspot"]
    object_type: Literal["contacts", "deals", "companies"] = "contacts"
    # Property used as upsert key (contacts → email, deals → dealname, etc.)
    id_property: str = "email"
    # Jinja2 template → JSON object of HubSpot properties
    # Example: '{"email": "{{ row.email }}", "firstname": "{{ row.name }}"}'
    properties_template: str | None = None
    auth: BearerAuth = Field(default_factory=lambda: BearerAuth(type="bearer"))
    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def describe(self) -> str:
        return f"{self.type} ({self.object_type})"


class ZendeskDestinationConfig(BaseModel):
    type: Literal["zendesk"]
    subdomain: str | None = None
    subdomain_env: str | None = None
    email: str | None = None
    email_env: str | None = None
    api_token: str | None = None
    api_token_env: str | None = None
    object: Literal["user", "organization"] = "user"
    id_field: str | None = None
    custom_fields_template: str | None = None
    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def describe(self) -> str:
        return f"{self.type} ({self.object})"


class AmplitudeDestinationConfig(BaseModel):
    type: Literal["amplitude"]
    api_key: str | None = None
    api_key_env: str | None = "AMPLITUDE_API_KEY"
    region: Literal["default", "eu"] = "default"
    endpoint: Literal["identify", "event"] = "identify"
    user_id_field: str = "user_id"
    device_id_field: str | None = None
    event_type_field: str | None = None
    event_type: str | None = None
    time_field: str | None = None
    insert_id_field: str | None = None
    properties_template: str | None = None
    batch_size: int = 1000
    min_id_length: int | None = None
    retry: RetryConfig | None = None

    def describe(self) -> str:
        return f"{self.type} ({self.endpoint}, {self.region})"

    @model_validator(mode="after")
    def _check_api_key(self) -> AmplitudeDestinationConfig:
        if not self.api_key and not self.api_key_env:
            raise ValueError("api_key or api_key_env is required.")
        return self

    @model_validator(mode="after")
    def _check_event_endpoint(self) -> AmplitudeDestinationConfig:
        if self.endpoint == "event" and not self.event_type and not self.event_type_field:
            raise ValueError("event_type or event_type_field is required when endpoint is 'event'.")
        return self

    @field_validator("batch_size", mode="after")
    @classmethod
    def _clamp_batch_size(cls, value: int) -> int:
        return max(1, min(value, 1000))


class MixpanelDestinationConfig(BaseModel):
    type: Literal["mixpanel"]
    # endpoint selects which Mixpanel API to target:
    #   people_set    -> /engage#profile-set (auth: project token, per-record)
    #   import_events -> /import             (auth: service account + project_id)
    endpoint: Literal["people_set", "import_events"] = "people_set"
    region: Literal["default", "eu"] = "default"

    # people_set auth — project token
    project_token: str | None = None
    project_token_env: str | None = "MIXPANEL_TOKEN"

    # import_events auth — service account + numeric project id
    project_id: str | None = None
    service_account_username: str | None = None
    service_account_username_env: str | None = "MIXPANEL_SA_USERNAME"
    service_account_secret: str | None = None
    service_account_secret_env: str | None = "MIXPANEL_SA_SECRET"

    # field mapping
    distinct_id_field: str = "distinct_id"
    event_name_field: str | None = None  # import_events: per-row event name
    event_name: str | None = None  # import_events: constant event name
    time_field: str | None = None  # import_events: row field for event time
    insert_id_field: str | None = None  # import_events: dedup id (else derived)
    properties_template: str | None = None

    batch_size: int = 2000
    retry: RetryConfig | None = None

    def describe(self) -> str:
        return f"{self.type} ({self.endpoint}, {self.region})"

    @model_validator(mode="after")
    def _check_auth(self) -> MixpanelDestinationConfig:
        if self.endpoint == "people_set":
            if not self.project_token and not self.project_token_env:
                raise ValueError(
                    "project_token or project_token_env is required for endpoint 'people_set'."
                )
        else:  # import_events
            if not self.project_id:
                raise ValueError("project_id is required for endpoint 'import_events'.")
            has_user = self.service_account_username or self.service_account_username_env
            has_secret = self.service_account_secret or self.service_account_secret_env
            if not has_user or not has_secret:
                raise ValueError(
                    "service account credentials (username + secret, or their *_env "
                    "vars) are required for endpoint 'import_events'."
                )
        return self

    @model_validator(mode="after")
    def _check_event_name(self) -> MixpanelDestinationConfig:
        if self.endpoint == "import_events" and not self.event_name and not self.event_name_field:
            raise ValueError(
                "event_name or event_name_field is required when endpoint is 'import_events'."
            )
        return self

    @field_validator("batch_size", mode="after")
    @classmethod
    def _clamp_batch_size(cls, value: int) -> int:
        # Mixpanel caps both /engage and /import at 2000 records per request.
        return max(1, min(value, 2000))


class IntercomDestinationConfig(BaseModel):
    type: Literal["intercom"]

    auth: AuthConfig

    # Jinja2 JSON template for contact payload
    properties_template: str

    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def describe(self) -> str:
        return f"{self.type} (contacts)"


class SendGridDestinationConfig(BaseModel):
    type: Literal["sendgrid"]
    from_email: str
    from_name: str | None = None
    subject_template: str
    body_template: str
    to_email_field: str = "email"
    list_ids: list[str] | None = None
    auth: BearerAuth = Field(default_factory=lambda: BearerAuth(type="bearer"))
    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def describe(self) -> str:
        return f"sendgrid ({self.from_email})"


class SnowflakeDestinationConfig(BaseModel):
    type: Literal["snowflake"]

    account_env: str
    user_env: str
    password_env: str

    database: str
    # Use alias because BaseModel.schema() shadows a plain `schema` attribute
    # under mypy strict mode; YAML key stays `schema:`.
    schema_: str = Field(alias="schema")
    table: str

    warehouse: str

    mode: Literal["insert", "merge"] = "insert"

    upsert_key: list[str] | None = None

    def describe(self) -> str:
        return f"{self.type} ({self.database}.{self.schema_}.{self.table})"


class DatabricksDestinationConfig(BaseModel):
    """Databricks Delta Lake destination — write data back to Databricks tables.

    Auth via the Databricks SQL Connector: a SQL warehouse HTTP path
    plus a personal access token (PAT). The token-bearing user needs
    USAGE on the catalog + schema and INSERT/MODIFY on the target
    table.
    """

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

    def describe(self) -> str:
        return f"{self.type} ({self.catalog}.{self.schema_}.{self.table})"


class LinearDestinationConfig(BaseModel):
    type: Literal["linear"]
    team_id: str | None = None
    team_id_env: str | None = None
    title_template: str
    description_template: str
    label_ids: list[str] = []
    assignee_id: str | None = None
    auth: BearerAuth = Field(default_factory=lambda: BearerAuth(type="bearer"))
    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def describe(self) -> str:
        return "linear (issue)"


class LookupConfig(BaseModel):
    """Resolve a column value by querying the destination DB.

    Used to resolve foreign key values when syncing related tables.
    The destination DB is queried once per lookup to build an in-memory
    mapping, then each source row is enriched with the resolved value.

    Example YAML::

        lookups:
          interviewer_profile_id:
            table: interviewer_profiles
            match: { user_id: user_id }
            select: id
            on_miss: skip
    """

    table: str  # destination DB table to query
    match: dict[str, str]  # { destination_column: source_column }
    select: str | None = None  # column to fetch; omitted when check_only=True
    on_miss: Literal["skip", "fail", "null"] = "skip"
    drop_match_columns: bool = True  # remove match source columns from INSERT
    check_only: bool = False  # filter-only mode: existence check, no value resolution

    @model_validator(mode="after")
    def _check_match_not_empty(self) -> LookupConfig:
        if not self.match:
            raise ValueError("lookups.match must contain at least one mapping.")
        return self

    @model_validator(mode="after")
    def _check_select_consistency(self) -> LookupConfig:
        if self.check_only and self.select is not None:
            raise ValueError(
                "lookups.select must be omitted when check_only=True "
                "(check_only is filter-only — no value is resolved)."
            )
        if not self.check_only and self.select is None:
            raise ValueError(
                "lookups.select is required (or set check_only=true for existence-only filtering)."
            )
        return self

    @model_validator(mode="after")
    def _check_on_miss_consistency(self) -> LookupConfig:
        if self.check_only and self.on_miss == "null":
            raise ValueError(
                "lookups.on_miss='null' is invalid with check_only=True "
                "(no target column to set NULL on; use 'skip' or 'fail')."
            )
        return self


class SslConfig(BaseModel):
    """SSL/TLS connection options for DB destinations."""

    enabled: bool = False
    ca_env: str | None = None  # env var for CA cert path
    cert_env: str | None = None  # env var for client cert path
    key_env: str | None = None  # env var for client key path


class BaseSqlDestinationConfig(BaseModel):
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

    connection_string_env: str | None = None
    host: str | None = None
    host_env: str | None = None
    port: int = 5432  # Postgres default; subclasses override
    user: str | None = None
    user_env: str | None = None
    password: str | None = None
    password_env: str | None = None
    lookups: dict[str, LookupConfig] | None = None


class PostgresDestinationConfig(BaseSqlDestinationConfig):
    type: Literal["postgres"]
    dbname: str | None = None
    dbname_env: str | None = None
    table: str  # e.g. "public.analytics_scores"
    upsert_key: list[str]  # columns for ON CONFLICT
    ssl: SslConfig | None = None
    json_columns: list[str] | None = None  # columns that hold JSON/JSONB data

    def describe(self) -> str:
        return f"{self.type} ({self.table})"

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

    def describe(self) -> str:
        return f"{self.type} ({self.table})"

    @model_validator(mode="after")
    def _check_connection(self) -> MySQLDestinationConfig:
        if self.connection_string_env:
            return self  # connection string takes precedence
        if not self.host and not self.host_env:
            raise ValueError("Either host, host_env, or connection_string_env is required.")
        if not self.dbname and not self.dbname_env:
            raise ValueError("Either dbname, dbname_env, or connection_string_env is required.")
        return self


class TeamsDestinationConfig(BaseModel):
    type: Literal["teams"]
    webhook_url: str | None = None
    webhook_url_env: str | None = None
    # Jinja2 template for the message card. Supports plain text or Adaptive Card JSON.
    message_template: str = "{{ row }}"
    # If True, treat message_template as an Adaptive Card JSON payload
    adaptive_card: bool = False
    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def describe(self) -> str:
        return f"{self.type} (webhook)"


class JiraDestinationConfig(BaseModel):
    type: Literal["jira"]
    base_url_env: str  # e.g. JIRA_BASE_URL -> https://myorg.atlassian.net
    email_env: str  # Jira account email env var
    token_env: str  # Jira API token env var
    project_key: str  # can include Jinja2 template syntax
    issue_type: str = "Task"  # can include Jinja2 template syntax
    summary_template: str
    description_template: str
    issue_id_field: str = "issue_id"  # row key that indicates update mode
    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def describe(self) -> str:
        return f"jira ({self.project_key})"


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

    def describe(self) -> str:
        return f"{self.type} ({self.table})"

    @model_validator(mode="after")
    def _check_connection(self) -> ClickHouseDestinationConfig:
        if self.connection_string_env:
            return self  # connection string takes precedence
        if not self.host and not self.host_env:
            raise ValueError("Either host, host_env, or connection_string_env is required.")
        if not self.database and not self.database_env:
            raise ValueError("Either database, database_env, or connection_string_env is required.")
        return self


class ParquetDestinationConfig(BaseModel):
    type: Literal["parquet"]
    path: str  # output file or directory path, e.g. "output/data.parquet"
    partition_by: list[str] | None = None  # optional partition columns
    compression: Literal["snappy", "gzip", "zstd", "none"] = "snappy"

    def describe(self) -> str:
        return f"{self.type} ({self.path})"


class FileDestinationConfig(BaseModel):
    type: Literal["file"]
    path: str  # output file path, e.g. "output/data.csv"
    format: Literal["csv", "json", "jsonl"] = "csv"

    def describe(self) -> str:
        return f"{self.type} ({self.path})"


class S3DestinationConfig(BaseModel):
    """S3 destination — upload records as CSV / JSON / JSONL / Parquet to S3."""

    type: Literal["s3"]
    bucket: str
    # Optional key prefix. The generated file name is appended to this prefix:
    # e.g. prefix="drt/users/" → "drt/users/20260605T123000Z.csv". For
    # per-sync routing, give each sync its own prefix.
    prefix: str = ""
    format: Literal["csv", "json", "jsonl", "parquet"] = "csv"
    # gzip-compress csv / json / jsonl uploads ("none" disables). Parquet
    # uses its native compression below; "gzip" here is ignored for parquet.
    compression: Literal["none", "gzip"] = "none"
    # Optional Parquet-specific compression (matches ParquetDestinationConfig).
    parquet_compression: Literal["snappy", "gzip", "zstd", "none"] = "snappy"
    region: str | None = None  # AWS region; defers to boto3 default if unset
    # AWS auth: by default, falls back to boto3's standard credential chain
    # (env vars, ~/.aws/credentials, instance profile, IAM role). Provide one
    # of the following for explicit overrides:
    aws_profile: str | None = None  # named profile in ~/.aws/credentials
    aws_access_key_id_env: str | None = None
    aws_secret_access_key_env: str | None = None
    aws_session_token_env: str | None = None
    # Optional endpoint URL — set when targeting an S3-compatible service
    # (MinIO, LocalStack, R2, etc.). None → real AWS S3.
    endpoint_url: str | None = None
    # Optional file-name template (Jinja2-free, supports one placeholder:
    # {timestamp} — UTC ISO 8601 basic format, e.g. "20260605T123000Z").
    # Default produces "<prefix><timestamp>.<ext>". For per-sync naming,
    # set ``prefix`` per sync (e.g. ``prefix: drt/active_users/``).
    key_template: str | None = None

    def describe(self) -> str:
        return f"{self.type} (s3://{self.bucket}/{self.prefix})"


class EmailSmtpDestinationConfig(BaseModel):
    type: Literal["email_smtp"] = "email_smtp"
    host: str
    port: int = 587
    sender: str
    recipients: list[str]
    subject_template: str
    body_template: str
    use_tls: bool = True
    username: str | None = None
    username_env: str | None = None
    password: str | None = None
    password_env: str | None = None

    def describe(self) -> str:
        return f"{self.type} ({self.host})"


class NotionDestinationConfig(BaseModel):
    type: Literal["notion"]
    database_id: str
    # Jinja2 template → JSON object of Notion page properties
    # Example: see https://developers.notion.com/reference/post-page for template format
    properties_template: str | None = None
    auth: BearerAuth = Field(default_factory=lambda: BearerAuth(type="bearer"))
    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def describe(self) -> str:
        return f"{self.type} (database {self.database_id})"


class GoogleAdsDestinationConfig(BaseModel):
    type: Literal["google_ads"]
    customer_id: str  # Google Ads customer ID (without hyphens)
    conversion_action: str  # e.g. "customers/123/conversionActions/456"
    gclid_field: str = "gclid"  # row field containing the click ID
    conversion_time_field: str = "conversion_time"  # row field for timestamp
    conversion_value_field: str | None = None  # optional: row field for value
    currency_code: str = "USD"
    developer_token_env: str = "GOOGLE_ADS_DEVELOPER_TOKEN"
    auth: AuthConfig | None = None  # typically oauth2_client_credentials
    retry: RetryConfig | None = None  # destination-level override of sync.retry

    def describe(self) -> str:
        return f"google_ads ({self.customer_id})"


class StagedUploadPhaseConfig(BaseModel):
    url: str
    method: str = "POST"
    headers: dict[str, str] | None = None
    auth: AuthConfig | None = None
    body_template: str | None = None
    response_extract: dict[str, str] | None = None


class StagedUploadPollConfig(BaseModel):
    url: str
    method: str = "GET"
    headers: dict[str, str] | None = None
    auth: AuthConfig | None = None
    status_field: str = "status"
    success_values: list[str] = ["SUCCEEDED", "COMPLETED"]
    failure_values: list[str] = ["FAILED", "ERROR"]
    interval_seconds: int = 30
    timeout_seconds: int = 3600


class StagedUploadDestinationConfig(BaseModel):
    type: Literal["staged_upload"]
    stage: StagedUploadPhaseConfig
    trigger: StagedUploadPhaseConfig
    poll: StagedUploadPollConfig | None = None
    format: Literal["csv", "json", "jsonl"] = "csv"

    def describe(self) -> str:
        return "staged_upload"


class SalesforceBulkDestinationConfig(BaseModel):
    type: Literal["salesforce_bulk"]
    instance_url: str | None = None
    instance_url_env: str | None = None
    object_name: str  # e.g. "Contact", "Account"
    operation: Literal["insert", "update", "upsert", "delete"] = "upsert"
    external_id_field: str = "Id"
    poll_timeout_seconds: int = 3600
    poll_interval_seconds: int = 30
    client_id_env: str
    client_secret_env: str
    username_env: str
    password_env: str

    def describe(self) -> str:
        return f"salesforce_bulk ({self.object_name})"

    @model_validator(mode="after")
    def _check_instance_url(self) -> SalesforceBulkDestinationConfig:
        if not self.instance_url and not self.instance_url_env:
            raise ValueError("Either instance_url or instance_url_env is required.")
        return self


# Discriminated union — add new destination types here
DestinationConfig = Annotated[
    RestApiDestinationConfig
    | SlackDestinationConfig
    | DiscordDestinationConfig
    | GitHubActionsDestinationConfig
    | HubSpotDestinationConfig
    | ZendeskDestinationConfig
    | AmplitudeDestinationConfig
    | MixpanelDestinationConfig
    | SendGridDestinationConfig
    | LinearDestinationConfig
    | GoogleSheetsDestinationConfig
    | PostgresDestinationConfig
    | MySQLDestinationConfig
    | TeamsDestinationConfig
    | JiraDestinationConfig
    | ClickHouseDestinationConfig
    | ParquetDestinationConfig
    | GoogleAdsDestinationConfig
    | FileDestinationConfig
    | S3DestinationConfig
    | EmailSmtpDestinationConfig
    | NotionDestinationConfig
    | IntercomDestinationConfig
    | StagedUploadDestinationConfig
    | SalesforceBulkDestinationConfig
    | TwilioDestinationConfig
    | SnowflakeDestinationConfig
    | DatabricksDestinationConfig,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Sync options
# ---------------------------------------------------------------------------


class RateLimitConfig(BaseModel):
    requests_per_second: int = 10


class RetryConfig(BaseModel):
    max_attempts: int = 3
    initial_backoff: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff: float = 60.0
    retryable_status_codes: tuple[int, ...] = (429, 500, 502, 503, 504)


class WatermarkConfig(BaseModel):
    """Configuration for remote watermark storage."""

    storage: Literal["local", "gcs", "bigquery"] = "local"
    # GCS
    bucket: str | None = None
    key: str | None = None
    # BigQuery
    project: str | None = None
    dataset: str | None = None
    # Fallback value used when no watermark exists yet (first run)
    default_value: str | None = None

    @model_validator(mode="after")
    def _check_backend_fields(self) -> WatermarkConfig:
        if self.storage == "gcs" and not self.bucket:
            raise ValueError("watermark.bucket is required when storage is 'gcs'.")
        if self.storage == "gcs" and not self.key:
            raise ValueError("watermark.key is required when storage is 'gcs'.")
        if self.storage == "bigquery" and not self.project:
            raise ValueError("watermark.project is required when storage is 'bigquery'.")
        if self.storage == "bigquery" and not self.dataset:
            raise ValueError("watermark.dataset is required when storage is 'bigquery'.")
        return self


class SyncOptions(BaseModel):
    mode: Literal["full", "incremental", "upsert", "replace", "mirror"] = "full"
    replace_strategy: Literal["truncate", "swap"] = "truncate"
    cursor_field: str | None = None  # required when mode=incremental
    watermark: WatermarkConfig | None = None
    batch_size: int = 100
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    on_error: Literal["skip", "fail"] = "fail"

    @model_validator(mode="after")
    def _check_incremental_cursor(self) -> SyncOptions:
        if self.mode == "incremental" and not self.cursor_field:
            raise ValueError("cursor_field is required when mode is 'incremental'.")
        return self

    @model_validator(mode="after")
    def _check_replace_strategy(self) -> SyncOptions:
        if self.replace_strategy == "swap" and self.mode != "replace":
            raise ValueError("replace_strategy='swap' requires mode='replace'.")
        return self


class RowCountTest(BaseModel):
    min: int | None = None
    max: int | None = None


class NotNullTest(BaseModel):
    columns: list[str]


class FreshnessTest(BaseModel):
    column: str
    max_age: str  # e.g., "7 days", "1 hour", "30 minutes"


class UniqueTest(BaseModel):
    columns: list[str] = Field(min_length=1)


class AcceptedValuesTest(BaseModel):
    column: str
    values: list[str] = Field(min_length=1)


class SyncTest(BaseModel):
    row_count: RowCountTest | None = None
    not_null: NotNullTest | None = None
    freshness: FreshnessTest | None = None
    unique: UniqueTest | None = None
    accepted_values: AcceptedValuesTest | None = None

    @model_validator(mode="after")
    def _check_exactly_one_test(self) -> SyncTest:
        configured_tests = [
            self.row_count,
            self.not_null,
            self.freshness,
            self.unique,
            self.accepted_values,
        ]
        configured_count = sum(test is not None for test in configured_tests)
        if configured_count != 1:
            raise ValueError("Exactly one sync test must be configured in each tests entry.")
        return self


class SlackAlertConfig(BaseModel):
    type: Literal["slack"]
    webhook_url: str | None = None
    webhook_url_env: str | None = None
    message: str = "drt sync `{sync_name}` failed: {error}"

    @model_validator(mode="after")
    def _check_url(self) -> SlackAlertConfig:
        if not self.webhook_url and not self.webhook_url_env:
            raise ValueError("Either webhook_url or webhook_url_env is required.")
        return self


class WebhookAlertConfig(BaseModel):
    type: Literal["webhook"]
    url: str | None = None
    url_env: str | None = None
    method: Literal["POST", "PUT"] = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    body_template: str | None = None  # JSON template; None → default JSON payload

    @model_validator(mode="after")
    def _check_url(self) -> WebhookAlertConfig:
        if not self.url and not self.url_env:
            raise ValueError("Either url or url_env is required.")
        return self


AlertItem = Annotated[
    SlackAlertConfig | WebhookAlertConfig,
    Field(discriminator="type"),
]


class AlertsConfig(BaseModel):
    on_failure: list[AlertItem] = Field(default_factory=list)


class SyncConfig(BaseModel):
    name: str
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    model: str
    destination: DestinationConfig
    sync: SyncOptions = Field(default_factory=SyncOptions)
    tests: list[SyncTest] = Field(default_factory=list)
    alerts: AlertsConfig | None = None
