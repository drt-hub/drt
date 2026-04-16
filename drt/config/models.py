"""Pydantic models for drt project and sync configuration."""

from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator

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


class ProjectConfig(BaseModel):
    name: str
    version: str = "0.1"
    profile: str = "default"
    source: SourceConfig | None = None  # optional; profile is authoritative


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

    def describe(self) -> str:
        return f"{self.type} (webhook)"


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

    def describe(self) -> str:
        return f"{self.type} ({self.object_type})"


class SendGridDestinationConfig(BaseModel):
    type: Literal["sendgrid"]
    from_email: str
    from_name: str | None = None
    subject_template: str
    body_template: str
    to_email_field: str = "email"
    list_ids: list[str] | None = None
    auth: BearerAuth = Field(default_factory=lambda: BearerAuth(type="bearer"))

    def describe(self) -> str:
        return f"sendgrid ({self.from_email})"


class LinearDestinationConfig(BaseModel):
    type: Literal["linear"]
    team_id: str | None = None
    team_id_env: str | None = None
    title_template: str
    description_template: str
    label_ids: list[str] = []
    assignee_id: str | None = None
    auth: BearerAuth = Field(default_factory=lambda: BearerAuth(type="bearer"))

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
    select: str  # column to fetch from the lookup table
    on_miss: Literal["skip", "fail", "null"] = "skip"

    @model_validator(mode="after")
    def _check_match_not_empty(self) -> "LookupConfig":
        if not self.match:
            raise ValueError("lookups.match must contain at least one mapping.")
        return self


class SslConfig(BaseModel):
    """SSL/TLS connection options for DB destinations."""

    enabled: bool = False
    ca_env: str | None = None  # env var for CA cert path
    cert_env: str | None = None  # env var for client cert path
    key_env: str | None = None  # env var for client key path


class PostgresDestinationConfig(BaseModel):
    type: Literal["postgres"]
    connection_string_env: str | None = None
    host: str | None = None
    host_env: str | None = None
    port: int = 5432
    dbname: str | None = None
    dbname_env: str | None = None
    user: str | None = None
    user_env: str | None = None
    password: str | None = None
    password_env: str | None = None
    table: str  # e.g. "public.analytics_scores"
    upsert_key: list[str]  # columns for ON CONFLICT
    ssl: SslConfig | None = None
    lookups: dict[str, LookupConfig] | None = None

    def describe(self) -> str:
        return f"{self.type} ({self.table})"

    @model_validator(mode="after")
    def _check_connection(self) -> "PostgresDestinationConfig":
        if self.connection_string_env:
            return self  # connection string takes precedence
        if not self.host and not self.host_env:
            raise ValueError("Either host, host_env, or connection_string_env is required.")
        if not self.dbname and not self.dbname_env:
            raise ValueError("Either dbname, dbname_env, or connection_string_env is required.")
        return self


class MySQLDestinationConfig(BaseModel):
    type: Literal["mysql"]
    connection_string_env: str | None = None
    host: str | None = None
    host_env: str | None = None
    port: int = 3306
    dbname: str | None = None
    dbname_env: str | None = None
    user: str | None = None
    user_env: str | None = None
    password: str | None = None
    password_env: str | None = None
    table: str  # e.g. "interviewer_learning_profiles"
    upsert_key: list[str]  # columns for ON DUPLICATE KEY
    ssl: SslConfig | None = None
    lookups: dict[str, LookupConfig] | None = None

    def describe(self) -> str:
        return f"{self.type} ({self.table})"

    @model_validator(mode="after")
    def _check_connection(self) -> "MySQLDestinationConfig":
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

    def describe(self) -> str:
        return f"jira ({self.project_key})"


class ClickHouseDestinationConfig(BaseModel):
    type: Literal["clickhouse"]
    connection_string_env: str | None = None
    host: str | None = None
    host_env: str | None = None
    port: int = 8123
    database: str | None = None
    database_env: str | None = None
    user: str | None = None
    user_env: str | None = None
    password: str | None = None
    password_env: str | None = None
    table: str  # unqualified table name (database set via database/database_env)

    # Informational only for ClickHouse. drt does not enforce/create
    # ReplacingMergeTree tables or apply upsert semantics from this field.
    upsert_key: list[str] | None = None
    secure: bool = False  # use HTTPS/TLS; set port explicitly for your deployment (commonly 8443)
    lookups: dict[str, LookupConfig] | None = None

    def describe(self) -> str:
        return f"{self.type} ({self.table})"

    @model_validator(mode="after")
    def _check_connection(self) -> "ClickHouseDestinationConfig":
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


# Discriminated union — add new destination types here
DestinationConfig = Annotated[
    RestApiDestinationConfig
    | SlackDestinationConfig
    | DiscordDestinationConfig
    | GitHubActionsDestinationConfig
    | HubSpotDestinationConfig
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
    | StagedUploadDestinationConfig,
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

    @model_validator(mode="after")
    def _check_backend_fields(self) -> "WatermarkConfig":
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
    mode: Literal["full", "incremental", "upsert", "replace"] = "full"
    cursor_field: str | None = None  # required when mode=incremental
    watermark: WatermarkConfig | None = None
    batch_size: int = 100
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    on_error: Literal["skip", "fail"] = "fail"

    @model_validator(mode="after")
    def _check_incremental_cursor(self) -> "SyncOptions":
        if self.mode == "incremental" and not self.cursor_field:
            raise ValueError("cursor_field is required when mode is 'incremental'.")
        return self


class RowCountTest(BaseModel):
    min: int | None = None
    max: int | None = None


class NotNullTest(BaseModel):
    columns: list[str]


class SyncTest(BaseModel):
    row_count: RowCountTest | None = None
    not_null: NotNullTest | None = None


class SyncConfig(BaseModel):
    name: str
    description: str = ""
    model: str
    destination: DestinationConfig
    sync: SyncOptions = Field(default_factory=SyncOptions)
    tests: list[SyncTest] = Field(default_factory=list)
