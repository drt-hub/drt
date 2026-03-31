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


AuthConfig = Annotated[
    BearerAuth | ApiKeyAuth | BasicAuth,
    Field(discriminator="type"),
]


# ---------------------------------------------------------------------------
# Source config (inline — kept for backward compat; prefer profiles.yml)
# ---------------------------------------------------------------------------


class SourceConfig(BaseModel):
    type: Literal["bigquery", "snowflake", "postgres", "duckdb"]
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


class HubSpotDestinationConfig(BaseModel):
    type: Literal["hubspot"]
    object_type: Literal["contacts", "deals", "companies"] = "contacts"
    # Property used as upsert key (contacts → email, deals → dealname, etc.)
    id_property: str = "email"
    # Jinja2 template → JSON object of HubSpot properties
    # Example: '{"email": "{{ row.email }}", "firstname": "{{ row.name }}"}'
    properties_template: str | None = None
    auth: BearerAuth = Field(default_factory=lambda: BearerAuth(type="bearer"))


# Discriminated union — add new destination types here
DestinationConfig = Annotated[
    RestApiDestinationConfig
    | SlackDestinationConfig
    | GitHubActionsDestinationConfig
    | HubSpotDestinationConfig,
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


class SyncOptions(BaseModel):
    mode: Literal["full", "incremental"] = "full"
    cursor_field: str | None = None  # required when mode=incremental
    batch_size: int = 100
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    on_error: Literal["skip", "fail"] = "fail"

    @model_validator(mode="after")
    def _check_incremental_cursor(self) -> "SyncOptions":
        if self.mode == "incremental" and not self.cursor_field:
            raise ValueError("cursor_field is required when mode is 'incremental'.")
        return self


class SyncConfig(BaseModel):
    name: str
    description: str = ""
    model: str
    destination: DestinationConfig
    sync: SyncOptions = Field(default_factory=SyncOptions)
