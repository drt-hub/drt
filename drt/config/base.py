"""Shared config primitives for drt (auth, pagination, retry, lookups, project).

Phase 1 of #721 lifted these out of the former monolithic ``models.py`` so the
destination and sync-option modules can share them without a circular import
(destinations need :class:`RetryConfig`; ``sync_options`` needs the destination
union). ``models.py`` re-exports everything here — import sites are unchanged.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, model_validator


class DescribableConfig(BaseModel):
    """Mixin base for destination configs whose ``describe()`` is the canonical
    ``f"{type} (detail)"`` shape (#721). Trivial subclasses supply
    ``_describe_detail``; connectors with a non-standard label (a hardcoded name
    or no parens) override ``describe()`` and inherit :class:`BaseModel` directly.
    """

    def describe(self) -> str:
        return f"{self.type} ({self._describe_detail()})"  # type: ignore[attr-defined]

    def _describe_detail(self) -> str:  # pragma: no cover - always overridden
        raise NotImplementedError


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


class RestIncrementalConfig(BaseModel):
    """Incremental extraction for the REST API source (#767).

    ``start_param`` names the request query parameter that receives the last
    watermark value (e.g. ``updated_since``). Cursor *tracking* stays
    engine-side: ``sync.cursor_field`` names the record field whose max value
    is persisted after each run — this config only tells the source where to
    put that value on the request.
    """

    start_param: str


class SourceConfig(BaseModel):
    type: Literal["bigquery", "snowflake", "postgres", "duckdb", "clickhouse"]
    project: str | None = None
    dataset: str | None = None
    credentials: str | None = None


class HistoryConfig(BaseModel):
    """Sync execution history retention (#276)."""

    enabled: bool = True
    retention_days: int = 30


class ProjectConfig(BaseModel):
    name: str
    version: str = "0.1"
    profile: str = "default"
    source: SourceConfig | None = None  # optional; profile is authoritative
    history: HistoryConfig = Field(default_factory=HistoryConfig)


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
    key_env: str | None = None


class RetryConfig(BaseModel):
    max_attempts: int = 3
    initial_backoff: float = 1.0
    backoff_multiplier: float = 2.0
    max_backoff: float = 60.0
    retryable_status_codes: tuple[int, ...] = (429, 500, 502, 503, 504)
