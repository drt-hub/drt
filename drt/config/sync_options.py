"""Sync options, tests, alerts, and the sync config root (#721 split from models.py).

Also home to the :data:`DestinationConfig` discriminated union, assembled from
the three ``destinations_*`` modules and consumed by :class:`SyncConfig`.
"""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, Field, PrivateAttr, model_validator

from drt.config.base import RetryConfig
from drt.config.destinations_saas import (
    AirtableDestinationConfig,
    AmplitudeDestinationConfig,
    DiscordDestinationConfig,
    EmailSmtpDestinationConfig,
    GitHubActionsDestinationConfig,
    GoogleAdsDestinationConfig,
    GoogleSheetsDestinationConfig,
    HubSpotDestinationConfig,
    IntercomDestinationConfig,
    JiraDestinationConfig,
    KlaviyoDestinationConfig,
    LinearDestinationConfig,
    MixpanelDestinationConfig,
    NotionDestinationConfig,
    RestApiDestinationConfig,
    SalesforceBulkDestinationConfig,
    SendGridDestinationConfig,
    SlackDestinationConfig,
    StagedUploadDestinationConfig,
    TeamsDestinationConfig,
    TwilioDestinationConfig,
    ZendeskDestinationConfig,
)
from drt.config.destinations_sql import (
    BigQueryDestinationConfig,
    ClickHouseDestinationConfig,
    DatabricksDestinationConfig,
    ElasticsearchDestinationConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    SnowflakeDestinationConfig,
)
from drt.config.destinations_storage import (
    AzureBlobDestinationConfig,
    FileDestinationConfig,
    GCSDestinationConfig,
    ParquetDestinationConfig,
    S3DestinationConfig,
)
from drt.config.duration import parse_duration


class RateLimitConfig(BaseModel):
    requests_per_second: int = 10


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
    # Overlap window (#759): widen the incremental *read* window by this much
    # behind the stored watermark so late-arriving rows are re-synced.
    # Timestamp cursors take a duration string ("1 hour" — grammar shared with
    # freshness.max_age); numeric cursors take a positive int (cursor units).
    # Applies only to storage-sourced watermarks — never to --cursor-value
    # overrides or default_value first runs — and the persisted watermark is
    # never lagged, so the window cannot regress. Rows inside the lag window
    # are re-sent every run: the destination must tolerate duplicates
    # (e.g. via upsert_key).
    lag: str | int | None = None

    @model_validator(mode="after")
    def _check_lag(self) -> WatermarkConfig:
        if isinstance(self.lag, bool):
            raise ValueError("watermark.lag must be a duration string or a positive integer.")
        if self.lag is None:
            return self
        if isinstance(self.lag, int):
            if self.lag <= 0:
                raise ValueError(
                    "watermark.lag must be a positive integer (units of the numeric cursor)."
                )
        else:
            parse_duration(self.lag, field_name="watermark.lag")
        return self

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


class DLQConfig(BaseModel):
    """Dead Letter Queue — persist per-record load failures for replay (#278).

    Opt-in: when ``enabled``, each record that fails during ``destination.load()``
    is written verbatim to ``.drt/dlq/<sync_name>.jsonl`` so ``drt retry <sync>``
    can re-send just the failures. Off by default because it writes full record
    payloads to disk (a PII decision the operator makes explicitly).
    """

    enabled: bool = False
    # Cap queue growth — oldest entries are dropped past this (0 = unbounded).
    max_records: int = 10_000

    @model_validator(mode="after")
    def _check_max_records(self) -> DLQConfig:
        if self.max_records < 0:
            raise ValueError("dlq.max_records must be >= 0 (0 disables the cap).")
        return self


class MaskRule(BaseModel):
    """Object form of a mask rule, for strategies that take a parameter (#660).

    The flat form (``field: "hash" | "redact"``) covers parameter-less strategies.
    This object form is used when a strategy needs options, for example
    ``{strategy: "truncate", length: 2}``.
    """

    strategy: Literal["hash", "redact", "truncate"]
    length: int | None = None

    @model_validator(mode="after")
    def _validate_length(self) -> MaskRule:
        if self.strategy == "truncate":
            if self.length is None or self.length < 0:
                raise ValueError(
                    "the 'truncate' strategy requires a non-negative 'length'"
                )
        elif self.length is not None:
            raise ValueError(
                f"'length' is not valid for the '{self.strategy}' strategy"
            )
        return self


MaskSpec = Literal["hash", "redact"] | MaskRule


class MirrorConfig(BaseModel):
    """``sync.mirror`` — mirror-mode delete behaviour (#686).

    - ``strategy: destination`` (default) — the original #340 behaviour:
      DELETE destination rows whose ``upsert_key`` was not observed this
      run. Correct only when drt exclusively owns the table.
    - ``strategy: tracked`` — DELETE only rows drt itself previously
      synced, tracked per sync in a drt-managed ``_drt_synced_keys`` side
      table in the destination. Safe on tables the application also
      writes to (Census-style semantics: first run baselines without
      deleting; lost state re-baselines with a warning).
    - ``scope`` (#687) — restrict destination-strategy deletes to rows
      whose scope-column values appeared in this run's source. The
      stateless fit for 1:N regeneration (parent + child link rows):
      stale children under observed parents are deleted, rows under
      unobserved parents are untouched.
    """

    strategy: Literal["destination", "tracked"] = "destination"
    scope: list[str] | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _check_scope_strategy(self) -> MirrorConfig:
        # Composing scope with tracked (pruning the state diff to observed
        # parents) is a #687 follow-up — reject rather than half-apply.
        if self.scope is not None and self.strategy == "tracked":
            raise ValueError(
                "mirror.scope with strategy: tracked is not supported yet — "
                "use scope (stateless) or tracked (stateful), not both."
            )
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
    # Declarative column rename (#415): {source_column: destination_field}.
    # Applied in the engine after extraction + cursor tracking + lookups,
    # immediately before the record reaches the destination — so
    # cursor_field and lookups still reference source-side column names,
    # while upsert_key / destination columns reference the mapped names.
    field_mappings: dict[str, str] | None = None
    # PII masking (#427, #660): {field_name: spec}, where spec is a flat strategy
    # name ("hash" | "redact") or the object form {strategy, length} for
    # param-bearing strategies (truncate). Applied in the engine at the same seam
    # as field_mappings (just before the destination), so keys reference the
    # post-rename field name. "hash" = SHA-256 hex digest; "redact" = "[REDACTED]";
    # "truncate" = the first `length` characters. Null passes through; non-strings
    # are stringified first.
    mask: dict[str, MaskSpec] | None = None
    # Dead Letter Queue (#278): opt-in persistence of failed records for
    # `drt retry`. None means disabled (same as DLQConfig(enabled=False)).
    dlq: DLQConfig | None = None
    # Mirror-mode delete behaviour (#686). None = destination strategy (#340).
    mirror: MirrorConfig | None = None

    # The owning sync's name, injected by SyncConfig after validation (not a
    # YAML field). Tracked mirror (#686) uses it to scope the per-sync key
    # state in the destination-side ``_drt_synced_keys`` table.
    _sync_name: str | None = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _check_incremental_cursor(self) -> SyncOptions:
        if self.mode == "incremental" and not self.cursor_field:
            raise ValueError("cursor_field is required when mode is 'incremental'.")
        return self

    @model_validator(mode="after")
    def _check_watermark_lag_mode(self) -> SyncOptions:
        if (
            self.watermark is not None
            and self.watermark.lag is not None
            and self.mode != "incremental"
        ):
            raise ValueError("watermark.lag requires mode='incremental'.")
        return self

    @model_validator(mode="after")
    def _check_mirror_config(self) -> SyncOptions:
        if self.mirror is not None and self.mode != "mirror":
            raise ValueError("sync.mirror requires mode='mirror'.")
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
    max_age: str


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


# Discriminated union — add new destination types here.
# PARITY: the members below are hand-maintained and must match the connector
# registry. tests/unit/test_cli_list_connectors.py::test_DESTINATIONS_matches_registry
# guards that DESTINATIONS (and thus this surface) stays in sync with
# drt/connectors/registry.py — update both when adding a destination.
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
    | GCSDestinationConfig
    | AzureBlobDestinationConfig
    | EmailSmtpDestinationConfig
    | NotionDestinationConfig
    | IntercomDestinationConfig
    | StagedUploadDestinationConfig
    | SalesforceBulkDestinationConfig
    | TwilioDestinationConfig
    | SnowflakeDestinationConfig
    | DatabricksDestinationConfig
    | ElasticsearchDestinationConfig
    | BigQueryDestinationConfig
    | AirtableDestinationConfig
    | KlaviyoDestinationConfig,
    Field(discriminator="type"),
]


class SyncConfig(BaseModel):
    name: str
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    model: str
    destination: DestinationConfig
    sync: SyncOptions = Field(default_factory=SyncOptions)
    tests: list[SyncTest] = Field(default_factory=list)
    alerts: AlertsConfig | None = None

    @model_validator(mode="after")
    def _inject_sync_name(self) -> SyncConfig:
        # Destinations need the sync name to scope tracked-mirror state
        # (#686), but the Destination protocol only receives SyncOptions —
        # so carry it on a private attr rather than widening the protocol
        # or exposing a user-settable YAML field.
        self.sync._sync_name = self.name
        return self
