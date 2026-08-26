"""Sync options, tests, alerts, and the sync config root (#721 split from models.py).

Also home to the :data:`DestinationConfig` discriminated union, assembled from
the three ``destinations_*`` modules and consumed by :class:`SyncConfig`.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Annotated, Any, Literal, get_args

from pydantic import BaseModel, Discriminator, Field, PrivateAttr, Tag, model_validator

# RateLimitConfig is defined in base.py (beside RetryConfig) because the
# destination configs imported below now carry a rate_limit override; it is
# re-exported here so drt.config.models and existing imports are unchanged.
# GenericDestinationConfig joins them for the same reason — it subclasses
# DescribableConfig and carries retry/rate_limit, so it belongs beside them
# rather than here, and is re-exported through drt.config.models (#997).
from drt.config.base import GenericDestinationConfig, RateLimitConfig, RetryConfig
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
from drt.templates.renderer import validate_template_syntax


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
    - ``scope`` (#687) — restrict deletes to rows whose scope-column values
      appeared in this run's source. The fit for 1:N regeneration (parent +
      child link rows): stale children under observed parents are deleted,
      rows under unobserved parents are untouched. With ``strategy:
      destination`` this narrows the whole-table diff; with ``strategy:
      tracked`` (#694) it additionally prunes the state read/rewrite to
      the observed scope, so a run touching one parent's children never
      re-baselines (or loses) tracked state for every other parent.

    ``scope`` + ``strategy: tracked`` requires ``scope`` to be a subset of
    ``destination.upsert_key`` (checked in
    ``BaseSqlDestination._validate_mirror_scope``, which also needs
    ``upsert_key`` — not visible from this model alone). Scope values are
    then derived positionally from the already-tracked key tuple rather
    than persisted separately, so no state-table schema change (and no
    migration story for tables created before #694) is needed.
    """

    strategy: Literal["destination", "tracked"] = "destination"
    scope: list[str] | None = Field(default=None, min_length=1)


class MetadataColumnsConfig(BaseModel):
    """``sync.metadata_columns`` — opt-in engine-injected bookkeeping columns (#762).

    Each field is ``None`` (not added) or the destination column name to add
    it as. Unlike ``computed_fields``, values here are never user templates —
    they're engine-owned facts about the run itself (dlt's ``_dlt_load_id``
    is the ecosystem-familiar precedent):

    - ``synced_at`` — the run's UTC start timestamp (one value per
      ``run_sync()`` call, not per-record or per-batch, so every row a run
      writes shares it — matching a load-id's "one per load" semantics).
    - ``run_id`` — the CLI-invocation-level id, ``None`` for library callers
      that don't pass one to ``run_sync()`` (same nullability as
      ``SyncResult.run_id``).
    - ``sync_name`` — the sync's own name, off by default; useful once
      multiple syncs write into a shared table and rows need to name their
      owner.

    Requires the target column to already exist on the destination — this
    is dict enrichment, not DDL. A destination without that column fails the
    write normally (governed by ``on_error``, like any other column mismatch).
    """

    synced_at: str | None = None
    run_id: str | None = None
    sync_name: str | None = None

    @model_validator(mode="after")
    def _check_no_blank_or_duplicate_targets(self) -> MetadataColumnsConfig:
        configured = [
            (name, v)
            for name, v in (
                ("synced_at", self.synced_at),
                ("run_id", self.run_id),
                ("sync_name", self.sync_name),
            )
            if v is not None
        ]
        for name, v in configured:
            if not v.strip():
                raise ValueError(
                    f"metadata_columns.{name} must be a non-empty column name "
                    "(e.g. after ${VAR} substitution resolves empty), not "
                    f"{v!r}."
                )
        targets = [v for _, v in configured]
        if len(targets) != len(set(targets)):
            raise ValueError(
                "metadata_columns entries must map to distinct column names "
                f"(got {targets})."
            )
        return self


class SyncOptions(BaseModel):
    mode: Literal["full", "incremental", "upsert", "replace", "mirror"] = "full"
    replace_strategy: Literal["truncate", "swap"] = "truncate"
    # Upsert match policy (#757). Applies to the per-row upsert write path
    # (modes full / upsert / incremental):
    #   - "upsert" (default): insert new rows, update existing — today's behaviour.
    #   - "update_only": only touch rows that already exist in the destination;
    #     rows with no match are skipped (counted in SyncResult.skipped, not
    #     errors). The reverse-ETL enrichment case — push warehouse-computed
    #     fields into CRM records reps already created, never create junk rows.
    #   - "create_only": only insert rows that do not yet exist; existing rows
    #     are left untouched (seed an audience once, never overwrite hand edits).
    # Rejected for mode: replace / mirror (see _check_match_policy_mode) and
    # fails fast on destinations that don't implement it (see the engine's
    # MatchPolicyCapable guard). Prior art: Census / Hightouch sync behaviours.
    match_policy: Literal["upsert", "update_only", "create_only"] = "upsert"
    cursor_field: str | None = None  # required when mode=incremental
    watermark: WatermarkConfig | None = None
    batch_size: int = 100
    rate_limit: RateLimitConfig = Field(default_factory=RateLimitConfig)
    retry: RetryConfig = Field(default_factory=RetryConfig)
    on_error: Literal["skip", "fail"] = "fail"
    # Declarative derived columns (#763): {field_name: jinja_template}.
    # Runs in the engine *before* field_mappings and mask, so templates
    # reference source-side column names (and lookup-resolved values) while
    # field_mappings/mask still see the computed result. Every template is
    # evaluated against the record as it arrived, so one computed field can
    # never read another — order-independent, the same guarantee
    # field_mappings makes. Writing an existing column name is allowed and
    # replaces it (in-place normalisation, e.g. phone -> E.164).
    computed_fields: dict[str, str] | None = None
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
    # Opt-in engine metadata columns (#762): see MetadataColumnsConfig.
    # Applied in the engine *last* of the payload transforms — after
    # computed_fields, field_mappings, and mask — since the column names
    # here are already destination-facing (chosen directly in this config)
    # and the values are engine bookkeeping, not source data that field
    # renames or masking rules should ever touch.
    metadata_columns: MetadataColumnsConfig | None = None
    # Dead Letter Queue (#278): opt-in persistence of failed records for
    # `drt retry`. None means disabled (same as DLQConfig(enabled=False)).
    dlq: DLQConfig | None = None
    # Mirror-mode delete behaviour (#686). None = destination strategy (#340).
    mirror: MirrorConfig | None = None

    # The owning sync's name, injected by SyncConfig after validation (not a
    # YAML field). Tracked mirror (#686) uses it to scope the per-sync key
    # state in the destination-side ``_drt_synced_keys`` table.
    _sync_name: str | None = PrivateAttr(default=None)

    # Query-tagging payload (#768), injected by the engine at run time (not a
    # YAML field, and not set by ``_inject_sync_name`` below — unlike the sync
    # name, this needs a fresh run_id per execution, not just per parse).
    # ``None`` when query_tagging.enabled is false. Destinations render it
    # into a SQL comment / native tag; see ``drt.config.query_tags``.
    _query_tags: dict[str, str] | None = PrivateAttr(default=None)

    @model_validator(mode="after")
    def _check_incremental_cursor(self) -> SyncOptions:
        if self.mode == "incremental" and not self.cursor_field:
            raise ValueError("cursor_field is required when mode is 'incremental'.")
        return self

    @model_validator(mode="after")
    def _check_computed_field_templates(self) -> SyncOptions:
        """Reject unparseable templates at config time rather than mid-run (#763).

        Only syntax is decidable here — whether ``row.foo`` exists depends on
        the query, so a missing column stays a run-time error under
        ``on_error``. An empty field name is rejected too: it would produce a
        record key of ``""`` that no destination can address.
        """
        for name, template in (self.computed_fields or {}).items():
            if not name.strip():
                raise ValueError("computed_fields keys must be non-empty field names.")
            try:
                validate_template_syntax(template)
            except ValueError as e:
                raise ValueError(f"computed_fields['{name}']: {e}") from e
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

    @model_validator(mode="after")
    def _check_match_policy_mode(self) -> SyncOptions:
        # match_policy governs the per-row upsert write path, which only runs
        # for the upsert-family modes. replace TRUNCATEs first (update_only /
        # create_only would be meaningless against an empty table) and mirror
        # layers a delete pass on top of the upsert (combining it with
        # create/update-only is a separate design) — reject both rather than
        # silently ignore the policy.
        if self.match_policy != "upsert" and self.mode in ("replace", "mirror"):
            raise ValueError(
                f"sync.match_policy: {self.match_policy} is not compatible with "
                f"mode: {self.mode} — match_policy applies to the upsert write "
                "path (mode: full / upsert / incremental)."
            )
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
    # Optional identifier (#779) — recommended for `query` tests (which have no
    # other natural label); also used to name the --store-failures sample file.
    # Falls back to a slugified test_display_name() when omitted.
    name: str | None = None
    row_count: RowCountTest | None = None
    not_null: NotNullTest | None = None
    freshness: FreshnessTest | None = None
    unique: UniqueTest | None = None
    accepted_values: AcceptedValuesTest | None = None
    # Custom SQL test (#779): arbitrary SQL that returns the FAILING rows —
    # 0 rows = pass. `{{ table }}` renders to the qualified destination table.
    # Same trust model as `model:` SQL: this is project config the operator
    # writes and reviews, not runtime user input.
    query: str | None = Field(default=None, min_length=1)
    # warn: runs, failures are reported + counted, exit code unaffected.
    # error (default): a failure exits non-zero, same as today.
    severity: Literal["warn", "error"] = "error"

    @model_validator(mode="after")
    def _check_exactly_one_test(self) -> SyncTest:
        configured_tests = [
            self.row_count,
            self.not_null,
            self.freshness,
            self.unique,
            self.accepted_values,
            self.query,
        ]
        configured_count = sum(test is not None for test in configured_tests)
        if configured_count != 1:
            raise ValueError("Exactly one sync test must be configured in each tests entry.")
        return self


class UnitTest(BaseModel):
    """Offline transform-pipeline test — fixture rows in, expected rows out (#780).

    Unlike ``SyncTest`` (which queries the *destination* after a real sync),
    a ``UnitTest`` never touches a destination or the network: ``given`` rows
    are run through the same ``computed_fields`` -> ``field_mappings`` ->
    ``mask`` -> ``metadata_columns`` chain ``run_sync()`` applies in
    production (via ``drt.engine.unit_test_runner``), and the result is
    compared against ``expect``. dbt unit-tests analog; Census/Hightouch
    mapper previews.
    """

    name: str
    # At least one row — an empty `given` would make every unit test
    # vacuously pass, silently, the moment a fixture typo drops its only row.
    given: list[dict[str, Any]] = Field(min_length=1)
    # Row count must match `given` exactly (a transform that's supposed to
    # drop/split rows is exactly the kind of change a unit test exists to
    # catch) — but each expected row is matched by the *keys it declares*,
    # not the full record. A sync's source columns grow over time; requiring
    # every unit test to enumerate every column it doesn't care about would
    # make each one a maintenance burden against unrelated schema growth.
    expect: list[dict[str, Any]] = Field(min_length=1)


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


class ConditionThreshold(BaseModel):
    """One comparison for a degraded-sync condition (#784).

    Exactly one operator is set — ``{gt: 1}``, ``{eq: 0}``, etc. The metric it
    applies to is the key it sits under in ``on_degraded.conditions``.
    """

    gt: float | None = None
    lt: float | None = None
    gte: float | None = None
    lte: float | None = None
    eq: float | None = None

    @model_validator(mode="after")
    def _exactly_one_operator(self) -> ConditionThreshold:
        set_ops = [op for op in (self.gt, self.lt, self.gte, self.lte, self.eq) if op is not None]
        if len(set_ops) != 1:
            raise ValueError(
                "each alert condition must set exactly one of gt/lt/gte/lte/eq "
                f"(got {len(set_ops)})."
            )
        return self

    @property
    def operator(self) -> str:
        for name in ("gt", "lt", "gte", "lte", "eq"):
            if getattr(self, name) is not None:
                return name
        raise AssertionError("unreachable: validated to have exactly one operator")

    @property
    def value(self) -> float:
        return float(getattr(self, self.operator))

    def compares(self, actual: float) -> bool:
        """True when *actual* satisfies this threshold (i.e. the condition trips)."""
        if self.gt is not None:
            return actual > self.gt
        if self.lt is not None:
            return actual < self.lt
        if self.gte is not None:
            return actual >= self.gte
        if self.lte is not None:
            return actual <= self.lte
        return actual == self.eq


class DegradedConditions(BaseModel):
    """Post-sync degradation thresholds (#784) — a mapping metric -> threshold.

    A mapping (not a list) forbids duplicate metrics for free. Every field is
    optional; an unset metric is simply not evaluated. Evaluated at the CLI seam
    from data already in ``SyncResult`` + the DLQ store — no new collection.
    """

    # failed / rows_extracted, as a percentage (0 when rows_extracted == 0, so an
    # empty source is the rows_extracted condition's job, never a false 100%).
    row_errors_pct: ConditionThreshold | None = None
    # whole-sync wall time; skipped when SyncResult.duration_seconds is unset.
    duration_seconds: ConditionThreshold | None = None
    # extracted source row count — ``{eq: 0}`` is the empty-source guard.
    rows_extracted: ConditionThreshold | None = None
    # cumulative DLQ backlog for this sync (accumulates across runs until retry).
    dlq_depth: ConditionThreshold | None = None


class OnDegradedConfig(BaseModel):
    """``alerts.on_degraded`` (#784) — thresholds + the channels they notify.

    Separate from ``on_failure`` (hard failure): degradation is partial — a
    creeping error rate, a duration SLA breach, an empty source, an accumulating
    DLQ. ``channels`` defaults to empty, so conditions can be JSON-only (surfaced
    in ``--output json`` for CI) without wiring a Slack/webhook target.
    """

    channels: list[AlertItem] = Field(default_factory=list)
    conditions: DegradedConditions = Field(default_factory=DegradedConditions)


class AlertsConfig(BaseModel):
    on_failure: list[AlertItem] = Field(default_factory=list)
    # Partial-degradation thresholds (#784) — see OnDegradedConfig. on_failure
    # (hard failure) is untouched.
    on_degraded: OnDegradedConfig | None = None


# The tag the discriminator returns for a third-party type (#997). Leading and
# trailing underscores keep it out of the built-in namespace — a connector may
# not register a `type` shaped like this — while still reading as "not one of
# yours" if it surfaces in pydantic's "expected one of [...]" list.
GENERIC_DESTINATION_TAG = "__plugin__"


def _destination_tag(value: Any) -> str | None:
    """Pick the union member that should parse ``value`` (#997).

    A *callable* discriminator rather than ``Field(discriminator="type")``
    because the built-in set is closed and the plugin set is not: pydantic
    matches a string discriminator against a fixed member list and rejects
    anything else outright, which is precisely the wall ADR 0009 documented.

    Three-way, and the middle case is the whole point:

    * a built-in ``type`` returns itself, so the payload lands on that exact
      concrete model and gets its existing per-field validation;
    * a ``type`` the connector registry recognizes returns the generic tag, so a
      plugin's config parses instead of being rejected before
      ``get_destination()`` is ever reached;
    * anything else returns the raw string, which pydantic reports as
      ``union_tag_invalid`` — the same error type, at the same location, that a
      typo produces today. Routing *every* unknown type to the generic model
      would have been simpler and is what makes a typo'd built-in silently
      parse and fail later; the registry check is what keeps `drt validate`
      honest about ``type: postgress``.

    Returning ``None`` for a missing/non-string ``type`` reproduces today's
    ``union_tag_not_found``.
    """
    # `Mapping`, not `dict`: pydantic accepts any mapping as model input, and the
    # string discriminator this replaced extracted the tag itself — narrowing to
    # `dict` here silently regressed MappingProxyType/UserDict callers (#997).
    type_name = value.get("type") if isinstance(value, Mapping) else getattr(value, "type", None)
    if not isinstance(type_name, str):
        return None
    if type_name in _BUILTIN_DESTINATION_TAGS:
        return type_name
    if type_name == GENERIC_DESTINATION_TAG:
        # The sentinel is a real tag in the union, so returning it verbatim here
        # would let `type: __plugin__` match the catch-all and parse — the exact
        # deferred failure the registry check below exists to prevent. `None`
        # yields `union_tag_not_found`, which is what an unusable type deserves.
        return None
    # Imported here, not at module scope: drt.connectors.registry imports the
    # destination implementations, which import this module back.
    from drt.connectors.registry import is_registered_destination

    return GENERIC_DESTINATION_TAG if is_registered_destination(type_name) else type_name


# Discriminated union — add new destination types here.
# PARITY: the members below are hand-maintained and must match the connector
# registry. tests/unit/test_cli_list_connectors.py::test_DESTINATIONS_matches_registry
# guards that DESTINATIONS (and thus this surface) stays in sync with
# drt/connectors/registry.py — update both when adding a destination.
#
# Every member carries an explicit `Tag` (#997): pydantic requires one per
# choice under a callable discriminator and raises `PydanticUserError` at import
# without it, so this cannot silently rot. The tags are written out rather than
# derived from each model's `Literal` so mypy still sees a union of concrete
# classes and keeps narrowing `sync.destination` at the call sites in
# drt/destinations/query.py and throughout drt/destinations/.
DestinationConfig = Annotated[
    Annotated[RestApiDestinationConfig, Tag("rest_api")]
    | Annotated[SlackDestinationConfig, Tag("slack")]
    | Annotated[DiscordDestinationConfig, Tag("discord")]
    | Annotated[GitHubActionsDestinationConfig, Tag("github_actions")]
    | Annotated[HubSpotDestinationConfig, Tag("hubspot")]
    | Annotated[ZendeskDestinationConfig, Tag("zendesk")]
    | Annotated[AmplitudeDestinationConfig, Tag("amplitude")]
    | Annotated[MixpanelDestinationConfig, Tag("mixpanel")]
    | Annotated[SendGridDestinationConfig, Tag("sendgrid")]
    | Annotated[LinearDestinationConfig, Tag("linear")]
    | Annotated[GoogleSheetsDestinationConfig, Tag("google_sheets")]
    | Annotated[PostgresDestinationConfig, Tag("postgres")]
    | Annotated[MySQLDestinationConfig, Tag("mysql")]
    | Annotated[TeamsDestinationConfig, Tag("teams")]
    | Annotated[JiraDestinationConfig, Tag("jira")]
    | Annotated[ClickHouseDestinationConfig, Tag("clickhouse")]
    | Annotated[ParquetDestinationConfig, Tag("parquet")]
    | Annotated[GoogleAdsDestinationConfig, Tag("google_ads")]
    | Annotated[FileDestinationConfig, Tag("file")]
    | Annotated[S3DestinationConfig, Tag("s3")]
    | Annotated[GCSDestinationConfig, Tag("gcs")]
    | Annotated[AzureBlobDestinationConfig, Tag("azure_blob")]
    | Annotated[EmailSmtpDestinationConfig, Tag("email_smtp")]
    | Annotated[NotionDestinationConfig, Tag("notion")]
    | Annotated[IntercomDestinationConfig, Tag("intercom")]
    | Annotated[StagedUploadDestinationConfig, Tag("staged_upload")]
    | Annotated[SalesforceBulkDestinationConfig, Tag("salesforce_bulk")]
    | Annotated[TwilioDestinationConfig, Tag("twilio")]
    | Annotated[SnowflakeDestinationConfig, Tag("snowflake")]
    | Annotated[DatabricksDestinationConfig, Tag("databricks")]
    | Annotated[ElasticsearchDestinationConfig, Tag("elasticsearch")]
    | Annotated[BigQueryDestinationConfig, Tag("bigquery")]
    | Annotated[AirtableDestinationConfig, Tag("airtable")]
    | Annotated[KlaviyoDestinationConfig, Tag("klaviyo")]
    | Annotated[GenericDestinationConfig, Tag(GENERIC_DESTINATION_TAG)],
    Discriminator(_destination_tag),
]

# Derived from the union itself rather than hand-listed a second time: a member
# added above without a matching entry here would otherwise route to the generic
# model and lose its own validation, silently. Excludes the generic member,
# whose tag is not a connector type.
_BUILTIN_DESTINATION_TAGS: frozenset[str] = frozenset(
    tag
    for member in get_args(get_args(DestinationConfig)[0])
    for meta in getattr(member, "__metadata__", ())
    if isinstance(meta, Tag) and (tag := meta.tag) != GENERIC_DESTINATION_TAG
)


class SyncConfig(BaseModel):
    name: str
    description: str = ""
    tags: list[str] = Field(default_factory=list)
    model: str
    destination: DestinationConfig
    sync: SyncOptions = Field(default_factory=SyncOptions)
    tests: list[SyncTest] = Field(default_factory=list)
    # Offline transform-pipeline tests (#780) — distinct from `tests:`, which
    # queries the real destination after a sync has run.
    unit_tests: list[UnitTest] = Field(default_factory=list)
    alerts: AlertsConfig | None = None

    @model_validator(mode="after")
    def _inject_sync_name(self) -> SyncConfig:
        # Destinations need the sync name to scope tracked-mirror state
        # (#686), but the Destination protocol only receives SyncOptions —
        # so carry it on a private attr rather than widening the protocol
        # or exposing a user-settable YAML field.
        self.sync._sync_name = self.name
        return self
