"""Shared config primitives for drt (auth, pagination, retry, lookups, project).

Phase 1 of #721 lifted these out of the former monolithic ``models.py`` so the
destination and sync-option modules can share them without a circular import
(destinations need :class:`RetryConfig`; ``sync_options`` needs the destination
union). ``models.py`` re-exports everything here — import sites are unchanged.
"""

from __future__ import annotations

from typing import Annotated, Any, ClassVar, Literal

from pydantic import BaseModel, ConfigDict, Field, GetJsonSchemaHandler, model_validator
from pydantic.json_schema import JsonSchemaValue

LITERAL_CREDENTIAL_KEY = "literal"
"""Rate-limit identity for a config that inlines its credential (#769).

Configs that name an env var (``token_env``) key on that name — it pins the
account without the secret ever reaching a dict key. When a config inlines the
literal instead, we deliberately do **not** derive a key from it, not even a
digest: a rate-limit key is not worth putting credential-derived material into
process memory for, and any fast digest of a credential-shaped value is
correctly flagged as weak password hashing (there is no "but it's only an
identifier" exception a scanner can see).

The cost is that two destinations of the same type both using inline literals
collapse into one bucket. That errs *toward* throttling — a shared bucket is
stricter than two, so the failure mode is a slower sync, never a 429. Naming
the env var is the documented way to get per-account buckets.
"""


class DescribableConfig(BaseModel):
    """Mixin base for destination configs whose ``describe()`` is the canonical
    ``f"{type} (detail)"`` shape (#721). Trivial subclasses supply
    ``_describe_detail``; connectors with a non-standard label (a hardcoded name
    or no parens) override ``describe()`` and inherit :class:`BaseModel` directly.
    """

    # Docs-safe labelling (#696): ``describe()`` output ships verbatim in the
    # generated docs site (manifest.json + every page), so details that carry
    # network locations or personal identifiers (URLs, hosts, phone numbers,
    # email addresses) must not flow through by default. Subclasses whose
    # detail is pure *object identity* (a table, channel, sheet, bucket…) opt
    # in with ``_detail_is_public = True``; everything else — including any
    # future connector added without thinking about this — renders type-only.
    _detail_is_public: ClassVar[bool] = False

    def describe(self) -> str:
        return f"{self.type} ({self._describe_detail()})"  # type: ignore[attr-defined]

    def describe_safe(self) -> str:
        """Label safe for a hosted docs site (#696) — never a network location
        or personal identifier. Connectors with a partially-safe detail
        (e.g. twilio keeps the country code) override this instead."""
        if self._detail_is_public:
            return self.describe()
        return str(self.type)  # type: ignore[attr-defined]

    def _describe_detail(self) -> str:  # pragma: no cover - always overridden
        raise NotImplementedError

    def rate_limit_key(self) -> str:
        """Identity of the endpoint whose vendor quota this config consumes (#769).

        The rate-limiter registry buckets by this key, so two configs share one
        limiter exactly when they share one quota. Deliberately *not*
        ``describe()``: that label is tuned for humans reading a docs site and
        gets this wrong in both directions — ``SlackDestinationConfig`` describes
        every webhook as the literal ``"webhook"`` (unrelated workspaces would
        throttle each other), while ``HubSpotDestinationConfig`` includes
        ``object_type`` (one portal's contacts and deals would get a bucket each
        despite sharing a quota). ``describe_safe()`` is lossy on purpose (#696),
        which is the opposite of what a key needs.

        The default is the connector type: correct whenever a quota is
        effectively global to the connector, and safe as a fallback because it
        over-shares (one bucket) rather than under-shares — the failure mode is
        pacing, not a 429. Connectors whose quota is per account, per host or per
        workspace override this and prefix the type so two connectors can never
        collide. Prefer env-var *names* over resolved secrets; a config that
        inlines its credential keys on :data:`LITERAL_CREDENTIAL_KEY` rather
        than deriving anything from the secret.

        Never log or serialize the return value.
        """
        return str(self.type)  # type: ignore[attr-defined]


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
    """Sync execution history retention (#276).

    Local history keeps every entry inside ``retention_days``. Remote object
    stores additionally retain only the newest ``max_entries`` per sync, which
    bounds the object downloaded and rewritten by each conditional update.
    """

    enabled: bool = True
    retention_days: int = 30
    max_entries: int = Field(default=500, ge=1)


class StateConfig(BaseModel):
    """State-backend selection and backend-specific settings (#756).

    This mirrors :class:`~drt.config.sync_options.WatermarkConfig`'s shape:
    one discriminating backend field plus optional fields validated against
    that choice. GCS and S3 use ``bucket`` plus an optional object-key
    ``prefix``. S3's authentication and endpoint fields deliberately match
    :class:`~drt.config.destinations_storage.S3DestinationConfig`, so state
    storage follows the same boto3 credential chain and override vocabulary.
    Local state continues to reject every remote-only field.
    """

    backend: Literal["local", "gcs", "s3"] = "local"
    bucket: str | None = None
    prefix: str | None = None
    region: str | None = None
    aws_profile: str | None = None
    aws_access_key_id_env: str | None = None
    aws_secret_access_key_env: str | None = None
    aws_session_token_env: str | None = None
    endpoint_url: str | None = None

    @model_validator(mode="after")
    def _check_backend_fields(self) -> StateConfig:
        s3_only_fields = (
            "region",
            "aws_profile",
            "aws_access_key_id_env",
            "aws_secret_access_key_env",
            "aws_session_token_env",
            "endpoint_url",
        )
        configured_s3_fields = [
            field for field in s3_only_fields if getattr(self, field) is not None
        ]
        if self.backend == "local" and (
            self.bucket is not None
            or self.prefix is not None
            or configured_s3_fields
        ):
            raise ValueError(
                "Remote state fields are not valid when backend is 'local'."
            )
        if self.backend == "gcs" and not self.bucket:
            raise ValueError("state.bucket is required when backend is 'gcs'.")
        if self.backend == "gcs" and configured_s3_fields:
            names = ", ".join(f"state.{field}" for field in configured_s3_fields)
            raise ValueError(f"{names} are only valid when backend is 's3'.")
        if self.backend == "s3" and not self.bucket:
            raise ValueError("state.bucket is required when backend is 's3'.")
        return self


class QueryTaggingConfig(BaseModel):
    """Cost-attribution tagging on extract/destination queries (#768).

    On by default (dbt's ``query_comment`` precedent): a query with no tag at
    all is the thing that made per-sync warehouse cost attribution impossible
    in the first place (see #710/#738's smoke-account digest, which had to
    fall back to account-level spend). ``extra`` is merged into every tag
    payload alongside drt's own ``app``/``sync``/``run_id`` — e.g.
    ``{"team": "growth"}`` to carry a cost-center through to
    ``INFORMATION_SCHEMA.JOBS`` / ``QUERY_HISTORY`` queries.
    """

    enabled: bool = True
    extra: dict[str, str] = Field(default_factory=dict)


class ProjectConfig(BaseModel):
    name: str
    version: str = "0.1"
    profile: str = "default"
    source: SourceConfig | None = None  # optional; profile is authoritative
    history: HistoryConfig = Field(default_factory=HistoryConfig)
    state: StateConfig = Field(default_factory=StateConfig)
    # Project vars (#783): reviewed, in-repo defaults for anything
    # project-shaped, referenced as {{ var('name') }} in model SQL and YAML
    # string fields. Overridden by DRT_VAR_* env and `--vars` at run time —
    # see drt.config.vars for the precedence chain.
    vars: dict[str, Any] = Field(default_factory=dict)
    query_tagging: QueryTaggingConfig = Field(default_factory=QueryTaggingConfig)


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


class RateLimitConfig(BaseModel):
    """Request pacing for a destination or a whole sync (#769).

    Lives here beside :class:`RetryConfig` rather than in ``sync_options``
    because the destination configs now carry a ``rate_limit`` override and
    ``sync_options`` imports *them* — defining it there would close an import
    cycle. ``sync_options`` re-exports it, so ``drt.config.models`` and every
    existing import path are unchanged.
    """

    # float rather than int (#769): RateLimiter.requests_per_second was already
    # annotated float and sub-1/s rates (2.5 → one request per 0.4 s) were
    # already exercised, so this widening is a compatibility fix, not a feature.
    requests_per_second: float = 10
    # Opt-in burst capacity (#769). None keeps the historical minimum-interval
    # behaviour exactly; a value lets an idle period accumulate up to N
    # requests' worth of credit that can be spent back-to-back.
    burst: int | None = Field(default=None, ge=1)


class GenericDestinationConfig(DescribableConfig):
    """A destination whose ``type`` came from a third-party package (#997).

    ADR 0009 recorded why a plugin could register a connector and still never
    be nameable in a sync YAML: :data:`~drt.config.sync_options.DestinationConfig`
    is a closed union, so an unrecognized ``type`` failed validation *before*
    :func:`drt.connectors.registry.get_destination` was ever consulted. This is
    the catch-all member that ends that — reached only for a ``type`` the
    connector registry already knows, never for one of the built-ins.

    ``extra="allow"`` because the plugin's own fields are, by definition,
    unknown to drt-core: they are carried verbatim so the plugin's destination
    implementation can read them off the config it is handed. That is the whole
    trade ADR 0009 flagged for this option — a typo'd *plugin* field is kept
    rather than rejected, because nothing here knows which fields are real. The
    registry stores a ``config_class`` that could tighten this later; wiring
    that second pass up is deliberately **not** part of #997.

    Built-in types never reach this model, so their strict per-field validation
    and their error messages are untouched.
    """

    model_config = ConfigDict(extra="allow")

    # `str`, not a Literal — that is the entire point. Every other member of the
    # union pins `type` to one value; this one accepts whatever the registry
    # recognizes, which is what lets a plugin type survive parsing.
    type: str

    # Mirrored from the built-in configs so the shared machinery keeps working
    # for a plugin destination: `resolve_retry(config.retry, sync_options)` is
    # called by connectors generically, and the rate-limiter registry reads
    # `rate_limit`. Without these two a plugin would silently lose both.
    retry: RetryConfig | None = None
    rate_limit: RateLimitConfig | None = None

    def describe(self) -> str:
        # Type-only, overriding DescribableConfig's `f"{type} (detail)"`: there
        # is no `_describe_detail` to call, and inventing one out of arbitrary
        # extra fields is exactly the #696 leak (hosts, URLs, phone numbers)
        # that `describe_safe` exists to prevent. `describe()` output ships
        # verbatim into the generated docs site.
        return self.type

    def describe_safe(self) -> str:
        return self.type

    @classmethod
    def __get_pydantic_json_schema__(
        cls, core_schema: Any, handler: GetJsonSchemaHandler
    ) -> JsonSchemaValue:
        """Pin ``type`` to the plugin types actually registered (#997).

        Two problems, one fix. ``SyncConfig``'s generated schema renders the
        destination union as ``oneOf``, which requires a payload to match
        *exactly* one member — and this model accepts ``type: str`` plus
        arbitrary extras, so left open a plain ``type: slack`` matches both
        ``SlackDestinationConfig`` and this one, and every valid built-in sync
        fails ``drt validate``. Widening it the other way (any string that is
        not a built-in) fixes that but makes the schema accept ``type:
        invalid_type``, losing the typo detection the closed union gave us.

        Enumerating the registry avoids both: the enum holds exactly the types
        registered but not built in, so a built-in never matches this member and
        an unregistered typo matches nothing at all. With no plugins installed
        the enum is empty and matches nothing, which is the pre-#997 behaviour
        exactly. This mirrors ``_destination_tag``'s three-way decision, so the
        schema and the parser cannot disagree.

        The schema therefore reflects the plugins installed when it is
        generated. That is a property of ``drt schema`` output, not a bug: a
        static file cannot describe types that arrive by ``pip install``.
        Imported lazily — the tag set is derived from the union, which imports
        this module.
        """
        schema = handler(core_schema)
        from drt.config.sync_options import _BUILTIN_DESTINATION_TAGS
        from drt.connectors.registry import registered_destination_types

        plugin_types = sorted(set(registered_destination_types()) - _BUILTIN_DESTINATION_TAGS)
        schema.setdefault("properties", {})["type"] = {
            "type": "string",
            "enum": plugin_types,
            "description": (
                "Connector type registered by a third-party package. Built-in "
                "types are validated against their own schema instead."
            ),
        }
        return schema
