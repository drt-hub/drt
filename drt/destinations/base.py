"""Destination Protocol — the interface all destinations must implement.

Designed with Rust-compatibility in mind: clear boundaries, no magic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from drt.config.models import DestinationConfig, SyncOptions

if TYPE_CHECKING:
    from drt.destinations.row_errors import RowError
    from drt.engine.diff import DiffResult


@dataclass
class SyncResult:
    """Result of a single sync batch."""

    rows_extracted: int = 0
    success: int = 0
    failed: int = 0
    skipped: int = 0
    # Subset of ``skipped``: rows a destination declined because ``match_policy``
    # (#757) had no create/update target — ``update_only`` with no matching row,
    # ``create_only`` on an existing row. Always ``<= skipped`` (these rows are
    # counted in ``skipped`` too, so ``total`` is unaffected); it just names
    # *why* they were skipped, distinct from lookup / mask / ``--limit`` skips.
    skipped_no_match: int = 0
    errors: list[str] = field(default_factory=list)
    row_errors: list[RowError] = field(default_factory=list)
    # Populated by run_sync(); covers full sync, not individual batches.
    duration_seconds: float | None = None
    # Watermark observability (#390 / #391)
    watermark_source: str | None = None  # "cli_override" | "storage" | "default_value"
    cursor_value_used: str | None = None
    # Overlap window (#759) — the watermark.lag that widened this run's read
    # window (e.g. "1 hour"), or None when no lag was applied.
    watermark_lag: str | None = None
    # Sampling (#774) — the --limit N that capped this run's extraction,
    # or None for a full run. Sampled runs never advance the watermark.
    limit_applied: int | None = None
    # Graceful shutdown (#279) — True if the sync stopped early due to a
    # cooperative cancellation signal (SIGTERM/SIGINT) between batches.
    interrupted: bool = False
    # Record-level diff (#413) — populated by run_sync when dry_run + diff
    # are both requested. Always None outside that path.
    diff: DiffResult | None = None
    # Correlation IDs (#762). sync_run_id is always set by run_sync() — every
    # execution gets one, including library callers that pass no run_id at
    # all. run_id is the invocation-level id the CLI generates once per
    # `drt run` process and threads through every sync in it; None for a
    # library caller that never supplied one. See drt._identifiers.
    run_id: str | None = None
    sync_run_id: str | None = None
    # True when this run extracted but never called destination.load() (#978).
    # Populated by run_sync() so a persisting SyncObserver can tell "rows
    # were seen" (rows_extracted/success still reflect the preview) apart
    # from "rows were sent" without a SyncObserver Protocol signature change
    # — every existing/custom SyncObserver implementation keeps working
    # unmodified; only observers that read this field need to care.
    # Appended after every pre-existing field (not inserted earlier) so a
    # positional SyncResult(...) construction reaching sync_run_id keeps
    # every existing positional slot's meaning unchanged (Codex review).
    dry_run: bool = False

    @property
    def total(self) -> int:
        return self.success + self.failed + self.skipped


@runtime_checkable
class Destination(Protocol):
    """Load records into an external service.

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).
    """

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Send a batch of records to the destination.

        Row-level failures are recorded in ``SyncResult.errors`` /
        ``row_errors`` — implementations MUST populate these rather than
        raise per-row, or the engine loses per-row error attribution
        entirely.

        Raises:
            Exception: an unrecoverable, batch-level failure (connection
                lost, auth rejected, API outage) — never caught by the
                engine; propagates and aborts the sync. Also raised, by
                convention (26 of the current SQL/API destinations do this;
                see ``bigquery.py``'s ``_insert`` for the reference shape),
                when ``sync_options.on_error == "fail"`` and at least one
                row failed: the batch's ``row_errors`` are still populated
                first, then the destination raises to abort the sync rather
                than silently continuing past a failure the operator asked
                to be fatal.
        """
        ...


@runtime_checkable
class ConnectionTestable(Protocol):
    """Optional destination capability for validating external connectivity.

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).
    """

    def test_connection(self, config: DestinationConfig) -> None:
        """Raise an exception if the destination cannot be reached."""
        ...


@runtime_checkable
class MatchPolicyCapable(Protocol):
    """Destination that honours ``sync.match_policy`` (#757).

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).

    ``match_policy: update_only | create_only`` narrows the per-row upsert to
    only-existing / only-new rows. Not every destination can express that
    (a bare append-only API, say), so support is an opt-in capability the
    engine checks structurally — ``isinstance(dest, MatchPolicyCapable)`` —
    before running a non-default policy. Destinations that don't implement it
    fail fast with a clear error instead of silently ignoring the policy
    (the same fail-fast philosophy as unsupported ``mirror`` configs).

    Implementations return the set of policies they honour so the engine can
    reject an unsupported *value* on an otherwise-capable destination.
    """

    def supported_match_policies(self) -> frozenset[str]:
        """Return the ``match_policy`` values this destination honours."""
        ...


@runtime_checkable
class ModeCapable(Protocol):
    """Destination that honours advanced ``sync.mode`` values (#1042).

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).

    ``sync.mode: replace | mirror`` requires destination-side machinery beyond
    the normal per-record load path. Not every destination provides that
    machinery (a SaaS API, say), so support is an opt-in capability the engine
    checks structurally — ``isinstance(dest, ModeCapable)`` — before running
    one of those modes. Destinations that don't implement it fail fast with a
    clear error instead of silently treating the requested mode as a normal
    load.

    Implementations return the subset of ``replace`` / ``mirror`` they honour
    so the engine can reject an unsupported *value* on an otherwise-capable
    destination. The always-safe ``full`` / ``incremental`` / ``upsert`` modes
    do not need to be declared.
    """

    def supported_modes(self) -> frozenset[str]:
        """Return the advanced ``sync.mode`` values this destination honours."""
        ...


@runtime_checkable
class StagedDestination(Protocol):
    """Destination that accumulates records, then uploads as a batch job.

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).

    Used for APIs that require file upload → job trigger → poll for completion
    (e.g. Salesforce Bulk API, Amazon Marketing Cloud).

    Engine calls stage() per batch, then finalize() once after all batches.
    """

    def stage(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> None:
        """Accumulate records for later upload.

        Raises:
            Exception: staging itself failed (e.g. local buffer/disk error).
                Row-level outcomes aren't known yet at this point — they
                surface later from ``finalize()``.
        """
        ...

    def finalize(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Upload staged file, trigger job, poll for completion.

        Job-level failure handling is not uniform across shipped
        implementations, and this Protocol does not mandate one — check
        the concrete destination's own docs/tests before assuming either
        shape:

        - The generic ``type: staged_upload`` destination
          (``staged_upload.py``) catches any exception raised during
          upload/trigger/poll and returns it as a failed ``SyncResult``
          (``failed=<count>``, ``errors=[...]``) rather than raising.
        - ``SalesforceBulkDestination`` (``salesforce_bulk.py``) raises
          ``RuntimeError`` directly on auth failure, job-creation failure,
          upload failure, or job-close failure — none of those become a
          ``SyncResult``.

        Raises:
            Exception: see above — some implementations raise on
                job-level failure, others convert it to a failed
                ``SyncResult`` instead.
        """
        ...


@runtime_checkable
class OrphanCleanup(Protocol):
    """Optional protocol for destinations that support orphan swap cleanup.

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).

    Kept separate from `Destination` so existing destination connectors
    remain valid without implementing cleanup methods.
    """

    def list_orphan_swap_tables(
        self,
        config: DestinationConfig,
        base_table: str,
        older_than: timedelta | None = None,
    ) -> list[str]:
        """List orphan shadow tables created by swap replace strategy.

        Returns fully qualified table names (schema.table) for any tables
        that appear to be shadow swap tables (ending with "__drt_swap") for
        the given *base_table*.

        Args:
            config: Destination configuration used to connect to the database.
            base_table: The current sync's base table name.
            older_than: Optional age filter in hours, if supported.

        Returns:
            List of fully qualified table names (schema.table) that are orphans.

        Raises:
            Exception: If the destination cannot query its catalog.

        Implementations MAY ignore *older_than* if the underlying DB
        cannot filter by age; callers should treat this as best-effort.
        """
        ...

    def drop_orphan_swap_tables(
        self, config: DestinationConfig, tables: list[str]
    ) -> tuple[list[str], list[str]]:
        """Drop the provided orphan swap tables.

        Returns a tuple of `(dropped, failed)` where each is a list of
        schema-qualified table names. Implementations MUST only drop
        tables that are known safe (e.g. end with "__drt_swap"). A single
        table's drop failure goes into `failed`, not raised — like
        `list_orphan_swap_tables`, only a catalog/connection-level failure
        that prevents attempting any drop at all should raise.

        Raises:
            Exception: If the destination cannot connect to attempt the
                drops at all.
        """
        ...


@runtime_checkable
class QueryableDestination(Protocol):
    """Optional destination capability backing ``drt test``'s per-sync
    validation queries (#469). A new, separate Protocol rather than an
    addition to ``Destination`` itself — per ADR 0007, adding a required
    method to an already-shipped Protocol breaks every existing
    implementer, so new capability goes here instead.

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).

    Replaces the old ``_QUERYABLE_TYPES`` config-class isinstance tuple that
    ``drt/destinations/query.py``'s ``is_queryable``/``get_table_name``/
    ``execute_test_query`` used to dispatch on: a new SQL destination gains
    ``drt test`` support by implementing these two methods, with no edit to
    ``query.py`` needed.

    **Scope note:** this covers ``drt test`` only. ``drt run --dry-run
    --diff``'s true-diff path (``drt/engine/diff.py``) and ``drt test
    --store-failures`` need more than this Protocol provides — they also
    call ``query.py``'s ``fetch_rows``/``fetch_rows_by_keys``/
    ``fetch_failing_rows``, which remain separately, config-class-dispatched
    (deliberately: their per-dialect coverage genuinely differs, e.g.
    ClickHouse has no keyed fetch). A destination implementing only this
    Protocol gets full ``drt test`` support but degrades gracefully — a
    sample-mode diff / a "could not store failure sample" warning, neither
    a crash — for those other two features, exactly as it did for any
    destination outside the original four before this Protocol existed.
    """

    def get_table_name(self, config: DestinationConfig) -> str:
        """Return the fully-qualified table name to run tests/diffs against.

        Pure formatting of ``config`` — no I/O, never raises.
        """
        ...

    def execute_test_query(self, config: DestinationConfig, query: str) -> int:
        """Run ``query`` against the destination and return a single int.

        Raises:
            Exception: connection or query failure.
        """
        ...
