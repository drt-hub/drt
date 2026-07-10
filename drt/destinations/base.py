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

    @property
    def total(self) -> int:
        return self.success + self.failed + self.skipped


@runtime_checkable
class Destination(Protocol):
    """Load records into an external service."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Send a batch of records to the destination."""
        ...


@runtime_checkable
class ConnectionTestable(Protocol):
    """Optional destination capability for validating external connectivity."""

    def test_connection(self, config: DestinationConfig) -> None:
        """Raise an exception if the destination cannot be reached."""
        ...


@runtime_checkable
class StagedDestination(Protocol):
    """Destination that accumulates records, then uploads as a batch job.

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
        """Accumulate records for later upload."""
        ...

    def finalize(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Upload staged file, trigger job, poll for completion."""
        ...


@runtime_checkable
class OrphanCleanup(Protocol):
    """Optional protocol for destinations that support orphan swap cleanup.

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
        tables that are known safe (e.g. end with "__drt_swap").
        """
        ...
