"""Destination Protocol — the interface all destinations must implement.

Designed with Rust-compatibility in mind: clear boundaries, no magic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from drt.config.models import DestinationConfig, SyncOptions

if TYPE_CHECKING:
    from drt.destinations.row_errors import RowError


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
