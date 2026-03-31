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

    success: int = 0
    failed: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)
    row_errors: list[RowError] = field(default_factory=list)

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
