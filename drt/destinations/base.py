"""Destination Protocol — the interface all destinations must implement.

Designed with Rust-compatibility in mind: clear boundaries, no magic.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from drt.config.models import DestinationConfig, SyncOptions
from drt.destinations.row_errors import RowError


@dataclass
class SyncResult:
    """Result of a single sync batch, with optional row-level error details."""

    success: int = 0
    failed: int = 0
    skipped: int = 0
    row_errors: list[RowError] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.success + self.failed + self.skipped

    @property
    def errors(self) -> list[str]:
        """Flat list of error messages (backward-compatible)."""
        return [e.error_message for e in self.row_errors]


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
