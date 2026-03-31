"""Row-level error tracking for detailed sync reporting.

DetailedSyncResult is backward-compatible with SyncResult:
it has all the same fields plus ``row_errors``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from drt.destinations.base import SyncResult


@dataclass
class RowError:
    """Error detail for a single record that failed to sync."""

    batch_index: int
    record_preview: str   # First 200 chars — avoids PII logging of full record
    http_status: int | None
    error_message: str
    timestamp: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )


class DetailedSyncResult:
    """Factory for SyncResult. Kept for backward compatibility.

    All destinations now return SyncResult directly.
    This wrapper avoids breaking existing imports.
    """

    def __new__(cls, **kwargs: int) -> SyncResult:  # type: ignore[misc]
        from drt.destinations.base import SyncResult

        return SyncResult(**kwargs)  # type: ignore[return-value]
