"""Row-level error tracking for detailed sync reporting."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


@dataclass
class RowError:
    """Error detail for a single record that failed to sync."""

    batch_index: int
    record_preview: str  # First 200 chars — avoids PII logging of full record
    http_status: int | None
    error_message: str
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
