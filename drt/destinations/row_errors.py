"""Row-level error tracking for detailed sync reporting."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


@dataclass
class RowError:
    """Error detail for a single record that failed to sync."""

    batch_index: int
    record_preview: str  # First 200 chars — avoids PII logging of full record
    http_status: int | None
    error_message: str
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


def record_preview(record: dict[str, Any]) -> str:
    """Best-effort 200-char JSON preview of a record for ``RowError``.

    Tolerates non-serializable values (``default=str``) and caps length so the
    full record — which may hold PII — is never logged. Shared by the
    destinations that previously each defined an identical ``_record_preview``.
    """
    return json.dumps(record, default=str)[:200]
