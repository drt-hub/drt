"""Row-level error tracking for detailed sync reporting."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from drt.destinations.base import SyncResult


@dataclass
class RowError:
    """Error detail for a single record that failed to sync."""

    batch_index: int
    record_preview: str  # First 200 chars — avoids PII logging of full record
    http_status: int | None
    error_message: str
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())


def record_row_error(
    result: SyncResult,
    batch_index: int,
    record_preview: str,
    exc: Exception,
    *,
    http_status: int | None = None,
    error_message: str | None = None,
) -> None:
    """Append the standard per-row RowError and increment result.failed.

    Callers compute record_preview themselves (the basis varies -- a JSON
    dict dump for most destinations, str() of an already-transformed bind
    list for a few warehouse ones -- this function does not guess it) and
    keep their own on_error control flow (break/continue/return/raise) --
    this function only does the "record the failure" half.
    """
    result.failed += 1
    result.row_errors.append(
        RowError(
            batch_index=batch_index,
            record_preview=record_preview,
            http_status=http_status,
            error_message=error_message if error_message is not None else str(exc),
        )
    )


def record_preview(record: dict[str, Any]) -> str:
    """Best-effort 200-char JSON preview of a record for ``RowError``.

    Tolerates non-serializable values (``default=str``) and caps length so the
    full record — which may hold PII — is never logged. Shared by the
    destinations that previously each defined an identical ``_record_preview``.
    """
    return json.dumps(record, default=str)[:200]
