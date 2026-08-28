"""Audit-log Protocol and registry for the two non-sync-lifecycle compliance
events (#299): `config_changed` and `secret_accessed`.

Design-only per #299: this module defines the seam and a no-op OSS default.
The other three events #299 names — `sync_started`, `sync_completed`,
`sync_failed` — are not duplicated here; they already exist as
`SyncObserver` callbacks (`drt/engine/observer.py`) and an Enterprise
audit implementation registers via `register_extra_observer` instead. See
ADR 0008 for why the five events split this way.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol, runtime_checkable

AuditEventType = Literal["config_changed", "secret_accessed"]


@dataclass(frozen=True)
class AuditEvent:
    """One audit-loggable event.

    `details` is event-specific and intentionally untyped at this design
    stage (ADR 0008 follow-up #2) — `secret_accessed`'s implementation MUST
    NOT put a resolved secret value in here, only the scheme/path that
    identifies which secret was read.
    """

    event_type: AuditEventType
    timestamp: str
    details: dict[str, Any] = field(default_factory=dict)


@runtime_checkable
class AuditLogger(Protocol):
    """Enterprise audit-log extension point for `config_changed` /
    `secret_accessed` (#299, ADR 0008).

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).

    A new, narrow Protocol — not a five-event duplicate of `SyncObserver`.
    Sync-lifecycle audit events (`sync_started`/`sync_completed`/
    `sync_failed`) go through `SyncObserver` + `register_extra_observer`
    instead; see `drt/engine/observer.py`.
    """

    def log_event(self, event: AuditEvent) -> None:
        """Record `event`. Best-effort — like `HistoryStore.append`, a
        logging failure must never fail the operation being audited, so
        implementations should log-and-swallow their own errors rather
        than raise.
        """
        ...


class NoOpAuditLogger:
    """OSS default (#299): every event is discarded."""

    def log_event(self, event: AuditEvent) -> None:
        return None


_lock = threading.Lock()
_logger: AuditLogger = NoOpAuditLogger()


def register_audit_logger(logger: AuditLogger) -> None:
    """Install `logger` as the active `AuditLogger` for this process.

    One active logger per process, same replace-not-error shape as
    `drt.security.register_permission_checker`.
    """
    global _logger
    with _lock:
        _logger = logger


def get_audit_logger() -> AuditLogger:
    """Return the currently active `AuditLogger` (the OSS default,
    `NoOpAuditLogger`, unless an Enterprise package registered its own via
    :func:`register_audit_logger`)."""
    with _lock:
        return _logger


def _reset_audit_logger() -> None:
    """Restore the OSS default. Test hook only — not called by production
    code, which registers at most once per process."""
    register_audit_logger(NoOpAuditLogger())
