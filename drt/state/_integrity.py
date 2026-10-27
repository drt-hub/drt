"""A side channel for "this record could not be read" (#1224).

State and history stores are best-effort by contract: a corrupt or malformed
record is skipped with a warning so a sync is never blocked by bookkeeping. A
freshness watchdog must not read that as "this sync never ran". Stores therefore
also call :func:`report_unreadable`, and a caller that cares (``drt status
--check``) wraps its reads in :func:`collect_unreadable`.

It is deliberately *not* built on log records: logging configuration (a host
application's ``logging.disable``, a level override) must not be able to switch
the signal off, and a ``ContextVar`` keeps concurrent collectors (for example two
MCP requests) from seeing each other's reports.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

_collector: ContextVar[list[str] | None] = ContextVar("drt_unreadable_collector", default=None)


def report_unreadable(message: str) -> None:
    """Tell the active collector (if any) that a record could not be read."""
    active = _collector.get()
    if active is not None:
        active.append(message)


@contextmanager
def collect_unreadable() -> Iterator[list[str]]:
    """Collect :func:`report_unreadable` calls made in this context."""
    messages: list[str] = []
    token = _collector.set(messages)
    try:
        yield messages
    finally:
        _collector.reset(token)
