"""Idempotency ledger — dedup fire-and-forget deliveries across runs (#1099).

Fills the gap #897 explicitly names as out of scope for itself: for a
destination with no native dedup mechanism (most webhook-style / fire-and-
forget destinations — Slack, generic REST API, Twilio), drt's own retry
assumes a failed send had no side effect and is safe to resend. That's false
whenever the destination processed the request but the *response* was lost
(timeout, dropped connection, a load balancer's retryable 5xx after the
origin already committed) — retrying in that case duplicates a side effect
that already happened once.

Only a warehouse backend implements this Protocol (see
``drt/state/warehouse.py``): a per-record ledger needs an atomic check-and-
mark at row-level scale (potentially millions of entries), which neither
local JSON state nor the object-storage backends (#756) provide — see the
issue's own "Why this needs the warehouse" section for the full comparison.
``StateBundle.ledger`` (``drt/state/factory.py``) is ``None`` for every
other backend, and for `warehouse` unless `state.idempotency: true` is also
set — the ledger is opt-in on top of the backend, not automatic.

Write-path contract: **check-then-send-then-mark, never claim-then-send.**
The engine (``drt/engine/sync.py``) calls ``already_delivered`` to filter a
batch *before* calling ``destination.load()``, and only calls
``mark_delivered`` for keys that were part of a *successful* load. Marking
before the send would mean a destination failure (timeout, 5xx) leaves the
key permanently marked "delivered" — so `drt retry`, a rerun, or
`on_error: skip` continuing past the failure would all silently skip that
record forever. That's the exact failure class retries exist to recover
from, so the ordering here is load-bearing, not incidental — see the
corrected design posted on #1099 before implementation.
"""

from __future__ import annotations

from collections.abc import Collection
from typing import Protocol, runtime_checkable


@runtime_checkable
class IdempotencyLedger(Protocol):
    """Optional warehouse capability: dedup records by a caller-computed key.

    Stability: New in #1099 — not yet frozen (see ADR 0007).

    All three methods key on ``sync_name`` — two projects sharing one
    connection profile and managed schema (and a matching sync name) would
    collide, the same documented, non-novel limitation
    ``PostgresWarehouseStateStore`` already carries (distinct
    ``managed_schema`` per project is the operator-level fix).
    """

    def already_delivered(self, sync_name: str, keys: Collection[str]) -> set[str]:
        """Return the subset of ``keys`` already marked delivered for this sync.

        Pure read, no side effect. Called once per batch, before
        ``destination.load()`` — the engine filters matching records out and
        counts them as ``SyncResult.skipped_duplicate``. An empty ``keys``
        collection returns an empty set without a query round trip.
        """
        ...

    def mark_delivered(self, sync_name: str, keys: Collection[str], delivered_at: str) -> None:
        """Record ``keys`` as delivered for this sync.

        Called only for keys whose batch already completed a *successful*
        ``destination.load()`` — never before the send (see the module
        docstring's write-path contract). Best-effort: a ledger write
        failure here must not fail an otherwise-successful sync, so
        implementations log and swallow rather than raise. A duplicate mark
        (the key is already present) is a no-op, not an error.
        """
        ...

    def prune(self, sync_name: str, retention_days: int) -> int:
        """Delete this sync's ledger rows older than ``retention_days``.

        Returns the number of rows removed. Mirrors
        ``HistoryStore.prune`` — the ledger needs a bound the same way
        history does (unlike history, an unbounded ledger isn't just noisy,
        it's a per-row-forever table), so the engine calls this alongside
        ``history_manager.prune`` at the end of every non-dry-run sync.
        """
        ...
