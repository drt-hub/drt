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
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from drt.templates.renderer import render_value

if TYPE_CHECKING:
    from drt.config.models import SyncConfig
    from drt.destinations.base import SyncResult


def resolve_idempotency_key_template(sync: SyncConfig) -> str | None:
    """Effective Jinja template for this ledger's per-record dedup key, or
    ``None``.

    Explicit ``sync.idempotency_key`` always wins. Otherwise falls back to
    the destination's ``upsert_key`` when present (joined on ``":"`` for a
    composite key) — never to ``run_id``, see ``SyncOptions.idempotency_key``'s
    docstring for why that default would silently defeat cross-run dedup.
    ``None`` means this sync has no way to compute a key, so the ledger (even
    if configured) has no effect for it — not an error, matching #897's own
    "no-op, not a failure" contract for an unresolvable idempotency setting.

    Lives here (not ``drt/engine/sync.py``) so both ``run_sync()`` and
    ``drt retry``'s ``replay_dead_letters()`` (#1118) share one
    implementation of the contract this module's own docstring documents,
    without a CLI command reaching into the engine's internals —
    ``engine/sync.py`` is the Rust-core migration candidate this repo keeps
    deliberately pure (see ``CLAUDE.md``).
    """
    if sync.sync.idempotency_key:
        return sync.sync.idempotency_key
    upsert_key = getattr(sync.destination, "upsert_key", None)
    if upsert_key:
        return ":".join(f"{{{{ row['{col}'] }}}}" for col in upsert_key)
    return None


def compute_idempotency_key(template: str, record: dict[str, Any]) -> str | None:
    """Render ``template`` against one record; ``None`` on template failure.

    Best-effort by design (see this module's docstring): the ledger is an
    opt-in protective layer, so a broken template disables dedup for that
    one row rather than failing the whole batch or sync.
    """
    try:
        return str(render_value(template, record))
    except Exception:
        return None


def successful_indices(record_batch: list[dict[str, Any]], result: SyncResult) -> set[int]:
    """Indices into ``record_batch`` this ``load()`` call actually reported
    success for, or an empty set when that can't be determined safely.

    Every ``RowError.batch_index`` is excluded (a positive, per-row
    failure signal). But some destinations skip a row *without* recording
    a ``RowError`` — ``match_policy: update_only``/``create_only``'s
    ``skipped_no_match`` (#757) is a bare counter with no per-row index at
    all. Treating "not in row_errors" as "therefore delivered" would
    wrongly mark a skipped-no-match row as successfully delivered (caught
    in Codex review on #1100, which shares this helper): a real, silent
    compliance/idempotency-ledger false positive, since a skip is neither
    a failure nor a delivery. So whenever this batch reports *any* skip
    (``result.skipped``, which ``skipped_no_match`` is a documented subset
    of), this returns an empty set rather than guess — fail closed, not
    open: missing one batch's dedup/audit protection is a far smaller cost
    than permanently marking a never-delivered record as delivered.

    Shared by ``run_sync()``'s per-batch ``mark_delivered``/audit-log write
    and ``drt retry``'s ``replay_dead_letters()`` (#1118, caught in Codex
    review on #1126 for the same reason #1100 caught it here first) — both
    need the same "which of these did the destination confirm" computation.
    """
    if result.skipped > 0:
        return set()
    failed_indices = {
        err.batch_index for err in result.row_errors if 0 <= err.batch_index < len(record_batch)
    }
    return {i for i in range(len(record_batch)) if i not in failed_indices}


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
