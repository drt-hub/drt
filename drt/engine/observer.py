"""SyncObserver — the engine's event surface.

The engine emits structured events through a ``SyncObserver``; concrete
observers decide what to do with them (write to logs, persist state,
publish OTel spans, format errors, etc.). Logging and state/watermark
persistence are fully routed through this protocol and enforced by a CI
boundary check (``tests/unit/test_engine_observer.py``). History
append/prune, alert dispatch, and final OTel span status are current,
accepted exceptions — this protocol's existing methods have nowhere to
carry the outcome/duration/exception data those three need, so they
stay as direct engine calls. See CLAUDE.md for the fuller note on why,
and why closing that gap wouldn't require breaking this Protocol.

Why a protocol, not concrete calls
----------------------------------

The engine is the load-bearing module for the future Rust migration
(see ROADMAP.md v1.x). Every direct ``logging.*`` or
``state_manager.save_sync(...)`` call inside the engine is a side
effect that must be reimplemented in Rust or wired through a Python
callback — both are friction. Funnelling all writes through one
protocol means the Rust port only has to call back into Python at the
observer boundary, not at every log line.

It also gives downstream consumers (OTel Phase 3 #527, ErrorFormatter
stage retrofit #544) a single seam to plug into.

Method shape
------------

All observer methods MUST be fire-and-forget — observers swallow their
own errors (typically via ``try/except`` + a logged warning) and never
raise back into the engine. A buggy observer must not crash a running
sync.
"""

from __future__ import annotations

import logging
import threading
from collections.abc import Iterable
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from drt.destinations.base import SyncResult
    from drt.state.dlq import DeadLetter, DlqBackend
    from drt.state.manager import StateStore
    from drt.state.watermark import WatermarkStorage


@runtime_checkable
class SyncObserver(Protocol):
    """The engine's event surface. All methods are fire-and-forget.

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).
    """

    def on_sync_started(self, sync_name: str, started_at: str) -> None:
        """Called once at the top of ``run_sync``."""
        ...

    def on_watermark_resolved(self, sync_name: str, source: str, cursor_value: str | None) -> None:
        """Called when cursor value is resolved for an incremental sync.

        ``source`` is one of ``"cli_override"``, ``"storage"``,
        ``"default_value"``.
        """
        ...

    def on_warning(self, sync_name: str, message: str) -> None:
        """Called for non-fatal warnings (lookup ambiguity, etc.)."""
        ...

    def on_records_failed(self, sync_name: str, dead_letters: list[DeadLetter]) -> None:
        """Called after a batch load when individual records failed.

        ``dead_letters`` pairs each failed record with its error detail —
        the engine has already correlated ``RowError.batch_index`` back to
        the full record it sent, so no record content is lost here. Fired
        once per batch that had pinpointed per-record failures (never on a
        clean batch). Observers that maintain a Dead Letter Queue buffer here
        and flush in ``on_sync_ended``; every other observer no-ops.
        """
        ...

    def on_interrupted(self, sync_name: str, batches_processed: int) -> None:
        """Called when ``stop_event`` triggers a graceful shutdown."""
        ...

    def on_sync_completed(
        self,
        sync_name: str,
        result: SyncResult,
        started_at: str,
        new_cursor_value: str | None,
        cursor_field: str | None,
    ) -> None:
        """Called once at the end of ``run_sync`` regardless of success.

        Carries everything an observer needs to persist state, emit a
        final span, or render a summary — without the engine reaching
        for storage itself.

        ``result.dry_run`` is ``True`` when nothing was actually written to
        the destination — ``new_cursor_value`` still reflects rows *seen*
        during extraction (dry-run previews still extract), not rows
        *sent*. Observers that persist durable state (cursors, run
        counts, watermarks) MUST no-op when ``result.dry_run`` is set;
        observers that only log or render a summary may use it as they
        see fit. Deliberately carried on ``result`` rather than as a
        separate parameter — every existing/custom ``SyncObserver``
        implementation keeps working unmodified (#978's original fix
        added a Protocol parameter; Codex review flagged that as a
        breaking change for any direct, non-``CompositeObserver`` caller,
        so this reads from ``result`` instead).
        """
        ...

    def on_sync_ended(self, sync_name: str) -> None:
        """Called from ``run_sync``'s outer ``finally``, on every exit path.

        Unlike ``on_sync_completed`` — which only fires on the normal
        return path inside the ``try`` block — this fires unconditionally,
        including when ``run_sync`` exits via an unhandled exception or a
        graceful-shutdown interruption. It exists for state that MUST be
        durable no matter how the sync ended: observers that buffer
        writes in memory (e.g. the Dead Letter Queue's per-batch buffer)
        do their final flush here rather than in ``on_sync_completed``,
        so a crash mid-sync cannot silently drop already-buffered entries.
        Every other observer no-ops.
        """
        ...


# ---------------------------------------------------------------------------
# Concrete observers — these reproduce the engine's prior direct behaviour
# ---------------------------------------------------------------------------


class NullObserver:
    """No-op observer. Useful as the default in tests and library callers."""

    def on_sync_started(self, sync_name: str, started_at: str) -> None: ...
    def on_watermark_resolved(
        self, sync_name: str, source: str, cursor_value: str | None
    ) -> None: ...
    def on_warning(self, sync_name: str, message: str) -> None: ...
    def on_records_failed(self, sync_name: str, dead_letters: list[DeadLetter]) -> None: ...
    def on_interrupted(self, sync_name: str, batches_processed: int) -> None: ...
    def on_sync_completed(
        self,
        sync_name: str,
        result: SyncResult,
        started_at: str,
        new_cursor_value: str | None,
        cursor_field: str | None,
    ) -> None: ...
    def on_sync_ended(self, sync_name: str) -> None: ...


class LoggingObserver:
    """Mirrors the engine's prior ``logger.info / warning`` calls.

    Logger name is ``"drt"`` to keep handler configuration backwards-
    compatible with the pre-refactor call sites.
    """

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self._logger = logger or logging.getLogger("drt")

    def on_sync_started(self, sync_name: str, started_at: str) -> None:
        # The pre-refactor engine did not log sync start; keep parity.
        pass

    def on_watermark_resolved(self, sync_name: str, source: str, cursor_value: str | None) -> None:
        # Storage-source resolutions used to not log (only CLI override /
        # default_value did). Preserve that asymmetry: it kept the log
        # signal:noise ratio reasonable for daily incremental runs.
        if source == "storage":
            return
        reason = " reason='no existing watermark'" if source == "default_value" else ""
        self._logger.info(
            "sync='%s' watermark_source=%s cursor_value='%s'%s",
            sync_name,
            source,
            cursor_value,
            reason,
        )

    def on_warning(self, sync_name: str, message: str) -> None:
        self._logger.warning("sync='%s' %s", sync_name, message)

    def on_records_failed(self, sync_name: str, dead_letters: list[DeadLetter]) -> None:
        # Intentionally silent: failed records carry full payloads (possible
        # PII). Row-level errors already surface via RowError/--verbose with
        # a truncated preview; the DLQ file is the durable record. Logging
        # full rows here would defeat that privacy boundary.
        pass

    def on_interrupted(self, sync_name: str, batches_processed: int) -> None:
        self._logger.info(
            "sync='%s' graceful shutdown requested — stopping after %d batches",
            sync_name,
            batches_processed,
        )

    def on_sync_completed(
        self,
        sync_name: str,
        result: SyncResult,
        started_at: str,
        new_cursor_value: str | None,
        cursor_field: str | None,
    ) -> None:
        # Pre-refactor engine did not log a "sync done" line at this level
        # (the CLI handled it). Keep parity to avoid double-logging.
        pass

    def on_sync_ended(self, sync_name: str) -> None:
        pass


class StatePersistingObserver:
    """Persists state on ``on_sync_completed``.

    Replaces the engine's prior direct calls to
    ``state_manager.save_sync(...)`` and ``watermark_storage.save(...)``.
    Errors are swallowed with a warning so a corrupt state file cannot
    fail an otherwise-successful sync.
    """

    def __init__(
        self,
        state_manager: StateStore | None,
        watermark_storage: WatermarkStorage | None,
    ) -> None:
        self._state_manager = state_manager
        self._watermark_storage = watermark_storage
        self._logger = logging.getLogger("drt")

    def on_sync_started(self, sync_name: str, started_at: str) -> None: ...
    def on_watermark_resolved(
        self, sync_name: str, source: str, cursor_value: str | None
    ) -> None: ...
    def on_warning(self, sync_name: str, message: str) -> None: ...
    def on_records_failed(self, sync_name: str, dead_letters: list[DeadLetter]) -> None: ...
    def on_interrupted(self, sync_name: str, batches_processed: int) -> None: ...
    def on_sync_ended(self, sync_name: str) -> None: ...

    def on_sync_completed(
        self,
        sync_name: str,
        result: SyncResult,
        started_at: str,
        new_cursor_value: str | None,
        cursor_field: str | None,
    ) -> None:
        # A dry run extracts (so new_cursor_value reflects rows *seen*) but
        # never calls destination.load() — persisting here would record a
        # cursor/run/count for data that was only previewed, never sent,
        # and the next real run would then skip it (#978).
        if result.dry_run:
            return

        from drt.engine.cursor import cursor_gt
        from drt.state.manager import SyncState

        if self._state_manager is not None:
            status = (
                "success" if result.failed == 0 else "partial" if result.success > 0 else "failed"
            )
            persist_cursor_value = new_cursor_value if cursor_field else None
            try:
                # Never regress an already-persisted watermark (#1074
                # round 5, Codex review on #1083): a #1074 rollback can
                # hand this observer an older (or, on a from-scratch
                # revert, a None) cursor than what's already stored, and —
                # independent of #1074 — so can an ordinary race between
                # two runs of the same sync. Reading the current value
                # first and refusing to move it backward is the cheap,
                # single-process-safe half of that; true cross-process
                # atomicity needs a CAS/generation-token primitive the
                # StateStore Protocol doesn't have yet (that's #756's
                # scope, not this fix's) — a run that reads here, loses a
                # race to a concurrent writer, and then writes anyway can
                # still regress the cursor in the narrow window between
                # this read and the save_sync() call below.
                #
                # persist_cursor_value being None must NOT short-circuit
                # this check (round 6, Codex review): a from-scratch
                # revert legitimately has nothing of its own to persist,
                # but that must mean "leave the existing cursor alone",
                # never "overwrite it with None" — round 5's fix only
                # skipped the read (and so the guard) when the proposed
                # value was already None, which reintroduced exactly the
                # unconditional-None-wipe failure mode this whole fix
                # exists to close, just gated on a race instead of on
                # every failed run.
                if cursor_field:
                    current = self._state_manager.get_last_sync(sync_name)
                    if (
                        current is not None
                        and current.last_cursor_value is not None
                        and (
                            persist_cursor_value is None
                            or not cursor_gt(persist_cursor_value, current.last_cursor_value)
                        )
                    ):
                        persist_cursor_value = current.last_cursor_value
                self._state_manager.save_sync(
                    SyncState(
                        sync_name=sync_name,
                        last_run_at=started_at,
                        records_synced=result.success,
                        status=status,
                        error=result.errors[0] if result.errors else None,
                        last_cursor_value=persist_cursor_value,
                    )
                )
            except Exception as exc:  # noqa: BLE001 — fire-and-forget contract
                self._logger.warning("State persist failure for '%s': %s", sync_name, exc)

        if self._watermark_storage is not None and cursor_field and new_cursor_value:
            try:
                # Same never-regress guard as the state_manager branch above
                # — but only skip on a TRUE regression (current strictly
                # greater). An unchanged value (e.g. an empty run) must
                # still write: #759's watermark.lag relies on this exact
                # call happening every time to re-persist the unlagged
                # value (see the comment at effective_cursor_value's
                # assignment in sync.py), and other code paths already
                # depend on save() being unconditional.
                current_wm = self._watermark_storage.get(sync_name)
                if current_wm is None or not cursor_gt(current_wm, new_cursor_value):
                    self._watermark_storage.save(sync_name, new_cursor_value)
            except Exception as exc:  # noqa: BLE001 — fire-and-forget contract
                self._logger.warning("Watermark save failure for '%s': %s", sync_name, exc)


class CompositeObserver:
    """Fan-out observer — broadcasts each event to a list of children.

    Children are called in order. If any child raises, the error is
    logged and the next child still runs (preserving fire-and-forget
    semantics even when an individual observer breaks the contract).
    """

    def __init__(self, observers: Iterable[SyncObserver]) -> None:
        self._observers = list(observers)
        self._logger = logging.getLogger("drt")

    def _broadcast(self, method_name: str, *args: object, **kwargs: object) -> None:
        for obs in self._observers:
            try:
                getattr(obs, method_name)(*args, **kwargs)
            except Exception as exc:  # noqa: BLE001 — protect the engine
                self._logger.warning(
                    "Observer %s.%s raised: %s",
                    type(obs).__name__,
                    method_name,
                    exc,
                )

    def on_sync_started(self, sync_name: str, started_at: str) -> None:
        self._broadcast("on_sync_started", sync_name, started_at)

    def on_watermark_resolved(self, sync_name: str, source: str, cursor_value: str | None) -> None:
        self._broadcast("on_watermark_resolved", sync_name, source, cursor_value)

    def on_warning(self, sync_name: str, message: str) -> None:
        self._broadcast("on_warning", sync_name, message)

    def on_records_failed(self, sync_name: str, dead_letters: list[DeadLetter]) -> None:
        self._broadcast("on_records_failed", sync_name, dead_letters)

    def on_interrupted(self, sync_name: str, batches_processed: int) -> None:
        self._broadcast("on_interrupted", sync_name, batches_processed)

    def on_sync_completed(
        self,
        sync_name: str,
        result: SyncResult,
        started_at: str,
        new_cursor_value: str | None,
        cursor_field: str | None,
    ) -> None:
        self._broadcast(
            "on_sync_completed",
            sync_name,
            result,
            started_at,
            new_cursor_value,
            cursor_field,
        )

    def on_sync_ended(self, sync_name: str) -> None:
        self._broadcast("on_sync_ended", sync_name)


class DlqObserver:
    """Persists per-record load failures to the Dead Letter Queue (#278).

    Wired into the run path only for syncs that set ``sync.dlq.enabled``.
    Like every other observer it is fire-and-forget: a DLQ write failure is
    logged and swallowed so it can never fail an otherwise-OK sync. Failed
    batches are buffered and written once at completion. This avoids a
    remote read-modify-write for every failed batch while preserving record
    order and identical local JSONL content.
    """

    def __init__(self, store: DlqBackend, *, max_records: int = 10_000) -> None:
        self._store = store
        self._max_records = max_records
        self._logger = logging.getLogger("drt")
        self._buffer: dict[str, list[DeadLetter]] = {}

    def on_sync_started(self, sync_name: str, started_at: str) -> None: ...
    def on_watermark_resolved(
        self, sync_name: str, source: str, cursor_value: str | None
    ) -> None: ...
    def on_warning(self, sync_name: str, message: str) -> None: ...
    def on_interrupted(self, sync_name: str, batches_processed: int) -> None: ...
    def on_sync_completed(
        self,
        sync_name: str,
        result: SyncResult,
        started_at: str,
        new_cursor_value: str | None,
        cursor_field: str | None,
    ) -> None: ...

    def on_sync_ended(self, sync_name: str) -> None:
        # Fires from run_sync's outer `finally` on every exit path (success,
        # exception, interruption) — unlike on_sync_completed, which only
        # fires on the normal-return path. The buffer must flush here, not
        # there, so a mid-sync crash cannot silently drop already-buffered
        # dead letters.
        dead_letters = self._buffer.pop(sync_name, [])
        if not dead_letters:
            return
        try:
            self._store.append(sync_name, dead_letters, max_records=self._max_records)
        except Exception as exc:  # noqa: BLE001 — fire-and-forget contract
            self._logger.warning("DLQ persist failure for '%s': %s", sync_name, exc)

    def on_records_failed(self, sync_name: str, dead_letters: list[DeadLetter]) -> None:
        if not dead_letters:
            return
        buffer = self._buffer.setdefault(sync_name, [])
        buffer.extend(dead_letters)
        # Cap the in-memory buffer as entries arrive, not just at flush —
        # a long-running sync with far more failures than max_records would
        # otherwise hold all of them in memory for the whole run. Only the
        # newest max_records survive `store.append`'s own truncation anyway,
        # so trimming here produces an identical final object.
        if self._max_records > 0 and len(buffer) > self._max_records:
            del buffer[: len(buffer) - self._max_records]


# ---------------------------------------------------------------------------
# Extra-observer registry (#299, ADR 0008) — lets an Enterprise audit
# implementation observe sync_started/sync_completed without OSS code
# knowing anything about audit logging. No new Protocol: an Enterprise
# audit observer is just a SyncObserver, appended to the same
# CompositeObserver every built-in observer already goes through
# (drt/cli/commands/run.py's _build_observer). Same register()-not-error,
# replace-on-duplicate shape as drt.security.register_permission_checker.
# ---------------------------------------------------------------------------

_extra_observers_lock = threading.Lock()
_extra_observers: list[SyncObserver] = []


def register_extra_observer(observer: SyncObserver) -> None:
    """Add `observer` to the observers every sync run also notifies.

    Cumulative, not replace-on-duplicate — unlike the single-active-policy
    shape of `register_permission_checker`/`register_audit_logger`,
    multiple extra observers (an audit logger, a metrics exporter) may
    coexist, matching how `CompositeObserver` already fans out to several
    built-in observers.
    """
    with _extra_observers_lock:
        _extra_observers.append(observer)


def registered_extra_observers() -> list[SyncObserver]:
    """Return the currently registered extra observers, in registration order."""
    with _extra_observers_lock:
        return list(_extra_observers)


def _reset_extra_observers() -> None:
    """Drop every registered extra observer. Test hook only — not called
    by production code, matching rate_limiter.py's _reset_limiter_registry."""
    with _extra_observers_lock:
        _extra_observers.clear()
