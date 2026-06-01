"""SyncObserver — the engine's event surface.

The engine emits structured events through a ``SyncObserver``; concrete
observers decide what to do with them (write to logs, persist state,
publish OTel spans, format errors, etc.). The engine itself stays free
of direct I/O — every write that used to live inside ``engine/sync.py``
now goes through this protocol.

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
from collections.abc import Iterable
from typing import TYPE_CHECKING, Protocol, runtime_checkable

if TYPE_CHECKING:
    from drt.destinations.base import SyncResult
    from drt.state.manager import StateManager
    from drt.state.watermark import WatermarkStorage


@runtime_checkable
class SyncObserver(Protocol):
    """The engine's event surface. All methods are fire-and-forget."""

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
    def on_interrupted(self, sync_name: str, batches_processed: int) -> None: ...
    def on_sync_completed(
        self,
        sync_name: str,
        result: SyncResult,
        started_at: str,
        new_cursor_value: str | None,
        cursor_field: str | None,
    ) -> None: ...


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


class StatePersistingObserver:
    """Persists state on ``on_sync_completed``.

    Replaces the engine's prior direct calls to
    ``state_manager.save_sync(...)`` and ``watermark_storage.save(...)``.
    Errors are swallowed with a warning so a corrupt state file cannot
    fail an otherwise-successful sync.
    """

    def __init__(
        self,
        state_manager: StateManager | None,
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
    def on_interrupted(self, sync_name: str, batches_processed: int) -> None: ...

    def on_sync_completed(
        self,
        sync_name: str,
        result: SyncResult,
        started_at: str,
        new_cursor_value: str | None,
        cursor_field: str | None,
    ) -> None:
        from drt.state.manager import SyncState

        if self._state_manager is not None:
            status = (
                "success" if result.failed == 0 else "partial" if result.success > 0 else "failed"
            )
            try:
                self._state_manager.save_sync(
                    SyncState(
                        sync_name=sync_name,
                        last_run_at=started_at,
                        records_synced=result.success,
                        status=status,
                        error=result.errors[0] if result.errors else None,
                        last_cursor_value=new_cursor_value if cursor_field else None,
                    )
                )
            except Exception as exc:  # noqa: BLE001 — fire-and-forget contract
                self._logger.warning("State persist failure for '%s': %s", sync_name, exc)

        if self._watermark_storage is not None and cursor_field and new_cursor_value:
            try:
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
