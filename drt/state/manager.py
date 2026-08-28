"""Run-state persistence — the ``StateStore`` Protocol and its local impl.

``LocalStateManager`` persists to local JSON and is the default. The Protocol
exists so state can also live somewhere that survives an ephemeral runner
(#756); ``StateManager`` remains as an alias for it.

Simple by design: no external dependencies, no infrastructure.
Future: bincode (Rust) for fast binary serialization.

Thread safety: ``drt run --threads N`` calls ``save_sync`` concurrently
from each worker. Every method that touches state.json runs under a
process-local :class:`threading.Lock` so the load-modify-save cycle is
atomic and parallel writers don't clobber each other's updates.
"""

from __future__ import annotations

import json
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, runtime_checkable


@dataclass
class SyncState:
    sync_name: str
    last_run_at: str
    records_synced: int
    status: str  # "success" | "failed" | "partial"
    error: str | None = None
    last_cursor_value: str | None = None  # watermark for incremental sync


@runtime_checkable
class StateStore(Protocol):
    """Read and write per-sync run state (#756).

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).

    Extracted so state can live somewhere that survives an ephemeral runner —
    object storage or a warehouse — rather than only in ``.drt/state.json``.
    Same optional-backend shape as :class:`~drt.state.watermark.WatermarkStorage`,
    which already has local, GCS and BigQuery implementations.

    Writes reach this through ``StatePersistingObserver``; the engine never
    calls it directly (guarded by ``tests/unit/test_engine_observer.py``).
    """

    def get_last_sync(self, sync_name: str) -> SyncState | None: ...
    def get_all(self) -> dict[str, SyncState]: ...

    def save_sync(self, state: SyncState) -> None:
        """Persist ``state`` as the sync's latest recorded run.

        Raises:
            StateContentionError: a conditional (read-modify-write) backend
                kept losing its race against concurrent writers and
                exhausted its retry budget. Unlike ``HistoryStore.append``/
                ``DlqBackend.append``, this must not be silently swallowed —
                lost sync state is the failure class #919 exists to prevent.
        """
        ...

    def reset(self, sync_name: str) -> bool:
        """Drop recorded state for ``sync_name``; return whether anything went.

        Part of the Protocol rather than a local-only extra: ``drt state reset``,
        ``drt run --full-refresh`` and two MCP tools all call it, so a backend
        without it is not a usable substitute for the local store.

        Raises:
            StateContentionError: see ``save_sync``.
        """
        ...

    def now(self) -> str: ...


class LocalStateManager:
    """Read and write sync state from .drt/state.json.

    All public methods are thread-safe via ``self._lock``. The lock
    serialises the load-modify-save cycle in :meth:`save_sync` and the
    read-only operations so a reader never observes a partially-written
    file in-memory either.
    """

    def __init__(self, project_dir: Path = Path(".")) -> None:
        self._state_dir = project_dir / ".drt"
        self._state_file = self._state_dir / "state.json"
        self._lock = threading.Lock()

    def _load_all(self) -> dict[str, Any]:
        if not self._state_file.exists():
            return {}
        try:
            with self._state_file.open() as f:
                result: dict[str, Any] = json.load(f) or {}
                return result
        except (json.JSONDecodeError, ValueError):
            import sys

            print(
                f"Warning: {self._state_file} is corrupted and will be reset.",
                file=sys.stderr,
            )
            return {}

    def _save_all(self, data: dict[str, Any]) -> None:
        self._state_dir.mkdir(exist_ok=True)
        with self._state_file.open("w") as f:
            json.dump(data, f, indent=2)

    def get_last_sync(self, sync_name: str) -> SyncState | None:
        with self._lock:
            data = self._load_all()
        if sync_name not in data:
            return None
        return SyncState(**data[sync_name])

    def get_all(self) -> dict[str, SyncState]:
        """Return all sync states keyed by sync name."""
        with self._lock:
            data = self._load_all()
        return {k: SyncState(**v) for k, v in data.items()}

    def save_sync(self, state: SyncState) -> None:
        with self._lock:
            data = self._load_all()
            data[state.sync_name] = asdict(state)
            self._save_all(data)

    def reset(self, sync_name: str) -> bool:
        """Drop the recorded run state for ``sync_name`` (#776).

        Returns whether anything was actually removed, so the CLI can report
        "nothing to reset" rather than implying it cleared something.

        This also drops ``last_cursor_value``, which matters more than it
        looks: that field is the **fallback watermark**. ``engine/sync.py``
        resolves a cursor from ``watermark_storage`` first and from here only
        when no storage is configured — an ``elif``, so exactly one applies.
        Leaving it behind would make a reset silently ineffective for every
        project without a configured watermark backend, which is the default.

        Takes the same lock as ``save_sync``: ``drt run --threads`` writes
        state concurrently, and a read-modify-write here would otherwise race
        a run finishing.
        """
        with self._lock:
            data = self._load_all()
            if sync_name not in data:
                return False  # never run — nothing to clear, and no file to create
            del data[sync_name]
            self._save_all(data)
            return True

    def now(self) -> str:
        return datetime.now(timezone.utc).isoformat()


# Back-compat alias — every existing caller imports ``StateManager``. Kept so
# this refactor is a pure introduce-interface step; call sites move to the
# backend factory Part by Part as each remote implementation lands (#756).
StateManager = LocalStateManager
