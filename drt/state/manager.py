"""StateManager — persists sync state to local JSON.

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
from typing import Any


@dataclass
class SyncState:
    sync_name: str
    last_run_at: str
    records_synced: int
    status: str  # "success" | "failed" | "partial"
    error: str | None = None
    last_cursor_value: str | None = None  # watermark for incremental sync


class StateManager:
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

    def now(self) -> str:
        return datetime.now(timezone.utc).isoformat()
