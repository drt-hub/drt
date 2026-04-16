"""StateManager — persists sync state to local JSON.

Simple by design: no external dependencies, no infrastructure.
Future: bincode (Rust) for fast binary serialization.
"""

from __future__ import annotations

import json
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
    """Read and write sync state from .drt/state.json."""

    def __init__(self, project_dir: Path = Path(".")) -> None:
        self._state_dir = project_dir / ".drt"
        self._state_file = self._state_dir / "state.json"

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
        data = self._load_all()
        if sync_name not in data:
            return None
        return SyncState(**data[sync_name])

    def get_all(self) -> dict[str, SyncState]:
        """Return all sync states keyed by sync name."""
        data = self._load_all()
        return {k: SyncState(**v) for k, v in data.items()}

    def save_sync(self, state: SyncState) -> None:
        data = self._load_all()
        data[state.sync_name] = asdict(state)
        self._save_all(data)

    def now(self) -> str:
        return datetime.now(timezone.utc).isoformat()
