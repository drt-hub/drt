"""Sync execution history — append-only JSONL per sync.

Each sync writes one HistoryEntry per execution to ``.drt/history/<sync_name>.jsonl``.
The CLI exposes recent entries via ``drt status --history``; the MCP server exposes
them as ``drt_get_history`` so AI agents can query past runs.

Why JSONL per-sync:
- POSIX ``O_APPEND`` makes single-line writes atomic across ``--threads`` workers,
  no lock file needed.
- Per-sync files keep retention prune trivial (rewrite the file once it crosses
  the cutoff) and let ``drt status --history <sync_name>`` read just one file.
- JSONL is grep/jq friendly without a database dependency.

Rust-migration note: pure JSON I/O, no rich types — straightforward to port.
"""

from __future__ import annotations

import json
import logging
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class HistoryEntry:
    """One execution of one sync."""

    sync_name: str
    started_at: str  # ISO-8601 UTC
    completed_at: str  # ISO-8601 UTC
    duration_seconds: float
    status: str  # "success" | "partial" | "failed"
    records_synced: int
    records_failed: int
    errors: list[str] = field(default_factory=list)  # truncated to first 5
    cursor_value_used: str | None = None  # for incremental syncs
    dry_run: bool = False  # always False on disk; reserved for future use


class HistoryManager:
    """Append-only per-sync execution history.

    Files live under ``<project_dir>/.drt/history/<sync_name>.jsonl``. All
    writes append a single JSON object per line. Reads return the most recent
    N entries (newest first).
    """

    _MAX_ERRORS_PER_ENTRY = 5

    def __init__(self, project_dir: Path = Path(".")) -> None:
        self._dir = project_dir / ".drt" / "history"
        self._lock = threading.Lock()  # protects prune's read-rewrite-write

    def _file_for(self, sync_name: str) -> Path:
        return self._dir / f"{sync_name}.jsonl"

    def append(self, entry: HistoryEntry) -> None:
        """Append one entry. Best-effort — failures are logged at WARNING and
        never propagate (sync results must not depend on history persistence).
        """
        try:
            self._dir.mkdir(parents=True, exist_ok=True)
            # Truncate errors to bound disk growth on long-failing syncs.
            entry.errors = entry.errors[: self._MAX_ERRORS_PER_ENTRY]
            line = json.dumps(asdict(entry), default=str)
            # POSIX O_APPEND makes single-line writes atomic across processes.
            with self._file_for(entry.sync_name).open("a") as f:
                f.write(line + "\n")
        except OSError as exc:  # disk full, permission denied, etc.
            logger.warning("history append failed for sync=%s: %s", entry.sync_name, exc)

    def read(
        self,
        sync_name: str | None = None,
        limit: int = 20,
    ) -> list[HistoryEntry]:
        """Return up to ``limit`` most recent entries, newest first.

        If ``sync_name`` is given, only that sync's history is read; otherwise
        all syncs are merged and re-sorted by ``started_at``.
        """
        if not self._dir.exists():
            return []

        files: list[Path]
        if sync_name is not None:
            target = self._file_for(sync_name)
            files = [target] if target.exists() else []
        else:
            files = sorted(self._dir.glob("*.jsonl"))

        entries: list[HistoryEntry] = []
        for path in files:
            entries.extend(_read_jsonl(path))

        entries.sort(key=lambda e: e.started_at, reverse=True)
        return entries[:limit]

    def prune(self, sync_name: str, retention_days: int) -> int:
        """Drop entries older than ``retention_days`` for one sync.

        Returns the number of entries removed. No-op if the file doesn't exist.
        Rewrites the file in place under a process-local lock so concurrent
        workers don't lose appends in the gap between read and write.
        """
        path = self._file_for(sync_name)
        if not path.exists():
            return 0

        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

        with self._lock:
            kept: list[HistoryEntry] = []
            removed = 0
            for entry in _read_jsonl(path):
                try:
                    started = datetime.fromisoformat(entry.started_at)
                except ValueError:
                    # Malformed timestamp — keep so a human can inspect.
                    kept.append(entry)
                    continue
                if started < cutoff:
                    removed += 1
                else:
                    kept.append(entry)

            if removed == 0:
                return 0

            # Rewrite (entries are already in the order we want — preserved
            # from the original file).
            tmp = path.with_suffix(".jsonl.tmp")
            with tmp.open("w") as f:
                for entry in kept:
                    f.write(json.dumps(asdict(entry), default=str) + "\n")
            tmp.replace(path)
            return removed


def _read_jsonl(path: Path) -> list[HistoryEntry]:
    """Read all entries from one JSONL file. Skips malformed lines with a warning."""
    entries: list[HistoryEntry] = []
    try:
        with path.open() as f:
            for lineno, raw in enumerate(f, start=1):
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    data = json.loads(raw)
                    entries.append(HistoryEntry(**data))
                except (json.JSONDecodeError, TypeError) as exc:
                    logger.warning(
                        "history: skipping malformed line %s in %s: %s",
                        lineno,
                        path,
                        exc,
                    )
    except OSError as exc:
        logger.warning("history: cannot read %s: %s", path, exc)
    return entries
