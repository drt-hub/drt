"""Dead Letter Queue store — persist records that failed during load (#278).

When a sync sets ``sync.dlq.enabled: true``, each per-record load failure
is routed to ``.drt/dlq/<sync_name>.jsonl`` (one JSON object per line).
``drt retry <sync>`` replays the queue; ``drt status`` reports its depth.

The store lives next to ``state.json`` and ``history/`` under the same
``.drt`` directory so a project's local state stays self-contained.

Privacy note
------------

Unlike :class:`~drt.destinations.row_errors.RowError` — which deliberately
keeps only a 200-char ``record_preview`` to avoid logging PII — the DLQ
persists the **full** record so it can be replayed verbatim. That is why
DLQ is opt-in per sync (see ``DLQConfig``): writing complete rows to disk
is a privacy decision the operator makes explicitly, not a default.
"""

from __future__ import annotations

import json
import threading
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


@dataclass
class DeadLetter:
    """A single record that failed to load, plus why and when.

    ``record`` is the final, post-mapping payload the engine sent to the
    destination — so ``drt retry`` can re-send it verbatim without
    re-running source extraction or field mapping.
    """

    record: dict[str, Any]
    error_message: str
    http_status: int | None = None
    timestamp: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    attempts: int = 1


class DlqStore:
    """Append / read / replace dead-letter entries under ``.drt/dlq/``.

    One JSONL file per sync (``<sync_name>.jsonl``). All mutating methods
    run under a process-local lock so ``drt run --threads N`` workers and
    a concurrent ``drt retry`` don't clobber each other's rewrites.
    """

    def __init__(self, project_dir: Path = Path(".")) -> None:
        self._dlq_dir = project_dir / ".drt" / "dlq"
        self._lock = threading.Lock()

    # -- path helpers -------------------------------------------------------

    def _path(self, sync_name: str) -> Path:
        return self._dlq_dir / f"{sync_name}.jsonl"

    @staticmethod
    def _count_lines(path: Path) -> int:
        if not path.exists():
            return 0
        return sum(1 for line in path.read_text().splitlines() if line.strip())

    def _read_raw(self, path: Path) -> list[str]:
        if not path.exists():
            return []
        return [line for line in path.read_text().splitlines() if line.strip()]

    # -- writes -------------------------------------------------------------

    def append(
        self,
        sync_name: str,
        entries: list[DeadLetter],
        *,
        max_records: int = 10_000,
    ) -> int:
        """Append ``entries`` to the queue and return the resulting depth.

        When the queue would exceed ``max_records``, the oldest entries are
        dropped so the newest failures are always retained (FIFO cap). A
        ``max_records`` of 0 disables the cap.
        """
        if not entries:
            return self.depth(sync_name)
        with self._lock:
            path = self._path(sync_name)
            path.parent.mkdir(parents=True, exist_ok=True)
            lines = self._read_raw(path)
            lines.extend(json.dumps(asdict(e)) for e in entries)
            if max_records > 0 and len(lines) > max_records:
                lines = lines[-max_records:]
            path.write_text("\n".join(lines) + "\n")
            return len(lines)

    def replace(self, sync_name: str, entries: list[DeadLetter]) -> None:
        """Overwrite the queue with ``entries`` (empty list removes the file).

        ``drt retry`` calls this to drop successfully-replayed records and
        write back the ones that failed again (with bumped ``attempts``).
        """
        with self._lock:
            path = self._path(sync_name)
            if not entries:
                path.unlink(missing_ok=True)
                return
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("\n".join(json.dumps(asdict(e)) for e in entries) + "\n")

    def clear(self, sync_name: str) -> None:
        """Remove the queue file for ``sync_name`` if it exists."""
        self.replace(sync_name, [])

    # -- reads --------------------------------------------------------------

    def read(self, sync_name: str) -> list[DeadLetter]:
        """Return every dead-letter entry for ``sync_name`` (corrupt lines skipped)."""
        out: list[DeadLetter] = []
        for line in self._read_raw(self._path(sync_name)):
            try:
                out.append(DeadLetter(**json.loads(line)))
            except (json.JSONDecodeError, TypeError):
                # A single malformed line should not abort an entire retry.
                continue
        return out

    def depth(self, sync_name: str) -> int:
        """Return the number of entries queued for ``sync_name``."""
        return self._count_lines(self._path(sync_name))

    def all_depths(self) -> dict[str, int]:
        """Map ``sync_name -> depth`` for every non-empty queue on disk."""
        if not self._dlq_dir.exists():
            return {}
        out: dict[str, int] = {}
        for path in sorted(self._dlq_dir.glob("*.jsonl")):
            depth = self._count_lines(path)
            if depth:
                out[path.stem] = depth
        return out
