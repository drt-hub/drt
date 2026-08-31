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

import hashlib
import json
import threading
import uuid
from collections.abc import Collection, Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from drt.state._filelock import advisory_lock


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
    # Correlation ID (#762) — which sync execution produced this dead letter.
    # See drt._identifiers. None on entries written before this field existed;
    # the JSONL reader tolerates the missing key via the dataclass default.
    sync_run_id: str | None = None
    # Stable identity (#955) — assigned once at creation and preserved by
    # `drt retry` across attempts (a retried-and-failed-again entry keeps its
    # id; only `attempts`/`timestamp`/`error_message` change), so `reconcile()`
    # below can remove/update entries by identity against a *fresh* read
    # instead of overwriting the whole queue from a snapshot that may already
    # be stale.
    #
    # This default only fires for a freshly-constructed entry that has never
    # touched JSON (e.g. `engine/sync.py`'s per-failure `DeadLetter(...)`) —
    # its id is assigned once, in Python, before the object is ever
    # serialized. It must NOT fire when *decoding* a legacy JSONL line that
    # predates this field: `replay_dead_letters()` reads the queue twice per
    # invocation (once to decide what to retry, again inside `reconcile()`
    # to compute the write), and two independent `DeadLetter(**json.loads(
    # line))` calls on the *same unchanged line* would each trigger this
    # factory fresh — producing two different random ids for one entry, so
    # every legacy entry's remove/update would silently never match
    # (caught in review, #955). `decode_dead_letter_line()` below is the
    # actual JSONL entry point and handles that case with a content hash
    # instead — deterministic for the same bytes, so two reads of the same
    # untouched line agree. Bypassing that function and constructing
    # directly from a legacy dict (as tests occasionally do to simulate a
    # pre-#955 file) is the only path that still exercises this default on
    # already-persisted data — a reminder to route JSONL reads through the
    # decoder, not this constructor default.
    id: str = field(default_factory=lambda: uuid.uuid4().hex)


def decode_dead_letter_line(raw_line: str) -> DeadLetter:
    """Parse one DLQ JSONL line into a ``DeadLetter``.

    Entries written before ``id`` existed get a deterministic id — the
    SHA-256 of the literal line content — rather than the dataclass
    default's random one, so repeated reads of the same unchanged line
    (``replay_dead_letters()`` reads the queue twice per invocation) agree
    on identity instead of producing entries ``reconcile()`` can never
    match (#955).
    """
    data = json.loads(raw_line)
    if "id" not in data:
        data["id"] = hashlib.sha256(raw_line.strip().encode()).hexdigest()
    return DeadLetter(**data)


@runtime_checkable
class DlqBackend(Protocol):
    """Persist and replay records that failed during load (#756).

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).

    Extracted first among the three stores because it carries the sharpest
    reported pain: on an ephemeral runner the queue dies with the container,
    so ``drt retry`` can never see a previous run's failures.
    """

    def append(
        self, sync_name: str, entries: list[DeadLetter], *, max_records: int = 10_000
    ) -> int:
        """Append ``entries`` and return the resulting depth (FIFO-capped).

        Object-store-backed implementations treat this as best-effort, like
        ``HistoryStore.append`` — a failure is logged at WARNING and
        swallowed rather than raised, so a DLQ persistence problem never
        fails the sync whose records it's recording. ``LocalDlqStore`` does
        not catch local I/O errors (disk full, permission denied); those
        still propagate.
        """
        ...

    def replace(self, sync_name: str, entries: list[DeadLetter]) -> None: ...
    def clear(self, sync_name: str) -> None: ...
    def read(self, sync_name: str) -> list[DeadLetter]: ...
    def depth(self, sync_name: str) -> int: ...
    def all_depths(self) -> dict[str, int]: ...

    def reconcile(
        self,
        sync_name: str,
        *,
        remove_ids: Collection[str] = (),
        updates: Mapping[str, DeadLetter] | None = None,
    ) -> list[DeadLetter]:
        """Remove/update entries by identity against a *fresh* read (#955).

        Unlike ``replace()``, which overwrites the whole queue with whatever
        the caller passes, ``reconcile()`` re-reads current state itself and
        only touches entries named in ``remove_ids``/``updates`` — entries
        the caller never saw (e.g. a concurrent ``drt run`` append that
        landed after the caller's own ``read()``) are left alone rather than
        silently dropped. Returns the resulting full entry list.
        """
        ...


class LocalDlqStore:
    """Append / read / replace dead-letter entries under ``.drt/dlq/``.

    One JSONL file per sync (``<sync_name>.jsonl``). All mutating methods run
    under ``self._lock`` for threads sharing this instance and an OS-level
    sidecar lock for separate drt processes. Single-writer-at-a-time remains
    the expected operational model. Lock nesting order is load-bearing — see
    ``drt.state._filelock``.
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
        path = self._path(sync_name)
        with advisory_lock(path):
            with self._lock:
                path.parent.mkdir(parents=True, exist_ok=True)
                lines = self._read_raw(path)
                lines.extend(json.dumps(asdict(e)) for e in entries)
                if max_records > 0 and len(lines) > max_records:
                    lines = lines[-max_records:]
                path.write_text("\n".join(lines) + "\n")
                return len(lines)

    def replace(self, sync_name: str, entries: list[DeadLetter]) -> None:
        """Overwrite the queue with ``entries`` (empty list removes the file).

        Wholesale — ``entries`` fully replaces whatever is on disk, including
        anything a concurrent writer appended since this call's caller last
        read the queue (#955). ``drt retry`` uses ``reconcile()`` instead,
        which re-reads fresh state and touches only named entries; this
        method still backs ``clear()`` (discard everything, intentionally)
        and stays available for callers that genuinely want a full overwrite.
        """
        path = self._path(sync_name)
        with advisory_lock(path):
            with self._lock:
                if not entries:
                    path.unlink(missing_ok=True)
                    return
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("\n".join(json.dumps(asdict(e)) for e in entries) + "\n")

    def clear(self, sync_name: str) -> None:
        """Remove the queue file for ``sync_name`` if it exists.

        Wholesale, like ``replace([])`` which backs it — a concurrent append
        racing this call can still be dropped. That's the documented
        contract ("discard the queue without replaying, records are
        unrecoverable" per the CLI's own ``--clear`` help text), not an
        oversight left over from ``reconcile()`` hardening the retry path.
        """
        self.replace(sync_name, [])

    # -- reads --------------------------------------------------------------

    def _read_entries(self, sync_name: str) -> list[DeadLetter]:
        out: list[DeadLetter] = []
        for line in self._read_raw(self._path(sync_name)):
            try:
                out.append(decode_dead_letter_line(line))
            except (json.JSONDecodeError, TypeError):
                # A single malformed line should not abort an entire retry.
                continue
        return out

    def read(self, sync_name: str) -> list[DeadLetter]:
        """Return every dead-letter entry for ``sync_name`` (corrupt lines skipped)."""
        return self._read_entries(sync_name)

    def reconcile(
        self,
        sync_name: str,
        *,
        remove_ids: Collection[str] = (),
        updates: Mapping[str, DeadLetter] | None = None,
    ) -> list[DeadLetter]:
        """Remove/update entries by identity against a fresh read (#955).

        See the ``DlqBackend`` Protocol docstring for the "why" — the short
        version is this is what ``drt retry`` uses instead of ``replace()``
        so a concurrent append isn't silently overwritten.

        The process-local and OS-level sidecar locks cover the fresh read,
        identity reconciliation, and write as one cross-process-safe span.
        """
        updates = updates or {}
        remove_ids = set(remove_ids)
        path = self._path(sync_name)
        with advisory_lock(path):
            with self._lock:
                current = self._read_entries(sync_name)
                result = [
                    updates.get(entry.id, entry) for entry in current if entry.id not in remove_ids
                ]
                if not result:
                    path.unlink(missing_ok=True)
                else:
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("\n".join(json.dumps(asdict(e)) for e in result) + "\n")
                return result

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


# Back-compat alias — see the note on ``StateManager`` in state/manager.py.
DlqStore = LocalDlqStore
