"""Backend-neutral object-storage implementations for drt state (#756).

All three stores use generation / ETag preconditions around whole-object
read-modify-write cycles. A process-local lock removes avoidable contention
between threads sharing a factory bundle; the remote token remains the source
of correctness between separate drt processes.
"""

from __future__ import annotations

import json
import logging
import random
import sys
import threading
import time
from collections.abc import Collection, Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol, runtime_checkable

from drt.state.dlq import DeadLetter, decode_dead_letter_line
from drt.state.errors import StateContentionError
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState

logger = logging.getLogger(__name__)

Token = Any
"""Opaque object-version token: an integer generation on GCS, ETag on S3."""


class ObjectPreconditionError(RuntimeError):
    """A concrete object client rejected a stale concurrency token."""


@runtime_checkable
class ObjectClient(Protocol):
    """Minimal conditional object API needed by all remote state stores."""

    def read_for_update(self, key: str) -> tuple[bytes | None, Token]: ...
    def write_if(self, key: str, body: bytes, token: Token) -> Token: ...
    def list_keys(self, prefix: str) -> list[str]: ...


class _ObjectStoreBase:
    # Eight attempts is intentionally higher than the watermark precedent's
    # five: state.json is shared by every --threads worker, rather than by two
    # independent writers. Backoff prevents a losing process from hot-looping.
    MAX_WRITE_ATTEMPTS = 8
    _BASE_BACKOFF_SECONDS = 0.01
    _MAX_BACKOFF_SECONDS = 0.25

    def __init__(self, client: ObjectClient, *, prefix: str | None = None) -> None:
        self._client = client
        self._prefix = prefix.strip("/") if prefix else ""
        self._lock = threading.Lock()

    def _key(self, relative: str) -> str:
        return f"{self._prefix}/{relative}" if self._prefix else relative

    @classmethod
    def _backoff(cls, failed_attempt: int) -> None:
        ceiling = min(
            cls._BASE_BACKOFF_SECONDS * (2 ** (failed_attempt - 1)),
            cls._MAX_BACKOFF_SECONDS,
        )
        time.sleep(random.uniform(ceiling * 0.5, ceiling * 1.5))


class ObjectStoreStateStore(_ObjectStoreBase):
    """StateStore backed by one conditional ``state.json`` object."""

    _STATE_KEY = "state.json"

    @staticmethod
    def _decode(body: bytes | None) -> dict[str, Any]:
        if body is None:
            return {}
        try:
            value = json.loads(body.decode())
            if not isinstance(value, dict):
                raise ValueError("state root is not an object")
            return value
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError):
            print("Warning: remote state.json is corrupted and will be reset.", file=sys.stderr)
            return {}

    @staticmethod
    def _encode(data: dict[str, Any]) -> bytes:
        # Matches LocalStateManager._save_all byte-for-byte.
        return json.dumps(data, indent=2).encode()

    def _read_all_unlocked(self) -> dict[str, Any]:
        body, _ = self._client.read_for_update(self._key(self._STATE_KEY))
        return self._decode(body)

    def get_last_sync(self, sync_name: str) -> SyncState | None:
        with self._lock:
            data = self._read_all_unlocked()
        if sync_name not in data:
            return None
        return SyncState(**data[sync_name])

    def get_all(self) -> dict[str, SyncState]:
        with self._lock:
            data = self._read_all_unlocked()
        return {name: SyncState(**value) for name, value in data.items()}

    def save_sync(self, state: SyncState) -> None:
        key = self._key(self._STATE_KEY)
        with self._lock:
            for attempt in range(1, self.MAX_WRITE_ATTEMPTS + 1):
                try:
                    body, token = self._client.read_for_update(key)
                    data = self._decode(body)
                    data[state.sync_name] = asdict(state)
                    self._client.write_if(key, self._encode(data), token)
                    return
                except ObjectPreconditionError as exc:
                    if attempt == self.MAX_WRITE_ATTEMPTS:
                        raise StateContentionError(
                            f"state update for '{state.sync_name}' exhausted "
                            f"{self.MAX_WRITE_ATTEMPTS} conditional-write attempts"
                        ) from exc
                    self._backoff(attempt)

    def reset(self, sync_name: str) -> bool:
        key = self._key(self._STATE_KEY)
        with self._lock:
            for attempt in range(1, self.MAX_WRITE_ATTEMPTS + 1):
                try:
                    body, token = self._client.read_for_update(key)
                    data = self._decode(body)
                    if sync_name not in data:
                        return False
                    del data[sync_name]
                    self._client.write_if(key, self._encode(data), token)
                    return True
                except ObjectPreconditionError as exc:
                    if attempt == self.MAX_WRITE_ATTEMPTS:
                        raise StateContentionError(
                            f"state reset for '{sync_name}' exhausted "
                            f"{self.MAX_WRITE_ATTEMPTS} conditional-write attempts"
                        ) from exc
                    self._backoff(attempt)
        return False  # pragma: no cover - loop either returns or raises

    def now(self) -> str:
        return datetime.now(timezone.utc).isoformat()


@dataclass
class _AppendSnapshot:
    entries: list[HistoryEntry]
    token: Token


class ObjectStoreHistoryStore(_ObjectStoreBase):
    """HistoryStore backed by one conditional JSONL object per sync.

    Unlike local history, remote history is capped by ``max_entries`` as well
    as age so the object used by each read-modify-write remains bounded.
    """

    _MAX_ERRORS_PER_ENTRY = 5

    def __init__(
        self,
        client: ObjectClient,
        *,
        prefix: str | None = None,
        max_entries: int = 500,
    ) -> None:
        super().__init__(client, prefix=prefix)
        self._max_entries = max_entries
        self._append_snapshots: dict[str, _AppendSnapshot] = {}

    def _history_key(self, sync_name: str) -> str:
        return self._key(f"history/{sync_name}.jsonl")

    @staticmethod
    def _decode(body: bytes | None, key: str) -> list[HistoryEntry]:
        if body is None:
            return []
        entries: list[HistoryEntry] = []
        for lineno, raw in enumerate(body.decode(errors="replace").splitlines(), start=1):
            if not raw.strip():
                continue
            try:
                entries.append(HistoryEntry(**json.loads(raw)))
            except (json.JSONDecodeError, TypeError) as exc:
                logger.warning(
                    "history: skipping malformed line %s in %s: %s", lineno, key, exc
                )
        return entries

    @staticmethod
    def _encode(entries: list[HistoryEntry]) -> bytes:
        if not entries:
            return b""
        lines = "\n".join(json.dumps(asdict(entry), default=str) for entry in entries)
        return (lines + "\n").encode()

    def append(self, entry: HistoryEntry) -> None:
        entry.errors = entry.errors[: self._MAX_ERRORS_PER_ENTRY]
        key = self._history_key(entry.sync_name)
        with self._lock:
            for attempt in range(1, self.MAX_WRITE_ATTEMPTS + 1):
                try:
                    body, token = self._client.read_for_update(key)
                    entries = self._decode(body, key)
                    entries.append(entry)
                    new_token = self._client.write_if(key, self._encode(entries), token)
                    self._append_snapshots[entry.sync_name] = _AppendSnapshot(
                        entries=list(entries), token=new_token
                    )
                    return
                except ObjectPreconditionError:
                    if attempt == self.MAX_WRITE_ATTEMPTS:
                        logger.warning(
                            "history append failed for sync=%s after %s contention attempts",
                            entry.sync_name,
                            self.MAX_WRITE_ATTEMPTS,
                        )
                        return
                    self._backoff(attempt)
                except Exception as exc:  # noqa: BLE001 — best-effort history contract
                    logger.warning(
                        "history append failed for sync=%s: %s", entry.sync_name, exc
                    )
                    return

    def read(
        self, sync_name: str | None = None, limit: int = 20
    ) -> list[HistoryEntry]:
        with self._lock:
            if sync_name is not None:
                keys = [self._history_key(sync_name)]
            else:
                keys = sorted(self._client.list_keys(self._key("history/")))
            entries: list[HistoryEntry] = []
            for key in keys:
                body, _ = self._client.read_for_update(key)
                entries.extend(self._decode(body, key))
        entries.sort(key=lambda item: item.started_at, reverse=True)
        return entries[:limit]

    def _pruned(
        self, entries: list[HistoryEntry], retention_days: int
    ) -> tuple[list[HistoryEntry], int]:
        cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)
        kept: list[HistoryEntry] = []
        for entry in entries:
            try:
                started = datetime.fromisoformat(entry.started_at)
            except ValueError:
                kept.append(entry)
                continue
            if started >= cutoff:
                kept.append(entry)

        if len(kept) > self._max_entries:
            # Cap by recency (started_at), not append/file order: two
            # overlapping runs of the same sync can complete out of start
            # order, and read() itself defines "newest" by started_at, not
            # by position in the object. Sorting ascending first makes the
            # tail slice below the actual newest max_entries.
            kept.sort(key=lambda item: item.started_at)
            kept = kept[-self._max_entries :]
        return kept, len(entries) - len(kept)

    def prune(self, sync_name: str, retention_days: int) -> int:
        key = self._history_key(sync_name)
        with self._lock:
            snapshot = self._append_snapshots.pop(sync_name, None)
            for attempt in range(1, self.MAX_WRITE_ATTEMPTS + 1):
                try:
                    if attempt == 1 and snapshot is not None:
                        entries, token = snapshot.entries, snapshot.token
                    else:
                        body, token = self._client.read_for_update(key)
                        if body is None:
                            return 0
                        entries = self._decode(body, key)

                    kept, removed = self._pruned(entries, retention_days)
                    # The engine always calls append then prune. Most runs land
                    # here, avoiding both a second GET and an unnecessary PUT.
                    if removed == 0:
                        return 0
                    self._client.write_if(key, self._encode(kept), token)
                    return removed
                except ObjectPreconditionError:
                    if attempt == self.MAX_WRITE_ATTEMPTS:
                        logger.warning(
                            "history prune failed for sync=%s after %s contention attempts",
                            sync_name,
                            self.MAX_WRITE_ATTEMPTS,
                        )
                        return 0
                    self._backoff(attempt)
        return 0  # pragma: no cover - loop always returns


class ObjectStoreDlqBackend(_ObjectStoreBase):
    """DlqBackend backed by one conditional JSONL object per sync."""

    def _dlq_key(self, sync_name: str) -> str:
        return self._key(f"dlq/{sync_name}.jsonl")

    @staticmethod
    def _decode(body: bytes | None) -> list[DeadLetter]:
        if body is None:
            return []
        entries: list[DeadLetter] = []
        for raw in body.decode(errors="replace").splitlines():
            if not raw.strip():
                continue
            try:
                entries.append(decode_dead_letter_line(raw))
            except (json.JSONDecodeError, TypeError):
                continue
        return entries

    @staticmethod
    def _encode(entries: list[DeadLetter]) -> bytes:
        if not entries:
            return b""
        return ("\n".join(json.dumps(asdict(entry)) for entry in entries) + "\n").encode()

    def _replace_with_retry(self, sync_name: str, entries: list[DeadLetter]) -> bool:
        key = self._dlq_key(sync_name)
        for attempt in range(1, self.MAX_WRITE_ATTEMPTS + 1):
            try:
                _, token = self._client.read_for_update(key)
                self._client.write_if(key, self._encode(entries), token)
                return True
            except ObjectPreconditionError:
                if attempt == self.MAX_WRITE_ATTEMPTS:
                    return False
                self._backoff(attempt)
        return False  # pragma: no cover

    def append(
        self,
        sync_name: str,
        entries: list[DeadLetter],
        *,
        max_records: int = 10_000,
    ) -> int:
        key = self._dlq_key(sync_name)
        with self._lock:
            if not entries:
                body, _ = self._client.read_for_update(key)
                return len(self._decode(body))
            last_depth = 0
            existing_depth = 0
            for attempt in range(1, self.MAX_WRITE_ATTEMPTS + 1):
                try:
                    body, token = self._client.read_for_update(key)
                    combined = self._decode(body)
                    existing_depth = len(combined)
                    combined.extend(entries)
                    if max_records > 0 and len(combined) > max_records:
                        combined = combined[-max_records:]
                    last_depth = len(combined)
                    self._client.write_if(key, self._encode(combined), token)
                    return last_depth
                except ObjectPreconditionError:
                    if attempt == self.MAX_WRITE_ATTEMPTS:
                        logger.warning(
                            "DLQ append failed for sync=%s after %s contention attempts",
                            sync_name,
                            self.MAX_WRITE_ATTEMPTS,
                        )
                        return existing_depth
                    self._backoff(attempt)
                except Exception as exc:  # noqa: BLE001 — best-effort DLQ contract
                    logger.warning("DLQ append failed for sync=%s: %s", sync_name, exc)
                    return existing_depth
        return last_depth  # pragma: no cover

    def replace(self, sync_name: str, entries: list[DeadLetter]) -> None:
        with self._lock:
            if not self._replace_with_retry(sync_name, entries):
                raise ObjectPreconditionError(
                    f"DLQ replace for '{sync_name}' exhausted "
                    f"{self.MAX_WRITE_ATTEMPTS} attempts"
                )

    def clear(self, sync_name: str) -> None:
        self.replace(sync_name, [])

    def reconcile(
        self,
        sync_name: str,
        *,
        remove_ids: Collection[str] = (),
        updates: Mapping[str, DeadLetter] | None = None,
    ) -> list[DeadLetter]:
        """Remove/update entries by identity against a fresh read (#955).

        Each attempt re-reads the object under a fresh generation/ETag
        rather than retrying the same stale content ``replace()`` would —
        that's what lets a concurrent ``append()`` (itself already
        precondition-safe) survive a racing ``drt retry`` instead of being
        silently overwritten.
        """
        updates = updates or {}
        remove_ids = set(remove_ids)
        key = self._dlq_key(sync_name)
        with self._lock:
            for attempt in range(1, self.MAX_WRITE_ATTEMPTS + 1):
                try:
                    body, token = self._client.read_for_update(key)
                    current = self._decode(body)
                    result = [
                        updates.get(entry.id, entry)
                        for entry in current
                        if entry.id not in remove_ids
                    ]
                    self._client.write_if(key, self._encode(result), token)
                    return result
                except ObjectPreconditionError:
                    if attempt == self.MAX_WRITE_ATTEMPTS:
                        raise ObjectPreconditionError(
                            f"DLQ reconcile for '{sync_name}' exhausted "
                            f"{self.MAX_WRITE_ATTEMPTS} attempts"
                        )
                    self._backoff(attempt)
        return []  # pragma: no cover

    def read(self, sync_name: str) -> list[DeadLetter]:
        with self._lock:
            body, _ = self._client.read_for_update(self._dlq_key(sync_name))
            return self._decode(body)

    def depth(self, sync_name: str) -> int:
        with self._lock:
            body, _ = self._client.read_for_update(self._dlq_key(sync_name))
            return len(self._decode(body))

    def all_depths(self) -> dict[str, int]:
        prefix = self._key("dlq/")
        with self._lock:
            keys = sorted(self._client.list_keys(prefix))
            depths: dict[str, int] = {}
            for key in keys:
                if not key.startswith(prefix) or not key.endswith(".jsonl"):
                    continue
                body, _ = self._client.read_for_update(key)
                depth = len(self._decode(body))
                if depth:
                    sync_name = key[len(prefix) : -len(".jsonl")]
                    depths[sync_name] = depth
            return depths
