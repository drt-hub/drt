"""Tests for the backend-neutral conditional object-store state primitive."""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

from drt.state._objectstore import (
    ObjectClient,
    ObjectPreconditionError,
    ObjectStoreDlqBackend,
    ObjectStoreHistoryStore,
    ObjectStoreStateStore,
    Token,
)
from drt.state.dlq import DeadLetter, DlqBackend, LocalDlqStore
from drt.state.errors import StateContentionError
from drt.state.history import HistoryEntry, HistoryStore, LocalHistoryManager
from drt.state.manager import LocalStateManager, StateStore, SyncState


class MemoryObjectClient:
    """Generation-aware fake with programmable precondition failures."""

    def __init__(self, *, failures: int = 0, always_conflict: bool = False) -> None:
        self.objects: dict[str, bytes] = {}
        self.generations: dict[str, int] = {}
        self.failures = failures
        self.always_conflict = always_conflict
        self.reads = 0
        self.writes = 0
        self._lock = threading.Lock()

    def read_for_update(self, key: str) -> tuple[bytes | None, Token]:
        with self._lock:
            self.reads += 1
            return self.objects.get(key), self.generations.get(key, 0)

    def write_if(self, key: str, body: bytes, token: Token) -> Token:
        with self._lock:
            self.writes += 1
            if self.always_conflict or self.failures > 0:
                self.failures -= 1
                raise ObjectPreconditionError("raced")
            if token != self.generations.get(key, 0):
                raise ObjectPreconditionError("stale")
            generation = self.generations.get(key, 0) + 1
            self.objects[key] = body
            self.generations[key] = generation
            return generation

    def list_keys(self, prefix: str) -> list[str]:
        with self._lock:
            return sorted(key for key in self.objects if key.startswith(prefix))


def _state(name: str = "s", records: int = 1) -> SyncState:
    return SyncState(name, "2026-08-08T00:00:00+00:00", records, "success")


def _history(name: str = "s", *, started_at: str | None = None) -> HistoryEntry:
    timestamp = started_at or datetime.now(timezone.utc).isoformat()
    return HistoryEntry(name, timestamp, timestamp, 1.0, "success", 1, 0)


def _dead(value: int) -> DeadLetter:
    return DeadLetter(
        record={"id": value},
        error_message=f"boom {value}",
        http_status=500,
        timestamp="2026-08-08T00:00:00+00:00",
    )


def test_remote_implementations_satisfy_store_protocols() -> None:
    client = MemoryObjectClient()
    assert isinstance(client, ObjectClient)
    assert isinstance(ObjectStoreStateStore(client), StateStore)
    assert isinstance(ObjectStoreHistoryStore(client), HistoryStore)
    assert isinstance(ObjectStoreDlqBackend(client), DlqBackend)


def test_state_retries_conflicts_with_exponential_nonzero_backoff() -> None:
    client = MemoryObjectClient(failures=3)
    store = ObjectStoreStateStore(client)

    with (
        patch("drt.state._objectstore.random.uniform", side_effect=lambda low, high: low),
        patch("drt.state._objectstore.time.sleep") as sleep,
    ):
        store.save_sync(_state())

    assert client.writes == 4
    delays = [call.args[0] for call in sleep.call_args_list]
    assert delays == [0.005, 0.01, 0.02]
    assert all(delay > 0 for delay in delays), "contention retries must not busy-loop"


def test_state_raises_after_bounded_contention_attempts() -> None:
    client = MemoryObjectClient(always_conflict=True)
    store = ObjectStoreStateStore(client)

    with (
        patch("drt.state._objectstore.time.sleep"),
        pytest.raises(StateContentionError, match="exhausted 8"),
    ):
        store.save_sync(_state())

    assert client.writes == store.MAX_WRITE_ATTEMPTS


@pytest.mark.parametrize("kind", ["history", "dlq"])
def test_best_effort_appends_warn_and_return_after_bounded_contention(
    kind: str, caplog: pytest.LogCaptureFixture
) -> None:
    client = MemoryObjectClient(always_conflict=True)
    with (
        patch("drt.state._objectstore.time.sleep"),
        caplog.at_level(logging.WARNING, logger="drt.state._objectstore"),
    ):
        if kind == "history":
            history_store = ObjectStoreHistoryStore(client)
            history_store.append(_history())
            attempts = history_store.MAX_WRITE_ATTEMPTS
        else:
            dlq_store = ObjectStoreDlqBackend(client)
            dlq_store.append("s", [_dead(1)])
            attempts = dlq_store.MAX_WRITE_ATTEMPTS

    assert client.writes == attempts
    assert any("contention attempts" in record.message for record in caplog.records)


def test_state_semantics_and_unknown_reset() -> None:
    store = ObjectStoreStateStore(MemoryObjectClient())
    assert store.get_last_sync("missing") is None
    assert store.reset("missing") is False

    store.save_sync(_state("a", 1))
    store.save_sync(_state("b", 2))
    store.save_sync(_state("a", 3))
    assert store.get_last_sync("a") == _state("a", 3)
    assert set(store.get_all()) == {"a", "b"}
    assert store.reset("a") is True
    assert set(store.get_all()) == {"b"}


def test_state_json_is_byte_compatible_with_local(tmp_path: Path) -> None:
    state = _state()
    local = LocalStateManager(tmp_path)
    local.save_sync(state)
    client = MemoryObjectClient()
    ObjectStoreStateStore(client, prefix="project").save_sync(state)

    assert client.objects["project/state.json"] == (
        tmp_path / ".drt" / "state.json"
    ).read_bytes()


def test_history_append_then_noop_prune_uses_cached_snapshot() -> None:
    client = MemoryObjectClient()
    store = ObjectStoreHistoryStore(client)
    store.append(_history())
    reads_after_append = client.reads

    assert store.prune("s", retention_days=30) == 0
    assert client.reads == reads_after_append
    assert client.writes == 1


def test_remote_history_applies_entry_cap_only_during_prune() -> None:
    client = MemoryObjectClient()
    store = ObjectStoreHistoryStore(client, max_entries=2)
    for day in range(1, 4):
        store.append(_history(started_at=f"2026-08-0{day}T00:00:00+00:00"))

    assert len(store.read("s", limit=20)) == 3
    assert store.prune("s", retention_days=30) == 1
    assert [entry.started_at[9] for entry in store.read("s")] == ["3", "2"]


def test_history_reads_all_syncs_newest_first_and_prunes_old_entries() -> None:
    client = MemoryObjectClient()
    store = ObjectStoreHistoryStore(client)
    store.append(_history("a", started_at="2026-08-01T00:00:00+00:00"))
    store.append(_history("b", started_at="2026-08-02T00:00:00+00:00"))

    assert [entry.sync_name for entry in store.read()] == ["b", "a"]
    assert store.prune("missing", retention_days=30) == 0


def test_history_jsonl_is_byte_compatible_with_local(tmp_path: Path) -> None:
    entry = _history("s")
    local = LocalHistoryManager(tmp_path)
    local.append(entry)
    client = MemoryObjectClient()
    ObjectStoreHistoryStore(client).append(entry)

    assert client.objects["history/s.jsonl"] == (
        tmp_path / ".drt" / "history" / "s.jsonl"
    ).read_bytes()


def test_dlq_conformance_and_nonempty_depth_listing() -> None:
    store = ObjectStoreDlqBackend(MemoryObjectClient())
    assert store.depth("missing") == 0
    assert store.append("alpha", [_dead(1), _dead(2)]) == 2
    assert store.append("alpha", [_dead(3)], max_records=2) == 2
    assert [entry.record["id"] for entry in store.read("alpha")] == [2, 3]
    store.append("beta", [_dead(4)])
    store.clear("beta")
    assert store.all_depths() == {"alpha": 2}
    store.replace("alpha", [_dead(5)])
    assert [entry.record["id"] for entry in store.read("alpha")] == [5]


def test_dlq_jsonl_is_byte_compatible_with_local(tmp_path: Path) -> None:
    entries = [_dead(1), _dead(2)]
    local = LocalDlqStore(tmp_path)
    local.append("s", entries)
    client = MemoryObjectClient()
    ObjectStoreDlqBackend(client).append("s", entries)

    assert client.objects["dlq/s.jsonl"] == (
        tmp_path / ".drt" / "dlq" / "s.jsonl"
    ).read_bytes()
