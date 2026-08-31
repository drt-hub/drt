"""GCS backend conformance and concurrency tests against fake-gcs-server."""

from __future__ import annotations

import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from drt.state._objectstore import (
    ObjectClient,
    ObjectStoreDlqBackend,
    ObjectStoreHistoryStore,
    ObjectStoreStateStore,
    Token,
)
from drt.state.dlq import DeadLetter
from drt.state.errors import StateContentionError
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState

pytestmark = pytest.mark.object_store_smoke


def _prefix() -> str:
    return f"tests/{uuid4()}"


def _state(name: str, value: int) -> SyncState:
    return SyncState(name, f"2026-08-08T00:00:{value:02d}+00:00", value, "success")


def _history(name: str, started_at: str) -> HistoryEntry:
    return HistoryEntry(name, started_at, started_at, 1.0, "success", 1, 0)


def _dead(value: int) -> DeadLetter:
    return DeadLetter(
        record={"id": value},
        error_message="boom",
        timestamp="2026-08-08T00:00:00+00:00",
    )


def test_00_generation_precondition_canary(
    generation_preconditions_supported: bool,
) -> None:
    assert generation_preconditions_supported


def test_state_store_conforms_for_read_write_overwrite_and_reset(
    gcs_client: ObjectClient,
) -> None:
    store = ObjectStoreStateStore(gcs_client, prefix=_prefix())
    assert store.get_last_sync("missing") is None
    assert store.reset("missing") is False
    store.save_sync(_state("a", 1))
    store.save_sync(_state("b", 2))
    store.save_sync(_state("a", 3))

    assert store.get_last_sync("a") == _state("a", 3)
    assert set(store.get_all()) == {"a", "b"}
    assert store.reset("a") is True
    assert store.reset("a") is False
    assert set(store.get_all()) == {"b"}


def test_history_store_conforms_for_merge_limit_and_prune(
    gcs_client: ObjectClient,
) -> None:
    store = ObjectStoreHistoryStore(gcs_client, prefix=_prefix(), max_entries=2)
    assert store.read("missing") == []
    old = (datetime.now(timezone.utc) - timedelta(days=60)).isoformat()
    recent = [
        (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
        for hours in (3, 2, 1)
    ]
    store.append(_history("a", old))
    for timestamp in recent:
        store.append(_history("a", timestamp))
    store.append(_history("b", datetime.now(timezone.utc).isoformat()))

    assert len(store.read("a", limit=20)) == 4
    assert store.prune("a", retention_days=30) == 2
    assert [entry.started_at for entry in store.read("a")] == recent[::-1][:2]
    assert [entry.sync_name for entry in store.read(limit=1)] == ["b"]
    assert store.prune("missing", retention_days=30) == 0

    malformed = _history("malformed", "not-a-date")
    malformed.errors = [f"error {index}" for index in range(10)]
    store.append(malformed)
    [loaded] = store.read("malformed")
    assert len(loaded.errors) == 5
    assert store.prune("malformed", retention_days=1) == 0


def test_dlq_store_conforms_for_fifo_replace_clear_and_depths(
    gcs_client: ObjectClient,
) -> None:
    prefix = _prefix()
    store = ObjectStoreDlqBackend(gcs_client, prefix=prefix)
    assert store.depth("missing") == 0
    assert store.append("a", [_dead(1), _dead(2)]) == 2
    assert store.append("a", []) == 2
    assert store.append("a", [_dead(3)], max_records=2) == 2
    assert [entry.record["id"] for entry in store.read("a")] == [2, 3]
    store.append("b", [_dead(4)])
    store.clear("b")
    assert store.all_depths() == {"a": 2}
    store.replace("a", [_dead(5)])
    assert [entry.record["id"] for entry in store.read("a")] == [5]
    assert store.append("uncapped", [_dead(index) for index in range(20)], max_records=0) == 20

    corrupt_key = f"{prefix}/dlq/corrupt.jsonl"
    store.append("corrupt", [_dead(6)])
    body, token = gcs_client.read_for_update(corrupt_key)
    assert body is not None
    gcs_client.write_if(
        corrupt_key, body + b"not json\n{\"unexpected\": true}\n", token
    )
    assert [entry.record["id"] for entry in store.read("corrupt")] == [6]


class CountingClient:
    def __init__(self, inner: ObjectClient) -> None:
        self.inner = inner
        self.write_attempts = 0
        self._lock = threading.Lock()

    def read_for_update(self, key: str) -> tuple[bytes | None, Token]:
        return self.inner.read_for_update(key)

    def write_if(self, key: str, body: bytes, token: Token) -> Token:
        with self._lock:
            self.write_attempts += 1
        return self.inner.write_if(key, body, token)

    def list_keys(self, prefix: str) -> list[str]:
        return self.inner.list_keys(prefix)


def test_concurrent_state_writers_never_silently_lose_updates(
    gcs_client: ObjectClient,
) -> None:
    workers = 8
    counting = CountingClient(gcs_client)
    prefix = _prefix()
    stores = [ObjectStoreStateStore(counting, prefix=prefix) for _ in range(workers)]
    barrier = threading.Barrier(workers)

    def write(index: int) -> tuple[str, Exception | None]:
        name = f"sync-{index}"
        barrier.wait()
        try:
            stores[index].save_sync(_state(name, index))
            return name, None
        except StateContentionError as exc:
            return name, exc

    with ThreadPoolExecutor(max_workers=workers) as pool:
        outcomes = list(pool.map(write, range(workers)))

    final = stores[0].get_all()
    for name, error in outcomes:
        if error is None:
            assert name in final, f"successful write for {name} silently vanished"
        else:
            assert isinstance(error, StateContentionError)
    assert counting.write_attempts <= workers * stores[0].MAX_WRITE_ATTEMPTS


def test_same_sync_race_finishes_with_one_acknowledged_value(
    gcs_client: ObjectClient,
) -> None:
    workers = 6
    counting = CountingClient(gcs_client)
    prefix = _prefix()
    stores = [ObjectStoreStateStore(counting, prefix=prefix) for _ in range(workers)]
    barrier = threading.Barrier(workers)

    def write(index: int) -> tuple[int, Exception | None]:
        barrier.wait()
        try:
            stores[index].save_sync(_state("shared", index))
            return index, None
        except StateContentionError as exc:
            return index, exc

    with ThreadPoolExecutor(max_workers=workers) as pool:
        outcomes = list(pool.map(write, range(workers)))

    successful_values = {index for index, error in outcomes if error is None}
    final = stores[0].get_last_sync("shared")
    assert final is not None
    assert final.records_synced in successful_values
    assert all(error is None or isinstance(error, StateContentionError) for _, error in outcomes)
    assert counting.write_attempts <= workers * stores[0].MAX_WRITE_ATTEMPTS
