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


@pytest.mark.parametrize("body", [b"{malformed", b"[]"])
def test_state_corrupted_json_returns_empty_and_warns(
    body: bytes, capsys: pytest.CaptureFixture[str]
) -> None:
    client = MemoryObjectClient()
    client.objects["state.json"] = body
    store = ObjectStoreStateStore(client)

    assert store.get_all() == {}
    assert capsys.readouterr().err == (
        "Warning: remote state.json is corrupted and will be reset.\n"
    )


def test_state_reset_raises_after_bounded_contention_attempts() -> None:
    client = MemoryObjectClient()
    store = ObjectStoreStateStore(client)
    store.save_sync(_state())
    client.always_conflict = True
    client.writes = 0

    with (
        patch("drt.state._objectstore.time.sleep"),
        pytest.raises(
            StateContentionError,
            match="state reset for 's' exhausted 8 conditional-write attempts",
        ),
    ):
        store.reset("s")

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


def test_history_append_skips_malformed_line_and_keeps_valid_entries(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = MemoryObjectClient()
    first = _history(started_at="2026-08-01T00:00:00+00:00")
    second = _history(started_at="2026-08-02T00:00:00+00:00")
    third = _history(started_at="2026-08-03T00:00:00+00:00")
    client.objects["history/s.jsonl"] = (
        ObjectStoreHistoryStore._encode([first])
        + b"\n{malformed\n"
        + ObjectStoreHistoryStore._encode([second])
    )

    store = ObjectStoreHistoryStore(client)
    with caplog.at_level(logging.WARNING, logger="drt.state._objectstore"):
        store.append(third)

    assert [entry.started_at for entry in store.read("s")] == [
        third.started_at,
        second.started_at,
        first.started_at,
    ]
    assert any(
        "history: skipping malformed line 3 in history/s.jsonl" in record.message
        for record in caplog.records
    )


def test_history_append_non_precondition_error_warns_without_retry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = MemoryObjectClient()
    store = ObjectStoreHistoryStore(client)

    with (
        patch.object(client, "write_if", side_effect=RuntimeError("network down")) as write,
        caplog.at_level(logging.WARNING, logger="drt.state._objectstore"),
    ):
        store.append(_history())

    assert write.call_count == 1
    assert client.reads == 1
    assert any(
        "history append failed for sync=s: network down" in record.message
        for record in caplog.records
    )


def test_remote_history_applies_entry_cap_only_during_prune() -> None:
    client = MemoryObjectClient()
    store = ObjectStoreHistoryStore(client, max_entries=2)
    for day in range(1, 4):
        store.append(_history(started_at=f"2026-08-0{day}T00:00:00+00:00"))

    assert len(store.read("s", limit=20)) == 3
    assert store.prune("s", retention_days=30) == 1
    assert [entry.started_at[9] for entry in store.read("s")] == ["3", "2"]


def test_prune_caps_by_started_at_not_append_order() -> None:
    """Two overlapping runs of the same sync can finish out of start order.

    read() defines "newest" by started_at, and max_entries promises the
    newest entries survive — the object's on-disk order (append order) must
    not decide which entries the cap keeps.
    """
    client = MemoryObjectClient()
    store = ObjectStoreHistoryStore(client, max_entries=2)
    # Appended oldest-started-at last, as if a later-starting run's batch
    # observer flushed and completed before an earlier-starting run did.
    store.append(_history(started_at="2026-08-03T00:00:00+00:00"))
    store.append(_history(started_at="2026-08-01T00:00:00+00:00"))
    store.append(_history(started_at="2026-08-02T00:00:00+00:00"))

    assert store.prune("s", retention_days=30) == 1
    assert [entry.started_at[9] for entry in store.read("s")] == ["3", "2"]


def test_history_reads_all_syncs_newest_first_and_prunes_old_entries() -> None:
    client = MemoryObjectClient()
    store = ObjectStoreHistoryStore(client)
    store.append(_history("a", started_at="2026-08-01T00:00:00+00:00"))
    store.append(_history("b", started_at="2026-08-02T00:00:00+00:00"))

    assert [entry.sync_name for entry in store.read()] == ["b", "a"]
    assert store.prune("missing", retention_days=30) == 0


def test_history_prune_warns_and_returns_zero_after_bounded_contention(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = MemoryObjectClient()
    store = ObjectStoreHistoryStore(client)
    store.append(_history(started_at="2000-01-01T00:00:00+00:00"))
    client.always_conflict = True
    client.writes = 0

    with (
        patch("drt.state._objectstore.time.sleep"),
        caplog.at_level(logging.WARNING, logger="drt.state._objectstore"),
    ):
        assert store.prune("s", retention_days=30) == 0

    assert client.writes == store.MAX_WRITE_ATTEMPTS
    assert any(
        "history prune failed for sync=s after 8 contention attempts" in record.message
        for record in caplog.records
    )


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


def test_dlq_decode_silently_skips_malformed_lines(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = MemoryObjectClient()
    client.objects["dlq/s.jsonl"] = (
        ObjectStoreDlqBackend._encode([_dead(1)])
        + b"\n{malformed\n"
        + ObjectStoreDlqBackend._encode([_dead(2)])
    )
    store = ObjectStoreDlqBackend(client)

    with caplog.at_level(logging.WARNING, logger="drt.state._objectstore"):
        entries = store.read("s")

    assert [entry.record["id"] for entry in entries] == [1, 2]
    assert caplog.records == []


@pytest.mark.parametrize("operation", ["replace", "clear"])
def test_dlq_replace_operations_raise_after_bounded_contention(
    operation: str,
) -> None:
    client = MemoryObjectClient(always_conflict=True)
    store = ObjectStoreDlqBackend(client)

    with (
        patch("drt.state._objectstore.time.sleep"),
        pytest.raises(
            ObjectPreconditionError,
            match="DLQ replace for 's' exhausted 8 attempts",
        ),
    ):
        if operation == "replace":
            store.replace("s", [_dead(1)])
        else:
            store.clear("s")

    assert client.writes == store.MAX_WRITE_ATTEMPTS


def test_dlq_append_non_precondition_error_warns_and_returns_existing_depth(
    caplog: pytest.LogCaptureFixture,
) -> None:
    client = MemoryObjectClient()
    store = ObjectStoreDlqBackend(client)
    assert store.append("s", [_dead(1)]) == 1

    with (
        patch.object(client, "write_if", side_effect=RuntimeError("network down")) as write,
        caplog.at_level(logging.WARNING, logger="drt.state._objectstore"),
    ):
        depth = store.append("s", [_dead(2)])

    assert depth == 1
    assert write.call_count == 1
    assert any(
        "DLQ append failed for sync=s: network down" in record.message
        for record in caplog.records
    )


def test_dlq_reconcile_removes_and_updates_by_id_leaves_others_untouched() -> None:
    store = ObjectStoreDlqBackend(MemoryObjectClient())
    store.append("s", [_dead(1), _dead(2), _dead(3)])
    [e1, e2, e3] = store.read("s")
    bumped = DeadLetter(
        id=e3.id, record=e3.record, error_message="still failing", attempts=2
    )

    result = store.reconcile("s", remove_ids={e2.id}, updates={e3.id: bumped})

    by_id = {e.id: e for e in result}
    assert set(by_id) == {e1.id, e3.id}
    assert by_id[e3.id].attempts == 2
    assert by_id[e3.id].error_message == "still failing"
    assert [e.id for e in store.read("s")] == [e1.id, e3.id]


def test_dlq_reconcile_survives_a_racing_writer() -> None:
    """A write conflict mid-reconcile (another writer's generation landed
    first) re-reads fresh state and retries — the #955 fix's whole point:
    ``replace()``'s retry loop retries the *same* stale content on conflict,
    ``reconcile()`` re-derives it from a fresh read each attempt."""
    client = MemoryObjectClient(failures=2)  # first 2 write_if calls conflict
    store = ObjectStoreDlqBackend(client)
    store.append("s", [_dead(1)])
    [e1] = store.read("s")

    with patch("drt.state._objectstore.time.sleep"):
        result = store.reconcile("s", remove_ids={e1.id})

    assert result == []
    assert store.depth("s") == 0


def test_dlq_reconcile_raises_after_bounded_contention() -> None:
    client = MemoryObjectClient(always_conflict=True)
    store = ObjectStoreDlqBackend(client)

    with (
        patch("drt.state._objectstore.time.sleep"),
        pytest.raises(
            ObjectPreconditionError,
            match="DLQ reconcile for 's' exhausted 8 attempts",
        ),
    ):
        store.reconcile("s", remove_ids={"whatever"})

    assert client.writes == store.MAX_WRITE_ATTEMPTS


def test_dlq_all_depths_skips_keys_outside_expected_prefix_or_suffix() -> None:
    client = MemoryObjectClient()
    client.objects["project/dlq/alpha.jsonl"] = ObjectStoreDlqBackend._encode(
        [_dead(1)]
    )
    client.objects["other/dlq/unrelated.jsonl"] = b"not a DLQ object"
    client.objects["project/dlq/readme.txt"] = b"not a DLQ object"
    store = ObjectStoreDlqBackend(client, prefix="project")

    with patch.object(
        client,
        "list_keys",
        return_value=[
            "other/dlq/unrelated.jsonl",
            "project/dlq/readme.txt",
            "project/dlq/alpha.jsonl",
        ],
    ):
        assert store.all_depths() == {"alpha": 1}

    assert client.reads == 1


def test_dlq_jsonl_is_byte_compatible_with_local(tmp_path: Path) -> None:
    entries = [_dead(1), _dead(2)]
    local = LocalDlqStore(tmp_path)
    local.append("s", entries)
    client = MemoryObjectClient()
    ObjectStoreDlqBackend(client).append("s", entries)

    assert client.objects["dlq/s.jsonl"] == (
        tmp_path / ".drt" / "dlq" / "s.jsonl"
    ).read_bytes()
