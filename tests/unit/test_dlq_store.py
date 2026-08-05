"""Tests for drt.state.dlq — the Dead Letter Queue store (#278)."""

from __future__ import annotations

from pathlib import Path

from drt.state.dlq import DeadLetter, DlqStore
from tests.conftest import public_methods


def _dl(value: int, *, attempts: int = 1) -> DeadLetter:
    return DeadLetter(
        record={"id": value},
        error_message=f"boom {value}",
        http_status=500,
        timestamp="2026-06-11T00:00:00Z",
        attempts=attempts,
    )


def test_depth_zero_when_no_queue(tmp_path: Path) -> None:
    assert DlqStore(tmp_path).depth("missing") == 0


def test_append_then_read_roundtrips(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    store.append("s", [_dl(1), _dl(2)])

    entries = store.read("s")
    assert [e.record["id"] for e in entries] == [1, 2]
    assert entries[0].error_message == "boom 1"
    assert entries[0].http_status == 500
    assert entries[0].attempts == 1


def test_append_is_additive_and_returns_depth(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    assert store.append("s", [_dl(1)]) == 1
    assert store.append("s", [_dl(2), _dl(3)]) == 3
    assert store.depth("s") == 3


def test_append_empty_is_noop(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    store.append("s", [_dl(1)])
    assert store.append("s", []) == 1
    assert store.depth("s") == 1


def test_file_lives_under_dlq_subdir(tmp_path: Path) -> None:
    DlqStore(tmp_path).append("my_sync", [_dl(1)])
    assert (tmp_path / ".drt" / "dlq" / "my_sync.jsonl").exists()


def test_max_records_cap_keeps_newest(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    store.append("s", [_dl(i) for i in range(5)], max_records=3)
    ids = [e.record["id"] for e in store.read("s")]
    assert ids == [2, 3, 4]  # oldest two dropped (FIFO cap)


def test_max_records_zero_disables_cap(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    store.append("s", [_dl(i) for i in range(50)], max_records=0)
    assert store.depth("s") == 50


def test_replace_overwrites_queue(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    store.append("s", [_dl(1), _dl(2), _dl(3)])
    store.replace("s", [_dl(2, attempts=2)])

    entries = store.read("s")
    assert len(entries) == 1
    assert entries[0].record["id"] == 2
    assert entries[0].attempts == 2


def test_replace_empty_removes_file(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    store.append("s", [_dl(1)])
    store.replace("s", [])
    assert store.depth("s") == 0
    assert not (tmp_path / ".drt" / "dlq" / "s.jsonl").exists()


def test_clear_is_replace_empty(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    store.append("s", [_dl(1), _dl(2)])
    store.clear("s")
    assert store.depth("s") == 0


def test_read_skips_corrupt_lines(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    store.append("s", [_dl(1)])
    path = tmp_path / ".drt" / "dlq" / "s.jsonl"
    path.write_text(path.read_text() + "not json\n" + '{"unexpected": true}\n')

    entries = store.read("s")
    # Valid line survives; the bare-string line and the schema-mismatched
    # line are both skipped rather than aborting the whole read.
    assert [e.record["id"] for e in entries] == [1]


def test_all_depths_reports_every_nonempty_queue(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    store.append("alpha", [_dl(1), _dl(2)])
    store.append("beta", [_dl(3)])
    store.clear("beta")
    store.append("gamma", [_dl(4)])

    assert store.all_depths() == {"alpha": 2, "gamma": 1}


def test_all_depths_empty_when_no_dir(tmp_path: Path) -> None:
    assert DlqStore(tmp_path).all_depths() == {}


# --- DlqBackend Protocol (#756) ----------------------------------------------

_DLQ_BACKEND_METHODS = {"append", "replace", "clear", "read", "depth", "all_depths"}


def test_local_dlq_store_satisfies_dlq_backend(tmp_path: Path) -> None:
    from drt.state.dlq import DlqBackend, LocalDlqStore

    assert isinstance(LocalDlqStore(tmp_path), DlqBackend)


def test_dlq_store_alias_preserved() -> None:
    from drt.state.dlq import DlqStore, LocalDlqStore

    assert DlqStore is LocalDlqStore


def test_dlq_backend_protocol_covers_local_public_api() -> None:
    from drt.state.dlq import LocalDlqStore

    assert public_methods(LocalDlqStore) == _DLQ_BACKEND_METHODS


# --- Correlation ID (#762) ----------------------------------------------------


def test_sync_run_id_defaults_to_none() -> None:
    assert _dl(1).sync_run_id is None


def test_sync_run_id_round_trips_through_append_and_read(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    entry = DeadLetter(
        record={"id": 1}, error_message="boom", http_status=500, sync_run_id="sync-1"
    )
    store.append("s", [entry])

    [loaded] = store.read("s")
    assert loaded.sync_run_id == "sync-1"


def test_a_pre_762_jsonl_line_still_loads(tmp_path: Path) -> None:
    """`read()` unpacks each line via DeadLetter(**data); a line written
    before this field existed has no sync_run_id key at all, and must still
    load — via the dataclass default, not a migration."""
    import json

    store = DlqStore(tmp_path)
    path = tmp_path / ".drt" / "dlq" / "s.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    old_format = {
        "record": {"id": 1},
        "error_message": "boom",
        "http_status": 500,
        "timestamp": "2026-01-01T00:00:00Z",
        "attempts": 1,
        # no sync_run_id key at all
    }
    path.write_text(json.dumps(old_format) + "\n")

    [loaded] = store.read("s")
    assert loaded.sync_run_id is None
