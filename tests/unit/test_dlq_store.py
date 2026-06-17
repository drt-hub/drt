"""Tests for drt.state.dlq — the Dead Letter Queue store (#278)."""

from __future__ import annotations

from pathlib import Path

from drt.state.dlq import DeadLetter, DlqStore


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
