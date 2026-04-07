"""Tests for incremental sync state persistence edge cases."""

from __future__ import annotations

import json
from pathlib import Path

from drt.state.manager import StateManager, SyncState

TS1 = "2024-01-01T00:00:00+00:00"
TS2 = "2024-01-02T00:00:00+00:00"

# ---------------------------------------------------------------------------
# Cursor persistence
# ---------------------------------------------------------------------------


def test_cursor_value_saved_and_loaded(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    state = SyncState(
        sync_name="inc_sync",
        last_run_at=TS1,
        records_synced=10,
        status="success",
        last_cursor_value="2024-06-15T12:00:00",
    )
    mgr.save_sync(state)

    loaded = mgr.get_last_sync("inc_sync")
    assert loaded is not None
    assert loaded.last_cursor_value == "2024-06-15T12:00:00"


def test_cursor_none_for_full_sync(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    state = SyncState(
        sync_name="full_sync",
        last_run_at=TS1,
        records_synced=5,
        status="success",
        last_cursor_value=None,
    )
    mgr.save_sync(state)

    loaded = mgr.get_last_sync("full_sync")
    assert loaded is not None
    assert loaded.last_cursor_value is None


def test_cursor_updated_on_subsequent_run(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    mgr.save_sync(
        SyncState("s", TS1, 10, "success", last_cursor_value="100")
    )
    mgr.save_sync(
        SyncState("s", TS2, 20, "success", last_cursor_value="200")
    )

    loaded = mgr.get_last_sync("s")
    assert loaded is not None
    assert loaded.last_cursor_value == "200"


def test_cursor_preserved_across_different_syncs(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    mgr.save_sync(
        SyncState("sync_a", TS1, 10, "success", last_cursor_value="100")
    )
    mgr.save_sync(
        SyncState("sync_b", TS1, 20, "success", last_cursor_value="200")
    )

    a = mgr.get_last_sync("sync_a")
    b = mgr.get_last_sync("sync_b")
    assert a is not None and a.last_cursor_value == "100"
    assert b is not None and b.last_cursor_value == "200"


# ---------------------------------------------------------------------------
# Partial failure state
# ---------------------------------------------------------------------------


def test_partial_failure_state(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    state = SyncState(
        sync_name="partial",
        last_run_at=TS1,
        records_synced=5,
        status="partial",
        error="row 3 failed: 500",
        last_cursor_value="50",
    )
    mgr.save_sync(state)

    loaded = mgr.get_last_sync("partial")
    assert loaded is not None
    assert loaded.status == "partial"
    assert loaded.error == "row 3 failed: 500"
    assert loaded.last_cursor_value == "50"


def test_failed_state_preserves_cursor(tmp_path: Path) -> None:
    """Even on failure, the previous cursor is overwritten by the new state."""
    mgr = StateManager(tmp_path)
    mgr.save_sync(
        SyncState("s", TS1, 10, "success", last_cursor_value="100")
    )
    mgr.save_sync(
        SyncState("s", TS2, 0, "failed", error="timeout")
    )

    loaded = mgr.get_last_sync("s")
    assert loaded is not None
    assert loaded.status == "failed"
    assert loaded.last_cursor_value is None


# ---------------------------------------------------------------------------
# Corrupted state
# ---------------------------------------------------------------------------


def test_corrupted_json_resets(tmp_path: Path) -> None:
    state_dir = tmp_path / ".drt"
    state_dir.mkdir()
    (state_dir / "state.json").write_text("{invalid json!!!")

    mgr = StateManager(tmp_path)
    assert mgr.get_last_sync("any") is None
    assert mgr.get_all() == {}


def test_empty_state_file(tmp_path: Path) -> None:
    state_dir = tmp_path / ".drt"
    state_dir.mkdir()
    (state_dir / "state.json").write_text("")

    mgr = StateManager(tmp_path)
    assert mgr.get_last_sync("any") is None


def test_null_state_file(tmp_path: Path) -> None:
    state_dir = tmp_path / ".drt"
    state_dir.mkdir()
    (state_dir / "state.json").write_text("null")

    mgr = StateManager(tmp_path)
    assert mgr.get_last_sync("any") is None


# ---------------------------------------------------------------------------
# State file persistence format
# ---------------------------------------------------------------------------


def test_state_file_is_valid_json(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    mgr.save_sync(
        SyncState("s", TS1, 10, "success", last_cursor_value="42")
    )

    raw = json.loads((tmp_path / ".drt" / "state.json").read_text())
    assert "s" in raw
    assert raw["s"]["last_cursor_value"] == "42"
    assert raw["s"]["status"] == "success"


def test_save_does_not_corrupt_other_syncs(tmp_path: Path) -> None:
    """Saving one sync must not lose or alter other syncs."""
    mgr = StateManager(tmp_path)
    mgr.save_sync(
        SyncState("first", TS1, 10, "success", last_cursor_value="A")
    )
    mgr.save_sync(
        SyncState("second", TS2, 20, "success", last_cursor_value="B")
    )

    first = mgr.get_last_sync("first")
    assert first is not None
    assert first.last_cursor_value == "A"
    assert first.records_synced == 10


def test_state_deleted_starts_fresh(tmp_path: Path) -> None:
    """If state.json is deleted between runs, next run starts fresh."""
    mgr = StateManager(tmp_path)
    mgr.save_sync(
        SyncState("s", TS1, 10, "success", last_cursor_value="100")
    )

    (tmp_path / ".drt" / "state.json").unlink()
    assert mgr.get_last_sync("s") is None
