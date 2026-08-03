"""Tests for StateManager."""

from __future__ import annotations

from pathlib import Path

from drt.state.manager import StateManager, SyncState


def test_get_last_sync_missing(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    assert mgr.get_last_sync("no_such_sync") is None


def test_save_and_get(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    state = SyncState(
        sync_name="my_sync",
        last_run_at="2024-01-01T00:00:00+00:00",
        records_synced=42,
        status="success",
    )
    mgr.save_sync(state)

    loaded = mgr.get_last_sync("my_sync")
    assert loaded is not None
    assert loaded.sync_name == "my_sync"
    assert loaded.records_synced == 42
    assert loaded.status == "success"
    assert loaded.error is None


def test_save_overwrites(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    mgr.save_sync(SyncState("s", "2024-01-01T00:00:00+00:00", 10, "success"))
    mgr.save_sync(SyncState("s", "2024-01-02T00:00:00+00:00", 20, "partial", error="oops"))

    loaded = mgr.get_last_sync("s")
    assert loaded is not None
    assert loaded.records_synced == 20
    assert loaded.error == "oops"


def test_get_all(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    mgr.save_sync(SyncState("a", "2024-01-01T00:00:00+00:00", 1, "success"))
    mgr.save_sync(SyncState("b", "2024-01-01T00:00:00+00:00", 2, "failed"))

    all_states = mgr.get_all()
    assert set(all_states.keys()) == {"a", "b"}
    assert all_states["b"].status == "failed"


def test_creates_drt_dir_on_demand(tmp_path: Path) -> None:
    mgr = StateManager(tmp_path)
    assert not (tmp_path / ".drt").exists()

    mgr.save_sync(SyncState("s", "2024-01-01T00:00:00+00:00", 0, "success"))
    assert (tmp_path / ".drt" / "state.json").exists()


class TestReset:
    """#776: `drt state reset --runs` clears one sync's recorded run state.

    Also clears `last_cursor_value`. That field is the **fallback watermark**:
    `drt/engine/sync.py` consults `watermark_storage` first and the state
    manager's cursor only when no storage is configured (an `elif` chain). A
    reset that left it behind would silently do nothing for every project
    without a configured watermark backend — which is the default.
    """

    def _state(self, name: str, cursor: str | None = None) -> SyncState:
        return SyncState(
            sync_name=name,
            last_run_at="2026-01-01T00:00:00Z",
            records_synced=10,
            status="success",
            last_cursor_value=cursor,
        )

    def test_reset_removes_only_that_sync(self, tmp_path: Path) -> None:
        mgr = StateManager(tmp_path)
        mgr.save_sync(self._state("a"))
        mgr.save_sync(self._state("b"))

        mgr.reset("a")

        assert mgr.get_last_sync("a") is None
        assert mgr.get_last_sync("b") is not None, "an unrelated sync was cleared"

    def test_reset_clears_the_fallback_watermark(self, tmp_path: Path) -> None:
        """The trap: with no watermark storage configured, this cursor *is*
        the watermark, so leaving it behind makes the reset a no-op."""
        mgr = StateManager(tmp_path)
        mgr.save_sync(self._state("a", cursor="2026-06-01T00:00:00Z"))

        mgr.reset("a")

        after = mgr.get_last_sync("a")
        assert after is None or after.last_cursor_value is None

    def test_reset_unknown_sync_is_a_noop(self, tmp_path: Path) -> None:
        mgr = StateManager(tmp_path)
        mgr.save_sync(self._state("a"))

        mgr.reset("never-run")  # must not raise

        assert mgr.get_last_sync("a") is not None

    def test_reset_with_no_state_file_is_a_noop(self, tmp_path: Path) -> None:
        StateManager(tmp_path).reset("a")
