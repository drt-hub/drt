"""Tests for --output json flag on drt status."""

from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from drt.cli.main import app
from drt.state.manager import StateManager, SyncState

runner = CliRunner()

TS = "2024-01-01T00:00:00+00:00"

# ---------------------------------------------------------------------------
# drt status --output json
# ---------------------------------------------------------------------------


def test_status_json_empty(tmp_path: Path, monkeypatch: object) -> None:
    import pytest

    mp = pytest.MonkeyPatch()
    mp.chdir(tmp_path)
    result = runner.invoke(app, ["status", "--output", "json"])
    data = json.loads(result.output)
    assert data["syncs"] == []
    mp.undo()


def test_status_json_with_state(
    tmp_path: Path, monkeypatch: object
) -> None:
    import pytest

    mp = pytest.MonkeyPatch()
    mp.chdir(tmp_path)

    mgr = StateManager(tmp_path)
    mgr.save_sync(SyncState("sync_a", TS, 42, "success"))
    mgr.save_sync(
        SyncState(
            "sync_b", TS, 5, "partial",
            error="row 3 failed", last_cursor_value="100",
        )
    )

    result = runner.invoke(app, ["status", "--output", "json"])
    data = json.loads(result.output)

    assert len(data["syncs"]) == 2
    a = next(s for s in data["syncs"] if s["name"] == "sync_a")
    b = next(s for s in data["syncs"] if s["name"] == "sync_b")

    assert a["status"] == "success"
    assert a["records_synced"] == 42
    assert a["error"] is None
    assert a["last_cursor_value"] is None

    assert b["status"] == "partial"
    assert b["error"] == "row 3 failed"
    assert b["last_cursor_value"] == "100"

    mp.undo()


def test_status_json_no_rich_markup(
    tmp_path: Path, monkeypatch: object
) -> None:
    """JSON output should not contain Rich markup like [green]."""
    import pytest

    mp = pytest.MonkeyPatch()
    mp.chdir(tmp_path)

    mgr = StateManager(tmp_path)
    mgr.save_sync(SyncState("s", TS, 1, "success"))

    result = runner.invoke(app, ["status", "--output", "json"])
    assert "[green]" not in result.output
    assert "[red]" not in result.output
    # Should be valid JSON
    json.loads(result.output)

    mp.undo()
