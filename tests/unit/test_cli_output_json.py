"""Tests for --output json flag on drt run and drt status."""

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


# ---------------------------------------------------------------------------
# drt run --output json
# ---------------------------------------------------------------------------


def test_run_json_no_project(tmp_path: Path, monkeypatch: object) -> None:
    """run --output json without drt_project.yml should exit 1."""
    import pytest

    mp = pytest.MonkeyPatch()
    mp.chdir(tmp_path)
    result = runner.invoke(app, ["run", "--output", "json"])
    assert result.exit_code == 1
    mp.undo()


def test_run_json_no_syncs(tmp_path: Path, monkeypatch: object) -> None:
    """run --output json with no syncs should not print rich output."""
    import pytest
    import yaml

    mp = pytest.MonkeyPatch()
    mp.chdir(tmp_path)

    # Create minimal project file
    (tmp_path / "drt_project.yml").write_text(yaml.dump({"version": "0.1", "profile": "default"}))
    # Create empty credentials
    creds_dir = tmp_path / ".drt"
    creds_dir.mkdir()
    (creds_dir / "credentials.yml").write_text(
        yaml.dump({"profiles": {"default": {"type": "duckdb"}}})
    )

    result = runner.invoke(app, ["run", "--output", "json"])
    # Should not contain rich markup
    assert "[dim]" not in result.output
    mp.undo()


# ---------------------------------------------------------------------------
# drt status --output json
# ---------------------------------------------------------------------------


def test_status_json_with_state(tmp_path: Path, monkeypatch: object) -> None:
    import pytest

    mp = pytest.MonkeyPatch()
    mp.chdir(tmp_path)

    mgr = StateManager(tmp_path)
    mgr.save_sync(SyncState("sync_a", TS, 42, "success"))
    mgr.save_sync(
        SyncState(
            "sync_b",
            TS,
            5,
            "partial",
            error="row 3 failed",
            last_cursor_value="100",
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


def test_status_json_no_rich_markup(tmp_path: Path, monkeypatch: object) -> None:
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


# ---------------------------------------------------------------------------
# drt list --output json
# ---------------------------------------------------------------------------


def test_list_json(tmp_path: Path, monkeypatch: object) -> None:
    import pytest
    import yaml

    mp = pytest.MonkeyPatch()
    mp.chdir(tmp_path)

    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    with (syncs_dir / "test.yml").open("w") as f:
        yaml.dump(
            {
                "name": "my-sync",
                "model": "SELECT 1",
                "destination": {
                    "type": "rest_api",
                    "url": "http://example.com",
                    "method": "POST",
                },
            },
            f,
        )

    result = runner.invoke(app, ["list", "--output", "json"])
    data = json.loads(result.output)
    assert len(data["syncs"]) == 1
    assert data["syncs"][0]["name"] == "my-sync"
    assert data["syncs"][0]["destination_type"] == "rest_api"
    assert data["syncs"][0]["mode"] == "full"

    mp.undo()


# ---------------------------------------------------------------------------
# drt validate --output json
# ---------------------------------------------------------------------------


def test_validate_json_valid(tmp_path: Path, monkeypatch: object) -> None:
    import pytest
    import yaml

    mp = pytest.MonkeyPatch()
    mp.chdir(tmp_path)

    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    with (syncs_dir / "ok.yml").open("w") as f:
        yaml.dump(
            {
                "name": "good",
                "model": "SELECT 1",
                "destination": {
                    "type": "rest_api",
                    "url": "http://example.com",
                    "method": "POST",
                },
            },
            f,
        )

    result = runner.invoke(app, ["validate", "--output", "json"])
    data = json.loads(result.output)
    assert len(data["results"]) == 1
    assert data["results"][0]["valid"] is True
    assert result.exit_code == 0

    mp.undo()


def test_validate_json_invalid(tmp_path: Path, monkeypatch: object) -> None:
    import pytest
    import yaml

    mp = pytest.MonkeyPatch()
    mp.chdir(tmp_path)

    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    with (syncs_dir / "bad.yml").open("w") as f:
        yaml.dump({"name": "broken"}, f)

    result = runner.invoke(app, ["validate", "--output", "json"])
    data = json.loads(result.output)
    assert any(r["valid"] is False for r in data["results"])
    assert result.exit_code == 1

    mp.undo()
