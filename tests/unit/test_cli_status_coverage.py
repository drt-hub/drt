"""Coverage tests for `drt status` text-mode rendering (#573 follow-up).

Covers the verbose / non-verbose dispatch at the end of the status
command (status.py:73-76) — both branches were silent in codecov on the
PR (b) move because no existing test exercised text-mode status without
``--history``.

Uses an empty StateManager (no state file present) so the renderers are
called with an empty mapping, which is enough to lock the dispatch wiring.
"""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()


@pytest.fixture
def empty_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "t", "version": "0.1", "profile": "default"})
    )
    return tmp_path


def test_status_text_mode_non_verbose_renders(empty_project: Path) -> None:
    """``drt status`` (default, no flags) takes the non-verbose branch."""
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0


def test_status_text_mode_verbose_renders(empty_project: Path) -> None:
    """``drt status --verbose`` takes the verbose branch."""
    result = runner.invoke(app, ["status", "--verbose"])
    assert result.exit_code == 0


def test_status_shows_dlq_depth_text(empty_project: Path) -> None:
    """A non-empty DLQ surfaces a warning line with the replay hint (#278)."""
    from drt.state.dlq import DeadLetter, DlqStore

    DlqStore(empty_project).append(
        "post_users", [DeadLetter(record={"id": 1}, error_message="boom")]
    )
    result = runner.invoke(app, ["status"])
    assert result.exit_code == 0
    assert "dead letter queue" in result.output.lower()
    assert "drt retry post_users" in result.output


def test_status_json_includes_dlq_depth(empty_project: Path) -> None:
    """JSON status reports dlq_depth per sync (#278)."""
    import json

    from drt.state.dlq import DeadLetter, DlqStore
    from drt.state.manager import StateManager, SyncState

    StateManager(empty_project).save_sync(
        SyncState(
            sync_name="post_users",
            last_run_at="2026-06-11T00:00:00Z",
            records_synced=0,
            status="partial",
        )
    )
    DlqStore(empty_project).append(
        "post_users",
        [DeadLetter(record={"id": i}, error_message="boom") for i in range(3)],
    )
    result = runner.invoke(app, ["status", "--output", "json"])
    assert result.exit_code == 0
    payload = json.loads(result.output)
    entry = next(s for s in payload["syncs"] if s["name"] == "post_users")
    assert entry["dlq_depth"] == 3
