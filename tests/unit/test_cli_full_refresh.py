"""Tests for `drt run --full-refresh` (#776).

Resets the watermark for the selected syncs, then runs normally so the new
watermark persists. dbt's `--full-refresh` is the mental model users arrive
with: "re-read everything", not "reset every kind of state".

**Deliberately watermark-only.** It does not touch `_drt_synced_keys` — that
re-baselines the destination and makes application-written rows deletion
candidates on the next mirror pass (#686). That has to be asked for by name,
via `drt state reset --tracked-mirror`.

The trap these tests exist for: `engine/sync.py` resolves a cursor from
`watermark_storage` first and from `StateManager.last_cursor_value` only when
no storage is configured — an `elif`. So a reset that clears one but not the
other is silently a no-op in whichever configuration it missed, and "no
watermark backend" is the default.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.state.manager import StateManager, SyncState

runner = CliRunner()


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    # Isolate ~/.drt so the test doesn't depend on the developer's profiles
    # (same approach as test_cli_profile.py).
    home = tmp_path / ".drt_home"
    home.mkdir()
    monkeypatch.setattr("drt.config.credentials._config_dir", lambda override=None: home)
    (home / "profiles.yml").write_text(
        yaml.safe_dump({"profiles": {"dev": {"type": "duckdb", "database": ":memory:"}}})
    )
    (tmp_path / "syncs").mkdir()
    (tmp_path / "drt_project.yml").write_text(
        yaml.safe_dump({"name": "p", "profile": "dev", "version": "1"})
    )
    (tmp_path / "syncs" / "users.yml").write_text(
        yaml.safe_dump(
            {
                "name": "users",
                "model": "SELECT 1 AS id",
                "destination": {
                    "type": "file",
                    "format": "csv",
                    "path": str(tmp_path / "out.csv"),
                },
                "sync": {"mode": "incremental", "cursor_field": "id"},
            }
        )
    )
    StateManager(tmp_path).save_sync(
        SyncState(
            sync_name="users",
            last_run_at="2026-01-01T00:00:00Z",
            records_synced=1,
            status="success",
            last_cursor_value="2026-06-01",
        )
    )
    return tmp_path


def test_clears_the_fallback_cursor_when_no_backend_is_configured(project: Path) -> None:
    """The default configuration — and the one an isolated watermark reset
    would silently miss.

    Asserts the *stale* cursor is gone rather than that no cursor exists: a
    real (non-dry) run re-reads from the start and then persists a fresh
    watermark, which is the whole point. What must not survive is the old
    value, since that is what would have limited the re-read.
    """
    runner.invoke(app, ["run", "--select", "users", "--full-refresh"])

    after = StateManager(project).get_last_sync("users")
    assert after is None or after.last_cursor_value != "2026-06-01", (
        "the stale watermark survived --full-refresh"
    )


def test_clears_a_configured_watermark_backend(project: Path) -> None:
    storage = MagicMock()
    with patch("drt.cli.commands.run.get_watermark_storage", return_value=storage):
        runner.invoke(app, ["run", "--select", "users", "--full-refresh", "--dry-run"])

    storage.delete.assert_called_once_with("users")


def test_without_the_flag_nothing_is_reset(project: Path) -> None:
    storage = MagicMock()
    with patch("drt.cli.commands.run.get_watermark_storage", return_value=storage):
        runner.invoke(app, ["run", "--select", "users", "--dry-run"])

    storage.delete.assert_not_called()
    after = StateManager(project).get_last_sync("users")
    assert after is not None and after.last_cursor_value == "2026-06-01"


def test_never_touches_tracked_mirror_state(project: Path) -> None:
    """--full-refresh is watermark-only. Re-baselining the destination is a
    separate, explicit decision (#686)."""
    with patch("drt.cli.commands.state._reset_tracked_state") as reset_tracked:
        runner.invoke(app, ["run", "--select", "users", "--full-refresh", "--dry-run"])

    reset_tracked.assert_not_called()
