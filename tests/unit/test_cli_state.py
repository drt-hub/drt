"""Tests for `drt state show` / `drt state reset` (#776).

The sanctioned replacement for hand-editing .drt/*.json — which the issue
notes does nothing at all for remote watermark backends, and nothing ever for
the tracked-mirror key table that lives in the destination.

Levels are deliberately separate (dlt's `drop_data`/`drop_resources`/
`drop_sources` lesson): `--tracked-mirror` re-baselines the destination's
`_drt_synced_keys`, which makes application-written rows deletion candidates
on the next mirror pass. That is the exact risk #686 exists to prevent, so it
is never implied by a broader flag.
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
    (tmp_path / "syncs").mkdir()
    (tmp_path / "drt_project.yml").write_text(
        yaml.safe_dump({"name": "p", "profile": "dev", "version": "1"})
    )
    mgr = StateManager(tmp_path)
    mgr.save_sync(
        SyncState(
            sync_name="users",
            last_run_at="2026-01-01T00:00:00Z",
            records_synced=42,
            status="success",
            last_cursor_value="2026-01-01",
        )
    )
    return tmp_path


class TestShow:
    def test_lists_known_syncs(self, project: Path) -> None:
        result = runner.invoke(app, ["state", "show"])
        assert result.exit_code == 0
        assert "users" in result.output

    def test_reports_a_sync_with_no_state(self, project: Path) -> None:
        result = runner.invoke(app, ["state", "show", "never-run"])
        assert result.exit_code == 0
        assert "no state" in result.output.lower()

    def test_shows_the_stored_cursor(self, project: Path) -> None:
        result = runner.invoke(app, ["state", "show", "users"])
        assert "2026-01-01" in result.output


class TestResetRequiresALevel:
    def test_no_flags_is_an_error_not_reset_everything(self, project: Path) -> None:
        """The safety property: an unqualified `reset` must never be taken to
        mean "all of it"."""
        result = runner.invoke(app, ["state", "reset", "users", "--yes"])

        assert result.exit_code != 0
        assert StateManager(project).get_last_sync("users") is not None
        assert "--watermark" in result.output


class TestResetRuns:
    def test_clears_run_state(self, project: Path) -> None:
        result = runner.invoke(app, ["state", "reset", "users", "--runs", "--yes"])

        assert result.exit_code == 0
        assert StateManager(project).get_last_sync("users") is None

    def test_dry_run_changes_nothing(self, project: Path) -> None:
        result = runner.invoke(
            app, ["state", "reset", "users", "--runs", "--dry-run", "--yes"]
        )

        assert result.exit_code == 0
        assert StateManager(project).get_last_sync("users") is not None, (
            "--dry-run wrote to state"
        )

    def test_refuses_without_yes_in_ci(self, project: Path) -> None:
        """EOF stdin — CliRunner with no input is exactly the CI shape."""
        result = runner.invoke(app, ["state", "reset", "users", "--runs"])

        assert result.exit_code != 0
        assert StateManager(project).get_last_sync("users") is not None


class TestResetTrackedMirrorIsOptIn:
    def test_runs_reset_does_not_touch_tracked_state(self, project: Path) -> None:
        """--runs must never reach the destination."""
        with patch("drt.cli.commands.state._reset_tracked_state") as reset_tracked:
            runner.invoke(app, ["state", "reset", "users", "--runs", "--yes"])

        reset_tracked.assert_not_called()


class TestShowEmptyProject:
    def test_reports_nothing_recorded(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.chdir(tmp_path)
        (tmp_path / "syncs").mkdir()
        (tmp_path / "drt_project.yml").write_text(
            yaml.safe_dump({"name": "p", "profile": "dev", "version": "1"})
        )

        result = runner.invoke(app, ["state", "show"])

        assert result.exit_code == 0
        assert "no state" in result.output.lower()


class TestResetWatermark:
    """The --watermark level, including the case with no backend configured.

    That path is worth pinning: with no watermark storage, the *fallback*
    cursor in .drt/state.json is the live watermark, so the command has to say
    so rather than silently reporting success at clearing nothing.
    """

    def test_reports_when_no_backend_is_configured(self, project: Path) -> None:
        result = runner.invoke(
            app, ["state", "reset", "users", "--watermark", "--yes"]
        )

        assert result.exit_code == 0
        assert "--runs" in result.output, (
            "with no backend the fallback cursor is cleared by --runs; say so"
        )

    def test_clears_a_configured_backend(self, project: Path) -> None:
        storage = MagicMock()
        with patch("drt.cli._helpers.get_watermark_storage", return_value=storage):
            result = runner.invoke(
                app, ["state", "reset", "users", "--watermark", "--yes"]
            )

        assert result.exit_code == 0


class TestResetDeclined:
    def test_declining_the_prompt_changes_nothing(self, project: Path) -> None:
        result = runner.invoke(app, ["state", "reset", "users", "--runs"], input="n\n")

        assert result.exit_code == 0
        assert "Aborted" in result.output
        assert StateManager(project).get_last_sync("users") is not None


class TestResetTrackedMirror:
    def test_warns_before_touching_the_destination(self, project: Path) -> None:
        """The re-baseline warning must appear — an operator has no other
        signal that this changes deletion semantics (#686)."""
        with patch("drt.cli.commands.state._reset_tracked_state", return_value=3):
            result = runner.invoke(
                app, ["state", "reset", "users", "--tracked-mirror", "--yes"]
            )

        assert result.exit_code == 0
        assert "re-baseline" in result.output.lower()
        assert "3" in result.output

    def test_reports_when_there_was_nothing_to_clear(self, project: Path) -> None:
        with patch("drt.cli.commands.state._reset_tracked_state", return_value=0):
            result = runner.invoke(
                app, ["state", "reset", "users", "--tracked-mirror", "--yes"]
            )

        assert "no tracked-mirror state" in result.output.lower()

    def test_unknown_sync_is_reported_not_crashed(self, project: Path) -> None:
        from drt.cli.commands.state import _reset_tracked_state

        removed = _reset_tracked_state("no-such-sync")

        assert removed == 0
