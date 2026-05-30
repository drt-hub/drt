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
