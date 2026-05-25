"""Tests for ``drt init --template`` (#545)."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli._init_templates import TEMPLATES
from drt.cli.main import app
from drt.config.models import SyncConfig

runner = CliRunner()


# ---------------------------------------------------------------------------
# --template list
# ---------------------------------------------------------------------------


def test_template_list_exits_zero_and_lists_all_templates() -> None:
    result = runner.invoke(app, ["init", "--template", "list"])
    assert result.exit_code == 0
    assert "Available templates:" in result.output
    for name in TEMPLATES:
        assert name in result.output


# ---------------------------------------------------------------------------
# --template <name> scaffolding
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("template_name", list(TEMPLATES))
def test_template_creates_sync_file_and_project_shell(
    template_name: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Invoking ``--template <name>`` in an empty dir produces a runnable shell:
    ``drt_project.yml``, ``.drt/.gitignore``, and ``syncs/<name>.yml``.
    """
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["init", "--template", template_name])
    assert result.exit_code == 0

    sync_file = tmp_path / "syncs" / f"{template_name}.yml"
    assert sync_file.exists(), f"Expected {sync_file} to be created"
    assert (tmp_path / "drt_project.yml").exists()
    assert (tmp_path / ".drt" / ".gitignore").exists()

    # Next-steps block from the registry must surface to the user
    assert "Next steps" in result.output


def test_template_does_not_overwrite_existing_drt_project_yml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "drt_project.yml").write_text("name: existing\nprofile: prod\n")

    result = runner.invoke(app, ["init", "--template", "duckdb_to_rest"])
    assert result.exit_code == 0
    # Original content preserved (no clobber on the project shell)
    assert (tmp_path / "drt_project.yml").read_text() == "name: existing\nprofile: prod\n"


def test_template_invalid_name_exits_one_and_directs_to_list(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["init", "--template", "made_up_name"])
    assert result.exit_code == 1
    assert "Unknown template" in result.output
    assert "drt init --template list" in result.output


# ---------------------------------------------------------------------------
# Every template file must parse as a valid SyncConfig
# (this is the "ready to run drt validate" bar from #545)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("template_name", list(TEMPLATES))
def test_every_template_parses_as_a_valid_sync_config(template_name: str) -> None:
    """Locks the contract: shipping a template that fails Pydantic validation
    would mean ``drt init --template <name> && drt validate`` errors out —
    not the "ready to run" experience the issue promises.
    """
    info = TEMPLATES[template_name]
    raw = info.read_yaml()
    parsed = yaml.safe_load(raw)
    # Pydantic raises ValidationError on shape mismatch — failing this test
    # surfaces it with the offending file name.
    SyncConfig.model_validate(parsed)
