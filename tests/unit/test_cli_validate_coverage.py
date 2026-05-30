"""Coverage tests for `drt validate` edge paths (#573 follow-up).

Covers two branches that were silent in codecov on the PR (b) move:

- `--select <name>` where the name doesn't match any sync → exits 1
  with a "No sync named '<name>' found." error.
- `--emit-schema` in text mode (the JSON-mode emit_schema path is
  already covered by other tests) → writes JSON schemas to
  ``.drt/schemas/`` and prints the paths.

Both paths require nothing more than a minimal ``drt_project.yml`` in
cwd; no profile loading or sync execution involved.
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
    """A drt project with no syncs/."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "t", "version": "0.1", "profile": "default"})
    )
    (tmp_path / "syncs").mkdir()
    return tmp_path


@pytest.fixture
def project_with_sync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A drt project with one minimal valid sync. emit_schema only fires
    once result.syncs is non-empty (the no-syncs branch returns early)."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "t", "version": "0.1", "profile": "default"})
    )
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "post_users.yml").write_text(
        yaml.dump(
            {
                "name": "post_users",
                "model": "SELECT 1 AS id",
                "destination": {
                    "type": "rest_api",
                    "url": "https://example.com",
                    "method": "POST",
                },
            }
        )
    )
    return tmp_path


def test_validate_select_nonexistent_sync_exits_1(empty_project: Path) -> None:
    """``--select <unknown>`` exits 1 with 'No sync named' error."""
    result = runner.invoke(app, ["validate", "--select", "nonexistent_sync"])
    assert result.exit_code == 1
    assert "No sync named 'nonexistent_sync' found." in result.output


def test_validate_emit_schema_text_mode_writes_files(project_with_sync: Path) -> None:
    """``--emit-schema`` (text mode) writes schemas under .drt/schemas/."""
    result = runner.invoke(app, ["validate", "--emit-schema"])
    assert result.exit_code == 0
    schemas_dir = project_with_sync / ".drt" / "schemas"
    assert schemas_dir.exists()
    schema_files = list(schemas_dir.glob("*.json"))
    assert len(schema_files) > 0
    assert "Schemas written to" in result.output
