"""Tests for `target/run_results.json` (#778) — a durable, machine-readable
per-invocation record, dbt's `run_results.json` pattern.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from drt import __version__
from drt.cli.main import app

runner = CliRunner()


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
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
            }
        )
    )
    return tmp_path


def test_written_by_default_text_mode(project: Path) -> None:
    """Written unconditionally, independent of --output — the whole point
    is a durable record even when JSON wasn't requested on stdout."""
    result = runner.invoke(app, ["run", "--select", "users"])
    assert result.exit_code == 0

    artifact_path = project / "target" / "run_results.json"
    assert artifact_path.exists()
    data = json.loads(artifact_path.read_text())
    assert data["schema_version"] == 1
    assert data["invocation"]["succeeded"] == 1
    assert data["invocation"]["failed"] == 0
    assert data["invocation"]["skipped"] == 0
    assert data["invocation"]["drt_version"] == __version__
    assert data["invocation"]["argv"] == sys.argv
    assert isinstance(data["invocation"]["run_id"], str) and data["invocation"]["run_id"]
    assert isinstance(data["invocation"]["duration_seconds"], (int, float))
    assert data["invocation"]["started_at"] < data["invocation"]["completed_at"]
    assert len(data["results"]) == 1
    assert data["results"][0]["name"] == "users"
    assert data["results"][0]["status"] == "success"


def test_matches_output_json_syncs_entries(project: Path) -> None:
    """The artifact's `results` reuse the exact same per-sync entries
    --output json already prints — one source of truth, not two shapes."""
    result = runner.invoke(app, ["run", "--select", "users", "--output", "json"])
    stdout_data = json.loads(result.output)

    artifact_data = json.loads((project / "target" / "run_results.json").read_text())

    assert artifact_data["results"] == stdout_data["syncs"]
    assert artifact_data["invocation"]["run_id"] == stdout_data["run_id"]
    assert artifact_data["invocation"]["succeeded"] == stdout_data["succeeded"]
    assert artifact_data["invocation"]["failed"] == stdout_data["failed"]
    assert artifact_data["invocation"]["skipped"] == stdout_data["skipped"]
    assert artifact_data["invocation"]["duration_seconds"] == stdout_data["total_duration_seconds"]


def test_target_path_option_redirects_the_write(project: Path) -> None:
    result = runner.invoke(app, ["run", "--select", "users", "--target-path", "custom_target"])
    assert result.exit_code == 0
    assert (project / "custom_target" / "run_results.json").exists()
    assert not (project / "target" / "run_results.json").exists()


def test_written_even_when_a_sync_fails(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Best-effort bookkeeping must not depend on the run's own success --
    the artifact is exactly what a CI system needs to inspect *why* a run
    failed."""
    (project / "syncs" / "broken.yml").write_text(
        yaml.safe_dump(
            {
                "name": "broken",
                "model": "SELECT 1 AS id",
                "destination": {
                    "type": "file",
                    "format": "csv",
                    "path": "/nonexistent/dir/out.csv",
                },
            }
        )
    )

    result = runner.invoke(app, ["run"])
    assert result.exit_code == 1

    data = json.loads((project / "target" / "run_results.json").read_text())
    assert data["invocation"]["failed"] == 1
    statuses = {entry["name"]: entry["status"] for entry in data["results"]}
    assert statuses["broken"] != "success"
    assert statuses["users"] == "success"


def test_write_failure_does_not_change_exit_code(project: Path) -> None:
    """A failure writing the artifact (here: a plain file already occupies
    the target directory's name, so `mkdir` raises `FileExistsError`, an
    `OSError` subclass -- standing in for a permissions/disk-full failure)
    must never mask the real run outcome or crash the command."""
    (project / "target").write_text("not a directory")

    result = runner.invoke(app, ["run", "--select", "users"])

    assert result.exit_code == 0
    assert (project / "target").read_text() == "not a directory"  # untouched, not overwritten
