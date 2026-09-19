"""Tests for `target/drt/run_results.json` (#778) — a durable, machine-readable
per-invocation record, dbt's `run_results.json` pattern.

Defaults to `target/drt`, not dbt's own `target/`, so the documented
co-located `dbt run && drt run` workflow (docs/guides/using-with-dbt.md)
doesn't clobber dbt's own `target/run_results.json`.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

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

    artifact_path = project / "target" / "drt" / "run_results.json"
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

    artifact_data = json.loads((project / "target" / "drt" / "run_results.json").read_text())

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
    assert not (project / "target" / "drt" / "run_results.json").exists()


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

    data = json.loads((project / "target" / "drt" / "run_results.json").read_text())
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


def test_written_on_no_op_exit(project: Path) -> None:
    """--failed against a fresh project (no prior run to have failed) exits
    0 via an early `raise typer.Exit(0)` before the dispatch loop ever runs
    -- one of the successful no-op paths a first review round found were
    skipping the artifact write entirely (Codex review, #778 PR)."""
    result = runner.invoke(app, ["run", "--failed"])
    assert result.exit_code == 0
    assert "Nothing failed" in result.output

    artifact_path = project / "target" / "drt" / "run_results.json"
    assert artifact_path.exists()
    data = json.loads(artifact_path.read_text())
    assert data["invocation"]["exit_code"] == 0
    assert data["invocation"]["succeeded"] == 0
    assert data["invocation"]["failed"] == 0
    assert data["invocation"]["skipped"] == 0
    assert data["results"] == []


def test_exit_code_distinguishes_rejected_invocation_from_a_clean_no_op(project: Path) -> None:
    """A rejected invocation (here: --limit 0) never attempts a sync either,
    so its results/counts look byte-identical to the clean no-op above --
    exit_code is what tells a CI/observability consumer these two 0/0/0
    artifacts are not the same outcome (Codex review, #778 PR round 2)."""
    result = runner.invoke(app, ["run", "--select", "users", "--limit", "0"])
    assert result.exit_code == 1

    data = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    assert data["invocation"]["exit_code"] == 1
    assert data["invocation"]["succeeded"] == 0
    assert data["invocation"]["failed"] == 0
    assert data["invocation"]["skipped"] == 0
    assert data["results"] == []


def test_error_field_is_redacted(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A connector exception embedding a DSN/credential must not land in the
    persisted artifact verbatim -- run_results.json is recommended for CI
    upload with `if: always()`, unlike a console line (Codex review, #778 PR
    round 2)."""
    from drt.engine import sync as sync_module

    def _boom(*_a: object, **_k: object) -> None:
        raise RuntimeError("connection to postgres://user:hunter2@db.internal:5432 failed")

    monkeypatch.setattr(sync_module, "run_sync", _boom, raising=False)

    result = runner.invoke(app, ["run", "--select", "users"])
    assert result.exit_code == 1

    data = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    entry = data["results"][0]
    assert "postgres://" not in entry["error"]
    assert "hunter2" not in entry["error"]
    assert "« redacted »" in entry["error"]


def test_argv_is_redacted(project: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """--vars values reach argv verbatim; unlike stdout, this artifact is a
    durable file recommended for CI upload, so a sensitive --vars value must
    not be persisted as-is (Codex review, #778 PR round 2). CliRunner.invoke
    doesn't touch the real process `sys.argv` (it dispatches in-process), so
    it's patched here to look like the invocation actually being tested."""
    monkeypatch.setattr(sys, "argv", ["drt", "run", "--vars", "api_key: super-secret-value"])
    result = runner.invoke(
        app, ["run", "--select", "users", "--vars", "api_key: super-secret-value"]
    )
    assert result.exit_code == 0

    data = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    argv_text = " ".join(data["invocation"]["argv"])
    assert "super-secret-value" not in argv_text
    assert "« redacted »" in argv_text


def test_diff_with_non_json_native_values_does_not_crash(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A diff sample can carry warehouse-native datetime/Decimal/UUID values
    `json.dumps` can't serialize natively; this must degrade to a readable
    string, not turn an otherwise-successful preview into a crash (Codex
    review, #778 PR round 2)."""
    from datetime import datetime as dt

    from drt.engine import diff as diff_mod
    from drt.engine import sync as sync_module

    sample_diff = diff_mod.DiffResult(
        sample=[{"id": 1, "created_at": dt(2026, 1, 1)}],
        total_source_rows=1,
        supported=False,
        fallback_reason="file: no comparison available",
    )

    class _FakeResult:
        success = 1
        failed = 0
        skipped = 0
        skipped_no_match = 0
        rows_extracted = 1
        row_errors: list[Any] = []
        errors: list[str] = []
        watermark_source: str | None = None
        cursor_value_used: str | None = None
        watermark_lag: str | None = None
        limit_applied: int | None = None
        duration_seconds = 0.01
        interrupted = False
        run_id: str | None = None
        sync_run_id: str | None = "fake-sync-run-id"
        diff: Any = sample_diff

    monkeypatch.setattr(sync_module, "run_sync", lambda *_a, **_k: _FakeResult(), raising=False)

    result = runner.invoke(app, ["run", "--select", "users", "--dry-run", "--diff"])
    assert result.exit_code == 0

    data = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    sample = data["results"][0]["diff"]["sample"]
    assert sample == [{"id": 1, "created_at": "2026-01-01 00:00:00"}]


def test_text_mode_diff_included_in_artifact(
    project: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A plain-text `--dry-run --diff` run (no --output json) must still
    carry diff data in the artifact -- the documented contract is that the
    artifact is independent of --output, but `entry["diff"]` used to be
    populated only `if ctx.json_mode` (Codex review, #778 PR)."""
    from drt.engine import diff as diff_mod
    from drt.engine import sync as sync_module

    sample_diff = diff_mod.DiffResult(
        sample=[{"id": 1}],
        total_source_rows=1,
        supported=False,
        fallback_reason="file: no comparison available",
    )

    class _FakeResult:
        success = 1
        failed = 0
        skipped = 0
        skipped_no_match = 0
        rows_extracted = 1
        row_errors: list[Any] = []
        errors: list[str] = []
        watermark_source: str | None = None
        cursor_value_used: str | None = None
        watermark_lag: str | None = None
        limit_applied: int | None = None
        duration_seconds = 0.01
        interrupted = False
        run_id: str | None = None
        sync_run_id: str | None = "fake-sync-run-id"
        diff: Any = sample_diff

    monkeypatch.setattr(sync_module, "run_sync", lambda *_a, **_k: _FakeResult(), raising=False)

    result = runner.invoke(app, ["run", "--select", "users", "--dry-run", "--diff"])
    assert result.exit_code == 0

    data = json.loads((project / "target" / "drt" / "run_results.json").read_text())
    entry = data["results"][0]
    assert entry["diff"]["supported"] is False
    assert entry["diff"]["fallback_reason"] == "file: no comparison available"
