"""Tests for `drt build` (#777) — run + tests per sync, sequential."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()

QUERYABLE_DEST = {
    "type": "postgres",
    "connection_string_env": "DB_CONN",
    "table": "t",
    "upsert_key": ["id"],
}


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "drt_project.yml").write_text("name: demo\nprofile: default\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "a_with_tests.yml").write_text(
        yaml.dump(
            {
                "name": "a_with_tests",
                "model": "SELECT 1",
                "destination": QUERYABLE_DEST,
                "tests": [{"not_null": {"columns": ["id"]}}],
            }
        )
    )
    (syncs_dir / "b_plain.yml").write_text(
        yaml.dump(
            {
                "name": "b_plain",
                "model": "SELECT 2",
                "destination": {"type": "rest_api", "url": "https://example.com"},
            }
        )
    )
    return tmp_path


class _FakeResult:
    def __init__(self, success: int = 1, failed: int = 0) -> None:
        self.success = success
        self.failed = failed
        self.skipped = 0
        self.rows_extracted = success
        self.row_errors: list[Any] = []
        self.errors: list[str] = ["boom"] if failed else []
        self.watermark_source: str | None = None
        self.cursor_value_used: str | None = None
        self.limit_applied: int | None = None
        self.duration_seconds = 0.01
        self.interrupted = False


@pytest.fixture
def patched_runtime(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Patch engine + credentials + query layer; return recorders."""
    from drt.cli import main as cli_main
    from drt.config import credentials as creds
    from drt.destinations import query as query_module
    from drt.engine import sync as sync_module

    run_calls: list[str] = []
    test_queries: list[str] = []
    fail_runs: set[str] = set()
    null_count = {"value": 0}  # not_null passes when 0

    def fake_run_sync(sync, *_a: Any, **_k: Any) -> _FakeResult:
        run_calls.append(sync.name)
        if sync.name in fail_runs:
            return _FakeResult(success=0, failed=1)
        return _FakeResult()

    monkeypatch.setattr(sync_module, "run_sync", fake_run_sync, raising=False)
    monkeypatch.setattr(
        creds,
        "load_profile",
        lambda *_a, **_k: creds.DuckDBProfile(type="duckdb"),
        raising=False,
    )
    monkeypatch.setattr(cli_main, "_get_source", lambda *_a, **_kw: object(), raising=False)
    monkeypatch.setattr(cli_main, "_get_destination", lambda *_a, **_kw: object(), raising=False)
    monkeypatch.setattr(query_module, "is_queryable", lambda d: d.type == "postgres")
    monkeypatch.setattr(query_module, "get_table_name", lambda d: "t")

    def fake_execute(d: Any, q: str) -> int:
        test_queries.append(q)
        return null_count["value"]

    monkeypatch.setattr(query_module, "execute_test_query", fake_execute)
    return {
        "run_calls": run_calls,
        "test_queries": test_queries,
        "fail_runs": fail_runs,
        "null_count": null_count,
    }


def test_build_runs_syncs_and_their_tests(
    project: Path, patched_runtime: dict[str, Any]
) -> None:
    result = runner.invoke(app, ["build", "--output", "json"])

    assert result.exit_code == 0, result.output
    assert patched_runtime["run_calls"] == ["a_with_tests", "b_plain"]
    assert len(patched_runtime["test_queries"]) == 1  # only a_with_tests has tests
    payload = json.loads(result.output)
    by_name = {e["name"]: e for e in payload["syncs"]}
    assert by_name["a_with_tests"]["tests"][0]["passed"] is True
    assert "tests" not in by_name["b_plain"]
    assert payload["succeeded"] == 2


def test_build_failing_test_marks_sync_failed(
    project: Path, patched_runtime: dict[str, Any]
) -> None:
    patched_runtime["null_count"]["value"] = 7  # not_null now fails

    result = runner.invoke(app, ["build", "--output", "json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    by_name = {e["name"]: e for e in payload["syncs"]}
    assert by_name["a_with_tests"]["status"] == "tests_failed"
    assert payload["failed"] == 1
    assert payload["succeeded"] == 1  # b_plain unaffected (no --fail-fast)


def test_build_failed_run_skips_its_tests(
    project: Path, patched_runtime: dict[str, Any]
) -> None:
    patched_runtime["fail_runs"].add("a_with_tests")

    result = runner.invoke(app, ["build", "--output", "json"])

    assert result.exit_code == 1
    assert patched_runtime["test_queries"] == []  # tests never ran for the failed sync
    payload = json.loads(result.output)
    by_name = {e["name"]: e for e in payload["syncs"]}
    assert "tests" not in by_name["a_with_tests"]


def test_build_fail_fast_skips_remaining(
    project: Path, patched_runtime: dict[str, Any]
) -> None:
    patched_runtime["null_count"]["value"] = 7  # first sync's test fails

    result = runner.invoke(app, ["build", "--fail-fast", "--output", "json"])

    assert result.exit_code == 1
    assert patched_runtime["run_calls"] == ["a_with_tests"]  # b never scheduled
    payload = json.loads(result.output)
    by_name = {e["name"]: e for e in payload["syncs"]}
    assert by_name["b_plain"]["status"] == "skipped"
    assert by_name["b_plain"]["reason"] == "fail_fast"
    assert payload["skipped"] == 1


def test_build_dry_run_previews_tests_without_executing(
    project: Path, patched_runtime: dict[str, Any]
) -> None:
    result = runner.invoke(app, ["build", "--dry-run", "--output", "json"])

    assert result.exit_code == 0, result.output
    assert patched_runtime["test_queries"] == []  # plan only, nothing executed
    payload = json.loads(result.output)
    by_name = {e["name"]: e for e in payload["syncs"]}
    assert by_name["a_with_tests"]["tests"][0]["dry_run"] is True


def test_build_selection_applies(project: Path, patched_runtime: dict[str, Any]) -> None:
    result = runner.invoke(app, ["build", "--select", "b_plain", "--output", "json"])

    assert result.exit_code == 0, result.output
    assert patched_runtime["run_calls"] == ["b_plain"]
