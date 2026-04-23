"""Tests for drt test CLI command."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()


def _write_sync(tmp_path: Path, data: dict) -> None:
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir(exist_ok=True)
    with (syncs_dir / "sync.yml").open("w") as f:
        yaml.dump(data, f)


def test_drt_test_no_syncs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["test"])
    assert "No syncs found" in result.output


def test_drt_test_no_tests_defined(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "no-tests",
            "model": "SELECT 1",
            "destination": {
                "type": "rest_api",
                "url": "http://example.com",
                "method": "POST",
            },
        },
    )
    result = runner.invoke(app, ["test"])
    assert "No tests defined" in result.output


def test_drt_test_skips_non_queryable(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "api-sync",
            "model": "SELECT 1",
            "destination": {
                "type": "rest_api",
                "url": "http://example.com",
                "method": "POST",
            },
            "tests": [{"row_count": {"min": 1}}],
        },
    )
    result = runner.invoke(app, ["test"])
    assert "not supported" in result.output.lower()


def test_drt_test_select_not_found(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "existing",
            "model": "SELECT 1",
            "destination": {
                "type": "rest_api",
                "url": "http://example.com",
                "method": "POST",
            },
            "tests": [{"row_count": {"min": 1}}],
        },
    )
    result = runner.invoke(app, ["test", "--select", "nonexistent"])
    assert result.exit_code == 1


def test_drt_test_dry_run_shows_plan(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that dry-run shows the test plan without executing tests."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "test-sync",
            "model": "SELECT 1",
            "destination": {
                "type": "postgres",
                "connection_string_env": "DB_CONN",
                "table": "test_table",
                "upsert_key": ["id"],
            },
            "tests": [{"row_count": {"min": 1}}],
        },
    )
    result = runner.invoke(app, ["test", "--dry-run"])
    assert result.exit_code == 0
    assert "(dry-run)" in result.output
    assert "row_count" in result.output


def test_drt_test_dry_run_skips_non_queryable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test dry-run shows skip message for non-queryable destinations."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "api-sync",
            "model": "SELECT 1",
            "destination": {
                "type": "rest_api",
                "url": "http://example.com",
                "method": "POST",
            },
            "tests": [{"row_count": {"min": 1}}],
        },
    )
    result = runner.invoke(app, ["test", "--dry-run"])
    assert result.exit_code == 0
    assert "would be skipped" in result.output


def test_drt_test_dry_run_json_output(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test dry-run with --output json produces valid JSON."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "json-sync",
            "model": "SELECT 1",
            "destination": {
                "type": "postgres",
                "connection_string_env": "DB_CONN",
                "table": "test_table",
                "upsert_key": ["id"],
            },
            "tests": [{"row_count": {"min": 1}}],
        },
    )
    result = runner.invoke(app, ["test", "--dry-run", "--output", "json"])
    assert result.exit_code == 0
    import json

    data = json.loads(result.output)
    assert data["dry_run"] is True
    assert len(data["results"]) == 1
    assert data["results"][0]["tests"][0]["dry_run"] is True


def test_drt_test_dry_run_not_null(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test dry-run with not_null test type shows correct label."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "nn-sync",
            "model": "SELECT 1",
            "destination": {
                "type": "postgres",
                "connection_string_env": "DB_CONN",
                "table": "test_table",
                "upsert_key": ["id"],
            },
            "tests": [{"not_null": {"columns": ["id", "name"]}}],
        },
    )
    result = runner.invoke(app, ["test", "--dry-run"])
    assert result.exit_code == 0
    assert "not_null" in result.output


def test_drt_test_dry_run_freshness(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test dry-run with freshness test type shows correct label."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "fresh-sync",
            "model": "SELECT 1",
            "destination": {
                "type": "postgres",
                "connection_string_env": "DB_CONN",
                "table": "test_table",
                "upsert_key": ["id"],
            },
            "tests": [{"freshness": {"column": "created_at", "max_age": "1 hour"}}],
        },
    )
    result = runner.invoke(app, ["test", "--dry-run"])
    assert result.exit_code == 0
    assert "freshness" in result.output


def test_drt_test_dry_run_unique(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test dry-run with unique test type shows correct label."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "uniq-sync",
            "model": "SELECT 1",
            "destination": {
                "type": "postgres",
                "connection_string_env": "DB_CONN",
                "table": "test_table",
                "upsert_key": ["id"],
            },
            "tests": [{"unique": {"columns": ["email"]}}],
        },
    )
    result = runner.invoke(app, ["test", "--dry-run"])
    assert result.exit_code == 0
    assert "unique" in result.output


def test_drt_test_dry_run_accepted_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test dry-run with accepted_values test type."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "av-sync",
            "model": "SELECT 1",
            "destination": {
                "type": "postgres",
                "connection_string_env": "DB_CONN",
                "table": "test_table",
                "upsert_key": ["id"],
            },
            "tests": [{"accepted_values": {"column": "status", "values": ["active", "inactive"]}}],
        },
    )
    result = runner.invoke(app, ["test", "--dry-run"])
    assert result.exit_code == 0
    assert "accepted_values" in result.output


def test_drt_test_dry_run_summary(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test dry-run shows summary line at the end."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "summary-sync",
            "model": "SELECT 1",
            "destination": {
                "type": "postgres",
                "connection_string_env": "DB_CONN",
                "table": "test_table",
                "upsert_key": ["id"],
            },
            "tests": [{"row_count": {"min": 1}}],
        },
    )
    result = runner.invoke(app, ["test", "--dry-run"])
    assert result.exit_code == 0
    assert "Preview of tests" in result.output
