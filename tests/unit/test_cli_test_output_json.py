"""Tests for drt test --output json."""

from __future__ import annotations

import json
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


def _write_credentials(tmp_path: Path) -> None:
    """Write minimal credentials for tests."""
    creds_dir = tmp_path / ".drt"
    creds_dir.mkdir(exist_ok=True)
    with (creds_dir / "credentials.yml").open("w") as f:
        yaml.dump(
            {"profiles": {"default": {"type": "duckdb", "path": "/tmp/test.db"}}},
            f,
        )


def test_test_json_no_syncs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """test --output json with no syncs should return empty results."""
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["test", "--output", "json"])
    data = json.loads(result.output)
    assert data["status"] == "no_syncs"
    assert data["results"] == []


def test_test_json_no_tests(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """test --output json with syncs but no tests defined."""
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
    result = runner.invoke(app, ["test", "--output", "json"])
    data = json.loads(result.output)
    assert data["status"] == "no_tests"
    assert data["results"] == []


def test_test_json_non_queryable_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """test --output json with non-queryable destination marks sync as skipped."""
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
    result = runner.invoke(app, ["test", "--output", "json"])
    data = json.loads(result.output)
    assert data["status"] == "passed"
    assert len(data["results"]) == 1
    assert data["results"][0]["sync"] == "api-sync"
    assert data["results"][0]["skipped"] is True


def test_test_json_no_rich_markup(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """JSON output should not contain Rich markup."""
    monkeypatch.chdir(tmp_path)
    _write_sync(
        tmp_path,
        {
            "name": "test-sync",
            "model": "SELECT 1",
            "destination": {
                "type": "rest_api",
                "url": "http://example.com",
                "method": "POST",
            },
            "tests": [{"row_count": {"min": 1}}],
        },
    )
    result = runner.invoke(app, ["test", "--output", "json"])
    # Should not contain rich markup
    assert "[dim]" not in result.output
    assert "[bold" not in result.output
    # Should be valid JSON
    json.loads(result.output)
