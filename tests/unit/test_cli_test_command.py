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
