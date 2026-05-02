"""Tests for the `drt config` Typer subapp."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from drt import telemetry
from drt.cli.main import app


@pytest.fixture(autouse=True)
def _isolate(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    user_dir = tmp_path / ".drt"
    monkeypatch.setattr(telemetry, "_user_dir", lambda: user_dir)
    telemetry._load_config_cached.cache_clear()
    for var in ("DO_NOT_TRACK", "DRT_TELEMETRY", "DRT_TELEMETRY_ENDPOINT", "DRT_TELEMETRY_API_KEY"):
        monkeypatch.delenv(var, raising=False)


runner = CliRunner()


def test_set_telemetry_enabled_true() -> None:
    result = runner.invoke(app, ["config", "set", "telemetry.enabled", "true"])
    assert result.exit_code == 0
    assert telemetry._load_config_cached() == {"enabled": True}


def test_set_telemetry_enabled_false() -> None:
    result = runner.invoke(app, ["config", "set", "telemetry.enabled", "false"])
    assert result.exit_code == 0
    assert telemetry._load_config_cached() == {"enabled": False}


def test_set_invalid_value() -> None:
    result = runner.invoke(app, ["config", "set", "telemetry.enabled", "maybe"])
    assert result.exit_code == 2


def test_set_unknown_key() -> None:
    result = runner.invoke(app, ["config", "set", "weird.key", "true"])
    assert result.exit_code == 2


def test_unset_telemetry_enabled() -> None:
    telemetry.set_enabled(True)
    result = runner.invoke(app, ["config", "unset", "telemetry.enabled"])
    assert result.exit_code == 0
    assert "enabled" not in telemetry._load_config_cached()


def test_show_telemetry_redacts_api_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_secret")
    result = runner.invoke(app, ["config", "show-telemetry"])
    assert result.exit_code == 0
    assert "phc_secret" not in result.stdout
    assert "sync_completed" in result.stdout
    assert "source_type" in result.stdout


def test_show_telemetry_does_not_create_anonymous_id(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Privacy: previewing must not generate ~/.drt/.anonymous_id."""
    monkeypatch.setenv("DRT_TELEMETRY_API_KEY", "phc_test")
    assert not telemetry._anon_id_path().exists()
    result = runner.invoke(app, ["config", "show-telemetry"])
    assert result.exit_code == 0
    assert not telemetry._anon_id_path().exists()
