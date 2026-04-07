"""Tests for drt validate CLI error cases."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.config.parser import SyncLoadResult, _format_validation_errors, load_syncs_safe

runner = CliRunner()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VALID_SYNC = {
    "name": "test-sync",
    "model": "SELECT 1",
    "destination": {
        "type": "rest_api",
        "url": "https://example.com/api",
        "method": "POST",
    },
}


def _write_sync(syncs_dir: Path, name: str, data: dict | str) -> None:
    """Write a sync YAML file. Accepts dict (auto-dumps) or raw string."""
    syncs_dir.mkdir(parents=True, exist_ok=True)
    path = syncs_dir / f"{name}.yml"
    if isinstance(data, str):
        path.write_text(data)
    else:
        with path.open("w") as f:
            yaml.dump(data, f)


# ---------------------------------------------------------------------------
# load_syncs_safe — collects errors instead of raising
# ---------------------------------------------------------------------------


def test_load_syncs_safe_valid(tmp_path: Path) -> None:
    _write_sync(tmp_path / "syncs", "ok", VALID_SYNC)
    result = load_syncs_safe(tmp_path)
    assert len(result.syncs) == 1
    assert not result.errors


def test_load_syncs_safe_missing_fields(tmp_path: Path) -> None:
    _write_sync(tmp_path / "syncs", "bad", {"name": "incomplete"})
    result = load_syncs_safe(tmp_path)
    assert not result.syncs
    assert "bad" in result.errors
    assert any("model" in e for e in result.errors["bad"])
    assert any("destination" in e for e in result.errors["bad"])


def test_load_syncs_safe_invalid_destination_type(tmp_path: Path) -> None:
    sync = {**VALID_SYNC, "destination": {"type": "nonexistent", "url": "x"}}
    _write_sync(tmp_path / "syncs", "bad-type", sync)
    result = load_syncs_safe(tmp_path)
    assert not result.syncs
    assert "bad-type" in result.errors
    assert any("nonexistent" in e for e in result.errors["bad-type"])


def test_load_syncs_safe_mixed_valid_and_invalid(tmp_path: Path) -> None:
    _write_sync(tmp_path / "syncs", "a_good", VALID_SYNC)
    _write_sync(tmp_path / "syncs", "b_bad", {"name": "broken"})
    result = load_syncs_safe(tmp_path)
    assert len(result.syncs) == 1
    assert result.syncs[0].name == "test-sync"
    assert "b_bad" in result.errors


def test_load_syncs_safe_no_syncs_dir(tmp_path: Path) -> None:
    result = load_syncs_safe(tmp_path)
    assert not result.syncs
    assert not result.errors


def test_load_syncs_safe_incremental_missing_cursor(tmp_path: Path) -> None:
    sync = {
        **VALID_SYNC,
        "sync": {"mode": "incremental"},
    }
    _write_sync(tmp_path / "syncs", "no-cursor", sync)
    result = load_syncs_safe(tmp_path)
    assert not result.syncs
    assert "no-cursor" in result.errors
    assert any("cursor_field" in e for e in result.errors["no-cursor"])


# ---------------------------------------------------------------------------
# _format_validation_errors
# ---------------------------------------------------------------------------


def test_format_validation_errors_shows_path() -> None:
    from pydantic import ValidationError

    from drt.config.models import SyncConfig

    with pytest.raises(ValidationError) as exc_info:
        SyncConfig.model_validate({"name": "test"})
    messages = _format_validation_errors(exc_info.value)
    assert len(messages) >= 2
    # Should contain path-like location info
    assert any("model" in m for m in messages)
    assert any("destination" in m for m in messages)


# ---------------------------------------------------------------------------
# CLI validate — error output
# ---------------------------------------------------------------------------


def test_cli_validate_no_syncs(tmp_path: Path) -> None:
    result = runner.invoke(app, ["validate"], catch_exceptions=False)
    # Without syncs dir it should show "No syncs found"
    assert "No syncs found" in result.output


def test_cli_validate_valid_sync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_sync(tmp_path / "syncs", "good", VALID_SYNC)
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["validate"], catch_exceptions=False)
    assert "✓" in result.output
    assert result.exit_code == 0


def test_cli_validate_invalid_sync_shows_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_sync(tmp_path / "syncs", "broken", {"name": "broken"})
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["validate"])
    assert "✗" in result.output
    assert "broken" in result.output
    assert result.exit_code == 1


def test_cli_validate_mixed_shows_both(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_sync(tmp_path / "syncs", "a_good", VALID_SYNC)
    _write_sync(tmp_path / "syncs", "b_bad", {"name": "bad"})
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["validate"])
    assert "✓" in result.output
    assert "✗" in result.output
    assert result.exit_code == 1


def test_cli_validate_error_shows_field_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sync = {**VALID_SYNC, "destination": {"type": "nonexistent"}}
    _write_sync(tmp_path / "syncs", "bad-dest", sync)
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["validate"])
    assert "destination" in result.output
    assert result.exit_code == 1
