"""Tests for deprecation handling in drt validate."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.config.parser import _check_deprecated_keys, load_syncs_safe

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

SYNC_WITH_DEPRECATED_BATCH_SIZE = {
    "name": "deprecated-sync",
    "model": "SELECT 1",
    "destination": {
        "type": "rest_api",
        "url": "https://example.com/api",
        "method": "POST",
    },
    "sync": {
        "batch_size": 500,  # deprecated
        "mode": "full",
    },
}

SYNC_WITH_NEW_BATCH_CONFIG = {
    "name": "new-sync",
    "model": "SELECT 1",
    "destination": {
        "type": "rest_api",
        "url": "https://example.com/api",
        "method": "POST",
    },
    "sync": {
        "batch_config": {
            "size": 500,
        },
        "mode": "full",
    },
}


def _write_sync(syncs_dir: Path, name: str, data: dict) -> None:
    """Write a sync YAML file."""
    syncs_dir.mkdir(parents=True, exist_ok=True)
    path = syncs_dir / f"{name}.yml"
    with path.open("w") as f:
        yaml.dump(data, f)


# ---------------------------------------------------------------------------
# Unit tests for deprecation detection
# ---------------------------------------------------------------------------


def test_check_deprecated_keys_detects_batch_size() -> None:
    """Test that batch_size is detected as deprecated."""
    warnings = _check_deprecated_keys(SYNC_WITH_DEPRECATED_BATCH_SIZE, "test-sync")
    assert len(warnings) == 1
    assert warnings[0]["key"] == "sync.batch_size"
    assert warnings[0]["replacement"] == "sync.batch_config.size"
    assert warnings[0]["removed_in"] == "v0.7.0"
    assert "v0.6-to-v0.7.md" in warnings[0]["docs_link"]


def test_check_deprecated_keys_no_warning_for_new_config() -> None:
    """Test that using the new batch_config doesn't trigger warnings."""
    warnings = _check_deprecated_keys(SYNC_WITH_NEW_BATCH_CONFIG, "test-sync")
    assert len(warnings) == 0


def test_check_deprecated_keys_no_warning_when_no_sync_key() -> None:
    """Test that syncs without a sync key have no deprecation warnings."""
    warnings = _check_deprecated_keys(VALID_SYNC, "test-sync")
    assert len(warnings) == 0


# ---------------------------------------------------------------------------
# Integration tests with load_syncs_safe
# ---------------------------------------------------------------------------


def test_load_syncs_safe_collects_deprecations(tmp_path: Path) -> None:
    """Test that deprecations are collected in load_syncs_safe."""
    _write_sync(tmp_path / "syncs", "deprecated", SYNC_WITH_DEPRECATED_BATCH_SIZE)
    result = load_syncs_safe(tmp_path)
    
    # Should have loaded the sync successfully (deprecations don't block)
    assert len(result.syncs) == 1
    assert result.syncs[0].name == "deprecated-sync"
    
    # Should have collected the deprecation (keyed by sync name, not file name)
    assert "deprecated-sync" in result.deprecations
    assert len(result.deprecations["deprecated-sync"]) == 1
    assert result.deprecations["deprecated-sync"][0]["key"] == "sync.batch_size"


def test_load_syncs_safe_multiple_syncs_with_mixed_deprecations(tmp_path: Path) -> None:
    """Test handling multiple syncs where some have deprecations."""
    _write_sync(tmp_path / "syncs", "deprecated", SYNC_WITH_DEPRECATED_BATCH_SIZE)
    _write_sync(tmp_path / "syncs", "modern", SYNC_WITH_NEW_BATCH_CONFIG)
    _write_sync(tmp_path / "syncs", "simple", VALID_SYNC)
    
    result = load_syncs_safe(tmp_path)
    
    # All three should load successfully
    assert len(result.syncs) == 3
    
    # Only deprecated-sync should have deprecations
    assert len(result.deprecations) == 1
    assert "deprecated-sync" in result.deprecations
    assert len(result.deprecations["deprecated-sync"]) == 1


# ---------------------------------------------------------------------------
# CLI integration tests for drt validate
# ---------------------------------------------------------------------------


def test_validate_text_output_shows_deprecation_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that drt validate text output shows deprecation warnings."""
    _write_sync(tmp_path / "syncs", "deprecated", SYNC_WITH_DEPRECATED_BATCH_SIZE)
    monkeypatch.chdir(tmp_path)
    
    result = runner.invoke(app, ["validate"], catch_exceptions=False)
    
    assert result.exit_code == 0
    assert "deprecated-sync" in result.output
    assert "⚠️" in result.output
    assert "sync.batch_size" in result.output
    assert "sync.batch_config.size" in result.output
    # Verify replacement instruction is printed
    assert "Use sync.batch_config.size instead" in result.output
    # Verify docs link is printed
    assert "v0.6-to-v0.7.md" in result.output


def test_validate_json_output_includes_deprecations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that drt validate --output json includes deprecations field."""
    _write_sync(tmp_path / "syncs", "deprecated", SYNC_WITH_DEPRECATED_BATCH_SIZE)
    monkeypatch.chdir(tmp_path)
    
    result = runner.invoke(app, ["validate", "--output", "json"], catch_exceptions=False)
    
    assert result.exit_code == 0
    output = json.loads(result.output)
    
    # Find the result for the deprecated sync
    sync_result = next(r for r in output["results"] if r["name"] == "deprecated-sync")
    
    assert "deprecations" in sync_result
    assert len(sync_result["deprecations"]) == 1
    assert sync_result["deprecations"][0]["key"] == "sync.batch_size"
    assert sync_result["deprecations"][0]["removed_in"] == "v0.7.0"


def test_validate_json_output_empty_deprecations_when_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that deprecations field is empty list when no deprecated keys."""
    _write_sync(tmp_path / "syncs", "modern", SYNC_WITH_NEW_BATCH_CONFIG)
    monkeypatch.chdir(tmp_path)
    
    result = runner.invoke(app, ["validate", "--output", "json"], catch_exceptions=False)
    
    assert result.exit_code == 0
    output = json.loads(result.output)
    
    sync_result = next(r for r in output["results"] if r["name"] == "new-sync")
    
    assert "deprecations" in sync_result
    assert sync_result["deprecations"] == []


def test_validate_exit_code_zero_with_deprecations_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that exit code is 0 when only deprecations (no errors) are found."""
    _write_sync(tmp_path / "syncs", "deprecated", SYNC_WITH_DEPRECATED_BATCH_SIZE)
    monkeypatch.chdir(tmp_path)
    
    result = runner.invoke(app, ["validate"], catch_exceptions=False)
    
    # Exit code should be 0 even with deprecations (they're non-blocking)
    assert result.exit_code == 0


def test_validate_select_filters_deprecations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that --select flag filters deprecations correctly."""
    _write_sync(tmp_path / "syncs", "deprecated", SYNC_WITH_DEPRECATED_BATCH_SIZE)
    _write_sync(tmp_path / "syncs", "modern", SYNC_WITH_NEW_BATCH_CONFIG)
    monkeypatch.chdir(tmp_path)
    
    result = runner.invoke(app, ["validate", "--select", "deprecated-sync"], catch_exceptions=False)
    
    assert result.exit_code == 0
    # Should show the deprecated sync
    assert "deprecated-sync" in result.output


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_deprecation_check_with_empty_sync_section(tmp_path: Path) -> None:
    """Test handling of syncs with empty sync section."""
    sync_data = VALID_SYNC.copy()
    sync_data["sync"] = {}
    
    warnings = _check_deprecated_keys(sync_data, "test")
    assert len(warnings) == 0


def test_deprecation_check_with_no_sync_section(tmp_path: Path) -> None:
    """Test handling of syncs without sync section."""
    warnings = _check_deprecated_keys(VALID_SYNC, "test")
    assert len(warnings) == 0
