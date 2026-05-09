"""Tests for deprecation handling in drt validate."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.config.parser import _check_deprecated_keys, load_syncs_safe
from drt.deprecations import DeprecatedFeature

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


def _write_sync(syncs_dir: Path, name: str, data: dict) -> None:
    """Write a sync YAML file."""
    syncs_dir.mkdir(parents=True, exist_ok=True)
    path = syncs_dir / f"{name}.yml"
    with path.open("w") as f:
        yaml.dump(data, f)


# ---------------------------------------------------------------------------
# Unit tests for deprecation detection
# ---------------------------------------------------------------------------


def test_check_deprecated_keys_returns_empty_when_registry_empty() -> None:
    """Test that _check_deprecated_keys returns empty list when registry is empty."""
    # The current registry is empty, so any sync should have no warnings
    warnings = _check_deprecated_keys(VALID_SYNC)
    assert len(warnings) == 0


def test_check_deprecated_keys_with_monkeypatched_deprecation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Test that deprecation detection works with a fake registered entry."""
    fake_sync_data = {
        "name": "test",
        "model": "SELECT 1",
        "destination": {"type": "rest_api", "url": "http://example.com"},
        "sync": {
            "old_field": "value",  # This will be marked as deprecated
            "mode": "full",
        },
    }
    
    # Monkeypatch the registry with a fake deprecation
    fake_feature = DeprecatedFeature(
        key="old_field",
        replacement="new.field.path",
        announced_in="v0.5.0",
        removed_in="v0.8.0",
        docs_link="docs/migration/v0.7-to-v0.8.md",
    )
    monkeypatch.setitem(
        __import__("drt.deprecations", fromlist=["DEPRECATED_SYNC_KEYS"]).DEPRECATED_SYNC_KEYS,
        "old_field",
        fake_feature,
    )
    
    warnings = _check_deprecated_keys(fake_sync_data)
    assert len(warnings) == 1
    assert warnings[0]["key"] == "sync.old_field"
    assert warnings[0]["replacement"] == "new.field.path"
    assert warnings[0]["removed_in"] == "v0.8.0"
    assert "v0.7-to-v0.8.md" in warnings[0]["docs_link"]


def test_check_deprecated_keys_no_warning_when_no_sync_key() -> None:
    """Test that syncs without a sync key have no deprecation warnings."""
    warnings = _check_deprecated_keys(VALID_SYNC)
    assert len(warnings) == 0


# ---------------------------------------------------------------------------
# Integration tests with load_syncs_safe
# ---------------------------------------------------------------------------


def test_load_syncs_safe_empty_deprecations_with_empty_registry(tmp_path: Path) -> None:
    """Test that no deprecations are collected when registry is empty."""
    _write_sync(tmp_path / "syncs", "normal", VALID_SYNC)
    result = load_syncs_safe(tmp_path)
    
    # Should have loaded the sync successfully
    assert len(result.syncs) == 1
    assert result.syncs[0].name == "test-sync"
    
    # Should have no deprecations (registry is empty)
    assert len(result.deprecations) == 0


# ---------------------------------------------------------------------------
# CLI integration tests for drt validate
# ---------------------------------------------------------------------------


def test_validate_text_output_with_monkeypatched_deprecation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that drt validate text output shows deprecation warnings end-to-end."""
    sync_with_deprecated_field = {
        "name": "deprecated-sync",
        "model": "SELECT 1",
        "destination": {
            "type": "rest_api",
            "url": "https://example.com/api",
            "method": "POST",
        },
        "sync": {
            "old_field": 500,  # Will be marked as deprecated
            "mode": "full",
        },
    }
    
    _write_sync(tmp_path / "syncs", "deprecated", sync_with_deprecated_field)
    
    # Monkeypatch the registry
    fake_feature = DeprecatedFeature(
        key="old_field",
        replacement="new.field.path",
        announced_in="v0.5.0",
        removed_in="v0.8.0",
        docs_link="docs/migration/v0.7-to-v0.8.md",
    )
    monkeypatch.setitem(
        __import__("drt.deprecations", fromlist=["DEPRECATED_SYNC_KEYS"]).DEPRECATED_SYNC_KEYS,
        "old_field",
        fake_feature,
    )
    
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["validate"], catch_exceptions=False)
    
    assert result.exit_code == 0
    assert "deprecated-sync" in result.output
    assert "⚠️" in result.output
    assert "sync.old_field" in result.output
    assert "new.field.path" in result.output
    # Verify replacement instruction is printed
    assert "Use new.field.path instead" in result.output
    # Verify docs link is printed
    assert "v0.7-to-v0.8.md" in result.output


def test_validate_json_output_with_monkeypatched_deprecation(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that drt validate --output json includes deprecations field."""
    sync_with_deprecated_field = {
        "name": "deprecated-sync",
        "model": "SELECT 1",
        "destination": {
            "type": "rest_api",
            "url": "https://example.com/api",
            "method": "POST",
        },
        "sync": {
            "old_field": 500,
            "mode": "full",
        },
    }
    
    _write_sync(tmp_path / "syncs", "deprecated", sync_with_deprecated_field)
    
    # Monkeypatch the registry
    fake_feature = DeprecatedFeature(
        key="old_field",
        replacement="new.field.path",
        announced_in="v0.5.0",
        removed_in="v0.8.0",
        docs_link="docs/migration/v0.7-to-v0.8.md",
    )
    monkeypatch.setitem(
        __import__("drt.deprecations", fromlist=["DEPRECATED_SYNC_KEYS"]).DEPRECATED_SYNC_KEYS,
        "old_field",
        fake_feature,
    )
    
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["validate", "--output", "json"], catch_exceptions=False)
    
    assert result.exit_code == 0
    output = json.loads(result.output)
    
    # Find the result for the deprecated sync
    sync_result = next(r for r in output["results"] if r["name"] == "deprecated-sync")
    
    assert "deprecations" in sync_result
    assert len(sync_result["deprecations"]) == 1
    assert sync_result["deprecations"][0]["key"] == "sync.old_field"
    assert sync_result["deprecations"][0]["removed_in"] == "v0.8.0"


def test_validate_json_output_empty_deprecations_when_none(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that deprecations field is empty list when no deprecated keys."""
    _write_sync(tmp_path / "syncs", "normal", VALID_SYNC)
    monkeypatch.chdir(tmp_path)
    
    result = runner.invoke(app, ["validate", "--output", "json"], catch_exceptions=False)
    
    assert result.exit_code == 0
    output = json.loads(result.output)
    
    sync_result = next(r for r in output["results"] if r["name"] == "test-sync")
    
    assert "deprecations" in sync_result
    assert sync_result["deprecations"] == []


def test_validate_exit_code_zero_with_deprecations_only(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that exit code is 0 when only deprecations (no errors) are found."""
    sync_with_deprecated_field = {
        "name": "deprecated-sync",
        "model": "SELECT 1",
        "destination": {
            "type": "rest_api",
            "url": "https://example.com/api",
            "method": "POST",
        },
        "sync": {
            "old_field": 500,
            "mode": "full",
        },
    }
    
    _write_sync(tmp_path / "syncs", "deprecated", sync_with_deprecated_field)
    
    # Monkeypatch the registry
    fake_feature = DeprecatedFeature(
        key="old_field",
        replacement="new.field.path",
        announced_in="v0.5.0",
        removed_in="v0.8.0",
        docs_link="docs/migration/v0.7-to-v0.8.md",
    )
    monkeypatch.setitem(
        __import__("drt.deprecations", fromlist=["DEPRECATED_SYNC_KEYS"]).DEPRECATED_SYNC_KEYS,
        "old_field",
        fake_feature,
    )
    
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["validate"], catch_exceptions=False)
    
    # Exit code should be 0 even with deprecations (they're non-blocking)
    assert result.exit_code == 0


def test_validate_select_filters_deprecations(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Test that --select flag filters deprecations correctly."""
    sync_with_deprecated_field = {
        "name": "deprecated-sync",
        "model": "SELECT 1",
        "destination": {
            "type": "rest_api",
            "url": "https://example.com/api",
            "method": "POST",
        },
        "sync": {
            "old_field": 500,
            "mode": "full",
        },
    }
    
    _write_sync(tmp_path / "syncs", "deprecated", sync_with_deprecated_field)
    _write_sync(tmp_path / "syncs", "normal", VALID_SYNC)
    
    # Monkeypatch the registry
    fake_feature = DeprecatedFeature(
        key="old_field",
        replacement="new.field.path",
        announced_in="v0.5.0",
        removed_in="v0.8.0",
        docs_link="docs/migration/v0.7-to-v0.8.md",
    )
    monkeypatch.setitem(
        __import__("drt.deprecations", fromlist=["DEPRECATED_SYNC_KEYS"]).DEPRECATED_SYNC_KEYS,
        "old_field",
        fake_feature,
    )
    
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(
        app, ["validate", "--select", "deprecated-sync"], catch_exceptions=False
    )
    
    assert result.exit_code == 0
    # Should show the deprecated sync
    assert "deprecated-sync" in result.output


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_deprecation_check_with_empty_sync_section() -> None:
    """Test handling of syncs with empty sync section."""
    sync_data = VALID_SYNC.copy()
    sync_data["sync"] = {}
    
    warnings = _check_deprecated_keys(sync_data)
    assert len(warnings) == 0


def test_deprecation_check_with_no_sync_section() -> None:
    """Test handling of syncs without sync section."""
    warnings = _check_deprecated_keys(VALID_SYNC)
    assert len(warnings) == 0

