"""Tests for Airflow integration helper."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from drt.integrations.airflow import DrtRunOperator, run_drt_sync


def test_run_drt_sync_not_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """run_drt_sync raises ValueError for unknown sync."""
    # Create minimal project
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "test", "version": "0.1", "profile": "default"})
    )
    # Create credentials in home-like dir
    creds = tmp_path / "drt_home"
    creds.mkdir()
    (creds / "profiles.yml").write_text(
        yaml.dump({"default": {"type": "duckdb", "database": ":memory:"}})
    )
    monkeypatch.setenv("HOME", str(tmp_path))
    # Patch config dir to use our temp dir
    monkeypatch.setattr(
        "drt.config.credentials._config_dir",
        lambda override=None: override or creds,
    )
    (tmp_path / "syncs").mkdir()

    with pytest.raises(ValueError, match="No sync named"):
        run_drt_sync("nonexistent", project_dir=str(tmp_path))


def test_run_drt_sync_missing_project(tmp_path: Path) -> None:
    """run_drt_sync raises FileNotFoundError without drt_project.yml."""
    with pytest.raises(FileNotFoundError):
        run_drt_sync("any", project_dir=str(tmp_path))


def test_drt_run_operator_requires_airflow() -> None:
    """DrtRunOperator raises ImportError without Airflow installed."""
    with pytest.raises(ImportError, match="Airflow"):
        DrtRunOperator(task_id="test", sync_name="test")


def test_run_drt_sync_return_type() -> None:
    """Verify return type annotation is dict."""
    import inspect

    sig = inspect.signature(run_drt_sync)
    assert "dict" in str(sig.return_annotation)
