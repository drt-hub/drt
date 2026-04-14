"""Tests for Prefect integration helper."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from drt.integrations.prefect import drt_sync_task, run_drt_sync


def test_run_drt_sync_not_found(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """run_drt_sync raises ValueError for unknown sync."""
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "test", "version": "0.1", "profile": "default"})
    )
    creds = tmp_path / "drt_home"
    creds.mkdir()
    (creds / "profiles.yml").write_text(
        yaml.dump({"default": {"type": "duckdb", "database": ":memory:"}})
    )
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


def test_drt_sync_task_requires_prefect() -> None:
    """drt_sync_task raises ImportError when Prefect is not installed."""
    with pytest.raises(ImportError, match="Prefect"):
        drt_sync_task(sync_name="x", project_dir=".")


def test_run_drt_sync_return_type() -> None:
    """Verify return type annotation is dict."""
    import inspect

    sig = inspect.signature(run_drt_sync)
    assert "dict" in str(sig.return_annotation)
