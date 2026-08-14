"""Tests for Airflow integration helper."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from drt.config.sync_options import SyncConfig, WatermarkConfig
from drt.integrations._runner import _watermark_storage
from drt.integrations.airflow import DrtRunOperator, run_drt_sync


def _make_sync(watermark: WatermarkConfig | None) -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": "users",
            "model": "SELECT 1 AS id",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {
                "mode": "incremental",
                "cursor_field": "id",
                "watermark": watermark,
            },
        }
    )


class TestWatermarkStorage:
    """_watermark_storage() branch coverage (#976) — mirrors
    drt.cli._helpers.get_watermark_storage, duplicated rather than shared
    per the module's no-typer-import-graph rule."""

    def test_no_watermark_configured_returns_none(self) -> None:
        assert _watermark_storage(_make_sync(None), Path(".")) is None

    def test_local_storage(self, tmp_path: Path) -> None:
        from drt.state.watermark import LocalWatermarkStorage

        sync = _make_sync(WatermarkConfig(storage="local"))
        storage = _watermark_storage(sync, tmp_path)
        assert isinstance(storage, LocalWatermarkStorage)

    def test_gcs_storage(self) -> None:
        from drt.state.watermark import GCSWatermarkStorage

        sync = _make_sync(WatermarkConfig(storage="gcs", bucket="b", key="k"))
        storage = _watermark_storage(sync, Path("."))
        assert isinstance(storage, GCSWatermarkStorage)
        assert storage._bucket_name == "b"
        assert storage._key == "k"

    def test_bigquery_storage(self) -> None:
        from drt.state.watermark import BigQueryWatermarkStorage

        sync = _make_sync(WatermarkConfig(storage="bigquery", project="p", dataset="d"))
        storage = _watermark_storage(sync, Path("."))
        assert isinstance(storage, BigQueryWatermarkStorage)
        assert storage._project == "p"
        assert storage._dataset == "d"


def test_run_drt_sync_not_found(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
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


def test_run_drt_sync_persists_state(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression test: a real run through run_drt_sync() must actually
    persist state, not just resolve the right backend to no-op against.

    build_state_bundle() picked the correct store, but nothing wired it
    into an observer — engine/sync.py's run_sync() falls back to
    NullObserver whenever observer= is omitted (sync.py:296-297) — so
    every save was silently a no-op, on the local backend exactly like a
    remote one. drt.integrations.prefect.run_drt_sync is the same function
    object, re-exported, so this covers both integrations. Found while
    researching #855/#856; same bug class as the dagster-drt fix in #973."""
    monkeypatch.chdir(tmp_path)
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
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "p", "profile": "default", "version": "1"})
    )
    (tmp_path / "syncs" / "users.yml").write_text(
        yaml.dump(
            {
                "name": "users",
                "model": "SELECT 1 AS id",
                "destination": {
                    "type": "file",
                    "format": "csv",
                    "path": str(tmp_path / "out.csv"),
                },
                "sync": {"mode": "incremental", "cursor_field": "id"},
            }
        )
    )

    result = run_drt_sync("users", project_dir=str(tmp_path))

    assert result["status"] == "success"

    from drt.state.history import LocalHistoryManager
    from drt.state.manager import StateManager

    state = StateManager(tmp_path).get_last_sync("users")
    assert state is not None, "run_drt_sync() ran the sync but never saved state"
    assert state.status == "success"
    assert state.last_cursor_value == "1"

    history = LocalHistoryManager(tmp_path).read("users")
    assert len(history) == 1, (
        "run_drt_sync() never passed history_manager= to run_sync(), "
        "so execution history stayed empty (Codex review, #976)"
    )
    assert history[0].status == "success"


def test_run_drt_sync_wires_dlq_observer(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """A sync with dlq.enabled must get a DlqObserver, not just StatePersistingObserver."""
    monkeypatch.chdir(tmp_path)
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
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "p", "profile": "default", "version": "1"})
    )
    (tmp_path / "syncs" / "users.yml").write_text(
        yaml.dump(
            {
                "name": "users",
                "model": "SELECT 1 AS id",
                "destination": {
                    "type": "file",
                    "format": "csv",
                    "path": str(tmp_path / "out.csv"),
                },
                "sync": {
                    "mode": "incremental",
                    "cursor_field": "id",
                    "dlq": {"enabled": True},
                },
            }
        )
    )

    from unittest.mock import patch

    from drt.engine.observer import DlqObserver

    with patch("drt.engine.sync.run_sync") as mock_run_sync:
        from drt.destinations.base import SyncResult

        mock_run_sync.return_value = SyncResult()
        run_drt_sync("users", project_dir=str(tmp_path))

    observer = mock_run_sync.call_args.kwargs["observer"]
    dlq_observers = [o for o in observer._observers if isinstance(o, DlqObserver)]
    assert len(dlq_observers) == 1


def test_drt_run_operator_requires_airflow() -> None:
    """DrtRunOperator raises ImportError without Airflow installed."""
    with pytest.raises(ImportError, match="Airflow"):
        DrtRunOperator(task_id="test", sync_name="test")


def test_run_drt_sync_return_type() -> None:
    """Verify return type annotation is dict."""
    import inspect

    sig = inspect.signature(run_drt_sync)
    assert "dict" in str(sig.return_annotation)
