"""Unit tests for dagster_drt.sensors (#855, PR2).

The `deltalake` and `pyiceberg` extras are mocked via sys.modules injection
(same pattern as tests/unit/test_deltalake_source.py / test_iceberg_source.py
in the main drt-core test suite), so these run without either installed.
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dagster import RunRequest, SkipReason, build_sensor_context, job

_P_LOAD_PROJECT = "drt.config.parser.load_project"
_P_LOAD_PROFILE = "drt.config.credentials.load_profile"
_P_CURRENT_SIGNAL = "dagster_drt.sensors._current_signal"


@job
def _dummy_job() -> None: ...


# ===================================================================
# _current_signal() — profile dispatch
# ===================================================================


class TestCurrentSignal:
    def test_delta_returns_version(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        from dagster_drt.sensors import _current_signal

        from drt.config.credentials import DeltaLakeProfile

        dt = MagicMock()
        dt.version.return_value = 7
        deltalake_mod = MagicMock()
        deltalake_mod.DeltaTable.return_value = dt
        monkeypatch.setitem(sys.modules, "deltalake", deltalake_mod)

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE) as mock_profile,
        ):
            mock_proj.return_value = MagicMock(profile="lake")
            mock_profile.return_value = DeltaLakeProfile(type="deltalake", location="s3://b/t")

            signal = _current_signal(tmp_path)

        assert signal == "7"
        deltalake_mod.DeltaTable.assert_called_once_with("s3://b/t", storage_options=None)

    def test_iceberg_returns_snapshot_id(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from dagster_drt.sensors import _current_signal

        from drt.config.credentials import IcebergProfile

        snapshot = MagicMock()
        snapshot.snapshot_id = 12345
        table = MagicMock()
        table.current_snapshot.return_value = snapshot
        catalog = MagicMock()
        catalog.load_table.return_value = table
        catalog_mod = MagicMock()
        catalog_mod.load_catalog.return_value = catalog
        pyiceberg_mod = MagicMock()
        monkeypatch.setitem(sys.modules, "pyiceberg", pyiceberg_mod)
        monkeypatch.setitem(sys.modules, "pyiceberg.catalog", catalog_mod)

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE) as mock_profile,
        ):
            mock_proj.return_value = MagicMock(profile="lake")
            mock_profile.return_value = IcebergProfile(type="iceberg", table="ns.t")

            signal = _current_signal(tmp_path)

        assert signal == "12345"

    def test_iceberg_no_snapshot_returns_zero(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A table with no snapshots yet (freshly created, no writes) must
        not crash — current_snapshot() returns None per pyiceberg's own API."""
        from dagster_drt.sensors import _current_signal

        from drt.config.credentials import IcebergProfile

        table = MagicMock()
        table.current_snapshot.return_value = None
        catalog = MagicMock()
        catalog.load_table.return_value = table
        catalog_mod = MagicMock()
        catalog_mod.load_catalog.return_value = catalog
        pyiceberg_mod = MagicMock()
        monkeypatch.setitem(sys.modules, "pyiceberg", pyiceberg_mod)
        monkeypatch.setitem(sys.modules, "pyiceberg.catalog", catalog_mod)

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE) as mock_profile,
        ):
            mock_proj.return_value = MagicMock(profile="lake")
            mock_profile.return_value = IcebergProfile(type="iceberg", table="ns.t")

            signal = _current_signal(tmp_path)

        assert signal == "0"

    def test_unsupported_profile_raises(self, tmp_path: Path) -> None:
        from dagster_drt.sensors import _current_signal

        from drt.config.credentials import RestApiProfile

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE) as mock_profile,
        ):
            mock_proj.return_value = MagicMock(profile="rest")
            mock_profile.return_value = RestApiProfile(type="rest_api", url="http://x.com")

            with pytest.raises(NotImplementedError, match="rest_api"):
                _current_signal(tmp_path)


# ===================================================================
# build_drt_change_sensor() — evaluation behaviour
# ===================================================================


class TestBuildDrtChangeSensor:
    def test_first_evaluation_fires_run_request(self, tmp_path: Path) -> None:
        """No prior sensor cursor — bootstrap behaviour: fire once, same as
        drt's own incremental sync on an unseeded watermark."""
        from dagster_drt.sensors import build_drt_change_sensor

        with patch(_P_CURRENT_SIGNAL, return_value="1"):
            sensor_def = build_drt_change_sensor(project_dir=tmp_path, job=_dummy_job)
            ctx = build_sensor_context(cursor=None)
            result = sensor_def(ctx)

        assert isinstance(result, RunRequest)
        assert result.run_key == "1"
        assert ctx.cursor == "1"

    def test_no_change_skips(self, tmp_path: Path) -> None:
        from dagster_drt.sensors import build_drt_change_sensor

        with patch(_P_CURRENT_SIGNAL, return_value="3"):
            sensor_def = build_drt_change_sensor(project_dir=tmp_path, job=_dummy_job)
            ctx = build_sensor_context(cursor="3")
            result = sensor_def(ctx)

        assert isinstance(result, SkipReason)
        assert ctx.cursor == "3"  # unchanged

    def test_change_fires_run_request_and_updates_cursor(self, tmp_path: Path) -> None:
        from dagster_drt.sensors import build_drt_change_sensor

        with patch(_P_CURRENT_SIGNAL, return_value="4"):
            sensor_def = build_drt_change_sensor(project_dir=tmp_path, job=_dummy_job)
            ctx = build_sensor_context(cursor="3")
            result = sensor_def(ctx)

        assert isinstance(result, RunRequest)
        assert result.run_key == "4"
        assert ctx.cursor == "4"

    def test_transient_error_skips_without_raising(self, tmp_path: Path) -> None:
        """A network blip / catalog hiccup must not crash the sensor daemon —
        skip this tick, try again next time."""
        from dagster_drt.sensors import build_drt_change_sensor

        with patch(_P_CURRENT_SIGNAL, side_effect=ConnectionError("boom")):
            sensor_def = build_drt_change_sensor(project_dir=tmp_path, job=_dummy_job)
            ctx = build_sensor_context(cursor="3")
            result = sensor_def(ctx)

        assert isinstance(result, SkipReason)
        assert "boom" in result.skip_message
        assert ctx.cursor == "3"  # not touched on error

    def test_not_implemented_error_propagates(self, tmp_path: Path) -> None:
        """A permanently-unsupported profile type must surface as a failed
        sensor tick, not a silent skip that hides a real misconfiguration
        forever."""
        from dagster_drt.sensors import build_drt_change_sensor

        with patch(_P_CURRENT_SIGNAL, side_effect=NotImplementedError("nope")):
            sensor_def = build_drt_change_sensor(project_dir=tmp_path, job=_dummy_job)
            ctx = build_sensor_context(cursor="3")

            with pytest.raises(NotImplementedError, match="nope"):
                sensor_def(ctx)
