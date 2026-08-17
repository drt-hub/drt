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

    def _mock_snowflake_module(
        self, monkeypatch: pytest.MonkeyPatch, fetchone_value: object
    ) -> MagicMock:
        cursor = MagicMock()
        cursor.__enter__.return_value = cursor
        cursor.fetchone.return_value = (fetchone_value,) if fetchone_value is not None else None
        conn = MagicMock()
        conn.cursor.return_value = cursor
        connector_mod = MagicMock()
        connector_mod.connect.return_value = conn
        snowflake_mod = MagicMock()
        snowflake_mod.connector = connector_mod
        monkeypatch.setitem(sys.modules, "snowflake", snowflake_mod)
        monkeypatch.setitem(sys.modules, "snowflake.connector", connector_mod)
        return connector_mod

    def test_snowflake_returns_last_change_commit_time(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from dagster_drt.sensors import _current_signal

        from drt.config.credentials import SnowflakeProfile

        connector_mod = self._mock_snowflake_module(monkeypatch, 1723766400123456700)

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE) as mock_profile,
        ):
            mock_proj.return_value = MagicMock(profile="sf")
            mock_profile.return_value = SnowflakeProfile(
                type="snowflake", account="acct", user="u", database="D", schema="S"
            )

            signal = _current_signal(
                tmp_path, watch_table="D.S.T", minimum_interval_seconds=60
            )

        assert signal == "1723766400123456700"
        connector_mod.connect.assert_called_once()

    def test_snowflake_requires_watch_table(self, tmp_path: Path) -> None:
        from dagster_drt.sensors import _current_signal

        from drt.config.credentials import SnowflakeProfile

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE) as mock_profile,
        ):
            mock_proj.return_value = MagicMock(profile="sf")
            mock_profile.return_value = SnowflakeProfile(type="snowflake", account="acct", user="u")

            with pytest.raises(ValueError, match="watch_table"):
                _current_signal(tmp_path, minimum_interval_seconds=60)

    def test_snowflake_requires_minimum_interval_seconds(self, tmp_path: Path) -> None:
        """#975: an unbounded poll cadence on a Snowflake profile pins its
        warehouse continuously resumed (AUTO_RESUME billing) — this must be
        an explicit choice, not a silent default."""
        from dagster_drt.sensors import _current_signal

        from drt.config.credentials import SnowflakeProfile

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE) as mock_profile,
        ):
            mock_proj.return_value = MagicMock(profile="sf")
            mock_profile.return_value = SnowflakeProfile(type="snowflake", account="acct", user="u")

            with pytest.raises(ValueError, match="minimum_interval_seconds"):
                _current_signal(tmp_path, watch_table="D.S.T")

    def test_snowflake_null_signal_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A NULL from SYSTEM$LAST_CHANGE_COMMIT_TIME must not coalesce to a
        fake baseline — that would repeat the STREAM_HAS_DATA failure mode
        #855 was filed to avoid: fire once, then go permanently silent."""
        from dagster_drt.sensors import _current_signal

        from drt.config.credentials import SnowflakeProfile

        self._mock_snowflake_module(monkeypatch, None)

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE) as mock_profile,
        ):
            mock_proj.return_value = MagicMock(profile="sf")
            mock_profile.return_value = SnowflakeProfile(
                type="snowflake", account="acct", user="u", database="D", schema="S"
            )

            with pytest.raises(ValueError, match="NULL"):
                _current_signal(tmp_path, watch_table="D.S.T", minimum_interval_seconds=60)

    def _mock_pymssql_module(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        min_valid_version: object = 1,
        current_version: object = 42,
    ) -> MagicMock:
        """min_valid_version is CHANGE_TRACKING_MIN_VALID_VERSION's result
        (checked first, per-table); current_version is
        CHANGE_TRACKING_CURRENT_VERSION's result (checked second, database-
        wide) — the two queries the sqlserver branch issues, in order."""
        cursor = MagicMock()
        cursor.__enter__.return_value = cursor
        cursor.fetchone.side_effect = [
            (min_valid_version,) if min_valid_version is not None else None,
            (current_version,) if current_version is not None else None,
        ]
        conn = MagicMock()
        conn.cursor.return_value = cursor
        pymssql_mod = MagicMock()
        pymssql_mod.connect.return_value = conn
        monkeypatch.setitem(sys.modules, "pymssql", pymssql_mod)
        return pymssql_mod

    def test_sqlserver_returns_current_version(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from dagster_drt.sensors import _current_signal

        from drt.config.credentials import SQLServerProfile

        pymssql_mod = self._mock_pymssql_module(monkeypatch, current_version=42)

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE) as mock_profile,
        ):
            mock_proj.return_value = MagicMock(profile="mssql")
            mock_profile.return_value = SQLServerProfile(
                type="sqlserver", host="h", database="D", user="u"
            )

            signal = _current_signal(tmp_path, watch_table="dbo.T")

        assert signal == "42"
        pymssql_mod.connect.assert_called_once()

    def test_sqlserver_requires_watch_table(self, tmp_path: Path) -> None:
        """Codex review (#984): CHANGE_TRACKING_CURRENT_VERSION() alone can't
        tell whether the specific table a sync reads is tracked — a database-
        level-only enablement would otherwise silently never advance for that
        table. watch_table= is what lets that be validated."""
        from dagster_drt.sensors import _current_signal

        from drt.config.credentials import SQLServerProfile

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE) as mock_profile,
        ):
            mock_proj.return_value = MagicMock(profile="mssql")
            mock_profile.return_value = SQLServerProfile(
                type="sqlserver", host="h", database="D", user="u"
            )

            with pytest.raises(ValueError, match="watch_table"):
                _current_signal(tmp_path)

    def test_sqlserver_watch_table_not_tracked_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A NULL from CHANGE_TRACKING_MIN_VALID_VERSION means watch_table
        itself was never table-level enabled, even if the database has
        Change Tracking on overall — must raise, not silently poll a signal
        that will never reflect this table's changes (Codex review, #984)."""
        from dagster_drt.sensors import _current_signal

        from drt.config.credentials import SQLServerProfile

        self._mock_pymssql_module(monkeypatch, min_valid_version=None)

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE) as mock_profile,
        ):
            mock_proj.return_value = MagicMock(profile="mssql")
            mock_profile.return_value = SQLServerProfile(
                type="sqlserver", host="h", database="D", user="u"
            )

            with pytest.raises(ValueError, match="CHANGE_TRACKING_MIN_VALID_VERSION"):
                _current_signal(tmp_path, watch_table="dbo.T")

    def test_sqlserver_null_signal_raises(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """NULL from CHANGE_TRACKING_CURRENT_VERSION means change tracking
        isn't enabled on the database at all — a permanent config error, not
        something to coalesce and poll forever."""
        from dagster_drt.sensors import _current_signal

        from drt.config.credentials import SQLServerProfile

        self._mock_pymssql_module(monkeypatch, current_version=None)

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE) as mock_profile,
        ):
            mock_proj.return_value = MagicMock(profile="mssql")
            mock_profile.return_value = SQLServerProfile(
                type="sqlserver", host="h", database="D", user="u"
            )

            with pytest.raises(ValueError, match="NULL"):
                _current_signal(tmp_path, watch_table="dbo.T")


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
        assert result.run_key == "None->1"
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
        assert result.run_key == "3->4"
        assert ctx.cursor == "4"

    def test_run_key_stays_unique_across_a_rollback(self, tmp_path: Path) -> None:
        """Regression test: Dagster dedupes run_key globally across every
        past evaluation of a sensor, not just consecutive ones. A run_key
        keyed on the bare destination value would collide the moment a
        signal revisits an old value — a source table rollback (A -> B -> A)
        or a recreated Delta table restarting its version counter both do
        exactly that, and the second "A" would silently never launch a run
        even though the cursor correctly reports it as changed."""
        from dagster_drt.sensors import build_drt_change_sensor

        sensor_def = build_drt_change_sensor(project_dir=tmp_path, job=_dummy_job)
        ctx = build_sensor_context(cursor=None)

        with patch(_P_CURRENT_SIGNAL, return_value="A"):
            first = sensor_def(ctx)
        with patch(_P_CURRENT_SIGNAL, return_value="B"):
            second = sensor_def(ctx)
        with patch(_P_CURRENT_SIGNAL, return_value="A"):  # rollback B -> A
            third = sensor_def(ctx)

        assert isinstance(first, RunRequest)
        assert isinstance(second, RunRequest)
        assert isinstance(third, RunRequest)
        run_keys = {first.run_key, second.run_key, third.run_key}
        assert len(run_keys) == 3, f"run_keys collided: {run_keys}"
        assert third.run_key == "B->A"

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

    def test_import_error_propagates(self, tmp_path: Path) -> None:
        """A missing optional driver (deltalake/pyiceberg/snowflake-connector
        -python/pymssql) is a permanent deploy-config error, not something a
        later tick would fix — catching it as transient would leave the
        sensor skipping forever, indistinguishable from a working sensor
        that just hasn't seen a change yet (Codex review, #984)."""
        from dagster_drt.sensors import build_drt_change_sensor

        with patch(_P_CURRENT_SIGNAL, side_effect=ModuleNotFoundError("no snowflake")):
            sensor_def = build_drt_change_sensor(project_dir=tmp_path, job=_dummy_job)
            ctx = build_sensor_context(cursor="3")

            with pytest.raises(ImportError, match="no snowflake"):
                sensor_def(ctx)

    def test_value_error_propagates(self, tmp_path: Path) -> None:
        """A ValueError (missing watch_table=, missing
        minimum_interval_seconds=, or a NULL signal) is a permanent config
        error just like NotImplementedError — must fail the tick, not
        silently skip forever (#975)."""
        from dagster_drt.sensors import build_drt_change_sensor

        with patch(_P_CURRENT_SIGNAL, side_effect=ValueError("bad config")):
            sensor_def = build_drt_change_sensor(project_dir=tmp_path, job=_dummy_job)
            ctx = build_sensor_context(cursor="3")

            with pytest.raises(ValueError, match="bad config"):
                sensor_def(ctx)

    def test_watch_table_and_interval_forwarded_to_current_signal(self, tmp_path: Path) -> None:
        """build_drt_change_sensor's watch_table= and minimum_interval_seconds=
        must actually reach _current_signal — otherwise the Snowflake
        required-args checks can never be satisfied through the public API."""
        from dagster_drt.sensors import build_drt_change_sensor

        with patch(_P_CURRENT_SIGNAL, return_value="1") as mock_signal:
            sensor_def = build_drt_change_sensor(
                project_dir=tmp_path,
                job=_dummy_job,
                watch_table="D.S.T",
                minimum_interval_seconds=60,
            )
            ctx = build_sensor_context(cursor=None)
            sensor_def(ctx)

        mock_signal.assert_called_once_with(
            tmp_path, watch_table="D.S.T", minimum_interval_seconds=60
        )
