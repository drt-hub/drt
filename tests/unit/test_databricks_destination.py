"""Unit tests for the Databricks destination.

Uses ``sys.modules`` injection to mock ``databricks.sql`` — no real
Databricks workspace or databricks-sql-connector install required
(matches the pattern in test_snowflake_destination.py).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import DatabricksDestinationConfig, SyncOptions
from drt.destinations.databricks import DatabricksDestination

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _options(**kwargs: Any) -> SyncOptions:
    return SyncOptions(**kwargs)


def _config(**overrides: Any) -> DatabricksDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "databricks",
        "host_env": "DB_HOST",
        "http_path_env": "DB_HTTP_PATH",
        "token_env": "DB_TOKEN",
        "catalog": "main",
        "schema": "default",  # alias form — populated into schema_
        "table": "user_scores",
    }
    defaults.update(overrides)
    return DatabricksDestinationConfig.model_validate(defaults)


def _set_creds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DB_HOST", "dbc-abc123.cloud.databricks.com")
    monkeypatch.setenv("DB_HTTP_PATH", "/sql/1.0/warehouses/xyz789")
    monkeypatch.setenv("DB_TOKEN", "dapi-test-token")


def _fake_conn() -> MagicMock:
    """Fake databricks.sql connection with a context-managed cursor."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    conn._cur = cur  # for assertions
    return conn


def _mocked_databricks_modules(conn: MagicMock | None = None) -> dict[str, MagicMock]:
    """Build sys.modules entries that satisfy ``from databricks import sql``."""
    mock_sql = MagicMock()
    if conn is not None:
        mock_sql.connect.return_value = conn

    mock_databricks = MagicMock()
    mock_databricks.sql = mock_sql

    return {"databricks": mock_databricks, "databricks.sql": mock_sql}


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestDatabricksDestinationConfig:
    def test_valid_config(self) -> None:
        config = _config()
        assert config.catalog == "main"
        assert config.schema_ == "default"
        assert config.table == "user_scores"
        assert config.mode == "insert"
        assert config.upsert_key is None

    def test_yaml_uses_schema_alias(self) -> None:
        """YAML key `schema:` populates the `schema_` field (mypy-strict workaround)."""
        config = DatabricksDestinationConfig.model_validate(
            {
                "type": "databricks",
                "host_env": "DB_HOST",
                "http_path_env": "DB_HTTP_PATH",
                "token_env": "DB_TOKEN",
                "catalog": "main",
                "schema": "analytics",
                "table": "users",
            }
        )
        assert config.schema_ == "analytics"

    def test_describe_uses_three_part_name(self) -> None:
        assert _config().describe() == "databricks (main.default.user_scores)"

    def test_hive_metastore_catalog_is_valid(self) -> None:
        """Workspaces on Hive Metastore use ``catalog: hive_metastore``."""
        config = _config(catalog="hive_metastore")
        assert config.describe() == "databricks (hive_metastore.default.user_scores)"


# ---------------------------------------------------------------------------
# Load behavior
# ---------------------------------------------------------------------------


class TestDatabricksDestinationLoad:
    def test_empty_records_short_circuits_before_import(self) -> None:
        """No records → returns early before even attempting the databricks import.

        Mirrors the empty-batch contract (#604–#606): if ``load([])``
        ever reaches the import, this test crashes with
        ``ModuleNotFoundError`` on CI's minimal install (no [databricks]).
        """
        result = DatabricksDestination().load([], _config(), _options())
        assert result.success == 0
        assert result.failed == 0

    def test_missing_credentials_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        monkeypatch.delenv("DB_HOST", raising=False)
        monkeypatch.delenv("DB_HTTP_PATH", raising=False)
        monkeypatch.delenv("DB_TOKEN", raising=False)
        monkeypatch.chdir(tmp_path)
        with patch.dict("sys.modules", _mocked_databricks_modules()):
            with pytest.raises(ValueError, match="Missing Databricks credentials"):
                DatabricksDestination().load([{"id": 1}], _config(), _options())

    def test_import_error_when_extras_missing(self) -> None:
        """No [databricks] extras → ImportError with the install hint."""
        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(ImportError, match=r"drt-core\[databricks\]"):
                DatabricksDestination().load([{"id": 1}], _config(), _options())

    def test_connect_uses_databricks_sql_kwargs(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Confirm the connect() call uses the Databricks SQL Connector
        kwargs (``server_hostname``, ``http_path``, ``access_token``)
        rather than e.g. the Snowflake shape — protects against
        silent template-copy drift between SQL destinations."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)

        with patch.dict("sys.modules", modules):
            DatabricksDestination().load([{"id": 1}], _config(), _options())

        conn_kwargs = modules["databricks.sql"].connect.call_args[1]
        assert conn_kwargs["server_hostname"] == "dbc-abc123.cloud.databricks.com"
        assert conn_kwargs["http_path"] == "/sql/1.0/warehouses/xyz789"
        assert conn_kwargs["access_token"] == "dapi-test-token"

    def test_insert_mode_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)

        records = [
            {"id": 1, "score": 0.95},
            {"id": 2, "score": 0.80},
        ]
        with patch.dict("sys.modules", modules):
            result = DatabricksDestination().load(records, _config(), _options())

        assert result.success == 2
        assert result.failed == 0
        cur = conn._cur
        assert cur.execute.call_count == 2
        first_sql = cur.execute.call_args_list[0][0][0]
        assert "INSERT INTO main.default.user_scores" in first_sql
        assert "id, score" in first_sql
        conn.close.assert_called_once()

    def test_merge_mode_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)

        records = [
            {"id": 1, "score": 0.95},
            {"id": 2, "score": 0.80},
        ]
        config = _config(mode="merge", upsert_key=["id"])
        with patch.dict("sys.modules", modules):
            result = DatabricksDestination().load(records, config, _options())

        assert result.success == 2
        sqls = [(call.args[0] if call.args else "") for call in conn._cur.execute.call_args_list]
        # Staging Delta table created from the target table's schema
        assert any(
            "CREATE OR REPLACE TABLE main.default.__drt_staging_user_scores" in s for s in sqls
        )
        # Staging gets INSERTed before MERGE
        assert any("INSERT INTO main.default.__drt_staging_user_scores" in s for s in sqls)
        # MERGE INTO target FROM staging
        assert any("MERGE INTO main.default.user_scores" in s for s in sqls)
        assert any("WHEN MATCHED THEN UPDATE" in s for s in sqls)
        assert any("WHEN NOT MATCHED THEN INSERT" in s for s in sqls)
        # Staging table is dropped at the end so subsequent syncs don't trip
        assert any("DROP TABLE IF EXISTS main.default.__drt_staging_user_scores" in s for s in sqls)

    def test_merge_mode_requires_upsert_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        modules = _mocked_databricks_modules(_fake_conn())
        config = _config(mode="merge", upsert_key=None)
        with patch.dict("sys.modules", modules):
            with pytest.raises(ValueError, match="upsert_key is required"):
                DatabricksDestination().load([{"id": 1}], config, _options())

    def test_insert_row_error_on_error_skip(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.execute.side_effect = [Exception("type mismatch"), None]
        modules = _mocked_databricks_modules(conn)

        records = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": 0.9},
        ]
        with patch.dict("sys.modules", modules):
            result = DatabricksDestination().load(records, _config(), _options(on_error="skip"))
        assert result.failed == 1
        assert result.success == 1
        assert len(result.row_errors) == 1
        assert "type mismatch" in result.row_errors[0].error_message

    def test_insert_row_error_on_error_fail_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.execute.side_effect = Exception("type mismatch")
        modules = _mocked_databricks_modules(conn)

        with patch.dict("sys.modules", modules):
            with pytest.raises(Exception, match="type mismatch"):
                DatabricksDestination().load([{"id": 1}], _config(), _options(on_error="fail"))
        # Connection still closed via the try/finally
        conn.close.assert_called_once()

    def test_merge_composite_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Composite ``upsert_key`` builds an AND-joined ON clause."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)

        records = [{"tenant_id": "a", "user_id": 1, "score": 0.95}]
        config = _config(mode="merge", upsert_key=["tenant_id", "user_id"])
        with patch.dict("sys.modules", modules):
            DatabricksDestination().load(records, config, _options())

        sqls = [(call.args[0] if call.args else "") for call in conn._cur.execute.call_args_list]
        merge_sql = next(s for s in sqls if "MERGE INTO" in s)
        assert (
            "target.tenant_id = source.tenant_id AND target.user_id = source.user_id" in merge_sql
        )

    def test_merge_staging_insert_failure_on_error_skip(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Row failure during the staging INSERT lands in row_errors,
        the sync continues, and the MERGE still runs against whatever
        the staging table holds."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        cur = conn._cur

        # Fail the FIRST staging INSERT (row 0), let everything else through.
        # The CREATE OR REPLACE TABLE statement runs first, then per-row
        # INSERT INTO __drt_staging_..., then MERGE INTO, then DROP TABLE.
        insert_call_count = {"n": 0}

        def execute_side_effect(sql: str, *args: Any) -> None:
            if "INSERT INTO main.default.__drt_staging_user_scores" in sql:
                insert_call_count["n"] += 1
                if insert_call_count["n"] == 1:
                    raise Exception("staging type mismatch")
            return None

        cur.execute.side_effect = execute_side_effect
        modules = _mocked_databricks_modules(conn)

        records = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": 0.9},
        ]
        config = _config(mode="merge", upsert_key=["id"])
        with patch.dict("sys.modules", modules):
            result = DatabricksDestination().load(
                records, config, _options(on_error="skip")
            )

        assert result.failed == 1
        assert result.success == 1
        assert len(result.row_errors) == 1
        assert "staging type mismatch" in result.row_errors[0].error_message
        # The MERGE statement still ran (against the staging table that
        # ended up with one row).
        sqls = [(call.args[0] if call.args else "") for call in cur.execute.call_args_list]
        assert any("MERGE INTO main.default.user_scores" in s for s in sqls)

    def test_merge_staging_insert_failure_on_error_fail_raises(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``on_error=fail`` re-raises the staging-INSERT exception
        immediately. The connection is still closed via try/finally."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        cur = conn._cur

        def execute_side_effect(sql: str, *args: Any) -> None:
            if "INSERT INTO main.default.__drt_staging_user_scores" in sql:
                raise Exception("staging type mismatch")
            return None

        cur.execute.side_effect = execute_side_effect
        modules = _mocked_databricks_modules(conn)

        config = _config(mode="merge", upsert_key=["id"])
        with patch.dict("sys.modules", modules):
            with pytest.raises(Exception, match="staging type mismatch"):
                DatabricksDestination().load(
                    [{"id": 1, "score": 0.5}], config, _options(on_error="fail")
                )
        conn.close.assert_called_once()

    def test_unsupported_mode_raises(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """An invalid ``config.mode`` raises ``ValueError``. Pydantic
        prevents this at config-load time so the path is defensive —
        this test bypasses Pydantic to exercise the fallthrough branch."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)

        config = _config(mode="insert")
        # Bypass Pydantic Literal validation by mutating after construction.
        object.__setattr__(config, "mode", "garbage")  # type: ignore[arg-type]

        with patch.dict("sys.modules", modules):
            with pytest.raises(ValueError, match="Unsupported mode: garbage"):
                DatabricksDestination().load([{"id": 1}], config, _options())

    def test_merge_all_columns_are_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """When every column is in upsert_key, the MERGE skips the
        UPDATE clause (no non-key columns to update)."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)

        records = [{"id": 1, "score": 0.95}]
        config = _config(mode="merge", upsert_key=["id", "score"])
        with patch.dict("sys.modules", modules):
            DatabricksDestination().load(records, config, _options())

        sqls = [(call.args[0] if call.args else "") for call in conn._cur.execute.call_args_list]
        merge_sql = next(s for s in sqls if "MERGE INTO" in s)
        assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
        assert "WHEN MATCHED THEN UPDATE" not in merge_sql


# ---------------------------------------------------------------------------
# sync.mode: mirror (#340 family — Databricks leg)
# ---------------------------------------------------------------------------


class TestDatabricksMirrorMode:
    def test_mirror_requires_upsert_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        modules = _mocked_databricks_modules(_fake_conn())
        config = _config(upsert_key=None)
        with patch.dict("sys.modules", modules):
            with pytest.raises(ValueError, match="mirror requires destination.upsert_key"):
                DatabricksDestination().load([{"id": 1}], config, _options(mode="mirror"))

    def test_mirror_forces_merge_path_regardless_of_config_mode(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``sync.mode: mirror`` forces MERGE write path even when
        ``config.mode: insert``. Matches Snowflake's leg of #340."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)

        config = _config(mode="insert", upsert_key=["id"])
        with patch.dict("sys.modules", modules):
            DatabricksDestination().load(
                [{"id": 1, "score": 0.95}], config, _options(mode="mirror")
            )

        sqls = [(call.args[0] if call.args else "") for call in conn._cur.execute.call_args_list]
        assert any("MERGE INTO main.default.user_scores" in s for s in sqls)

    def test_mirror_finalize_issues_delete_not_in(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """End-of-sync DELETE removes destination rows whose
        upsert_key was not observed in the source."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)

        config = _config(mode="merge", upsert_key=["id"])
        dest = DatabricksDestination()
        with patch.dict("sys.modules", modules):
            dest.load(
                [{"id": 1, "score": 0.5}, {"id": 2, "score": 0.9}],
                config,
                _options(mode="mirror"),
            )
            dest.finalize_sync(config, _options(mode="mirror"))

        # The DELETE was issued (in a separate connection cycle)
        sqls = [(call.args[0] if call.args else "") for call in conn._cur.execute.call_args_list]
        delete_sql = next(s for s in sqls if s.startswith("DELETE FROM"))
        assert "DELETE FROM main.default.user_scores" in delete_sql
        assert "WHERE id NOT IN" in delete_sql

    def test_mirror_finalize_composite_key_uses_tuple_form(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Composite upsert_key uses ``WHERE (c1, c2) NOT IN ((..., ...))``."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)

        config = _config(mode="merge", upsert_key=["tenant_id", "user_id"])
        dest = DatabricksDestination()
        with patch.dict("sys.modules", modules):
            dest.load(
                [{"tenant_id": "a", "user_id": 1, "score": 0.5}],
                config,
                _options(mode="mirror"),
            )
            dest.finalize_sync(config, _options(mode="mirror"))

        sqls = [(call.args[0] if call.args else "") for call in conn._cur.execute.call_args_list]
        delete_sql = next(s for s in sqls if s.startswith("DELETE FROM"))
        assert "WHERE (tenant_id, user_id) NOT IN" in delete_sql

    def test_mirror_skips_failed_keys_from_delete_observed_set(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Mirror's ``_mirror_keys`` accumulator must skip records that
        failed the staging INSERT — those rows never made it into the
        destination, so they shouldn't count as "observed in source"
        for the end-of-sync DELETE."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        cur = conn._cur

        # First record's staging INSERT fails; second succeeds.
        insert_call_count = {"n": 0}

        def execute_side_effect(sql: str, *args: Any) -> None:
            if "INSERT INTO main.default.__drt_staging_user_scores" in sql:
                insert_call_count["n"] += 1
                if insert_call_count["n"] == 1:
                    raise Exception("staging type mismatch")
            return None

        cur.execute.side_effect = execute_side_effect
        modules = _mocked_databricks_modules(conn)

        config = _config(mode="merge", upsert_key=["id"])
        dest = DatabricksDestination()
        with patch.dict("sys.modules", modules):
            dest.load(
                [{"id": 1, "score": 0.5}, {"id": 2, "score": 0.9}],
                config,
                _options(mode="mirror", on_error="skip"),
            )
            dest.finalize_sync(config, _options(mode="mirror"))

        # The DELETE was issued and includes only id=2 (the survivor),
        # not id=1 (which failed staging).
        sqls = [(call.args[0] if call.args else "") for call in cur.execute.call_args_list]
        delete_call = next(
            call
            for call in cur.execute.call_args_list
            if call.args and call.args[0].startswith("DELETE FROM")
        )
        delete_params = delete_call.args[1] if len(delete_call.args) > 1 else []
        assert delete_params == [2]
        # And the DELETE was actually issued (not skipped — at least one
        # record made it through).
        assert any(s.startswith("DELETE FROM") for s in sqls)

    def test_mirror_finalize_skipped_when_no_records_observed(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """No batch ever produced records → ``finalize_sync`` skips the
        DELETE entirely. Safety guard against wiping the destination
        when the source is transiently empty."""
        _set_creds(monkeypatch)
        modules = _mocked_databricks_modules(_fake_conn())
        config = _config(mode="merge", upsert_key=["id"])
        dest = DatabricksDestination()
        with patch.dict("sys.modules", modules):
            # No load() call — _mirror_keys stays None
            result = dest.finalize_sync(config, _options(mode="mirror"))

        assert result is None

    def test_finalize_sync_skipped_for_non_mirror_modes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """``finalize_sync`` is a no-op for any mode that isn't mirror."""
        _set_creds(monkeypatch)
        modules = _mocked_databricks_modules(_fake_conn())
        config = _config(mode="merge", upsert_key=["id"])
        dest = DatabricksDestination()
        with patch.dict("sys.modules", modules):
            assert dest.finalize_sync(config, _options(mode="full")) is None
            assert dest.finalize_sync(config, _options(mode="upsert")) is None
            assert dest.finalize_sync(config, _options(mode="replace")) is None


# ---------------------------------------------------------------------------
# test_connection
# ---------------------------------------------------------------------------


class TestDatabricksConnection:
    def test_test_connection_runs_select_1(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)

        with patch.dict("sys.modules", modules):
            DatabricksDestination().test_connection(_config())

        conn.close.assert_called_once()
        assert any("SELECT 1" in str(call.args[0]) for call in conn._cur.execute.call_args_list)


# ---------------------------------------------------------------------------
# sync.mode: replace  (#643 — truncate default + swap via INSERT OVERWRITE)
# ---------------------------------------------------------------------------

_FQ = "main.default.user_scores"
_SHADOW = "main.default.user_scores__drt_swap"


def _sqls(cur: MagicMock) -> list[str]:
    return [(c.args[0] if c.args else "") for c in cur.execute.call_args_list]


class TestDatabricksReplaceMode:
    @staticmethod
    def _swap_opts(**kw: Any) -> SyncOptions:
        return _options(mode="replace", replace_strategy="swap", **kw)

    def test_replace_truncate_truncates_then_inserts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)
        records = [{"id": 1, "score": 0.95}, {"id": 2, "score": 0.80}]
        with patch.dict("sys.modules", modules):
            result = DatabricksDestination().load(
                records, _config(), _options(mode="replace")
            )
        assert result.success == 2
        sqls = _sqls(conn._cur)
        assert any(s.startswith(f"TRUNCATE TABLE {_FQ}") for s in sqls)
        assert sum(f"INSERT INTO {_FQ} (" in s for s in sqls) == 2

    def test_replace_truncate_only_once_across_batches(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)
        dest = DatabricksDestination()
        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1}], _config(), _options(mode="replace"))
            dest.load([{"id": 2}], _config(), _options(mode="replace"))
        assert sum(s.startswith("TRUNCATE TABLE") for s in _sqls(conn._cur)) == 1

    def test_replace_swap_creates_shadow_and_inserts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.fetchall.return_value = [("user_scores",)]  # target exists
        modules = _mocked_databricks_modules(conn)
        with patch.dict("sys.modules", modules):
            result = DatabricksDestination().load(
                [{"id": 1, "score": 0.95}], _config(), self._swap_opts()
            )
        assert result.success == 1
        sqls = _sqls(conn._cur)
        assert any(
            f"CREATE OR REPLACE TABLE {_SHADOW} AS SELECT * FROM {_FQ} WHERE 1=0" in s
            for s in sqls
        )
        assert any(f"INSERT INTO {_SHADOW} (" in s for s in sqls)

    def test_replace_swap_finalize_overwrites_and_drops(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.fetchall.return_value = [("user_scores",)]
        modules = _mocked_databricks_modules(conn)
        dest = DatabricksDestination()
        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1}], _config(), self._swap_opts())
            fin = dest.finalize_sync(_config(), self._swap_opts())
        assert fin is not None
        sqls = _sqls(conn._cur)
        assert any(f"INSERT OVERWRITE {_FQ} SELECT * FROM {_SHADOW}" in s for s in sqls)
        assert any(s.startswith(f"DROP TABLE IF EXISTS {_SHADOW}") for s in sqls)
        assert dest._swap_shadow_created is False
        assert dest._swap_table is None

    def test_replace_swap_first_run_target_absent_writes_direct(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.fetchall.return_value = []  # target does not exist
        modules = _mocked_databricks_modules(conn)
        dest = DatabricksDestination()
        with patch.dict("sys.modules", modules):
            result = dest.load([{"id": 1}], _config(), self._swap_opts())
            fin = dest.finalize_sync(_config(), self._swap_opts())
        assert result.success == 1
        sqls = _sqls(conn._cur)
        assert not any("__drt_swap" in s for s in sqls)  # no shadow involved
        assert any(f"INSERT INTO {_FQ} (" in s for s in sqls)
        assert fin is None

    def test_replace_swap_on_error_fail_drops_shadow(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        cur = conn._cur
        cur.fetchall.return_value = [("user_scores",)]

        def side_effect(sql: str, *args: Any) -> None:
            if f"INSERT INTO {_SHADOW} (" in sql:
                raise Exception("type mismatch")
            return None

        cur.execute.side_effect = side_effect
        modules = _mocked_databricks_modules(conn)
        dest = DatabricksDestination()
        with patch.dict("sys.modules", modules):
            with pytest.raises(Exception, match="type mismatch"):
                dest.load([{"id": 1}], _config(), self._swap_opts(on_error="fail"))
        assert any(s.startswith(f"DROP TABLE IF EXISTS {_SHADOW}") for s in _sqls(cur))
        assert dest._swap_shadow_created is False

    def test_finalize_noop_for_insert_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)
        dest = DatabricksDestination()
        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1}], _config(), _options())  # insert mode
            fin = dest.finalize_sync(_config(), _options())
        assert fin is None


class TestDatabricksOrphanCleanup:
    def test_list_orphan_swap_tables_found(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.fetchall.return_value = [("user_scores__drt_swap",)]
        modules = _mocked_databricks_modules(conn)
        with patch.dict("sys.modules", modules):
            orphans = DatabricksDestination().list_orphan_swap_tables(_config(), "user_scores")
        assert orphans == [_SHADOW]

    def test_list_orphan_swap_tables_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.fetchall.return_value = []
        modules = _mocked_databricks_modules(conn)
        with patch.dict("sys.modules", modules):
            orphans = DatabricksDestination().list_orphan_swap_tables(_config(), "user_scores")
        assert orphans == []

    def test_drop_orphan_only_drops_suffixed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)
        with patch.dict("sys.modules", modules):
            dropped, failed = DatabricksDestination().drop_orphan_swap_tables(
                _config(), [_SHADOW, "main.default.important_table"]
            )
        assert dropped == [_SHADOW]
        assert failed == ["main.default.important_table"]
        sqls = _sqls(conn._cur)
        assert any(s.startswith(f"DROP TABLE {_SHADOW}") for s in sqls)
        assert not any("important_table" in s for s in sqls)

    def test_drop_orphan_reports_drop_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.execute.side_effect = Exception("permission denied")
        modules = _mocked_databricks_modules(conn)
        with patch.dict("sys.modules", modules):
            dropped, failed = DatabricksDestination().drop_orphan_swap_tables(
                _config(), [_SHADOW]
            )
        assert dropped == []
        assert failed == [_SHADOW]
