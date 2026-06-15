"""Unit tests for Snowflake destination.

Uses sys.modules injection to mock snowflake.connector — no real Snowflake
account or snowflake-connector-python install required (matches the pattern
in test_snowflake.py for the source-side connector).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import SnowflakeDestinationConfig, SyncOptions
from drt.destinations.snowflake import SnowflakeDestination

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _options(**kwargs: Any) -> SyncOptions:
    return SyncOptions(**kwargs)


def _config(**overrides: Any) -> SnowflakeDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "snowflake",
        "account_env": "SF_ACCOUNT",
        "user_env": "SF_USER",
        "password_env": "SF_PASSWORD",
        "database": "ANALYTICS",
        "schema": "PUBLIC",  # alias form — populated into schema_ on the model
        "table": "USER_SCORES",
        "warehouse": "COMPUTE_WH",
    }
    defaults.update(overrides)
    return SnowflakeDestinationConfig.model_validate(defaults)


def _set_creds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SF_ACCOUNT", "acct.us-east-1")
    monkeypatch.setenv("SF_USER", "test_user")
    monkeypatch.setenv("SF_PASSWORD", "test_pass")


def _fake_conn() -> MagicMock:
    """Fake snowflake.connector connection with a context-managed cursor."""
    conn = MagicMock()
    cur = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    conn.cursor.return_value.__exit__.return_value = False
    conn._cur = cur  # for assertions
    return conn


def _mocked_snowflake_modules(conn: MagicMock | None = None) -> dict[str, MagicMock]:
    """Build sys.modules entries that satisfy `import snowflake.connector`."""
    mock_module = MagicMock()
    mock_connector = MagicMock()
    if conn is not None:
        mock_connector.connect.return_value = conn
    mock_module.connector = mock_connector
    return {"snowflake": mock_module, "snowflake.connector": mock_connector}


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestSnowflakeDestinationConfig:
    def test_valid_config(self) -> None:
        config = _config()
        assert config.database == "ANALYTICS"
        assert config.schema_ == "PUBLIC"
        assert config.table == "USER_SCORES"
        assert config.mode == "insert"

    def test_yaml_uses_schema_alias(self) -> None:
        """YAML key `schema:` populates the `schema_` field (mypy-strict workaround)."""
        config = SnowflakeDestinationConfig.model_validate(
            {
                "type": "snowflake",
                "account_env": "SF_ACCOUNT",
                "user_env": "SF_USER",
                "password_env": "SF_PASSWORD",
                "database": "DB",
                "schema": "SCH",
                "table": "T",
                "warehouse": "WH",
            }
        )
        assert config.schema_ == "SCH"

    def test_describe_uses_schema(self) -> None:
        assert _config().describe() == "snowflake (ANALYTICS.PUBLIC.USER_SCORES)"


# ---------------------------------------------------------------------------
# Load behavior
# ---------------------------------------------------------------------------


class TestSnowflakeDestinationLoad:
    def test_empty_records_short_circuits_before_import(self) -> None:
        """No records → returns early before even attempting the snowflake import."""
        # No sys.modules patch; if load() reached the import it would raise.
        result = SnowflakeDestination().load([], _config(), _options())
        assert result.success == 0
        assert result.failed == 0

    def test_missing_credentials_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        monkeypatch.delenv("SF_ACCOUNT", raising=False)
        monkeypatch.delenv("SF_USER", raising=False)
        monkeypatch.delenv("SF_PASSWORD", raising=False)
        monkeypatch.chdir(tmp_path)
        with patch.dict("sys.modules", _mocked_snowflake_modules()):
            with pytest.raises(ValueError, match="Missing Snowflake credentials"):
                SnowflakeDestination().load([{"id": 1}], _config(), _options())

    def test_credentials_fallback_to_secrets_toml(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        monkeypatch.delenv("SF_ACCOUNT", raising=False)
        monkeypatch.delenv("SF_USER", raising=False)
        monkeypatch.delenv("SF_PASSWORD", raising=False)
        monkeypatch.chdir(tmp_path)
        
        secrets_dir = tmp_path / ".drt"
        secrets_dir.mkdir()
        (secrets_dir / "secrets.toml").write_text(
            '[destinations]\nSF_ACCOUNT = "acct"\nSF_USER = "user"\nSF_PASSWORD = "pwd"\n'
        )

        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)
        with patch.dict("sys.modules", modules):
            result = SnowflakeDestination().load([{"id": 1}], _config(), _options())
            
        assert result.failed == 0
        conn_kwargs = modules["snowflake.connector"].connect.call_args[1]
        assert conn_kwargs["account"] == "acct"
        assert conn_kwargs["user"] == "user"
        assert conn_kwargs["password"] == "pwd"

    def test_import_error_when_extras_missing(self) -> None:
        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(ImportError, match="drt-core\\[snowflake\\]"):
                SnowflakeDestination().load([{"id": 1}], _config(), _options())

    def test_insert_mode_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)

        records = [
            {"id": 1, "score": 0.95},
            {"id": 2, "score": 0.80},
        ]
        with patch.dict("sys.modules", modules):
            result = SnowflakeDestination().load(records, _config(), _options())

        assert result.success == 2
        assert result.failed == 0
        cur = conn._cur
        assert cur.execute.call_count == 2
        first_sql = cur.execute.call_args_list[0][0][0]
        assert "INSERT INTO ANALYTICS.PUBLIC.USER_SCORES" in first_sql
        assert "id, score" in first_sql
        conn.close.assert_called_once()

    def test_merge_mode_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)

        records = [
            {"id": 1, "score": 0.95},
            {"id": 2, "score": 0.80},
        ]
        config = _config(mode="merge", upsert_key=["id"])
        with patch.dict("sys.modules", modules):
            result = SnowflakeDestination().load(records, config, _options())

        assert result.success == 2
        sqls = [
            (call.args[0] if call.args else "")
            for call in conn._cur.execute.call_args_list
        ]
        assert any("CREATE TEMP TABLE" in s for s in sqls)
        assert any("MERGE INTO ANALYTICS.PUBLIC.USER_SCORES" in s for s in sqls)
        assert any("WHEN MATCHED THEN UPDATE" in s for s in sqls)

    def test_merge_mode_requires_upsert_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        modules = _mocked_snowflake_modules(_fake_conn())
        config = _config(mode="merge", upsert_key=None)
        with patch.dict("sys.modules", modules):
            with pytest.raises(ValueError, match="upsert_key is required"):
                SnowflakeDestination().load([{"id": 1}], config, _options())

    def test_insert_row_error_on_error_skip(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.execute.side_effect = [Exception("type mismatch"), None]
        modules = _mocked_snowflake_modules(conn)

        records = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": 0.9},
        ]
        with patch.dict("sys.modules", modules):
            result = SnowflakeDestination().load(
                records, _config(), _options(on_error="skip")
            )
        assert result.failed == 1
        assert result.success == 1
        assert len(result.row_errors) == 1
        assert "type mismatch" in result.row_errors[0].error_message

    def test_merge_insert_partial_fail_on_error_skip(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        cur = conn._cur

        insert_call_count = {"n": 0}

        def execute_side_effect(sql: str, *args: Any) -> None:
            if "INSERT INTO TMP_" in sql:
                insert_call_count["n"] += 1
                if insert_call_count["n"] == 1:
                    raise Exception("type mismatch")
            return None

        cur.execute.side_effect = execute_side_effect
        modules = _mocked_snowflake_modules(conn)

        records = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": 0.9},
        ]
        config = _config(mode="merge", upsert_key=["id"])
        with patch.dict("sys.modules", modules):
            result = SnowflakeDestination().load(
                records, config, _options(on_error="skip")
            )

        assert result.failed == 1
        assert result.success == 1
        assert len(result.row_errors) == 1
        
        sqls = [(call.args[0] if call.args else "") for call in cur.execute.call_args_list]
        assert any("MERGE INTO ANALYTICS.PUBLIC.USER_SCORES" in s for s in sqls)

    def test_merge_all_columns_are_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)

        records = [{"id": 1, "score": 0.95}]
        config = _config(mode="merge", upsert_key=["id", "score"])
        with patch.dict("sys.modules", modules):
            SnowflakeDestination().load(records, config, _options())

        sqls = [
            (call.args[0] if call.args else "")
            for call in conn._cur.execute.call_args_list
        ]
        merge_sql = next(s for s in sqls if "MERGE INTO" in s)
        assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
        assert "WHEN MATCHED THEN UPDATE" not in merge_sql


class TestSnowflakeConnection:
    def test_test_connection_success(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)
        
        with patch.dict("sys.modules", modules):
            dest = SnowflakeDestination()
            dest.test_connection(_config())
        
        conn.close.assert_called_once()
        # Snowflake uses cursor.execute("SELECT 1")
        assert any("SELECT 1" in str(call.args[0]) for call in conn._cur.execute.call_args_list)


# ---------------------------------------------------------------------------
# sync.mode: replace  (#434 — truncate default + swap)
# ---------------------------------------------------------------------------


def _sqls(cur: MagicMock) -> list[str]:
    return [(c.args[0] if c.args else "") for c in cur.execute.call_args_list]


class TestSnowflakeReplaceMode:
    @staticmethod
    def _swap_opts(**kw: Any) -> SyncOptions:
        return _options(mode="replace", replace_strategy="swap", **kw)

    def test_replace_truncate_truncates_then_inserts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)
        records = [{"id": 1, "score": 0.95}, {"id": 2, "score": 0.80}]
        with patch.dict("sys.modules", modules):
            result = SnowflakeDestination().load(
                records, _config(), _options(mode="replace")
            )
        assert result.success == 2
        sqls = _sqls(conn._cur)
        assert any(s.startswith("TRUNCATE TABLE ANALYTICS.PUBLIC.USER_SCORES") for s in sqls)
        assert sum("INSERT INTO ANALYTICS.PUBLIC.USER_SCORES" in s for s in sqls) == 2

    def test_replace_truncate_only_once_across_batches(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)
        dest = SnowflakeDestination()
        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1}], _config(), _options(mode="replace"))
            dest.load([{"id": 2}], _config(), _options(mode="replace"))
        assert sum(s.startswith("TRUNCATE TABLE") for s in _sqls(conn._cur)) == 1

    def test_replace_swap_creates_shadow_and_inserts(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.fetchall.return_value = [("USER_SCORES",)]  # target exists
        modules = _mocked_snowflake_modules(conn)
        with patch.dict("sys.modules", modules):
            result = SnowflakeDestination().load(
                [{"id": 1, "score": 0.95}], _config(), self._swap_opts()
            )
        assert result.success == 1
        sqls = _sqls(conn._cur)
        assert any(
            "CREATE OR REPLACE TABLE ANALYTICS.PUBLIC.USER_SCORES__drt_swap "
            "LIKE ANALYTICS.PUBLIC.USER_SCORES" in s
            for s in sqls
        )
        assert any("INSERT INTO ANALYTICS.PUBLIC.USER_SCORES__drt_swap" in s for s in sqls)

    def test_replace_swap_finalize_swaps_and_drops(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.fetchall.return_value = [("USER_SCORES",)]
        modules = _mocked_snowflake_modules(conn)
        dest = SnowflakeDestination()
        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1}], _config(), self._swap_opts())
            fin = dest.finalize_sync(_config(), self._swap_opts())
        assert fin is not None
        sqls = _sqls(conn._cur)
        assert any(
            "ALTER TABLE ANALYTICS.PUBLIC.USER_SCORES SWAP WITH "
            "ANALYTICS.PUBLIC.USER_SCORES__drt_swap" in s
            for s in sqls
        )
        assert any(s.startswith("DROP TABLE ANALYTICS.PUBLIC.USER_SCORES__drt_swap") for s in sqls)
        assert dest._swap_shadow_created is False
        assert dest._swap_table is None

    def test_replace_swap_first_run_target_absent_writes_direct(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.fetchall.return_value = []  # target does not exist
        modules = _mocked_snowflake_modules(conn)
        dest = SnowflakeDestination()
        with patch.dict("sys.modules", modules):
            result = dest.load([{"id": 1}], _config(), self._swap_opts())
            fin = dest.finalize_sync(_config(), self._swap_opts())
        assert result.success == 1
        sqls = _sqls(conn._cur)
        assert not any("__drt_swap" in s for s in sqls)  # no shadow involved
        assert any("INSERT INTO ANALYTICS.PUBLIC.USER_SCORES" in s for s in sqls)
        assert fin is None  # nothing to finalize

    def test_replace_swap_on_error_fail_drops_shadow(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        cur = conn._cur
        cur.fetchall.return_value = [("USER_SCORES",)]

        def side_effect(sql: str, *args: Any) -> None:
            if "INSERT INTO ANALYTICS.PUBLIC.USER_SCORES__drt_swap" in sql:
                raise Exception("type mismatch")
            return None

        cur.execute.side_effect = side_effect
        modules = _mocked_snowflake_modules(conn)
        dest = SnowflakeDestination()
        with patch.dict("sys.modules", modules):
            with pytest.raises(Exception, match="type mismatch"):
                dest.load([{"id": 1}], _config(), self._swap_opts(on_error="fail"))
        assert any(
            s.startswith("DROP TABLE IF EXISTS ANALYTICS.PUBLIC.USER_SCORES__drt_swap")
            for s in _sqls(cur)
        )
        assert dest._swap_shadow_created is False

    def test_finalize_noop_for_insert_mode(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)
        dest = SnowflakeDestination()
        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1}], _config(), _options())  # insert mode
            fin = dest.finalize_sync(_config(), _options())
        assert fin is None

    def test_finalize_swap_failure_preserves_state(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        # If SWAP raises, in-memory state is NOT reset — the shadow stays
        # recoverable (drt clean --orphans) and a retry is possible.
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.fetchall.return_value = [("USER_SCORES",)]
        modules = _mocked_snowflake_modules(conn)
        dest = SnowflakeDestination()
        with patch.dict("sys.modules", modules):
            dest.load([{"id": 1}], _config(), self._swap_opts())  # shadow built
            conn._cur.execute.side_effect = Exception("swap boom")
            with pytest.raises(Exception, match="swap boom"):
                dest.finalize_sync(_config(), self._swap_opts())
        assert dest._swap_shadow_created is True
        assert dest._swap_table == "ANALYTICS.PUBLIC.USER_SCORES"


class TestSnowflakeOrphanCleanup:
    def test_list_orphan_swap_tables_found(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.fetchall.return_value = [("USER_SCORES__drt_swap",)]
        modules = _mocked_snowflake_modules(conn)
        with patch.dict("sys.modules", modules):
            orphans = SnowflakeDestination().list_orphan_swap_tables(_config(), "USER_SCORES")
        assert orphans == ["ANALYTICS.PUBLIC.USER_SCORES__drt_swap"]

    def test_list_orphan_swap_tables_none(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.fetchall.return_value = []
        modules = _mocked_snowflake_modules(conn)
        with patch.dict("sys.modules", modules):
            orphans = SnowflakeDestination().list_orphan_swap_tables(_config(), "USER_SCORES")
        assert orphans == []

    def test_drop_orphan_only_drops_suffixed(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)
        with patch.dict("sys.modules", modules):
            dropped, failed = SnowflakeDestination().drop_orphan_swap_tables(
                _config(),
                ["ANALYTICS.PUBLIC.USER_SCORES__drt_swap", "ANALYTICS.PUBLIC.IMPORTANT_TABLE"],
            )
        assert dropped == ["ANALYTICS.PUBLIC.USER_SCORES__drt_swap"]
        assert failed == ["ANALYTICS.PUBLIC.IMPORTANT_TABLE"]
        sqls = _sqls(conn._cur)
        assert any(s.startswith("DROP TABLE ANALYTICS.PUBLIC.USER_SCORES__drt_swap") for s in sqls)
        assert not any("IMPORTANT_TABLE" in s for s in sqls)

    def test_drop_orphan_reports_drop_failure(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.execute.side_effect = Exception("permission denied")
        modules = _mocked_snowflake_modules(conn)
        with patch.dict("sys.modules", modules):
            dropped, failed = SnowflakeDestination().drop_orphan_swap_tables(
                _config(), ["ANALYTICS.PUBLIC.USER_SCORES__drt_swap"]
            )
        assert dropped == []
        assert failed == ["ANALYTICS.PUBLIC.USER_SCORES__drt_swap"]
