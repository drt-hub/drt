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
from drt.destinations.snowflake import (
    SnowflakeDestination,
    _bind_row,
    _rows_per_merge_chunk,
)
from drt.destinations.sql_base import BaseSqlDestination

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
        # These tests assert exact SQL / driver call ordering; Layer-3
        # introspection (#317) would add an INFORMATION_SCHEMA round-trip and
        # rewrite VARIANT binds. It has its own tests — keep it off here.
        "introspect_schema": False,
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
    conn.cursor.return_value = cur
    cur.__enter__.return_value = cur
    cur.__exit__.return_value = False
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


def test_snowflake_subclasses_sql_base() -> None:
    dest = SnowflakeDestination()
    phase_3_hooks = {
        "_build_mirror_delete",
        "_shadow_name",
        "_old_name",
        "_rename_swap",
        "_state_table_ident",
        "_state_table_exists",
        "_create_state_table",
        "_state_scope_columns_exist",
        "_add_state_scope_columns",
        "_state_sql",
    }

    assert isinstance(dest, BaseSqlDestination)
    assert phase_3_hooks.isdisjoint(SnowflakeDestination.__dict__)
    assert {"_load_replace_swap", "_load_replace", "_load_upsert"}.issubset(
        SnowflakeDestination.__dict__
    )
    assert "load" not in SnowflakeDestination.__dict__
    assert "finalize_sync" in SnowflakeDestination.__dict__
    assert dest._replace_truncated is False
    assert dest._swap_shadow_created is False
    assert dest._swap_table is None
    assert dest._mirror_keys is None
    assert dest._mirror_scopes is None
    assert dest._schema_cache == {}


def test_snowflake_dialect_hooks_forward_query_tags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}

    def _fake_connect(
        cls: type[SnowflakeDestination],
        config: SnowflakeDestinationConfig,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> str:
        calls.update(cls=cls, config=config, query_tags=query_tags)
        return "CONN"

    monkeypatch.setattr(SnowflakeDestination, "_connect", classmethod(_fake_connect))
    config = _config()
    tags = {"sync": "users"}

    dest = SnowflakeDestination()
    assert dest._dialect_connect(config, tags) == "CONN"
    assert calls == {
        "cls": SnowflakeDestination,
        "config": config,
        "query_tags": tags,
    }
    assert dest._qualify_ident("DB.PUBLIC.USERS") == "DB.PUBLIC.USERS"


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

    def test_query_tags_set_session_parameter_and_comment(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """#768 — query_tags become both the native QUERY_TAG session
        parameter at connect and a SQL comment on the executed statement."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)

        options = _options()
        options._query_tags = {"sync": "s", "run_id": "r"}
        with patch.dict("sys.modules", modules):
            result = SnowflakeDestination().load([{"id": 1}], _config(), options)

        assert result.failed == 0
        conn_kwargs = modules["snowflake.connector"].connect.call_args[1]
        import json

        assert json.loads(conn_kwargs["session_parameters"]["QUERY_TAG"]) == {
            "sync": "s",
            "run_id": "r",
        }
        query = conn._cur.execute.call_args.args[0]
        assert query.startswith("/* drt sync=s run_id=r */\n")

    def test_no_query_tags_omits_session_parameter(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)

        with patch.dict("sys.modules", modules):
            SnowflakeDestination().load([{"id": 1}], _config(), _options())

        conn_kwargs = modules["snowflake.connector"].connect.call_args[1]
        assert "session_parameters" not in conn_kwargs
        query = conn._cur.execute.call_args.args[0]
        assert not query.startswith("/* drt")

    def test_import_error_when_extras_missing(self) -> None:
        # Build config/options BEFORE patching __import__ — pydantic may
        # lazily finish a deferred validator on first model_validate, and
        # under a global import patch that surfaces as a bare ImportError
        # instead of the connector-extra message under test.
        config = _config()
        options = _options()
        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(ImportError, match="drt-core\\[snowflake\\]"):
                SnowflakeDestination().load([{"id": 1}], config, options)

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
        sqls = [(call.args[0] if call.args else "") for call in conn._cur.execute.call_args_list]
        # #988: no staging table — the MERGE sources from a VALUES subquery.
        assert not any("CREATE" in s for s in sqls)
        assert any("MERGE INTO ANALYTICS.PUBLIC.USER_SCORES" in s for s in sqls)
        assert any("WHEN MATCHED THEN UPDATE" in s for s in sqls)

    def test_merge_mode_requires_upsert_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        modules = _mocked_snowflake_modules(_fake_conn())
        config = _config(mode="merge", upsert_key=None)
        with patch.dict("sys.modules", modules):
            with pytest.raises(ValueError, match="upsert_key is required"):
                SnowflakeDestination().load([{"id": 1}], config, _options())

    def test_insert_row_error_on_error_skip(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.execute.side_effect = [Exception("type mismatch"), None]
        modules = _mocked_snowflake_modules(conn)

        records = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": 0.9},
        ]
        with patch.dict("sys.modules", modules):
            result = SnowflakeDestination().load(records, _config(), _options(on_error="skip"))
        assert result.failed == 1
        assert result.success == 1
        assert len(result.row_errors) == 1
        assert "type mismatch" in result.row_errors[0].error_message

    def test_merge_insert_partial_fail_on_error_skip(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """#988: a bulk chunk MERGE failure falls back to one MERGE per row,
        isolating exactly which row(s) actually fail rather than losing the
        whole chunk."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        cur = conn._cur

        call_count = {"n": 0}

        def execute_side_effect(sql: str, *args: Any) -> None:
            call_count["n"] += 1
            # call 1: bulk MERGE for the 2-row chunk — fail, forcing fallback.
            # call 2: per-row MERGE for record idx 0 — fail (the real error).
            # call 3: per-row MERGE for record idx 1 — succeed.
            if call_count["n"] in (1, 2):
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
            result = SnowflakeDestination().load(records, config, _options(on_error="skip"))

        assert result.failed == 1
        assert result.success == 1
        assert len(result.row_errors) == 1
        assert result.row_errors[0].batch_index == 0

        sqls = [(call.args[0] if call.args else "") for call in cur.execute.call_args_list]
        assert any("MERGE INTO ANALYTICS.PUBLIC.USER_SCORES" in s for s in sqls)
        assert not any("CREATE" in s for s in sqls)

    def test_merge_all_columns_are_key(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)

        records = [{"id": 1, "score": 0.95}]
        config = _config(mode="merge", upsert_key=["id", "score"])
        with patch.dict("sys.modules", modules):
            SnowflakeDestination().load(records, config, _options())

        sqls = [(call.args[0] if call.args else "") for call in conn._cur.execute.call_args_list]
        merge_sql = next(s for s in sqls if "MERGE INTO" in s)
        assert "WHEN NOT MATCHED THEN INSERT" in merge_sql
        assert "WHEN MATCHED THEN UPDATE" not in merge_sql

    def test_rows_per_merge_chunk_scales_with_column_count(self) -> None:
        """#988: chunk size is a param-budget / column-count derivation, the
        same shape as databricks.py's _rows_per_chunk — not a fixed row
        count, so a wide table automatically gets smaller chunks."""
        assert _rows_per_merge_chunk(2) == 1000  # budget 2000 // 2 cols
        assert _rows_per_merge_chunk(20) == 100
        assert _rows_per_merge_chunk(2000) == 1
        assert _rows_per_merge_chunk(10_000) == 1  # never below 1

    def test_merge_chunks_records_into_multiple_bulk_statements(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """N records over the per-chunk budget emit multiple bulk MERGE
        statements, each sized to its own chunk — not one unbounded
        statement, and not one statement per row either."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)

        # Force a tiny chunk size (2 rows) without needing thousands of
        # records to exercise the boundary.
        monkeypatch.setattr("drt.destinations.snowflake._MERGE_PARAM_BUDGET", 4)

        records = [{"id": i, "score": float(i)} for i in range(5)]  # 2 cols -> chunk=2
        config = _config(mode="merge", upsert_key=["id"])
        with patch.dict("sys.modules", modules):
            result = SnowflakeDestination().load(records, config, _options())

        assert result.success == 5
        assert result.failed == 0

        sqls = [(call.args[0] if call.args else "") for call in conn._cur.execute.call_args_list]
        merge_sqls = [s for s in sqls if "MERGE INTO" in s]
        # 5 rows / chunk size 2 -> chunks of 2, 2, 1 -> 3 bulk MERGE statements.
        assert len(merge_sqls) == 3
        # Each statement's USING subquery has exactly as many VALUES rows as
        # its chunk — verified by counting the row placeholder groups.
        row_counts = [s.count("(%s, %s)") for s in merge_sqls]
        assert row_counts == [2, 2, 1]


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

    def test_replace_truncate_truncates_then_inserts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_snowflake_modules(conn)
        records = [{"id": 1, "score": 0.95}, {"id": 2, "score": 0.80}]
        with patch.dict("sys.modules", modules):
            result = SnowflakeDestination().load(records, _config(), _options(mode="replace"))
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

    def test_replace_swap_creates_shadow_and_inserts(self, monkeypatch: pytest.MonkeyPatch) -> None:
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

    def test_replace_swap_finalize_swaps_and_drops(self, monkeypatch: pytest.MonkeyPatch) -> None:
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

    def test_replace_swap_on_error_fail_drops_shadow(self, monkeypatch: pytest.MonkeyPatch) -> None:
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

    def test_finalize_swap_failure_preserves_state(self, monkeypatch: pytest.MonkeyPatch) -> None:
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


class TestSnowflakeSchemaIntrospection:
    """Layer 3 (#317) — INFORMATION_SCHEMA-driven VARIANT serialization."""

    def test_introspect_disabled_skips_describe(self) -> None:
        with patch("drt.destinations.schema.describe_columns") as desc:
            out = SnowflakeDestination()._resolve_schema(_config(introspect_schema=False))
        assert out is None
        desc.assert_not_called()

    def test_resolve_schema_caches_across_batches(self) -> None:
        dest = SnowflakeDestination()
        cfg = _config(introspect_schema=True)
        with patch(
            "drt.destinations.schema.describe_columns",
            return_value={"PAYLOAD": "json"},
        ) as desc:
            assert dest._resolve_schema(cfg) == {"PAYLOAD": "json"}
            assert dest._resolve_schema(cfg) == {"PAYLOAD": "json"}
        desc.assert_called_once()

    def test_variant_column_uses_parse_json_and_dumps_value(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A VARIANT column → INSERT ... SELECT PARSE_JSON(%s), value json.dumps'd."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        with (
            patch.dict("sys.modules", _mocked_snowflake_modules(conn)),
            patch(
                "drt.destinations.schema.describe_columns",
                return_value={"id": "scalar", "payload": "json"},
            ),
        ):
            result = SnowflakeDestination().load(
                [{"id": 1, "payload": {"a": 1}}],
                _config(introspect_schema=True),
                _options(),
            )
        assert result.success == 1
        sql, bound = conn._cur.execute.call_args_list[0][0]
        assert "SELECT %s, PARSE_JSON(%s)" in sql
        assert "VALUES" not in sql
        assert bound == [1, '{"a": 1}']  # id raw, payload JSON-encoded

    def test_variant_match_is_case_insensitive(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Snowflake reports UPPERCASE column names for unquoted DDL while source
        keys are lowercase — PARSE_JSON wrapping must still fire (#317 review)."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        with (
            patch.dict("sys.modules", _mocked_snowflake_modules(conn)),
            patch(
                "drt.destinations.schema.describe_columns",
                # UPPERCASE keys, exactly as Snowflake's INFORMATION_SCHEMA reports them.
                return_value={"ID": "scalar", "PAYLOAD": "json"},
            ),
        ):
            result = SnowflakeDestination().load(
                [{"id": 1, "payload": {"a": 1}}],  # lowercase source keys
                _config(introspect_schema=True),
                _options(),
            )
        assert result.success == 1
        sql, bound = conn._cur.execute.call_args_list[0][0]
        assert "SELECT %s, PARSE_JSON(%s)" in sql
        assert "VALUES" not in sql
        assert bound == [1, '{"a": 1}']

    def test_no_variant_keeps_values_form(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """All-scalar schema → unchanged VALUES form, values bound as-is."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        with (
            patch.dict("sys.modules", _mocked_snowflake_modules(conn)),
            patch(
                "drt.destinations.schema.describe_columns",
                return_value={"id": "scalar", "score": "scalar"},
            ),
        ):
            SnowflakeDestination().load(
                [{"id": 1, "score": 0.5}],
                _config(introspect_schema=True),
                _options(),
            )
        sql, bound = conn._cur.execute.call_args_list[0][0]
        assert "VALUES (%s, %s)" in sql
        assert "PARSE_JSON" not in sql
        assert bound == [1, 0.5]

    def test_merge_variant_column_wraps_parse_json_outside_values(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """#988: a JSON/VARIANT column in MERGE mode gets PARSE_JSON applied
        in the outer SELECT that projects the VALUES table, not inside the
        VALUES() clause itself -- Snowflake disallows functions there (same
        constraint _value_clause already works around for INSERT)."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        with (
            patch.dict("sys.modules", _mocked_snowflake_modules(conn)),
            patch(
                "drt.destinations.schema.describe_columns",
                return_value={"id": "scalar", "payload": "json"},
            ),
        ):
            result = SnowflakeDestination().load(
                [{"id": 1, "payload": {"a": 1}}],
                _config(introspect_schema=True, mode="merge", upsert_key=["id"]),
                _options(),
            )
        assert result.success == 1
        merge_sql, bound = conn._cur.execute.call_args_list[0][0]
        assert "MERGE INTO" in merge_sql
        assert "FROM (VALUES (%s, %s)) AS t(v0, v1)" in merge_sql
        assert "PARSE_JSON(v1) AS payload" in merge_sql
        # PARSE_JSON is outside VALUES(), not inside it.
        assert "VALUES (%s, PARSE_JSON(%s))" not in merge_sql
        assert bound == [1, '{"a": 1}']


def test_bind_row_orders_by_columns_regardless_of_row_key_order() -> None:
    """The order-safe path (#699): ``_bind_row`` orders values by ``columns`` via
    ``row.get``, so rows with a varying key order/set (REST especially) never
    misalign. This is why the old ``list(row.values())`` fallback was unsafe and
    is now removed — ``columns``/``json_cols`` are required."""
    columns = ["id", "name", "email"]
    same_order = {"id": 1, "name": "Alice", "email": "a@x.com"}
    diff_order = {"email": "b@x.com", "id": 2, "name": "Bob"}  # keys reordered
    missing_key = {"id": 3, "name": "Carol"}  # no "email"
    assert _bind_row(same_order, columns, []) == [1, "Alice", "a@x.com"]
    # ordered by columns, NOT by dict insertion order:
    assert _bind_row(diff_order, columns, []) == [2, "Bob", "b@x.com"]
    # a missing key becomes None in its column slot, never a shifted misalignment:
    assert _bind_row(missing_key, columns, []) == [3, "Carol", None]


class TestSnowflakeKeyPairConnect:
    """_connect passes DER private_key for key-pair auth (#737)."""

    def _config(self, **auth: str):
        return SnowflakeDestinationConfig(
            **{
                "type": "snowflake",
                "account_env": "SF_ACCOUNT",
                "user_env": "SF_USER",
                "database": "DB",
                "schema": "PUBLIC",
                "table": "T",
                "warehouse": "WH",
                "introspect_schema": False,
                **auth,
            }
        )

    @staticmethod
    def _pem() -> str:
        cryptography = pytest.importorskip("cryptography")  # noqa: F841
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa

        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        return key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode()

    def test_private_key_env_wins_and_passes_der(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SF_ACCOUNT", "acct")
        monkeypatch.setenv("SF_USER", "svc_user")
        monkeypatch.setenv("SF_PK", self._pem())
        monkeypatch.setenv("SF_PASS", "should-not-be-used")

        conn = MagicMock()
        fake = MagicMock()
        fake.connector.connect = MagicMock(return_value=conn)
        with patch.dict(
            "sys.modules", {"snowflake": fake, "snowflake.connector": fake.connector}
        ):
            dest = SnowflakeDestination()
            got = dest._connect(
                self._config(private_key_env="SF_PK", password_env="SF_PASS")
            )

        assert got is conn
        kwargs = fake.connector.connect.call_args.kwargs
        assert isinstance(kwargs["private_key"], bytes)  # DER bytes
        assert "password" not in kwargs

    def test_password_fallback_when_no_key(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("SF_ACCOUNT", "acct")
        monkeypatch.setenv("SF_USER", "user")
        monkeypatch.setenv("SF_PASS", "pw")

        fake = MagicMock()
        with patch.dict(
            "sys.modules", {"snowflake": fake, "snowflake.connector": fake.connector}
        ):
            SnowflakeDestination()._connect(self._config(password_env="SF_PASS"))

        kwargs = fake.connector.connect.call_args.kwargs
        assert kwargs["password"] == "pw"
        assert "private_key" not in kwargs
