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
from drt.destinations.sql_base import BaseSqlDestination

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
        # Layer 3 (#317) is on by default; these pre-existing tests assert exact
        # mock call counts, so disable introspection here (schema-aware behaviour
        # is covered in test_databricks_schema_aware.py). Mirrors the Snowflake
        # destination tests.
        "introspect_schema": False,
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


def test_databricks_subclasses_sql_base() -> None:
    dest = DatabricksDestination()
    phase_2_or_3_hooks = {
        "_build_mirror_delete",
        "_shadow_name",
        "_old_name",
        "_rename_swap",
        "_load_replace_swap",
        "_load_replace",
        "_load_upsert",
        "_state_table_ident",
        "_state_table_exists",
        "_create_state_table",
        "_state_scope_columns_exist",
        "_add_state_scope_columns",
        "_state_sql",
    }

    assert isinstance(dest, BaseSqlDestination)
    assert phase_2_or_3_hooks.isdisjoint(DatabricksDestination.__dict__)
    assert "load" in DatabricksDestination.__dict__
    assert "finalize_sync" in DatabricksDestination.__dict__
    assert dest._replace_truncated is False
    assert dest._swap_shadow_created is False
    assert dest._swap_table is None
    assert dest._mirror_keys is None
    assert dest._mirror_scopes is None
    assert dest._schema_cache == {}
    assert dest._ddl_cache == {}


def test_databricks_dialect_hooks_forward_query_tags(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: dict[str, Any] = {}

    def _fake_connect(
        cls: type[DatabricksDestination],
        config: DatabricksDestinationConfig,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> str:
        calls.update(cls=cls, config=config, query_tags=query_tags)
        return "CONN"

    monkeypatch.setattr(DatabricksDestination, "_connect", classmethod(_fake_connect))
    config = _config()
    tags = {"sync": "users"}

    dest = DatabricksDestination()
    assert dest._dialect_connect(config, tags) == "CONN"
    assert calls == {
        "cls": DatabricksDestination,
        "config": config,
        "query_tags": tags,
    }
    assert dest._qualify_ident("main.default.users") == "main.default.users"


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
        # Build config/options BEFORE patching __import__ — pydantic may
        # lazily finish a deferred validator on first model_validate, and
        # under a global import patch that surfaces as a bare ImportError
        # instead of the connector-extra message under test.
        config = _config()
        options = _options()
        with patch("builtins.__import__", side_effect=ImportError):
            with pytest.raises(ImportError, match=r"drt-core\[databricks\]"):
                DatabricksDestination().load([{"id": 1}], config, options)

    def test_query_tags_set_native_kwarg_and_comment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """#768 — query_tags pass straight through to the driver's native
        `query_tags` connect kwarg, and also land as a SQL comment."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)

        options = _options()
        options._query_tags = {"sync": "s", "run_id": "r"}
        with patch.dict("sys.modules", modules):
            result = DatabricksDestination().load([{"id": 1}], _config(), options)

        assert result.failed == 0
        connect_kwargs = modules["databricks.sql"].connect.call_args[1]
        assert connect_kwargs["query_tags"] == {"sync": "s", "run_id": "r"}
        query = conn._cur.execute.call_args.args[0]
        assert query.startswith("/* drt sync=s run_id=r */\n")

    def test_no_query_tags_omits_native_kwarg(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)

        with patch.dict("sys.modules", modules):
            DatabricksDestination().load([{"id": 1}], _config(), _options())

        connect_kwargs = modules["databricks.sql"].connect.call_args[1]
        assert "query_tags" not in connect_kwargs
        query = conn._cur.execute.call_args.args[0]
        assert not query.startswith("/* drt")

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
        # #707: the destination now binds with native `?` placeholders (the
        # connector's default paramstyle, server-side binding), so it must NOT
        # opt into the deprecated client-side inline rendering — assert the
        # `use_inline_params` flag added in #706 is gone.
        assert "use_inline_params" not in conn_kwargs

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
        # #734: scalar rows are batched into one multi-row VALUES INSERT.
        assert cur.execute.call_count == 1
        first_sql = cur.execute.call_args_list[0][0][0]
        assert "INSERT INTO main.default.user_scores" in first_sql
        assert "id, score" in first_sql
        assert first_sql.count("(?, ?)") == 2  # one marker group per row
        assert cur.execute.call_args_list[0][0][1] == [1, 0.95, 2, 0.80]
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
        # #734: the multi-row chunk fails first, then the row-by-row replay
        # pins the failure on row 0 while row 1 goes through.
        conn._cur.execute.side_effect = [
            Exception("type mismatch"),  # chunked INSERT
            Exception("type mismatch"),  # replay: row 0
            None,  # replay: row 1
        ]
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
        assert result.row_errors[0].batch_index == 0
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

        # Fail the chunked staging INSERT, then row 0 of the row-by-row
        # replay (#734) — so the failure is attributed to row 0 and row 1
        # still lands. CREATE OR REPLACE TABLE runs first, then the staging
        # INSERTs, then MERGE INTO, then DROP TABLE.
        insert_call_count = {"n": 0}

        def execute_side_effect(sql: str, *args: Any) -> None:
            if "INSERT INTO main.default.__drt_staging_user_scores" in sql:
                insert_call_count["n"] += 1
                if insert_call_count["n"] <= 2:  # chunk, then replay row 0
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
            result = DatabricksDestination().load(records, config, _options(on_error="skip"))

        assert result.failed == 1
        assert result.success == 1
        assert len(result.row_errors) == 1
        assert result.row_errors[0].batch_index == 0
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

        # #707: keys are staged into a scratch Delta table and removed via
        # anti-join, so the DELETE binds no key parameters (scales past the
        # native paramstyle limit).
        calls = conn._cur.execute.call_args_list
        sqls = [(c.args[0] if c.args else "") for c in calls]
        keys_tbl = "main.default.__drt_mirror_keys_user_scores"
        assert any(s.startswith(f"CREATE OR REPLACE TABLE {keys_tbl}") for s in sqls)
        key_insert_calls = [
            c for c in calls if c.args and c.args[0].startswith(f"INSERT INTO {keys_tbl}")
        ]
        # #734: both observed keys staged in one chunked multi-row INSERT.
        assert len(key_insert_calls) == 1
        assert key_insert_calls[0].args[0].count("(?)") == 2
        assert sorted(key_insert_calls[0].args[1]) == [1, 2]
        delete_call = next(c for c in calls if c.args and c.args[0].startswith("DELETE FROM"))
        assert delete_call.args[0] == (
            f"DELETE FROM main.default.user_scores WHERE id NOT IN (SELECT id FROM {keys_tbl})"
        )
        assert len(delete_call.args) == 1  # no bound key params
        assert any(s.startswith(f"DROP TABLE IF EXISTS {keys_tbl}") for s in sqls)

    def test_mirror_finalize_composite_key_uses_merge_not_tuple_in(
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

        calls = conn._cur.execute.call_args_list
        keys_tbl = "main.default.__drt_mirror_keys_user_scores"
        # the delete MERGE, not the upsert MERGE that load() emits first
        merge_call = next(
            c
            for c in calls
            if c.args and c.args[0].startswith("MERGE INTO") and "THEN DELETE" in c.args[0]
        )
        assert (
            f"MERGE INTO main.default.user_scores AS t USING {keys_tbl} AS s "
            "ON t.tenant_id = s.tenant_id AND t.user_id = s.user_id "
            "WHEN NOT MATCHED BY SOURCE THEN DELETE" in merge_call.args[0]
        )
        assert len(merge_call.args) == 1  # anti-join binds no key params
        key_inserts = [
            c.args[1] for c in calls if c.args and c.args[0].startswith(f"INSERT INTO {keys_tbl}")
        ]
        assert key_inserts == [["a", 1]]  # the composite key staged as a tuple

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

        # #734: the chunked staging INSERT fails, then row 0 of the replay
        # fails — so record id=1 is the failure and id=2 survives.
        insert_call_count = {"n": 0}

        def execute_side_effect(sql: str, *args: Any) -> None:
            if "INSERT INTO main.default.__drt_staging_user_scores" in sql:
                insert_call_count["n"] += 1
                if insert_call_count["n"] <= 2:  # chunk, then replay row 0
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

        # #707: observed keys are staged (chunked multi-row INSERTs, #734) into
        # the mirror-keys table and removed via anti-join — so it's the
        # mirror-keys INSERTs, not the DELETE params, that must reflect only
        # id=2 (id=1 failed staging).
        key_inserts = [
            call.args[1]
            for call in cur.execute.call_args_list
            if call.args
            and call.args[0].startswith("INSERT INTO main.default.__drt_mirror_keys_user_scores")
        ]
        assert key_inserts == [[2]]  # only the survivor's key was staged
        # The anti-join DELETE was issued and binds no key parameters.
        delete_call = next(
            call
            for call in cur.execute.call_args_list
            if call.args and call.args[0].startswith("DELETE FROM")
        )
        assert len(delete_call.args) == 1  # SQL only — no inline key params
        assert (
            "NOT IN (SELECT id FROM main.default.__drt_mirror_keys_user_scores)"
            in delete_call.args[0]
        )

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

    def test_replace_truncate_truncates_then_inserts(self, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)
        records = [{"id": 1, "score": 0.95}, {"id": 2, "score": 0.80}]
        with patch.dict("sys.modules", modules):
            result = DatabricksDestination().load(records, _config(), _options(mode="replace"))
        assert result.success == 2
        sqls = _sqls(conn._cur)
        assert any(s.startswith(f"TRUNCATE TABLE {_FQ}") for s in sqls)
        # #734: both rows land in one chunked multi-row INSERT.
        assert sum(f"INSERT INTO {_FQ} (" in s for s in sqls) == 1

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

    def test_replace_swap_creates_shadow_and_inserts(self, monkeypatch: pytest.MonkeyPatch) -> None:
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
            f"CREATE OR REPLACE TABLE {_SHADOW} AS SELECT * FROM {_FQ} WHERE 1=0" in s for s in sqls
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

    def test_replace_swap_on_error_fail_drops_shadow(self, monkeypatch: pytest.MonkeyPatch) -> None:
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
            dropped, failed = DatabricksDestination().drop_orphan_swap_tables(_config(), [_SHADOW])
        assert dropped == []
        assert failed == [_SHADOW]


def test_tracked_mirror_strategy_accepted_on_databricks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``mirror.strategy: tracked`` (#692) is now supported on Databricks."""
    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    conn = _fake_conn()
    config = _config(upsert_key=["id"])
    opts = _options(mode="mirror", mirror={"strategy": "tracked"})

    with patch.dict("sys.modules", _mocked_databricks_modules(conn)):
        result = dest.load([{"id": 1, "score": 100}], config, opts)

    assert result.failed == 0


def test_scope_accepted_on_databricks(monkeypatch: pytest.MonkeyPatch) -> None:
    """``mirror.scope`` (#692, destination strategy) is now supported."""
    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    conn = _fake_conn()
    config = _config(upsert_key=["id"])
    opts = _options(mode="mirror", mirror={"scope": ["parent_id"]})

    with patch.dict("sys.modules", _mocked_databricks_modules(conn)):
        result = dest.load([{"id": 1, "parent_id": 10}], config, opts)

    assert result.failed == 0


def test_scope_missing_column_fails_fast_on_databricks(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    conn = _fake_conn()
    config = _config(upsert_key=["id"])
    opts = _options(mode="mirror", mirror={"scope": ["parent_id"]})

    with patch.dict("sys.modules", _mocked_databricks_modules(conn)):
        with pytest.raises(ValueError, match="mirror.scope columns missing"):
            dest.load([{"id": 1}], config, opts)


def test_scoped_mirror_deletes_within_observed_parents_only_databricks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Destination-strategy scope: the DELETE only ever considers rows under
    parents this run actually observed."""
    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    load_conn = _fake_conn()
    finalize_conn = _fake_conn()
    config = _config(upsert_key=["parent_id", "id"])
    opts = _options(mode="mirror", mirror={"scope": ["parent_id"]})

    with patch.dict("sys.modules", _mocked_databricks_modules(load_conn)):
        dest.load([{"parent_id": 1, "id": "a", "score": 1}], config, opts)
    with patch.dict("sys.modules", _mocked_databricks_modules(finalize_conn)):
        dest.finalize_sync(config, opts)

    delete_call = next(
        c
        for c in finalize_conn._cur.execute.call_args_list
        if c.args and c.args[0].startswith("MERGE INTO") and "THEN DELETE" in c.args[0]
    )
    # #908: a composite upsert_key goes through MERGE — Delta rejects
    # `(a, b) NOT IN (SELECT …)`. The scope restriction rides in the WHEN
    # clause, column-qualified against the target alias.
    assert "ON t.parent_id = s.parent_id AND t.id = s.id" in delete_call.args[0]
    assert "WHEN NOT MATCHED BY SOURCE AND t.parent_id IN (?) THEN DELETE" in delete_call.args[0]
    assert delete_call.args[1] == [1]


def test_scope_rejected_with_tracked_when_not_subset_of_upsert_key_databricks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#694's composition constraint applies on Databricks too."""
    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    conn = _fake_conn()
    config = _config(upsert_key=["id"])
    opts = _options(mode="mirror", mirror={"strategy": "tracked", "scope": ["parent_id"]})

    with patch.dict("sys.modules", _mocked_databricks_modules(conn)):
        with pytest.raises(ValueError, match="mirror.scope columns must be part of"):
            dest.load([{"id": 1, "parent_id": 10}], config, opts)


# ---------------------------------------------------------------------------
# mirror.strategy: tracked (#692, mirroring Postgres/MySQL/Snowflake/ClickHouse's #686)
# ---------------------------------------------------------------------------


def _tracked_options(**kwargs: Any) -> SyncOptions:
    defaults: dict[str, Any] = {"mode": "mirror", "mirror": {"strategy": "tracked"}}
    defaults.update(kwargs)
    opts = _options(**defaults)
    opts._sync_name = "scores_sync"
    return opts


def _tracked_scoped_options(scope: list[str] = ["parent_id"]) -> SyncOptions:
    opts = _options(mode="mirror", mirror={"strategy": "tracked", "scope": scope})
    opts._sync_name = "scores_sync"
    return opts


def _state_conn(
    raw_diff: list[tuple[str, str]] | None = None,
    to_insert: list[tuple[str, str]] | None = None,
    previous_exists: bool = True,
    exists: bool = True,
    scope_key_of: dict[str, str | None] | None = None,
) -> MagicMock:
    """A fake connection wired for the #694 part 2 read path — ``SHOW
    TABLES``, a baseline existence probe, the SQL-side diff, and the
    genuinely-new-keys probe, dispatched by the executed SQL's text since
    the cursor now answers up to four distinct reads per run instead of
    two. ``raw_diff`` is what ``previous - current`` would have computed
    server-side; ``to_insert`` is what ``current - previous`` would have."""
    conn = _fake_conn()
    cur = conn._cur
    scope_key_of = scope_key_of or {}

    def fetchone_side_effect() -> Any:
        return (1,) if previous_exists else None

    def fetchall_side_effect() -> list[tuple[str, str]]:
        sql = cur.execute.call_args.args[0] if cur.execute.call_args.args else ""
        if sql.startswith("SHOW TABLES"):
            return [("_drt_synced_keys",)] if exists else []
        if sql.startswith("SELECT s.key_hash"):
            # #890: model the projection actually asked for — a scoped run adds
            # scope_key as a third column so pre-#890 rows can be spotted.
            rows = list(raw_diff or [])
            if "s.scope_key" in sql:
                return [(h, kj, scope_key_of.get(h)) for h, kj in rows]
            return rows
        if sql.startswith("SELECT c.key_hash"):
            return list(to_insert or [])
        return []

    cur.fetchone.side_effect = fetchone_side_effect
    cur.fetchall.side_effect = fetchall_side_effect
    return conn


def test_tracked_creates_state_table_when_absent_databricks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    load_conn = _fake_conn()
    finalize_conn = _state_conn(exists=False)
    config = _config(upsert_key=["id"])

    with patch.dict("sys.modules", _mocked_databricks_modules(load_conn)):
        dest.load([{"id": 1}], config, _tracked_options())
    with patch.dict("sys.modules", _mocked_databricks_modules(finalize_conn)):
        dest.finalize_sync(config, _tracked_options())

    create_calls = [
        c.args[0]
        for c in finalize_conn._cur.execute.call_args_list
        if c.args and c.args[0].startswith("CREATE TABLE IF NOT EXISTS")
    ]
    assert len(create_calls) == 1
    assert "_drt_synced_keys" in create_calls[0]
    assert "USING DELTA" in create_calls[0]


def test_tracked_skips_create_when_state_table_preprovisioned_databricks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    load_conn = _fake_conn()
    finalize_conn = _state_conn(exists=True)
    config = _config(upsert_key=["id"])

    with patch.dict("sys.modules", _mocked_databricks_modules(load_conn)):
        dest.load([{"id": 1}], config, _tracked_options())
    with patch.dict("sys.modules", _mocked_databricks_modules(finalize_conn)):
        dest.finalize_sync(config, _tracked_options())

    assert not any(
        c.args and c.args[0].startswith("CREATE TABLE IF NOT EXISTS")
        for c in finalize_conn._cur.execute.call_args_list
    )


def test_tracked_first_run_baselines_without_deleting_databricks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    load_conn = _fake_conn()
    finalize_conn = _state_conn(
        raw_diff=[],
        to_insert=[(key_hash((k,)), key_json((k,))) for k in (1, 2)],
        previous_exists=False,
    )
    config = _config(upsert_key=["id"])

    with patch.dict("sys.modules", _mocked_databricks_modules(load_conn)):
        dest.load([{"id": 1}, {"id": 2}], config, _tracked_options())
    with patch.dict("sys.modules", _mocked_databricks_modules(finalize_conn)):
        result = dest.finalize_sync(config, _tracked_options())

    assert result is not None
    for c in finalize_conn._cur.execute.call_args_list:
        sql = c.args[0] if c.args else ""
        if sql.startswith("DELETE FROM"):
            assert "user_scores" not in sql
    insert_calls = [
        c
        for c in finalize_conn._cur.execute.call_args_list
        if c.args and c.args[0].startswith("INSERT INTO main.default._drt_synced_keys")
    ]
    assert len(insert_calls) == 1
    params = insert_calls[0].args[1]
    # 2 rows x 3 cols (sync_name, key_hash, key_json) flattened — both rows'
    # sync_name (positions 0 and 3) must be this sync's name.
    assert len(params) == 6
    assert params[0] == "scores_sync"
    assert params[3] == "scores_sync"


def test_tracked_second_run_deletes_only_stale_tracked_keys_databricks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """prev={1,2,3}, current={1,2} -> stale-key delete for {3} only, via the
    same staged anti-/semi-join shape as the destination-strategy path."""
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    load_conn = _fake_conn()
    finalize_conn = _state_conn(raw_diff=[(key_hash((3,)), key_json((3,)))])
    config = _config(upsert_key=["id"])

    with patch.dict("sys.modules", _mocked_databricks_modules(load_conn)):
        dest.load([{"id": 1}, {"id": 2}], config, _tracked_options())
    with patch.dict("sys.modules", _mocked_databricks_modules(finalize_conn)):
        dest.finalize_sync(config, _tracked_options())

    calls = finalize_conn._cur.execute.call_args_list
    keys_tbl = "main.default.__drt_mirror_keys_user_scores"
    key_insert = next(
        c for c in calls if c.args and c.args[0].startswith(f"INSERT INTO {keys_tbl}")
    )
    assert key_insert.args[1] == [3]
    delete_call = next(
        c for c in calls if c.args and c.args[0].startswith("DELETE FROM main.default.user_scores")
    )
    assert f"id IN (SELECT id FROM {keys_tbl})" in delete_call.args[0]
    assert "NOT IN" not in delete_call.args[0]


def test_tracked_stages_current_keys_before_diffing_databricks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#694 part 2: current's keys are staged into the diff scratch table
    (distinct from the target-delete's own ``keys_table``) before the diff
    query runs, and it's dropped at the end."""
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    load_conn = _fake_conn()
    finalize_conn = _state_conn()
    config = _config(upsert_key=["id"])
    diff_tbl = "main.default.__drt_mirror_diff_keys_user_scores"

    with patch.dict("sys.modules", _mocked_databricks_modules(load_conn)):
        dest.load([{"id": 1}, {"id": 2}], config, _tracked_options())
    with patch.dict("sys.modules", _mocked_databricks_modules(finalize_conn)):
        dest.finalize_sync(config, _tracked_options())

    calls = finalize_conn._cur.execute.call_args_list
    create_sql = f"CREATE OR REPLACE TABLE {diff_tbl}"
    create_idx = next(i for i, c in enumerate(calls) if c.args and c.args[0].startswith(create_sql))
    diff_idx = next(
        i for i, c in enumerate(calls) if c.args and c.args[0].startswith("SELECT s.key_hash")
    )
    drop_sql = f"DROP TABLE IF EXISTS {diff_tbl}"
    drop_idx = next(i for i, c in enumerate(calls) if c.args and c.args[0] == drop_sql)
    assert create_idx < diff_idx < drop_idx
    stage_insert = next(
        c for c in calls if c.args and c.args[0].startswith(f"INSERT INTO {diff_tbl}")
    )
    assert stage_insert.args[1] == [key_hash((1,)), key_json((1,)), key_hash((2,)), key_json((2,))]


def test_tracked_diff_staging_table_is_disambiguated_by_target_table(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Databricks has no session-local temp tables, so the diff scratch
    table is a real, shared, persistent object per catalog.schema. Two
    tracked-mirror syncs targeting different tables in the same
    catalog.schema — a realistic `drt run --threads N>1` scenario — must
    get distinct scratch table names, or they'd race on a shared one
    (caught in review; the diff scratch table lacked the suffix its
    sibling ``keys_table`` already had)."""
    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    load_conn = _fake_conn()
    finalize_conn = _state_conn()
    config = _config(upsert_key=["id"], table="orders")

    with patch.dict("sys.modules", _mocked_databricks_modules(load_conn)):
        dest.load([{"id": 1}], config, _tracked_options())
    with patch.dict("sys.modules", _mocked_databricks_modules(finalize_conn)):
        dest.finalize_sync(config, _tracked_options())

    create_prefix = "CREATE OR REPLACE TABLE main.default.__drt_mirror_diff_keys"
    create_calls = [
        c.args[0]
        for c in finalize_conn._cur.execute.call_args_list
        if c.args and c.args[0].startswith(create_prefix)
    ]
    assert len(create_calls) == 1
    assert create_calls[0].startswith(
        "CREATE OR REPLACE TABLE main.default.__drt_mirror_diff_keys_orders "
    )


def test_tracked_inserts_only_genuinely_new_keys_databricks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A current key already tracked under the same hash never needs
    rewriting — only rows the new-keys query actually returns get
    inserted."""
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    load_conn = _fake_conn()
    finalize_conn = _state_conn(raw_diff=[], to_insert=[(key_hash((2,)), key_json((2,)))])
    config = _config(upsert_key=["id"])

    with patch.dict("sys.modules", _mocked_databricks_modules(load_conn)):
        dest.load([{"id": 1}, {"id": 2}], config, _tracked_options())
    with patch.dict("sys.modules", _mocked_databricks_modules(finalize_conn)):
        dest.finalize_sync(config, _tracked_options())

    insert_calls = [
        c
        for c in finalize_conn._cur.execute.call_args_list
        if c.args and c.args[0].startswith("INSERT INTO main.default._drt_synced_keys")
    ]
    assert len(insert_calls) == 1
    assert insert_calls[0].args[1] == ["scores_sync", key_hash((2,)), key_json((2,))]


def test_tracked_empty_source_is_noop_databricks(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    finalize_conn = _fake_conn()
    config = _config(upsert_key=["id"])

    with patch.dict("sys.modules", _mocked_databricks_modules(finalize_conn)):
        result = dest.finalize_sync(config, _tracked_options())

    assert result is None
    finalize_conn._cur.execute.assert_not_called()


def test_tracked_baseline_logs_warning_databricks(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    load_conn = _fake_conn()
    finalize_conn = _state_conn(
        raw_diff=[], to_insert=[(key_hash((1,)), key_json((1,)))], previous_exists=False
    )
    config = _config(upsert_key=["id"])

    with patch.dict("sys.modules", _mocked_databricks_modules(load_conn)):
        dest.load([{"id": 1}], config, _tracked_options())
    with (
        patch.dict("sys.modules", _mocked_databricks_modules(finalize_conn)),
        caplog.at_level("WARNING"),
    ):
        dest.finalize_sync(config, _tracked_options())

    assert any("baselin" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# mirror.scope + strategy: tracked (#694, extended to Databricks by #692)
# ---------------------------------------------------------------------------


def test_tracked_scoped_deletes_only_stale_keys_within_observed_scope_databricks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prior state has parent 1: {(1,"a"),(1,"b")} and parent 2: {(2,"x")}.
    This run only touches parent 1 with just (1,"a") -> (1,"b") is stale and
    deleted; (2,"x") is under a parent this run never saw and must survive
    (#694 part 2: never read or rewritten at all)."""
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    load_conn = _fake_conn()
    finalize_conn = _state_conn(
        raw_diff=[(key_hash(k), key_json(k)) for k in ((1, "b"), (2, "x"))],
        to_insert=[],  # (1,"a") already tracked
    )
    config = _config(upsert_key=["parent_id", "id"])

    with patch.dict("sys.modules", _mocked_databricks_modules(load_conn)):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with patch.dict("sys.modules", _mocked_databricks_modules(finalize_conn)):
        dest.finalize_sync(config, _tracked_scoped_options())

    calls = finalize_conn._cur.execute.call_args_list
    keys_tbl = "main.default.__drt_mirror_keys_user_scores"
    key_insert = next(
        c for c in calls if c.args and c.args[0].startswith(f"INSERT INTO {keys_tbl}")
    )
    assert key_insert.args[1] == [1, "b"]

    state_delete_calls = [
        c
        for c in calls
        if c.args and c.args[0].startswith("DELETE FROM main.default._drt_synced_keys")
    ]
    assert len(state_delete_calls) == 1
    assert state_delete_calls[0].args[1] == ["scores_sync", key_hash((1, "b"))]
    # #694 part 2 pinned that an out-of-scope row is never touched at all. #890
    # narrows that: it may be touched *once*, by the scope backfill, and only
    # while its scope columns are still NULL. It is still never deleted and
    # never re-inserted, and once healed it is filtered out in SQL. Asserting
    # the shape rather than dropping the check.
    touched = [c for c in calls if len(c.args) > 1 and key_hash((2, "x")) in c.args[1]]
    assert len(touched) <= 1
    for call in touched:
        assert "SET scope_spec" in str(call.args[0])
    assert not any(
        c.args and c.args[0].startswith("INSERT INTO main.default._drt_synced_keys") for c in calls
    )


def test_tracked_scoped_first_touch_of_a_scope_is_not_a_baseline_warning_databricks(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    load_conn = _fake_conn()
    finalize_conn = _state_conn(
        raw_diff=[(key_hash((2, "x")), key_json((2, "x")))],
        to_insert=[(key_hash((1, "a")), key_json((1, "a")))],
    )
    config = _config(upsert_key=["parent_id", "id"])

    with patch.dict("sys.modules", _mocked_databricks_modules(load_conn)):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with (
        patch.dict("sys.modules", _mocked_databricks_modules(finalize_conn)),
        caplog.at_level("WARNING"),
    ):
        dest.finalize_sync(config, _tracked_scoped_options())

    assert not any("baselin" in r.message.lower() for r in caplog.records)
    for c in finalize_conn._cur.execute.call_args_list:
        sql = c.args[0] if c.args else ""
        if sql.startswith("DELETE FROM"):
            assert "user_scores" not in sql
    insert_calls = [
        c
        for c in finalize_conn._cur.execute.call_args_list
        if c.args and c.args[0].startswith("INSERT INTO main.default._drt_synced_keys")
    ]
    assert len(insert_calls) == 1
    # A scoped run now also records the scope it was computed under (#890), so
    # the row carries five values rather than three. The first three are what
    # this test has always been about; the last two are asserted here so the
    # widening is deliberate rather than absorbed silently.
    assert insert_calls[0].args[1] == [
        "scores_sync",
        key_hash((1, "a")),
        key_json((1, "a")),
        '["parent_id"]',
        "[1]",
    ]


def test_tracked_scoped_genuinely_no_prior_state_still_warns_baseline_databricks(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    from drt.destinations._mirror_state import key_hash, key_json

    _set_creds(monkeypatch)
    dest = DatabricksDestination()
    load_conn = _fake_conn()
    finalize_conn = _state_conn(
        raw_diff=[], to_insert=[(key_hash((1, "a")), key_json((1, "a")))], previous_exists=False
    )
    config = _config(upsert_key=["parent_id", "id"])

    with patch.dict("sys.modules", _mocked_databricks_modules(load_conn)):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with (
        patch.dict("sys.modules", _mocked_databricks_modules(finalize_conn)),
        caplog.at_level("WARNING"),
    ):
        dest.finalize_sync(config, _tracked_scoped_options())

    assert any("baselin" in r.message.lower() for r in caplog.records)


class TestDatabricksChunkedInserts:
    """#734 — scalar loads batch into multi-row ``VALUES`` chunks under the
    native 255-marker limit; a failed chunk replays row-by-row to keep exact
    ``RowError`` attribution; json ``SELECT``-form loads stay one per row."""

    def test_rows_per_chunk_math(self) -> None:
        from drt.destinations.databricks import _rows_per_chunk

        assert _rows_per_chunk(1) == 255
        assert _rows_per_chunk(2) == 127
        assert _rows_per_chunk(255) == 1
        assert _rows_per_chunk(300) == 1  # wider than the limit still progresses

    def test_insert_chunks_at_native_param_limit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """200 two-column rows -> a 127-row chunk + a 73-row chunk."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)
        records = [{"id": i, "score": float(i)} for i in range(200)]
        with patch.dict("sys.modules", modules):
            result = DatabricksDestination().load(records, _config(), _options())

        assert result.success == 200
        assert result.failed == 0
        calls = conn._cur.execute.call_args_list
        assert len(calls) == 2
        assert calls[0].args[0].count("(?, ?)") == 127  # 254 markers <= 255
        assert len(calls[0].args[1]) == 254
        assert calls[1].args[0].count("(?, ?)") == 73
        assert len(calls[1].args[1]) == 146

    def test_chunk_failure_replays_and_attributes_row(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A failed chunk lands nothing (atomic); the replay pins the RowError
        on the exact record (original batch index) and the rest still load."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.execute.side_effect = [
            Exception("bad row"),  # chunked INSERT (3 rows)
            None,  # replay: row 0
            Exception("bad row"),  # replay: row 1
            None,  # replay: row 2
        ]
        modules = _mocked_databricks_modules(conn)
        records = [
            {"id": 1, "score": 0.1},
            {"id": 2, "score": 0.2},
            {"id": 3, "score": 0.3},
        ]
        with patch.dict("sys.modules", modules):
            result = DatabricksDestination().load(records, _config(), _options(on_error="skip"))

        assert result.success == 2
        assert result.failed == 1
        assert [e.batch_index for e in result.row_errors] == [1]

    def test_chunk_failure_on_error_fail_raises_from_replay(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_creds(monkeypatch)
        conn = _fake_conn()
        conn._cur.execute.side_effect = Exception("bad row")
        modules = _mocked_databricks_modules(conn)
        records = [{"id": 1, "score": 0.1}, {"id": 2, "score": 0.2}]
        with patch.dict("sys.modules", modules):
            with pytest.raises(Exception, match="bad row"):
                DatabricksDestination().load(records, _config(), _options(on_error="fail"))
        conn.close.assert_called_once()

    def test_json_columns_stay_row_by_row(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """``SELECT``-form (from_json) inserts don't compose into multi-row
        ``VALUES`` — they stay one statement per row (#734)."""
        _set_creds(monkeypatch)
        monkeypatch.setattr(
            DatabricksDestination,
            "_resolve_schema",
            lambda self, config: {"attrs": "json"},
        )
        monkeypatch.setattr(
            DatabricksDestination,
            "_resolve_ddls",
            lambda self, config: {"attrs": "map<string,string>"},
        )
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)
        records = [{"id": 1, "attrs": {"a": "1"}}, {"id": 2, "attrs": {"b": "2"}}]
        with patch.dict("sys.modules", modules):
            result = DatabricksDestination().load(records, _config(), _options())

        assert result.success == 2
        calls = conn._cur.execute.call_args_list
        assert len(calls) == 2  # one SELECT-form INSERT per row
        assert all("from_json" in c.args[0] for c in calls)

    def test_mirror_key_staging_chunks_past_limit(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """300 observed single-column keys stage in two INSERTs (255 + 45)."""
        _set_creds(monkeypatch)
        conn = _fake_conn()
        modules = _mocked_databricks_modules(conn)
        config = _config(mode="merge", upsert_key=["id"])
        dest = DatabricksDestination()
        dest._mirror_keys = [(i,) for i in range(300)]
        with patch.dict("sys.modules", modules):
            dest.finalize_sync(config, _options(mode="mirror"))

        keys_tbl = "main.default.__drt_mirror_keys_user_scores"
        key_inserts = [
            c
            for c in conn._cur.execute.call_args_list
            if c.args and c.args[0].startswith(f"INSERT INTO {keys_tbl}")
        ]
        assert len(key_inserts) == 2
        assert sorted(len(c.args[1]) for c in key_inserts) == [45, 255]
        # Marker groups match the bound values chunk for chunk…
        assert all(c.args[0].count("(?)") == len(c.args[1]) for c in key_inserts)
        # …and every key is staged exactly once.
        staged = [v for c in key_inserts for v in c.args[1]]
        assert sorted(staged) == list(range(300))


# ---------------------------------------------------------------------------
# #890 — scope-aware SQL diff (Databricks leg; design lives on #904)
# ---------------------------------------------------------------------------


def _scope_columns(conn: MagicMock, *, present: bool) -> None:
    """Answer the #890 information_schema probe on top of the state-conn fake."""
    cur = conn._cur
    inner = cur.fetchone.side_effect

    def fetchone(*a: Any, **k: Any) -> Any:
        sql = cur.execute.call_args.args[0] if cur.execute.call_args.args else ""
        if "information_schema.columns" in sql:
            return (2 if present else 0,)
        return inner()

    cur.fetchone.side_effect = fetchone


def _diff_call(conn: MagicMock) -> Any:
    return next(
        c
        for c in conn._cur.execute.call_args_list
        if c.args and str(c.args[0]).startswith("SELECT s.key_hash")
    )


def _run_scoped(finalize_conn: MagicMock) -> None:
    dest = DatabricksDestination()
    config = _config(upsert_key=["parent_id", "id"])
    with patch.object(DatabricksDestination, "_connect", return_value=_fake_conn()):
        dest.load([{"parent_id": 1, "id": "a"}], config, _tracked_scoped_options())
    with patch.object(DatabricksDestination, "_connect", return_value=finalize_conn):
        dest.finalize_sync(config, _tracked_scoped_options())


def test_scoped_diff_is_narrowed_in_sql_databricks() -> None:
    conn = _state_conn(raw_diff=[], to_insert=[])
    _scope_columns(conn, present=True)

    _run_scoped(conn)

    sql, params = _diff_call(conn).args
    assert "s.scope_key IN" in sql
    # both escape branches — what keeps this a purely coarse filter
    assert "s.scope_key IS NULL" in sql
    assert "s.scope_spec <> ?" in sql
    assert params == ["scores_sync", '["parent_id"]', "[1]"]


def test_scoped_diff_falls_back_when_alter_is_refused_databricks() -> None:
    """No ALTER privilege is a supported state, not an error.

    Delta spells the DDL ``ADD COLUMNS (...)`` — plural and parenthesised,
    unlike every other leg's ``ADD COLUMN`` — so this also pins that the
    statement drt emits is the one Databricks actually accepts.
    """
    conn = _state_conn(raw_diff=[], to_insert=[])
    _scope_columns(conn, present=False)
    attempted: list[str] = []

    def execute(sql: str, *a: Any, **k: Any) -> Any:
        if sql.startswith("ALTER TABLE") and "scope_spec" in sql:
            attempted.append(sql)
            raise Exception("PERMISSION_DENIED: does not have MODIFY on table")
        return None

    conn._cur.execute.side_effect = execute

    _run_scoped(conn)

    assert attempted and "ADD COLUMNS (scope_spec STRING, scope_key STRING)" in attempted[0]
    assert "scope_key" not in str(_diff_call(conn).args[0])


def test_unscoped_tracked_never_probes_scope_columns_databricks() -> None:
    conn = _state_conn(raw_diff=[], to_insert=[])
    dest = DatabricksDestination()
    config = _config(upsert_key=["id"])
    with patch.object(DatabricksDestination, "_connect", return_value=_fake_conn()):
        dest.load([{"id": 1}], config, _tracked_options())
    with patch.object(DatabricksDestination, "_connect", return_value=conn):
        dest.finalize_sync(config, _tracked_options())

    assert not any(
        "information_schema.columns" in str(c.args[0])
        for c in conn._cur.execute.call_args_list
        if c.args
    )
