"""Tests for destination query helpers."""

from __future__ import annotations

import importlib.util
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import (
    ClickHouseDestinationConfig,
    DatabricksDestinationConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    RestApiDestinationConfig,
    SnowflakeDestinationConfig,
)
from drt.destinations.query import (
    execute_test_query,
    fetch_all_keys,
    fetch_failing_rows,
    fetch_rows,
    fetch_rows_by_keys,
    fetch_tracked_state,
    get_table_name,
    is_queryable,
)

# ---------------------------------------------------------------------------
# QueryableDestination — capability discovery (#469)
#
# is_queryable/get_table_name/execute_test_query dispatch on the destination
# *instance* via QueryableDestination (isinstance check), not on the config
# class via a hardcoded tuple. These tests exercise that dispatch directly on
# the Protocol, separate from the config-based tests above/below which cover
# the same functions through their public, unchanged signatures.
# ---------------------------------------------------------------------------


def test_queryable_destination_isinstance_all_five_sql_dialects() -> None:
    from drt.destinations.base import QueryableDestination
    from drt.destinations.clickhouse import ClickHouseDestination
    from drt.destinations.databricks import DatabricksDestination
    from drt.destinations.mysql import MySQLDestination
    from drt.destinations.postgres import PostgresDestination
    from drt.destinations.snowflake import SnowflakeDestination

    assert isinstance(PostgresDestination(), QueryableDestination)
    assert isinstance(MySQLDestination(), QueryableDestination)
    assert isinstance(ClickHouseDestination(), QueryableDestination)
    assert isinstance(SnowflakeDestination(), QueryableDestination)
    assert isinstance(DatabricksDestination(), QueryableDestination)


def test_queryable_destination_isinstance_false_for_non_sql_destination() -> None:
    from drt.destinations.base import QueryableDestination
    from drt.destinations.slack import SlackDestination

    assert not isinstance(SlackDestination(), QueryableDestination)


def test_new_destination_becomes_queryable_without_touching_query_py() -> None:
    """The architectural point of #469: implementing the two Protocol
    methods is sufficient for is_queryable/get_table_name/execute_test_query
    to pick a destination up — no change to query.py's dispatch code, unlike
    the old ``_QUERYABLE_TYPES`` config-class tuple it replaced.
    """
    from drt.destinations.base import QueryableDestination

    class _FakeQueryableDestination:
        def get_table_name(self, config: Any) -> str:
            return "fake_table"

        def execute_test_query(self, config: Any, query: str) -> int:
            return 7

    dest = _FakeQueryableDestination()
    assert isinstance(dest, QueryableDestination)
    assert dest.get_table_name(None) == "fake_table"
    assert dest.execute_test_query(None, "SELECT 1") == 7


def test_queryable_destination_requires_both_methods() -> None:
    """Structural typing: implementing only one of the two methods does not
    satisfy the Protocol — a half-implemented destination is correctly
    treated as not queryable rather than crashing on the missing method."""
    from drt.destinations.base import QueryableDestination

    class _OnlyGetTableName:
        def get_table_name(self, config: Any) -> str:
            return "t"

    assert not isinstance(_OnlyGetTableName(), QueryableDestination)


def _has_psycopg2() -> bool:
    try:
        return importlib.util.find_spec("psycopg2") is not None
    except (ImportError, ValueError):
        # find_spec raises rather than returning None for some broken/partial
        # installs; either way the extra isn't usable here.
        return False


# The Postgres cases below reach into ``psycopg2.sql`` to render a ``Composed``
# without a live connection, so they need the [postgres] extra. The MySQL /
# Snowflake / ClickHouse cases in this module don't — mark only the Postgres
# ones rather than skipping the whole file, so a minimal install (the release
# workflow's verify job) still exercises the rest.
needs_psycopg2 = pytest.mark.skipif(
    not _has_psycopg2(), reason="requires drt-core[postgres]"
)


def test_postgres_is_queryable() -> None:
    config = PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="test",
        table="public.users",
        upsert_key=["id"],
    )
    assert is_queryable(config) is True


def test_rest_api_is_not_queryable() -> None:
    config = RestApiDestinationConfig(
        type="rest_api",
        url="http://example.com",
        method="POST",
    )
    assert is_queryable(config) is False


def test_get_table_name_postgres() -> None:
    config = PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="test",
        table="public.users",
        upsert_key=["id"],
    )
    assert get_table_name(config) == "public.users"


def test_execute_test_query_postgres_returns_int() -> None:
    cursor = MagicMock()
    cursor.fetchone.return_value = (42,)
    conn = _plain_conn(cursor)

    config = PostgresDestinationConfig(
        type="postgres",
        host="localhost",
        dbname="test",
        table="public.users",
        upsert_key=["id"],
    )
    with patch(
        "drt.destinations.postgres.PostgresDestination._connect", return_value=conn
    ):
        result = execute_test_query(config, "SELECT COUNT(*) FROM t")

    assert result == 42
    cursor.execute.assert_called_once_with("SELECT COUNT(*) FROM t")
    conn.close.assert_called_once()


def test_mysql_is_queryable_and_table_name() -> None:
    config = MySQLDestinationConfig(
        type="mysql", host="localhost", dbname="test", table="users", upsert_key=["id"]
    )
    assert is_queryable(config) is True
    assert get_table_name(config) == "users"


def test_execute_test_query_mysql_tuple_row() -> None:
    cursor = MagicMock()
    cursor.fetchone.return_value = (7,)
    conn = _plain_conn(cursor)

    config = MySQLDestinationConfig(
        type="mysql", host="localhost", dbname="test", table="users", upsert_key=["id"]
    )
    with patch("drt.destinations.mysql.MySQLDestination._connect", return_value=conn):
        result = execute_test_query(config, "SELECT COUNT(*) FROM t")

    assert result == 7
    cursor.execute.assert_called_once_with("SELECT COUNT(*) FROM t")
    conn.close.assert_called_once()


def test_execute_test_query_mysql_dict_cursor_row() -> None:
    """pymysql's DictCursor yields a mapping, not a positional tuple."""
    cursor = MagicMock()
    cursor.fetchone.return_value = {"COUNT(*)": 9}
    conn = _plain_conn(cursor)

    config = MySQLDestinationConfig(
        type="mysql", host="localhost", dbname="test", table="users", upsert_key=["id"]
    )
    with patch("drt.destinations.mysql.MySQLDestination._connect", return_value=conn):
        result = execute_test_query(config, "SELECT COUNT(*) FROM t")

    assert result == 9


def test_clickhouse_is_queryable_and_table_name() -> None:
    config = ClickHouseDestinationConfig(
        type="clickhouse", host="localhost", database="test", table="users", upsert_key=["id"]
    )
    assert is_queryable(config) is True
    assert get_table_name(config) == "users"


def test_execute_test_query_clickhouse_returns_int() -> None:
    client = MagicMock()
    result_obj = MagicMock()
    result_obj.result_rows = [(3,)]
    client.query.return_value = result_obj

    config = ClickHouseDestinationConfig(
        type="clickhouse", host="localhost", database="test", table="users", upsert_key=["id"]
    )
    with patch(
        "drt.destinations.clickhouse.ClickHouseDestination._connect", return_value=client
    ):
        result = execute_test_query(config, "SELECT COUNT(*) FROM t")

    assert result == 3
    client.query.assert_called_once_with("SELECT COUNT(*) FROM t")
    client.close.assert_called_once()


# ---------------------------------------------------------------------------
# Snowflake queryable integration (#468)
# ---------------------------------------------------------------------------


def _snowflake_config(**overrides: Any) -> SnowflakeDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "snowflake",
        "account_env": "SF_ACCOUNT",
        "user_env": "SF_USER",
        "password_env": "SF_PASSWORD",
        "database": "ANALYTICS",
        "schema": "PUBLIC",
        "table": "USER_SCORES",
        "warehouse": "COMPUTE_WH",
    }
    defaults.update(overrides)
    return SnowflakeDestinationConfig.model_validate(defaults)


def _fake_conn(cursor: MagicMock) -> MagicMock:
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cursor
    conn.cursor.return_value.__exit__.return_value = False
    return conn


def test_snowflake_is_queryable() -> None:
    assert is_queryable(_snowflake_config()) is True


def test_get_table_name_snowflake_is_fully_qualified() -> None:
    # FQN database.schema.table — Snowflake has explicit db/schema/table fields.
    assert get_table_name(_snowflake_config()) == "ANALYTICS.PUBLIC.USER_SCORES"


def test_snowflake_lookups_field_parses() -> None:
    config = _snowflake_config(
        lookups={"fk_id": {"table": "parents", "match": {"pk": "pk"}, "select": "id"}}
    )
    assert config.lookups is not None
    assert "fk_id" in config.lookups


def test_execute_test_query_snowflake_returns_int() -> None:
    cursor = MagicMock()
    cursor.fetchone.return_value = (42,)
    conn = _fake_conn(cursor)

    with patch(
        "drt.destinations.snowflake.SnowflakeDestination._connect", return_value=conn
    ):
        result = execute_test_query(_snowflake_config(), "SELECT COUNT(*) FROM t")

    assert result == 42
    cursor.execute.assert_called_once_with("SELECT COUNT(*) FROM t")
    conn.close.assert_called_once()


def test_fetch_rows_snowflake_returns_dicts() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [(1, "alice"), (2, "bob")]
    conn = _fake_conn(cursor)

    with patch(
        "drt.destinations.snowflake.SnowflakeDestination._connect", return_value=conn
    ):
        rows = fetch_rows(
            _snowflake_config(), "SELECT id, name FROM t", columns=["id", "name"]
        )

    assert rows == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    conn.close.assert_called_once()


def test_databricks_is_queryable_and_table_name() -> None:
    config = _databricks_config()
    assert is_queryable(config) is True
    assert get_table_name(config) == "main.analytics.users"


def test_execute_test_query_databricks_returns_int() -> None:
    cursor = MagicMock()
    cursor.fetchone.return_value = (42,)
    conn = _fake_conn(cursor)

    with patch(
        "drt.destinations.databricks.DatabricksDestination._connect",
        return_value=conn,
    ):
        result = execute_test_query(_databricks_config(), "SELECT COUNT(*) FROM t")

    assert result == 42
    cursor.execute.assert_called_once_with("SELECT COUNT(*) FROM t")
    conn.close.assert_called_once()


def test_fetch_rows_databricks_returns_dicts() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [(1, "alice"), (2, "bob")]
    conn = _fake_conn(cursor)

    with patch(
        "drt.destinations.databricks.DatabricksDestination._connect",
        return_value=conn,
    ):
        rows = fetch_rows(
            _databricks_config(),
            "SELECT id, name FROM main.analytics.users",
            columns=["id", "name"],
        )

    assert rows == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    conn.close.assert_called_once()


def test_fetch_rows_databricks_empty_columns_uses_cursor_description() -> None:
    """compute_diff() passes columns=[] for the replace-mode full-table scan
    (caught in review, #1060): dict(zip([], row)) would collapse every row
    to {}, which compute_diff() then keys on via row.get(c) -- silently
    understating a real replace run's deletions. Real column names must
    come from the cursor's own description instead."""
    cursor = MagicMock()
    cursor.description = [("id", None), ("name", None)]
    cursor.fetchall.return_value = [(1, "alice"), (2, "bob")]
    conn = _fake_conn(cursor)

    with patch(
        "drt.destinations.databricks.DatabricksDestination._connect",
        return_value=conn,
    ):
        rows = fetch_rows(
            _databricks_config(),
            "SELECT * FROM main.analytics.users",
            columns=[],
        )

    assert rows == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]


# ---------------------------------------------------------------------------
# fetch_rows_by_keys — parameterized keyed batched fetch (#470)
# ---------------------------------------------------------------------------


def _pg_config(**overrides: Any) -> PostgresDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "postgres",
        "host": "localhost",
        "dbname": "test",
        "table": "public.users",
        "upsert_key": ["id"],
    }
    defaults.update(overrides)
    return PostgresDestinationConfig.model_validate(defaults)


def _mysql_config(**overrides: Any) -> MySQLDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "mysql",
        "host": "localhost",
        "dbname": "test",
        "table": "users",
        "upsert_key": ["id"],
    }
    defaults.update(overrides)
    return MySQLDestinationConfig.model_validate(defaults)


def _clickhouse_config(**overrides: Any) -> ClickHouseDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "clickhouse",
        "host": "localhost",
        "database": "test",
        "table": "users",
        "upsert_key": ["id"],
    }
    defaults.update(overrides)
    return ClickHouseDestinationConfig.model_validate(defaults)


def _databricks_config(**overrides: Any) -> DatabricksDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "databricks",
        "host_env": "DATABRICKS_HOST",
        "http_path_env": "DATABRICKS_HTTP_PATH",
        "token_env": "DATABRICKS_TOKEN",
        "catalog": "main",
        "schema": "analytics",
        "table": "users",
        "upsert_key": ["id"],
    }
    defaults.update(overrides)
    return DatabricksDestinationConfig.model_validate(defaults)


def _plain_conn(cursor: MagicMock) -> MagicMock:
    """Fake conn whose .cursor() returns the cursor directly (pg / mysql)."""
    conn = MagicMock()
    conn.cursor.return_value = cursor
    return conn


def _render_pg(composed: Any) -> str:
    """Render a psycopg2 ``sql.Composed`` to text without a live connection."""
    from psycopg2.sql import SQL, Composed, Identifier

    parts: list[str] = []
    for part in composed.seq:
        if isinstance(part, Composed):
            parts.append(_render_pg(part))
        elif isinstance(part, SQL):
            parts.append(part.string)
        elif isinstance(part, Identifier):
            parts.append(".".join(f'"{s}"' for s in part.strings))
    return "".join(parts)


@needs_psycopg2
def test_fetch_rows_by_keys_postgres_parameterized_and_batched() -> None:
    cursor = MagicMock()
    # 3 keys, batch_size 2 -> two batches (2 rows then 1 row).
    cursor.fetchall.side_effect = [
        [(1, "alice"), (2, "bob")],
        [(3, "carol")],
    ]
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.postgres.PostgresDestination._connect", return_value=conn
    ):
        rows = fetch_rows_by_keys(
            _pg_config(),
            key_cols=["id"],
            key_tuples=[(1,), (2,), (3,)],
            columns=["id", "name"],
            batch_size=2,
        )

    # (b) batching: execute called once per batch.
    assert cursor.execute.call_count == 2
    # (a) SQL uses parameterized IN (...) placeholders. (psycopg2 sql.Composed)
    first_sql, first_params = cursor.execute.call_args_list[0][0]
    first_rendered = _render_pg(first_sql)
    assert "IN (" in first_rendered
    assert "%s" in first_rendered
    # (c) key values flow through params (never embedded in SQL text).
    assert first_params == [1, 2]
    assert "1" not in first_rendered and "2" not in first_rendered
    _, second_params = cursor.execute.call_args_list[1][0]
    assert second_params == [3]
    # (d) rows keyed by explicit columns, union of both batches.
    assert rows == [
        {"id": 1, "name": "alice"},
        {"id": 2, "name": "bob"},
        {"id": 3, "name": "carol"},
    ]
    conn.close.assert_called_once()


@needs_psycopg2
def test_fetch_rows_by_keys_postgres_composite_key_placeholders() -> None:
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[(1, "eu", "x")]]
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.postgres.PostgresDestination._connect", return_value=conn
    ):
        rows = fetch_rows_by_keys(
            _pg_config(upsert_key=["id", "region"]),
            key_cols=["id", "region"],
            key_tuples=[(1, "eu")],
            columns=["id", "region", "val"],
        )

    stmt, params = cursor.execute.call_args_list[0][0]
    rendered = _render_pg(stmt)
    assert "IN (" in rendered
    assert "(%s, %s)" in rendered
    # composite params are flattened row-by-row.
    assert params == [1, "eu"]
    assert rows == [{"id": 1, "region": "eu", "val": "x"}]


def test_fetch_rows_by_keys_mysql_parameterized() -> None:
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[(1, "alice")]]
    conn = _plain_conn(cursor)

    with patch("drt.destinations.mysql.MySQLDestination._connect", return_value=conn):
        rows = fetch_rows_by_keys(
            _mysql_config(),
            key_cols=["id"],
            key_tuples=[(1,)],
            columns=["id", "name"],
        )

    sql, params = cursor.execute.call_args_list[0][0]
    assert "IN (" in sql
    assert "%s" in sql
    assert params == [1]
    assert rows == [{"id": 1, "name": "alice"}]


def test_fetch_rows_by_keys_snowflake_parameterized() -> None:
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[(1, "alice")]]
    conn = _fake_conn(cursor)

    with patch(
        "drt.destinations.snowflake.SnowflakeDestination._connect", return_value=conn
    ):
        rows = fetch_rows_by_keys(
            _snowflake_config(),
            key_cols=["ID"],
            key_tuples=[(1,)],
            columns=["ID", "NAME"],
        )

    sql, params = cursor.execute.call_args_list[0][0]
    assert "IN (" in sql
    assert "%s" in sql
    assert params == [1]
    assert rows == [{"ID": 1, "NAME": "alice"}]


def test_fetch_rows_by_keys_databricks_native_parameterized() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [(1, "alice")]
    conn = _fake_conn(cursor)

    with patch(
        "drt.destinations.databricks.DatabricksDestination._connect",
        return_value=conn,
    ):
        rows = fetch_rows_by_keys(
            _databricks_config(),
            key_cols=["id"],
            key_tuples=[(1,)],
            columns=["id", "name"],
        )

    sql, params = cursor.execute.call_args_list[0][0]
    assert sql == "SELECT id, name FROM main.analytics.users WHERE id IN (?)"
    assert params == [1]
    assert rows == [{"id": 1, "name": "alice"}]


def test_fetch_rows_by_keys_databricks_composite_avoids_delta_tuple_in() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [("c1", "u1", "alice")]
    conn = _fake_conn(cursor)

    with patch(
        "drt.destinations.databricks.DatabricksDestination._connect",
        return_value=conn,
    ):
        rows = fetch_rows_by_keys(
            _databricks_config(upsert_key=["company_id", "user_id"]),
            key_cols=["company_id", "user_id"],
            key_tuples=[("c1", "u1"), ("c2", "u2")],
            columns=["company_id", "user_id", "name"],
        )

    sql, params = cursor.execute.call_args.args
    assert sql == (
        "SELECT company_id, user_id, name FROM main.analytics.users WHERE "
        "((company_id = ? AND user_id = ?) OR (company_id = ? AND user_id = ?))"
    )
    assert params == ["c1", "u1", "c2", "u2"]
    assert rows == [{"company_id": "c1", "user_id": "u1", "name": "alice"}]


def test_fetch_rows_by_keys_databricks_caps_batch_at_255_params() -> None:
    """The generic default batch_size=1000 (caught in review, #1060) would
    bind 1000 markers for a single-column key -- over Databricks' 255
    native-parameter limit -- so the effective batch size must be capped
    via _rows_per_chunk regardless of what the caller's batch_size allows."""
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[(i, f"n{i}") for i in range(255)], [(255, "n255")]]
    conn = _fake_conn(cursor)

    key_tuples = [(i,) for i in range(256)]  # > 255, forces 2 chunks

    with patch(
        "drt.destinations.databricks.DatabricksDestination._connect",
        return_value=conn,
    ):
        rows = fetch_rows_by_keys(
            _databricks_config(),
            key_cols=["id"],
            key_tuples=key_tuples,
            columns=["id", "name"],
            batch_size=1000,
        )

    assert cursor.execute.call_count == 2
    first_markers = cursor.execute.call_args_list[0].args[0].count("?")
    second_markers = cursor.execute.call_args_list[1].args[0].count("?")
    assert first_markers <= 255
    assert second_markers <= 255
    assert first_markers + second_markers == 256
    assert len(rows) == 256


def test_fetch_rows_by_keys_mysql_composite_key_placeholders() -> None:
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[(1, 5, "alice")]]
    conn = _plain_conn(cursor)

    with patch("drt.destinations.mysql.MySQLDestination._connect", return_value=conn):
        rows = fetch_rows_by_keys(
            _mysql_config(),
            key_cols=["user_id", "company_id"],
            key_tuples=[(1, 5)],
            columns=["user_id", "company_id", "name"],
        )

    sql, params = cursor.execute.call_args_list[0][0]
    assert "(`user_id`, `company_id`) IN ((%s, %s))" in sql
    assert params == [1, 5]
    assert rows == [{"user_id": 1, "company_id": 5, "name": "alice"}]


def test_fetch_rows_by_keys_mysql_dict_cursor_rows() -> None:
    # pymysql with a DictCursor yields dict rows — the helper must project by
    # the requested columns rather than zip a positional tuple.
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[{"id": 1, "name": "alice", "extra": "x"}]]
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.mysql.MySQLDestination._connect", return_value=conn
    ):
        rows = fetch_rows_by_keys(
            _mysql_config(),
            key_cols=["id"],
            key_tuples=[(1,)],
            columns=["id", "name"],
        )

    assert rows == [{"id": 1, "name": "alice"}]


def test_fetch_rows_by_keys_snowflake_composite_key_placeholders() -> None:
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[(1, 5, "alice")]]
    conn = _fake_conn(cursor)

    with patch(
        "drt.destinations.snowflake.SnowflakeDestination._connect", return_value=conn
    ):
        rows = fetch_rows_by_keys(
            _snowflake_config(),
            key_cols=["USER_ID", "COMPANY_ID"],
            key_tuples=[(1, 5)],
            columns=["USER_ID", "COMPANY_ID", "NAME"],
        )

    sql, params = cursor.execute.call_args_list[0][0]
    assert "IN ((%s, %s))" in sql
    assert params == [1, 5]
    assert rows == [{"USER_ID": 1, "COMPANY_ID": 5, "NAME": "alice"}]


def test_fetch_rows_by_keys_empty_returns_empty_without_query() -> None:
    cursor = MagicMock()
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.postgres.PostgresDestination._connect", return_value=conn
    ) as connect:
        rows = fetch_rows_by_keys(
            _pg_config(),
            key_cols=["id"],
            key_tuples=[],
            columns=["id", "name"],
        )

    assert rows == []
    connect.assert_not_called()
    cursor.execute.assert_not_called()


def test_fetch_rows_by_keys_clickhouse_raises_not_implemented() -> None:
    with pytest.raises(NotImplementedError):
        fetch_rows_by_keys(
            _clickhouse_config(),
            key_cols=["id"],
            key_tuples=[(1,)],
            columns=["id", "name"],
        )


# ---------------------------------------------------------------------------
# fetch_tracked_state — read-only tracked-mirror state read (#693)
# ---------------------------------------------------------------------------


_WRITE_KEYWORDS = ("INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "TRUNCATE", "ALTER")


def _assert_read_only(cursor: MagicMock) -> None:
    """Every statement the cursor saw must be a bare SELECT."""
    for call in cursor.execute.call_args_list:
        stmt = call[0][0]
        text = _render_pg(stmt) if hasattr(stmt, "seq") else str(stmt)
        upper = text.upper()
        assert upper.lstrip().startswith("SELECT"), text
        for kw in _WRITE_KEYWORDS:
            assert kw not in upper, f"write keyword {kw} in {text}"


def _assert_show_or_select_only(cursor: MagicMock) -> None:
    """Allow a dialect's read-only SHOW existence probe plus SELECTs."""
    for call in cursor.execute.call_args_list:
        text = str(call.args[0])
        upper = text.upper().lstrip()
        assert upper.startswith(("SHOW", "SELECT")), text
        for keyword in _WRITE_KEYWORDS:
            assert keyword not in upper, f"write keyword {keyword} in {text}"


@needs_psycopg2
def test_fetch_tracked_state_postgres_selects_only() -> None:
    cursor = MagicMock()
    # to_regclass probe -> exists; then the state rows.
    cursor.fetchone.return_value = ("public._drt_synced_keys",)
    cursor.fetchall.return_value = [("h1", '["a"]'), ("h2", '["b"]')]
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.postgres.PostgresDestination._connect", return_value=conn
    ):
        state = fetch_tracked_state(_pg_config(table="public.users"), "users_sync")

    assert state == {"h1": '["a"]', "h2": '["b"]'}
    _assert_read_only(cursor)
    # State table is resolved in the target table's schema.
    probe_params = cursor.execute.call_args_list[0][0][1]
    assert probe_params == ("public._drt_synced_keys",)
    select_stmt, select_params = cursor.execute.call_args_list[1][0]
    rendered = _render_pg(select_stmt)
    assert "SELECT key_hash, key_json" in rendered
    assert '"public"."_drt_synced_keys"' in rendered
    assert select_params == ("users_sync",)
    conn.close.assert_called_once()


@needs_psycopg2
def test_fetch_tracked_state_postgres_unqualified_table() -> None:
    cursor = MagicMock()
    cursor.fetchone.return_value = ("_drt_synced_keys",)
    cursor.fetchall.return_value = []
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.postgres.PostgresDestination._connect", return_value=conn
    ):
        state = fetch_tracked_state(_pg_config(table="users"), "s")

    assert state == {}
    _assert_read_only(cursor)
    assert cursor.execute.call_args_list[0][0][1] == ("_drt_synced_keys",)
    assert '"_drt_synced_keys"' in _render_pg(cursor.execute.call_args_list[1][0][0])


@needs_psycopg2
def test_fetch_tracked_state_postgres_missing_table_returns_empty() -> None:
    cursor = MagicMock()
    cursor.fetchone.return_value = (None,)  # to_regclass -> NULL
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.postgres.PostgresDestination._connect", return_value=conn
    ):
        state = fetch_tracked_state(_pg_config(table="public.users"), "s")

    assert state == {}
    # Probe only — no SELECT against a table that doesn't exist, and no DDL.
    assert cursor.execute.call_count == 1
    _assert_read_only(cursor)


def test_fetch_tracked_state_mysql_selects_only() -> None:
    cursor = MagicMock()
    cursor.fetchone.return_value = (1,)  # information_schema count
    cursor.fetchall.return_value = [("h1", '["a"]')]
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.mysql.MySQLDestination._connect", return_value=conn
    ):
        state = fetch_tracked_state(_mysql_config(table="mydb.users"), "users_sync")

    assert state == {"h1": '["a"]'}
    _assert_read_only(cursor)
    probe_sql, probe_params = cursor.execute.call_args_list[0][0]
    assert "information_schema.tables" in probe_sql
    assert probe_params == ("mydb", "_drt_synced_keys")
    select_sql, select_params = cursor.execute.call_args_list[1][0]
    assert "SELECT key_hash, key_json" in select_sql
    assert "`mydb`.`_drt_synced_keys`" in select_sql
    assert select_params == ("users_sync",)
    conn.close.assert_called_once()


def test_fetch_tracked_state_mysql_unqualified_uses_current_database() -> None:
    cursor = MagicMock()
    # DictCursor shapes for both the probe and the state rows.
    cursor.fetchone.return_value = {"COUNT(*)": 1}
    cursor.fetchall.return_value = [{"key_hash": "h1", "key_json": '["a"]'}]
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.mysql.MySQLDestination._connect", return_value=conn
    ):
        state = fetch_tracked_state(_mysql_config(table="users"), "s")

    # Dict cursors (pymysql DictCursor) are handled too.
    assert state == {"h1": '["a"]'}
    _assert_read_only(cursor)
    probe_sql, probe_params = cursor.execute.call_args_list[0][0]
    assert "DATABASE()" in probe_sql
    assert probe_params == ("_drt_synced_keys",)
    assert "`_drt_synced_keys`" in cursor.execute.call_args_list[1][0][0]


def test_fetch_tracked_state_mysql_missing_table_returns_empty() -> None:
    cursor = MagicMock()
    cursor.fetchone.return_value = (0,)
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.mysql.MySQLDestination._connect", return_value=conn
    ):
        state = fetch_tracked_state(_mysql_config(table="mydb.users"), "s")

    assert state == {}
    assert cursor.execute.call_count == 1
    _assert_read_only(cursor)


def test_fetch_tracked_state_snowflake_uses_fully_qualified_state_table() -> None:
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [("_DRT_SYNCED_KEYS",)],
        [("h1", '["a"]'), ("h2", '["b"]')],
    ]
    conn = _fake_conn(cursor)

    with patch(
        "drt.destinations.snowflake.SnowflakeDestination._connect",
        return_value=conn,
    ):
        state = fetch_tracked_state(_snowflake_config(), "users_sync")

    assert state == {"h1": '["a"]', "h2": '["b"]'}
    assert cursor.execute.call_args_list[0][0][0] == (
        "SHOW TABLES LIKE '_drt_synced_keys' IN SCHEMA ANALYTICS.PUBLIC"
    )
    select_sql, select_params = cursor.execute.call_args_list[1][0]
    assert select_sql == (
        "SELECT key_hash, key_json FROM ANALYTICS.PUBLIC._drt_synced_keys WHERE sync_name = %s"
    )
    assert select_params == ["users_sync"]
    _assert_show_or_select_only(cursor)
    conn.close.assert_called_once()


def test_fetch_tracked_state_clickhouse_uses_named_parameter() -> None:
    client = MagicMock()
    exists = MagicMock(result_rows=[(1,)])
    rows = MagicMock(result_rows=[("h1", '["a"]')])
    client.query.side_effect = [exists, rows]

    with patch(
        "drt.destinations.clickhouse.ClickHouseDestination._connect",
        return_value=client,
    ):
        state = fetch_tracked_state(_clickhouse_config(), "users_sync")

    assert state == {"h1": '["a"]'}
    assert client.query.call_args_list[0].args == ("EXISTS TABLE `_drt_synced_keys`",)
    assert client.query.call_args_list[1].args == (
        "SELECT key_hash, key_json FROM `_drt_synced_keys` WHERE sync_name = {sync_name:String}",
    )
    assert client.query.call_args_list[1].kwargs == {"parameters": {"sync_name": "users_sync"}}
    client.command.assert_not_called()
    client.close.assert_called_once()


def test_fetch_tracked_state_databricks_uses_catalog_schema_state_table() -> None:
    cursor = MagicMock()
    cursor.fetchall.side_effect = [
        [("analytics", "_drt_synced_keys", False)],
        [("h1", '["a"]')],
    ]
    conn = _fake_conn(cursor)

    with patch(
        "drt.destinations.databricks.DatabricksDestination._connect",
        return_value=conn,
    ):
        state = fetch_tracked_state(_databricks_config(), "users_sync")

    assert state == {"h1": '["a"]'}
    assert cursor.execute.call_args_list[0].args == (
        "SHOW TABLES IN main.analytics LIKE '_drt_synced_keys'",
    )
    assert cursor.execute.call_args_list[1].args == (
        "SELECT key_hash, key_json FROM main.analytics._drt_synced_keys WHERE sync_name = ?",
        ["users_sync"],
    )
    _assert_show_or_select_only(cursor)
    conn.close.assert_called_once()


def test_fetch_tracked_state_unsupported_config_raises() -> None:
    with pytest.raises(TypeError):
        fetch_tracked_state(
            RestApiDestinationConfig(type="rest_api", url="http://x", method="POST"),
            "s",
        )


# ---------------------------------------------------------------------------
# fetch_all_keys — read-only destination key read for the destination-strategy
# mirror delete preview (#693)
#
# The destination strategy DELETEs ``dest_keys - source_keys``, i.e. exactly
# the complement that ``fetch_rows_by_keys`` (#470) can never return. Previewing
# it needs the destination's *own* key set, optionally narrowed by mirror.scope
# the same way ``_build_mirror_delete``'s scope_clause narrows the real DELETE.
# ---------------------------------------------------------------------------


@needs_psycopg2
def test_fetch_all_keys_postgres_selects_key_columns_only() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [(1,), (2,), (3,)]
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.postgres.PostgresDestination._connect", return_value=conn
    ):
        keys = fetch_all_keys(_pg_config(table="public.users"), ["id"])

    assert keys == [(1,), (2,), (3,)]
    _assert_read_only(cursor)
    stmt, = cursor.execute.call_args_list[0][0][:1]
    rendered = _render_pg(stmt)
    assert rendered == 'SELECT "id" FROM "public"."users"'
    # No scope → no params bound at all.
    assert len(cursor.execute.call_args_list[0][0]) == 1
    conn.close.assert_called_once()


@needs_psycopg2
def test_fetch_all_keys_postgres_composite_key() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [("c1", "u1"), ("c1", "u2")]
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.postgres.PostgresDestination._connect", return_value=conn
    ):
        keys = fetch_all_keys(_pg_config(), ["company_id", "user_id"])

    assert keys == [("c1", "u1"), ("c1", "u2")]
    rendered = _render_pg(cursor.execute.call_args_list[0][0][0])
    assert 'SELECT "company_id", "user_id"' in rendered
    _assert_read_only(cursor)


@needs_psycopg2
def test_fetch_all_keys_postgres_scope_filters_server_side() -> None:
    """mirror.scope narrows the read in SQL, not in Python.

    The clause must be the same shape ``PostgresDestination._build_mirror_delete``
    emits (``<col> IN %s`` with psycopg2 tuple auto-expansion), so the preview
    and the real DELETE select the same rows.
    """
    cursor = MagicMock()
    cursor.fetchall.return_value = [(1,)]
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.postgres.PostgresDestination._connect", return_value=conn
    ):
        keys = fetch_all_keys(
            _pg_config(table="public.users"),
            ["id"],
            scope_cols=["region"],
            scopes=[("eu",), ("us",)],
        )

    assert keys == [(1,)]
    stmt, params = cursor.execute.call_args_list[0][0]
    rendered = _render_pg(stmt)
    assert rendered == 'SELECT "id" FROM "public"."users" WHERE "region" IN %s'
    # Single scope column → one auto-expanded tuple of scalars (psycopg2).
    assert params == (("eu", "us"),)
    _assert_read_only(cursor)


@needs_psycopg2
def test_fetch_all_keys_postgres_composite_scope() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.postgres.PostgresDestination._connect", return_value=conn
    ):
        fetch_all_keys(
            _pg_config(),
            ["id"],
            scope_cols=["region", "tier"],
            scopes=[("eu", "gold"), ("us", "silver")],
        )

    stmt, params = cursor.execute.call_args_list[0][0]
    rendered = _render_pg(stmt)
    assert 'WHERE ("region", "tier") IN %s' in rendered
    # Composite scope → tuple of tuples (psycopg2 renders row constructors).
    assert params == ((("eu", "gold"), ("us", "silver")),)


@needs_psycopg2
def test_fetch_all_keys_postgres_empty_scopes_reads_whole_table() -> None:
    """``scope_cols`` set but nothing observed → no scope clause.

    Matches ``_build_mirror_delete``, which only prepends the scope clause when
    ``scope_cols and scopes`` are both truthy.
    """
    cursor = MagicMock()
    cursor.fetchall.return_value = [(1,)]
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.postgres.PostgresDestination._connect", return_value=conn
    ):
        fetch_all_keys(_pg_config(), ["id"], scope_cols=["region"], scopes=[])

    rendered = _render_pg(cursor.execute.call_args_list[0][0][0])
    assert "WHERE" not in rendered


def test_fetch_all_keys_mysql_selects_key_columns_only() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [(1,), (2,)]
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.mysql.MySQLDestination._connect", return_value=conn
    ):
        keys = fetch_all_keys(_mysql_config(table="mydb.users"), ["id"])

    assert keys == [(1,), (2,)]
    sql = cursor.execute.call_args_list[0][0][0]
    assert sql == "SELECT `id` FROM `mydb`.`users`"
    _assert_read_only(cursor)
    conn.close.assert_called_once()


def test_fetch_all_keys_mysql_scope_uses_explicit_placeholders() -> None:
    """pymysql has no tuple auto-expansion, so the scope clause is an explicit
    ``%s`` list with flattened params — same as ``MySQLDestination._build_mirror_delete``.
    """
    cursor = MagicMock()
    cursor.fetchall.return_value = [(1,)]
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.mysql.MySQLDestination._connect", return_value=conn
    ):
        fetch_all_keys(
            _mysql_config(),
            ["id"],
            scope_cols=["region"],
            scopes=[("eu",), ("us",)],
        )

    sql, params = cursor.execute.call_args_list[0][0]
    assert sql == "SELECT `id` FROM `users` WHERE `region` IN (%s, %s)"
    assert params == ["eu", "us"]


def test_fetch_all_keys_mysql_composite_scope() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    conn = _plain_conn(cursor)

    with patch("drt.destinations.mysql.MySQLDestination._connect", return_value=conn):
        fetch_all_keys(
            _mysql_config(),
            ["id"],
            scope_cols=["region", "tier"],
            scopes=[("eu", "gold")],
        )

    sql, params = cursor.execute.call_args_list[0][0]
    assert "WHERE (`region`, `tier`) IN ((%s, %s))" in sql
    assert params == ["eu", "gold"]


def test_fetch_all_keys_mysql_dict_cursor_rows() -> None:
    """A DictCursor yields mappings; keys come back in ``key_cols`` order."""
    cursor = MagicMock()
    cursor.fetchall.return_value = [
        {"user_id": "u1", "company_id": "c1"},
        {"user_id": "u2", "company_id": "c1"},
    ]
    conn = _plain_conn(cursor)

    with patch("drt.destinations.mysql.MySQLDestination._connect", return_value=conn):
        keys = fetch_all_keys(_mysql_config(), ["company_id", "user_id"])

    assert keys == [("c1", "u1"), ("c1", "u2")]


def test_fetch_all_keys_snowflake_scope_uses_mirror_placeholder_shape() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [(1,), (2,)]
    conn = _fake_conn(cursor)

    with patch(
        "drt.destinations.snowflake.SnowflakeDestination._connect",
        return_value=conn,
    ):
        keys = fetch_all_keys(
            _snowflake_config(),
            ["ID"],
            scope_cols=["REGION"],
            scopes=[("EU",), ("US",)],
        )

    assert keys == [(1,), (2,)]
    assert cursor.execute.call_args.args == (
        "SELECT ID FROM ANALYTICS.PUBLIC.USER_SCORES WHERE REGION IN (%s, %s)",
        ["EU", "US"],
    )
    _assert_read_only(cursor)
    conn.close.assert_called_once()


def test_fetch_all_keys_clickhouse_scope_uses_named_array() -> None:
    client = MagicMock()
    client.query.return_value = MagicMock(result_rows=[(1,), (2,)])

    with patch(
        "drt.destinations.clickhouse.ClickHouseDestination._connect",
        return_value=client,
    ):
        keys = fetch_all_keys(
            _clickhouse_config(table="analytics.users"),
            ["id"],
            scope_cols=["region"],
            scopes=[("eu",), ("us",)],
        )

    assert keys == [(1,), (2,)]
    assert client.query.call_args.args == (
        "SELECT toString(`id`) FROM `analytics`.`users` "
        "WHERE toString(`region`) IN {scope_keys:Array(String)}",
    )
    assert client.query.call_args.kwargs == {"parameters": {"scope_keys": ["eu", "us"]}}
    client.command.assert_not_called()
    client.close.assert_called_once()


def test_fetch_all_keys_clickhouse_keys_are_stringified() -> None:
    """The SELECT wraps key columns in toString() (#1060) so a typed column
    (e.g. UUID) compares correctly against the source's plain-string key --
    matching _build_mirror_delete's own toString()-on-both-sides approach.
    A raw UUID object returned here would never equal the source's string
    form, silently misreporting a live row as a preview deletion."""
    client = MagicMock()
    client.query.return_value = MagicMock(
        result_rows=[("3fa85f64-5717-4562-b3fc-2c963f66afa6",)]
    )

    with patch(
        "drt.destinations.clickhouse.ClickHouseDestination._connect",
        return_value=client,
    ):
        keys = fetch_all_keys(_clickhouse_config(table="users"), ["id"])

    assert keys == [("3fa85f64-5717-4562-b3fc-2c963f66afa6",)]
    assert client.query.call_args.args == ("SELECT toString(`id`) FROM `users`",)


def test_fetch_all_keys_databricks_composite_scope_uses_or_of_ands() -> None:
    cursor = MagicMock()
    cursor.fetchall.return_value = [("c1", "u1"), ("c1", "u2")]
    conn = _fake_conn(cursor)

    with patch(
        "drt.destinations.databricks.DatabricksDestination._connect",
        return_value=conn,
    ):
        keys = fetch_all_keys(
            _databricks_config(upsert_key=["company_id", "user_id"]),
            ["company_id", "user_id"],
            scope_cols=["region", "tier"],
            scopes=[("eu", "gold"), ("us", "silver")],
        )

    assert keys == [("c1", "u1"), ("c1", "u2")]
    assert cursor.execute.call_args.args == (
        "SELECT company_id, user_id FROM main.analytics.users "
        "WHERE ((region = ? AND tier = ?) OR (region = ? AND tier = ?))",
        ["eu", "gold", "us", "silver"],
    )


def test_fetch_all_keys_databricks_chunks_past_255_param_limit() -> None:
    """Databricks' 255-native-parameter-marker limit (caught in review,
    #1060): more than 255 single-column scope values must split into
    multiple queries rather than building one query that would fail
    outright. Reuses the same _rows_per_chunk math every other Databricks
    write path already chunks by."""
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[("k1",)], [("k2",)]]
    conn = _fake_conn(cursor)

    scopes = [(f"v{i}",) for i in range(300)]  # > 255, forces 2 chunks

    with patch(
        "drt.destinations.databricks.DatabricksDestination._connect",
        return_value=conn,
    ):
        keys = fetch_all_keys(
            _databricks_config(),
            ["id"],
            scope_cols=["region"],
            scopes=scopes,
        )

    assert keys == [("k1",), ("k2",)]
    assert cursor.execute.call_count == 2
    first_markers = cursor.execute.call_args_list[0].args[0].count("?")
    second_markers = cursor.execute.call_args_list[1].args[0].count("?")
    assert first_markers <= 255
    assert second_markers <= 255
    assert first_markers + second_markers == 300
    _assert_read_only(cursor)
    conn.close.assert_called_once()


def test_fetch_all_keys_unsupported_config_raises() -> None:
    with pytest.raises(TypeError):
        fetch_all_keys(
            RestApiDestinationConfig(type="rest_api", url="http://x", method="POST"),
            ["id"],
        )


# ---------------------------------------------------------------------------
# fetch_failing_rows driver coverage (#834) — only the Postgres path had a
# test (test_store_failures.py); the MySQL DictCursor branch and ClickHouse's
# distinct result-shape API were unexercised.
# ---------------------------------------------------------------------------


def test_fetch_failing_rows_mysql_tuple_rows() -> None:
    cursor = MagicMock()
    cursor.description = [("id",), ("email",)]
    cursor.fetchall.return_value = [(1, "a@example.com"), (2, "b@example.com")]
    conn = _plain_conn(cursor)

    with patch("drt.destinations.mysql.MySQLDestination._connect", return_value=conn):
        rows = fetch_failing_rows(_mysql_config(), "SELECT * FROM t", limit=5)

    assert rows == [
        {"id": 1, "email": "a@example.com"},
        {"id": 2, "email": "b@example.com"},
    ]
    cursor.execute.assert_called_once()
    (sql,) = cursor.execute.call_args[0]
    assert "LIMIT 5" in sql
    conn.close.assert_called_once()


def test_fetch_failing_rows_mysql_dict_cursor_rows() -> None:
    """pymysql's DictCursor yields mappings, not positional tuples — the
    ``isinstance(row, dict)`` branch this exercises is the one #834 flagged
    as untested and most likely to break against a real DictCursor config."""
    cursor = MagicMock()
    cursor.description = [("id",), ("email",)]
    cursor.fetchall.return_value = [{"id": 1, "email": "a@example.com"}]
    conn = _plain_conn(cursor)

    with patch("drt.destinations.mysql.MySQLDestination._connect", return_value=conn):
        rows = fetch_failing_rows(_mysql_config(), "SELECT * FROM t", limit=5)

    assert rows == [{"id": 1, "email": "a@example.com"}]


def test_fetch_failing_rows_clickhouse_uses_column_names_and_result_rows() -> None:
    """ClickHouse's client shares no code path with the DB-API drivers: no
    ``cursor()``/``description``/``fetchall`` — ``client.query(...)`` returns
    an object with ``.column_names`` / ``.result_rows`` instead."""
    client = MagicMock()
    result = MagicMock()
    result.column_names = ["id", "email"]
    result.result_rows = [(1, "a@example.com")]
    client.query.return_value = result

    with patch(
        "drt.destinations.clickhouse.ClickHouseDestination._connect",
        return_value=client,
    ):
        rows = fetch_failing_rows(_clickhouse_config(), "SELECT * FROM t", limit=5)

    assert rows == [{"id": 1, "email": "a@example.com"}]
    (sql,) = client.query.call_args[0]
    assert "LIMIT 5" in sql
    client.close.assert_called_once()


def test_fetch_failing_rows_snowflake_smoke() -> None:
    cursor = MagicMock()
    cursor.description = [("ID",), ("EMAIL",)]
    cursor.fetchall.return_value = [(1, "a@example.com")]
    conn = _fake_conn(cursor)

    with patch(
        "drt.destinations.snowflake.SnowflakeDestination._connect",
        return_value=conn,
    ):
        rows = fetch_failing_rows(_snowflake_config(), "SELECT * FROM t", limit=5)

    assert rows == [{"ID": 1, "EMAIL": "a@example.com"}]
    conn.close.assert_called_once()
