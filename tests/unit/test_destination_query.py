"""Tests for destination query helpers."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import (
    ClickHouseDestinationConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    RestApiDestinationConfig,
    SnowflakeDestinationConfig,
)
from drt.destinations.query import (
    execute_test_query,
    fetch_all_keys,
    fetch_rows,
    fetch_rows_by_keys,
    fetch_tracked_state,
    get_table_name,
    is_queryable,
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

    with patch(
        "drt.destinations.mysql.MySQLDestination._connect", return_value=conn
    ):
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


def test_fetch_rows_by_keys_mysql_composite_key_placeholders() -> None:
    cursor = MagicMock()
    cursor.fetchall.side_effect = [[(1, 5, "alice")]]
    conn = _plain_conn(cursor)

    with patch(
        "drt.destinations.mysql.MySQLDestination._connect", return_value=conn
    ):
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


def test_fetch_tracked_state_unsupported_dialect_returns_empty() -> None:
    # Tracked mirror is Postgres/MySQL-only; other dialects have no state
    # table to read, so the preview degrades to "no deletions known".
    assert fetch_tracked_state(_clickhouse_config(), "s") == {}
    assert fetch_tracked_state(_snowflake_config(), "s") == {}
    assert (
        fetch_tracked_state(
            RestApiDestinationConfig(type="rest_api", url="http://x", method="POST"),
            "s",
        )
        == {}
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

    with patch("drt.destinations.mysql.MySQLDestination._connect", return_value=conn):
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

    with patch("drt.destinations.mysql.MySQLDestination._connect", return_value=conn):
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


def test_fetch_all_keys_unsupported_dialect_raises() -> None:
    """ClickHouse / Snowflake implement ``_finalize_mirror`` themselves, so the
    shared destination-strategy preview does not apply — raise so the caller
    degrades to "no preview" explicitly rather than silently reporting zero
    deletions for a mirror that will in fact delete rows.
    """
    with pytest.raises(NotImplementedError):
        fetch_all_keys(_clickhouse_config(), ["id"])
    with pytest.raises(NotImplementedError):
        fetch_all_keys(_snowflake_config(), ["id"])
    with pytest.raises(TypeError):
        fetch_all_keys(
            RestApiDestinationConfig(type="rest_api", url="http://x", method="POST"),
            ["id"],
        )
