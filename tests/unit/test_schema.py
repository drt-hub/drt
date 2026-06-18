"""Tests for drt.destinations.schema.describe_columns (#317, Layer 3).

Introspects INFORMATION_SCHEMA to learn each destination column's type so the
serializer can route dict/list values without a json_columns declaration. The
DB driver is mocked — these tests lock the query shape, the data_type →
category mapping, schema.table parsing, and the best-effort None fallback.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from drt.config.models import (
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    SnowflakeDestinationConfig,
)
from drt.destinations.schema import describe_columns


def _pg_config(table: str = "public.events") -> PostgresDestinationConfig:
    return PostgresDestinationConfig(
        type="postgres", host="h", dbname="d", table=table, upsert_key=["id"]
    )


def _mysql_config(table: str = "events") -> MySQLDestinationConfig:
    return MySQLDestinationConfig(
        type="mysql", host="h", dbname="d", table=table, upsert_key=["id"]
    )


def _conn_returning(rows: list[Any]) -> MagicMock:
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = rows
    conn.cursor.return_value = cur
    return conn


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------


@patch("drt.destinations.postgres.PostgresDestination._connect")
def test_postgres_maps_data_types_to_categories(mock_connect: MagicMock) -> None:
    mock_connect.return_value = _conn_returning(
        [
            ("id", "integer"),
            ("profile", "jsonb"),
            ("meta", "json"),
            ("tags", "ARRAY"),
            ("name", "text"),
        ]
    )
    out = describe_columns(_pg_config())
    assert out == {
        "id": "scalar",
        "profile": "json",
        "meta": "json",
        "tags": "array",
        "name": "scalar",
    }


@patch("drt.destinations.postgres.PostgresDestination._connect")
def test_postgres_qualified_table_filters_by_schema(mock_connect: MagicMock) -> None:
    conn = _conn_returning([("id", "integer")])
    mock_connect.return_value = conn
    describe_columns(_pg_config("analytics.events"))
    sql, params = conn.cursor.return_value.execute.call_args[0]
    assert "table_name = %s" in sql
    assert "table_schema = %s" in sql
    assert params == ["events", "analytics"]


@patch("drt.destinations.postgres.PostgresDestination._connect")
def test_postgres_unqualified_table_excludes_system_schemas(mock_connect: MagicMock) -> None:
    conn = _conn_returning([("id", "integer")])
    mock_connect.return_value = conn
    describe_columns(_pg_config("events"))
    sql, params = conn.cursor.return_value.execute.call_args[0]
    assert "NOT IN ('pg_catalog', 'information_schema')" in sql
    assert params == ["events"]


@patch("drt.destinations.postgres.PostgresDestination._connect")
def test_postgres_empty_result_returns_none(mock_connect: MagicMock) -> None:
    mock_connect.return_value = _conn_returning([])
    assert describe_columns(_pg_config()) is None


@patch("drt.destinations.postgres.PostgresDestination._connect")
def test_postgres_connection_failure_returns_none(mock_connect: MagicMock) -> None:
    """Best-effort: a locked-down information_schema must not break the sync."""
    mock_connect.side_effect = RuntimeError("permission denied for schema")
    assert describe_columns(_pg_config()) is None


@patch("drt.destinations.postgres.PostgresDestination._connect")
def test_postgres_closes_connection(mock_connect: MagicMock) -> None:
    conn = _conn_returning([("id", "integer")])
    mock_connect.return_value = conn
    describe_columns(_pg_config())
    conn.close.assert_called_once()


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------


@patch("drt.destinations.mysql.MySQLDestination._connect")
def test_mysql_maps_json_only(mock_connect: MagicMock) -> None:
    """MySQL has no array type — only ``json`` maps to the json category."""
    mock_connect.return_value = _conn_returning([("id", "int"), ("data", "json"), ("n", "varchar")])
    out = describe_columns(_mysql_config())
    assert out == {"id": "scalar", "data": "json", "n": "scalar"}


@patch("drt.destinations.mysql.MySQLDestination._connect")
def test_mysql_dict_cursor_rows(mock_connect: MagicMock) -> None:
    """A DictCursor yields dict rows; values stay in SELECT order."""
    mock_connect.return_value = _conn_returning(
        [{"column_name": "data", "data_type": "json"}]
    )
    assert describe_columns(_mysql_config()) == {"data": "json"}


@patch("drt.destinations.mysql.MySQLDestination._connect")
def test_mysql_qualified_table_filters_by_schema(mock_connect: MagicMock) -> None:
    conn = _conn_returning([("id", "int")])
    mock_connect.return_value = conn
    describe_columns(_mysql_config("appdb.events"))
    sql, params = conn.cursor.return_value.execute.call_args[0]
    assert params == ["events", "appdb"]


# ---------------------------------------------------------------------------
# Unsupported configs
# ---------------------------------------------------------------------------


def test_non_pg_mysql_config_returns_none() -> None:
    """DWH destinations (Snowflake/BigQuery/...) are later phases of #317."""
    cfg = SnowflakeDestinationConfig(
        type="snowflake",
        account_env="A",
        user_env="U",
        password_env="P",
        database="db",
        **{"schema": "public"},
        table="t",
        warehouse="wh",
    )
    assert describe_columns(cfg) is None
