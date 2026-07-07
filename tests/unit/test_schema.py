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
    DatabricksDestinationConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    SnowflakeDestinationConfig,
)
from drt.destinations.schema import describe_columns, describe_databricks_ddls


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
    mock_connect.return_value = _conn_returning([{"column_name": "data", "data_type": "json"}])
    assert describe_columns(_mysql_config()) == {"data": "json"}


@patch("drt.destinations.mysql.MySQLDestination._connect")
def test_mysql_empty_result_returns_none(mock_connect: MagicMock) -> None:
    mock_connect.return_value = _conn_returning([])
    assert describe_columns(_mysql_config()) is None


@patch("drt.destinations.mysql.MySQLDestination._connect")
def test_mysql_qualified_table_filters_by_schema(mock_connect: MagicMock) -> None:
    conn = _conn_returning([("id", "int")])
    mock_connect.return_value = conn
    describe_columns(_mysql_config("appdb.events"))
    sql, params = conn.cursor.return_value.execute.call_args[0]
    assert params == ["events", "appdb"]


@patch("drt.destinations.mysql.MySQLDestination._connect")
def test_mysql_unqualified_table_scopes_to_current_database(mock_connect: MagicMock) -> None:
    """Unqualified table must be scoped to DATABASE(); otherwise information_schema
    spans every visible DB and a same-named table in another schema collides
    (#317 review)."""
    conn = _conn_returning([("id", "int")])
    mock_connect.return_value = conn
    describe_columns(_mysql_config("events"))
    sql, params = conn.cursor.return_value.execute.call_args[0]
    assert "table_schema = DATABASE()" in sql
    assert params == ["events"]


# ---------------------------------------------------------------------------
# Unsupported configs
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Snowflake — VARIANT/OBJECT/ARRAY → json (semi-structured loads via PARSE_JSON)
# ---------------------------------------------------------------------------


def _sf_config() -> SnowflakeDestinationConfig:
    return SnowflakeDestinationConfig(
        type="snowflake",
        account_env="A",
        user_env="U",
        password_env="P",
        database="DB",
        **{"schema": "PUBLIC"},
        table="EVENTS",
        warehouse="WH",
    )


def _sf_conn_returning(rows: list[Any]) -> MagicMock:
    """Snowflake uses ``with conn.cursor() as cur`` — wire the context manager."""
    conn = MagicMock()
    cur = MagicMock()
    cur.fetchall.return_value = rows
    conn.cursor.return_value.__enter__.return_value = cur
    return conn


@patch("drt.destinations.snowflake.SnowflakeDestination._connect")
def test_snowflake_maps_semi_structured_to_json(mock_connect: MagicMock) -> None:
    mock_connect.return_value = _sf_conn_returning(
        [
            ("ID", "NUMBER"),
            ("PAYLOAD", "VARIANT"),
            ("PROFILE", "OBJECT"),
            ("TAGS", "ARRAY"),
            ("NAME", "TEXT"),
        ]
    )
    assert describe_columns(_sf_config()) == {
        "ID": "scalar",
        "PAYLOAD": "json",
        "PROFILE": "json",
        "TAGS": "json",
        "NAME": "scalar",
    }


@patch("drt.destinations.snowflake.SnowflakeDestination._connect")
def test_snowflake_query_targets_db_information_schema_case_insensitively(
    mock_connect: MagicMock,
) -> None:
    conn = _sf_conn_returning([("ID", "NUMBER")])
    mock_connect.return_value = conn
    describe_columns(_sf_config())
    cur = conn.cursor.return_value.__enter__.return_value
    sql, params = cur.execute.call_args[0]
    assert "DB.information_schema.columns" in sql
    assert "UPPER(table_schema) = UPPER(%s)" in sql
    assert params == ["PUBLIC", "EVENTS"]


@patch("drt.destinations.snowflake.SnowflakeDestination._connect")
def test_snowflake_empty_result_returns_none(mock_connect: MagicMock) -> None:
    mock_connect.return_value = _sf_conn_returning([])
    assert describe_columns(_sf_config()) is None


# ---------------------------------------------------------------------------
# Databricks — STRUCT/MAP/ARRAY/VARIANT → json (load via from_json / parse_json)
# ---------------------------------------------------------------------------


def _dbx_config() -> DatabricksDestinationConfig:
    return DatabricksDestinationConfig(
        type="databricks",
        host_env="H",
        http_path_env="HP",
        token_env="T",
        catalog="main",
        **{"schema": "analytics"},
        table="events",
        upsert_key=["id"],
    )


@patch("drt.destinations.databricks.DatabricksDestination._connect")
def test_databricks_maps_complex_types_to_json(mock_connect: MagicMock) -> None:
    # Databricks uses ``with conn.cursor() as cur`` — same context-manager wiring
    # as Snowflake, so reuse that helper.
    mock_connect.return_value = _sf_conn_returning(
        [
            ("id", "BIGINT"),
            ("payload", "VARIANT"),
            ("profile", "STRUCT"),
            ("attrs", "MAP"),
            ("tags", "ARRAY"),
            ("name", "STRING"),
        ]
    )
    assert describe_columns(_dbx_config()) == {
        "id": "scalar",
        "payload": "json",
        "profile": "json",
        "attrs": "json",
        "tags": "json",
        "name": "scalar",
    }


@patch("drt.destinations.databricks.DatabricksDestination._connect")
def test_databricks_query_targets_catalog_information_schema(
    mock_connect: MagicMock,
) -> None:
    conn = _sf_conn_returning([("id", "BIGINT")])
    mock_connect.return_value = conn
    describe_columns(_dbx_config())
    cur = conn.cursor.return_value.__enter__.return_value
    sql, params = cur.execute.call_args[0]
    assert "main.information_schema.columns" in sql
    assert "lower(table_schema) = lower(%s)" in sql
    assert params == ["analytics", "events"]


@patch("drt.destinations.databricks.DatabricksDestination._connect")
def test_databricks_empty_result_returns_none(mock_connect: MagicMock) -> None:
    mock_connect.return_value = _sf_conn_returning([])
    assert describe_columns(_dbx_config()) is None


@patch("drt.destinations.databricks.DatabricksDestination._connect")
def test_databricks_ddls_returns_struct_array_map_only(mock_connect: MagicMock) -> None:
    # full_data_type carries the parameterised DDL from_json() needs. VARIANT
    # (parse_json, no DDL) and scalars are excluded.
    conn = _sf_conn_returning(
        [
            ("id", "BIGINT"),
            ("payload", "VARIANT"),
            ("profile", "STRUCT<name: STRING, age: INT>"),
            ("attrs", "MAP<STRING, STRING>"),
            ("tags", "ARRAY<STRING>"),
            ("name", "STRING"),
        ]
    )
    mock_connect.return_value = conn
    assert describe_databricks_ddls(_dbx_config()) == {
        "profile": "STRUCT<name: STRING, age: INT>",
        "attrs": "MAP<STRING, STRING>",
        "tags": "ARRAY<STRING>",
    }
    # the query must read full_data_type (not data_type)
    sql, _ = conn.cursor.return_value.__enter__.return_value.execute.call_args[0]
    assert "full_data_type" in sql


@patch("drt.destinations.databricks.DatabricksDestination._connect")
def test_databricks_ddls_none_when_no_complex_columns(mock_connect: MagicMock) -> None:
    mock_connect.return_value = _sf_conn_returning([("id", "BIGINT"), ("payload", "VARIANT")])
    assert describe_databricks_ddls(_dbx_config()) is None


@patch("drt.destinations.databricks.DatabricksDestination._connect")
def test_databricks_ddls_none_on_failure(mock_connect: MagicMock) -> None:
    mock_connect.side_effect = RuntimeError("information_schema locked down")
    assert describe_databricks_ddls(_dbx_config()) is None


@patch("drt.destinations.snowflake.SnowflakeDestination._connect")
def test_snowflake_failure_returns_none(mock_connect: MagicMock) -> None:
    mock_connect.side_effect = RuntimeError("insufficient privileges")
    assert describe_columns(_sf_config()) is None


# ---------------------------------------------------------------------------
# Unsupported configs — ClickHouse/BigQuery/Databricks are later phases of #317
# ---------------------------------------------------------------------------


def test_unsupported_config_returns_none() -> None:
    from drt.config.models import ClickHouseDestinationConfig

    cfg = ClickHouseDestinationConfig(type="clickhouse", host="h", database="d", table="t")
    assert describe_columns(cfg) is None
