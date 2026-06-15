"""Tests for destination query helpers."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

from drt.config.models import (
    PostgresDestinationConfig,
    RestApiDestinationConfig,
    SnowflakeDestinationConfig,
)
from drt.destinations.query import (
    execute_test_query,
    fetch_rows,
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
