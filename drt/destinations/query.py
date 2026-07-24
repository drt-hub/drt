"""Query destination databases for test validation and lookups."""

from __future__ import annotations

from typing import Any

from drt.config.models import (
    ClickHouseDestinationConfig,
    DestinationConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    SnowflakeDestinationConfig,
)

_QUERYABLE_TYPES = (
    PostgresDestinationConfig,
    MySQLDestinationConfig,
    ClickHouseDestinationConfig,
    SnowflakeDestinationConfig,
)


def is_queryable(config: DestinationConfig) -> bool:
    """Return True if we can run validation queries against this destination."""
    return isinstance(config, _QUERYABLE_TYPES)


def get_table_name(config: DestinationConfig) -> str:
    """Extract the target table name from a DB destination config."""
    if isinstance(config, PostgresDestinationConfig):
        return config.table
    if isinstance(config, MySQLDestinationConfig):
        return config.table
    if isinstance(config, ClickHouseDestinationConfig):
        return config.table
    if isinstance(config, SnowflakeDestinationConfig):
        # Snowflake needs the fully-qualified name; the connection sets the
        # database/schema context but the FQN matches how the destination
        # writes and is unambiguous for test / diff queries.
        return f"{config.database}.{config.schema_}.{config.table}"
    raise TypeError(f"Cannot get table name from {type(config).__name__}")


def execute_test_query(config: DestinationConfig, query: str) -> int:
    """Execute a query against a DB destination and return a single int."""
    if isinstance(config, PostgresDestinationConfig):
        return _query_postgres(config, query)
    if isinstance(config, MySQLDestinationConfig):
        return _query_mysql(config, query)
    if isinstance(config, ClickHouseDestinationConfig):
        return _query_clickhouse(config, query)
    if isinstance(config, SnowflakeDestinationConfig):
        return _query_snowflake(config, query)
    raise TypeError(f"Cannot query {type(config).__name__}")


def _query_postgres(config: PostgresDestinationConfig, query: str) -> int:
    from drt.destinations.postgres import PostgresDestination

    conn = PostgresDestination._connect(config)
    try:
        cur = conn.cursor()
        cur.execute(query)
        result: Any = cur.fetchone()[0]
        return int(result)
    finally:
        conn.close()


def _query_mysql(config: MySQLDestinationConfig, query: str) -> int:
    from drt.destinations.mysql import MySQLDestination

    conn = MySQLDestination._connect(config)
    try:
        cur = conn.cursor()
        cur.execute(query)
        row = cur.fetchone()
        val: Any = row[0] if isinstance(row, tuple) else list(row.values())[0]
        return int(val)
    finally:
        conn.close()


def _query_clickhouse(config: ClickHouseDestinationConfig, query: str) -> int:
    from drt.destinations.clickhouse import ClickHouseDestination

    client = ClickHouseDestination._connect(config)
    try:
        result = client.query(query)
        val: Any = result.result_rows[0][0]
        return int(val)
    finally:
        client.close()


def _query_snowflake(config: SnowflakeDestinationConfig, query: str) -> int:
    from drt.destinations.snowflake import SnowflakeDestination

    # _connect is an instance method on the Snowflake destination (unlike the
    # staticmethod on Postgres); the no-arg constructor has no side effects.
    conn = SnowflakeDestination()._connect(config)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            result: Any = cur.fetchone()[0]
            return int(result)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# fetch_failing_rows — capped row sample for `drt test --store-failures` (#779)
# ---------------------------------------------------------------------------


def fetch_failing_rows(
    config: DestinationConfig, query: str, limit: int
) -> list[dict[str, Any]]:
    """Run *query* (the failing-rows SELECT from ``build_failing_rows_query``)
    and return up to *limit* rows as dicts.

    Same ``_connect()`` per connector as :func:`execute_test_query` — no new
    connection/execution path. Unlike :func:`fetch_rows` (used by
    ``destination_lookup``, which already knows its select columns), the
    caller here can't know the columns of arbitrary result rows in advance, so
    columns are introspected from cursor metadata — the same
    ``[desc[0] for desc in cur.description]`` (``result.column_names`` for
    ClickHouse) pattern every source connector already uses in its own
    ``extract()``. The cap is applied in SQL (``LIMIT``, on an internally
    built integer — no injection surface) rather than fetched-then-sliced, so
    a broad failing-rows query never pulls more than *limit* rows over the
    wire.
    """
    capped = f"SELECT * FROM ({query}) AS _drt_sample LIMIT {int(limit)}"
    if isinstance(config, PostgresDestinationConfig):
        return _fetch_failing_rows_postgres(config, capped)
    if isinstance(config, MySQLDestinationConfig):
        return _fetch_failing_rows_mysql(config, capped)
    if isinstance(config, ClickHouseDestinationConfig):
        return _fetch_failing_rows_clickhouse(config, capped)
    if isinstance(config, SnowflakeDestinationConfig):
        return _fetch_failing_rows_snowflake(config, capped)
    raise TypeError(f"Cannot fetch rows from {type(config).__name__}")


def _fetch_failing_rows_postgres(
    config: PostgresDestinationConfig, query: str
) -> list[dict[str, Any]]:
    from drt.destinations.postgres import PostgresDestination

    conn = PostgresDestination._connect(config)
    try:
        cur = conn.cursor()
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _fetch_failing_rows_mysql(
    config: MySQLDestinationConfig, query: str
) -> list[dict[str, Any]]:
    from drt.destinations.mysql import MySQLDestination

    conn = MySQLDestination._connect(config)
    try:
        cur = conn.cursor()
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            if isinstance(row, dict):
                result.append(dict(row))
            else:
                result.append(dict(zip(columns, row)))
        return result
    finally:
        conn.close()


def _fetch_failing_rows_clickhouse(
    config: ClickHouseDestinationConfig, query: str
) -> list[dict[str, Any]]:
    from drt.destinations.clickhouse import ClickHouseDestination

    client = ClickHouseDestination._connect(config)
    try:
        result = client.query(query)
        columns = result.column_names
        return [dict(zip(columns, row)) for row in result.result_rows]
    finally:
        client.close()


def _fetch_failing_rows_snowflake(
    config: SnowflakeDestinationConfig, query: str
) -> list[dict[str, Any]]:
    from drt.destinations.snowflake import SnowflakeDestination

    conn = SnowflakeDestination()._connect(config)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# fetch_rows — multi-row SELECT for destination_lookup
# ---------------------------------------------------------------------------


def fetch_rows(
    config: DestinationConfig,
    query: str,
    columns: list[str],
) -> list[dict[str, Any]]:
    """Execute a SELECT against a DB destination and return rows as dicts."""
    if isinstance(config, PostgresDestinationConfig):
        return _fetch_rows_postgres(config, query, columns)
    if isinstance(config, MySQLDestinationConfig):
        return _fetch_rows_mysql(config, query, columns)
    if isinstance(config, ClickHouseDestinationConfig):
        return _fetch_rows_clickhouse(config, query, columns)
    if isinstance(config, SnowflakeDestinationConfig):
        return _fetch_rows_snowflake(config, query, columns)
    raise TypeError(f"Cannot fetch rows from {type(config).__name__}")


def _fetch_rows_postgres(
    config: PostgresDestinationConfig,
    query: str,
    columns: list[str],
) -> list[dict[str, Any]]:
    from drt.destinations.postgres import PostgresDestination

    conn = PostgresDestination._connect(config)
    try:
        cur = conn.cursor()
        cur.execute(query)
        return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()


def _fetch_rows_mysql(
    config: MySQLDestinationConfig,
    query: str,
    columns: list[str],
) -> list[dict[str, Any]]:
    from drt.destinations.mysql import MySQLDestination

    conn = MySQLDestination._connect(config)
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        result: list[dict[str, Any]] = []
        for row in rows:
            if isinstance(row, dict):
                result.append({c: row[c] for c in columns})
            else:
                result.append(dict(zip(columns, row)))
        return result
    finally:
        conn.close()


def _fetch_rows_clickhouse(
    config: ClickHouseDestinationConfig,
    query: str,
    columns: list[str],
) -> list[dict[str, Any]]:
    from drt.destinations.clickhouse import ClickHouseDestination

    client = ClickHouseDestination._connect(config)
    try:
        result = client.query(query)
        return [dict(zip(columns, row)) for row in result.result_rows]
    finally:
        client.close()


def _fetch_rows_snowflake(
    config: SnowflakeDestinationConfig,
    query: str,
    columns: list[str],
) -> list[dict[str, Any]]:
    from drt.destinations.snowflake import SnowflakeDestination

    conn = SnowflakeDestination()._connect(config)
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return [dict(zip(columns, row)) for row in cur.fetchall()]
    finally:
        conn.close()
