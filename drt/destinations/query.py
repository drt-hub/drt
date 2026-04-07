"""Query destination databases for test validation."""

from __future__ import annotations

from typing import Any

from drt.config.models import (
    ClickHouseDestinationConfig,
    DestinationConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
)

_QUERYABLE_TYPES = (
    PostgresDestinationConfig,
    MySQLDestinationConfig,
    ClickHouseDestinationConfig,
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
    raise TypeError(
        f"Cannot get table name from {type(config).__name__}"
    )


def execute_test_query(config: DestinationConfig, query: str) -> int:
    """Execute a query against a DB destination and return a single int."""
    if isinstance(config, PostgresDestinationConfig):
        return _query_postgres(config, query)
    if isinstance(config, MySQLDestinationConfig):
        return _query_mysql(config, query)
    if isinstance(config, ClickHouseDestinationConfig):
        return _query_clickhouse(config, query)
    raise TypeError(f"Cannot query {type(config).__name__}")


def _query_postgres(
    config: PostgresDestinationConfig, query: str
) -> int:
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
        val: Any = (
            row[0] if isinstance(row, tuple) else list(row.values())[0]
        )
        return int(val)
    finally:
        conn.close()


def _query_clickhouse(
    config: ClickHouseDestinationConfig, query: str
) -> int:
    from drt.destinations.clickhouse import ClickHouseDestination

    client = ClickHouseDestination._connect(config)
    try:
        result = client.query(query)
        val: Any = result.result_rows[0][0]
        return int(val)
    finally:
        client.close()
