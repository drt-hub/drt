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


# ---------------------------------------------------------------------------
# fetch_rows_by_keys — parameterized keyed batched SELECT (#470)
# ---------------------------------------------------------------------------


def _in_placeholder(n_keys: int, n_cols: int) -> str:
    """Build an explicit ``%s``-placeholder body for an ``IN (…)`` clause.

    Single key column -> ``%s, %s, …`` (one per key).
    Composite key     -> ``(%s, %s), (%s, %s), …`` (one row-tuple per key).
    """
    if n_cols == 1:
        return ", ".join(["%s"] * n_keys)
    row = "(" + ", ".join(["%s"] * n_cols) + ")"
    return ", ".join([row] * n_keys)


def _flatten_key_params(key_tuples: list[tuple[Any, ...]]) -> list[Any]:
    """Flatten key tuples into a flat, positional ``%s`` params list."""
    return [v for key in key_tuples for v in key]


def _chunks(
    seq: list[tuple[Any, ...]], size: int
) -> list[list[tuple[Any, ...]]]:
    return [seq[i : i + size] for i in range(0, len(seq), size)]


def fetch_rows_by_keys(
    config: DestinationConfig,
    key_cols: list[str],
    key_tuples: list[tuple[Any, ...]],
    columns: list[str],
    batch_size: int = 1000,
) -> list[dict[str, Any]]:
    """Fetch only rows whose key columns match ``key_tuples``.

    Runs ``SELECT <columns> FROM <table> WHERE <key> IN (…)`` in bounded
    batches, executed **parameterized** (``%s`` placeholders + a positional
    params list) so key values are never embedded into the SQL text. Returns
    the same ``list[dict]`` shape as :func:`fetch_rows`, keyed by the explicit
    ``columns``.

    Supported for Postgres / MySQL / Snowflake only. ClickHouse uses a
    different paramstyle (``client.query`` in ``_fetch_rows_clickhouse``) and is
    unsupported here — a :class:`NotImplementedError` is raised so the caller
    can fall back to a full scan rather than a silently-wrong query.
    """
    if not key_tuples:
        return []
    if isinstance(config, PostgresDestinationConfig):
        return _fetch_rows_by_keys_postgres(
            config, key_cols, key_tuples, columns, batch_size
        )
    if isinstance(config, MySQLDestinationConfig):
        return _fetch_rows_by_keys_mysql(
            config, key_cols, key_tuples, columns, batch_size
        )
    if isinstance(config, SnowflakeDestinationConfig):
        return _fetch_rows_by_keys_snowflake(
            config, key_cols, key_tuples, columns, batch_size
        )
    if isinstance(config, ClickHouseDestinationConfig):
        raise NotImplementedError(
            "fetch_rows_by_keys does not support ClickHouse "
            "(different paramstyle); caller should fall back to a full scan."
        )
    raise TypeError(f"Cannot fetch rows by keys from {type(config).__name__}")


def _fetch_rows_by_keys_postgres(
    config: PostgresDestinationConfig,
    key_cols: list[str],
    key_tuples: list[tuple[Any, ...]],
    columns: list[str],
    batch_size: int,
) -> list[dict[str, Any]]:
    from psycopg2 import sql as _pgsql

    from drt.destinations.postgres import PostgresDestination, _qualified_ident

    col_list = _pgsql.SQL(", ").join(_pgsql.Identifier(c) for c in columns)
    if len(key_cols) == 1:
        key_expr: Any = _pgsql.Identifier(key_cols[0])
    else:
        key_expr = _pgsql.SQL("({})").format(
            _pgsql.SQL(", ").join(_pgsql.Identifier(c) for c in key_cols)
        )

    conn = PostgresDestination._connect(config)
    try:
        cur = conn.cursor()
        result: list[dict[str, Any]] = []
        for batch in _chunks(key_tuples, batch_size):
            placeholders = _pgsql.SQL(
                _in_placeholder(len(batch), len(key_cols))
            )
            stmt = _pgsql.SQL(
                "SELECT {cols} FROM {table} WHERE {key} IN ({ph})"
            ).format(
                cols=col_list,
                table=_qualified_ident(config.table),
                key=key_expr,
                ph=placeholders,
            )
            cur.execute(stmt, _flatten_key_params(batch))
            result.extend(dict(zip(columns, row)) for row in cur.fetchall())
        return result
    finally:
        conn.close()


def _fetch_rows_by_keys_mysql(
    config: MySQLDestinationConfig,
    key_cols: list[str],
    key_tuples: list[tuple[Any, ...]],
    columns: list[str],
    batch_size: int,
) -> list[dict[str, Any]]:
    from drt.destinations.mysql import MySQLDestination

    quote = MySQLDestination._quote_ident
    col_list = ", ".join(f"`{c}`" for c in columns)
    if len(key_cols) == 1:
        key_expr = f"`{key_cols[0]}`"
    else:
        key_expr = "(" + ", ".join(f"`{c}`" for c in key_cols) + ")"
    table_q = quote(config.table)

    conn = MySQLDestination._connect(config)
    try:
        cur = conn.cursor()
        result: list[dict[str, Any]] = []
        for batch in _chunks(key_tuples, batch_size):
            placeholders = _in_placeholder(len(batch), len(key_cols))
            stmt = (
                f"SELECT {col_list} FROM {table_q} "
                f"WHERE {key_expr} IN ({placeholders})"
            )
            cur.execute(stmt, _flatten_key_params(batch))
            for row in cur.fetchall():
                if isinstance(row, dict):
                    result.append({c: row[c] for c in columns})
                else:
                    result.append(dict(zip(columns, row)))
        return result
    finally:
        conn.close()


def _fetch_rows_by_keys_snowflake(
    config: SnowflakeDestinationConfig,
    key_cols: list[str],
    key_tuples: list[tuple[Any, ...]],
    columns: list[str],
    batch_size: int,
) -> list[dict[str, Any]]:
    from drt.destinations.snowflake import SnowflakeDestination

    table_fq = get_table_name(config)
    col_list = ", ".join(columns)
    if len(key_cols) == 1:
        key_expr = key_cols[0]
    else:
        key_expr = "(" + ", ".join(key_cols) + ")"

    conn = SnowflakeDestination()._connect(config)
    try:
        result: list[dict[str, Any]] = []
        with conn.cursor() as cur:
            for batch in _chunks(key_tuples, batch_size):
                placeholders = _in_placeholder(len(batch), len(key_cols))
                stmt = (
                    f"SELECT {col_list} FROM {table_fq} "
                    f"WHERE {key_expr} IN ({placeholders})"
                )
                cur.execute(stmt, _flatten_key_params(batch))
                result.extend(dict(zip(columns, row)) for row in cur.fetchall())
        return result
    finally:
        conn.close()
