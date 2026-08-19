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
from drt.connectors.registry import get_destination
from drt.destinations.base import QueryableDestination


def _queryable_destination(config: DestinationConfig) -> QueryableDestination | None:
    """Return the destination instance for ``config`` if it implements
    ``QueryableDestination`` (#469), else ``None``.

    Dispatches structurally on the destination class via
    ``QueryableDestination`` — an ``isinstance()`` capability check on a
    Protocol, not a hardcoded config-class tuple. A new SQL destination
    gains ``drt test``'s validation-query capability by implementing
    ``get_table_name`` / ``execute_test_query`` on itself; this file needs
    no edit for that. This does **not** by itself make the destination
    fully "queryable" in every sense the name might suggest — see
    ``QueryableDestination``'s docstring for what it does and doesn't cover
    (``compute_diff()``'s true-diff path and ``--store-failures`` need
    ``fetch_rows``/``fetch_rows_by_keys``/``fetch_failing_rows`` below,
    which are separate, still config-class-dispatched capabilities that
    both already degrade gracefully — not crash — for an unsupported type).
    Instantiation (``get_destination``) is a cheap, side-effect-free no-arg
    constructor for every current destination — no connection is opened
    here.
    """
    dest = get_destination(config)
    return dest if isinstance(dest, QueryableDestination) else None


def is_queryable(config: DestinationConfig) -> bool:
    """Return True if we can run validation queries against this destination."""
    return _queryable_destination(config) is not None


def get_table_name(config: DestinationConfig) -> str:
    """Extract the target table name from a DB destination config."""
    dest = _queryable_destination(config)
    if dest is None:
        raise TypeError(f"Cannot get table name from {type(config).__name__}")
    return dest.get_table_name(config)


def execute_test_query(config: DestinationConfig, query: str) -> int:
    """Execute a query against a DB destination and return a single int."""
    dest = _queryable_destination(config)
    if dest is None:
        raise TypeError(f"Cannot query {type(config).__name__}")
    return dest.execute_test_query(config, query)


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

    conn = SnowflakeDestination._connect(config)
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

    conn = SnowflakeDestination._connect(config)
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


# ---------------------------------------------------------------------------
# fetch_tracked_state — read-only tracked-mirror state read (#693)
# ---------------------------------------------------------------------------

_STATE_SELECT = "SELECT key_hash, key_json FROM {} WHERE sync_name = %s"


def fetch_tracked_state(
    config: DestinationConfig, sync_name: str
) -> dict[str, str]:
    """Read the tracked-mirror key state for *sync_name*, **read-only**.

    Returns ``{key_hash: key_json}`` — the same mapping
    ``BaseSqlDestination._finalize_mirror_tracked`` builds — so the pure
    ``_mirror_state.diff_keys`` can compute the would-be mirror DELETE set for
    ``drt run --dry-run --diff`` (#693) without touching the destination.

    Strictly a reader: it issues the dialect's existence probe and, only if the
    ``_drt_synced_keys`` table is already there, one parameterized ``SELECT``.
    It **never** emits DDL, DML, or a ``COMMIT`` — a dry run must not create the
    state table that a real run would lazily create.

    An absent state table yields ``{}``, matching the first-run baseline
    semantics of ``_finalize_mirror_tracked`` (no prior state → no deletes).

    Postgres / MySQL only — the tracked strategy itself is PG/MySQL-only today
    (other destinations fail fast at load time), so any other config type also
    yields ``{}`` rather than raising. Returning ``{}`` (not
    ``NotImplementedError``) keeps the caller a preview-only best-effort path:
    "no tracked state known" and "no deletions to preview" are the same answer.
    """
    if isinstance(config, PostgresDestinationConfig):
        return _fetch_tracked_state_postgres(config, sync_name)
    if isinstance(config, MySQLDestinationConfig):
        return _fetch_tracked_state_mysql(config, sync_name)
    return {}


def _fetch_tracked_state_postgres(
    config: PostgresDestinationConfig, sync_name: str
) -> dict[str, str]:
    """Postgres leg — mirrors ``PostgresDestination._state_table_ident`` /
    ``_state_table_exists`` (the state table lives in the target table's
    schema; ``to_regclass`` takes the unquoted qualified name)."""
    from psycopg2 import sql as _pgsql

    from drt.destinations._mirror_state import STATE_TABLE
    from drt.destinations.postgres import (
        PostgresDestination,
        _join_qualified,
        _qualified_ident,
        _split_qualified,
    )

    schema, _relation = _split_qualified(config.table)
    qualified = _join_qualified(schema, STATE_TABLE)

    conn = PostgresDestination._connect(config)
    try:
        cur = conn.cursor()
        cur.execute("SELECT to_regclass(%s)", (qualified,))
        row = cur.fetchone()
        if row is None or row[0] is None:
            return {}
        cur.execute(
            _pgsql.SQL(_STATE_SELECT).format(_qualified_ident(qualified)),
            (sync_name,),
        )
        return {r[0]: r[1] for r in cur.fetchall()}
    finally:
        conn.close()


def _fetch_tracked_state_mysql(
    config: MySQLDestinationConfig, sync_name: str
) -> dict[str, str]:
    """MySQL leg — mirrors ``MySQLDestination._state_table_ident`` /
    ``_state_table_exists`` (state table lives in the target's database; the
    probe binds the database name, or falls back to ``DATABASE()``)."""
    from drt.destinations._mirror_state import STATE_TABLE
    from drt.destinations.mysql import MySQLDestination

    if "." in config.table:
        database: str | None = config.table.rsplit(".", 1)[0]
        ident = MySQLDestination._quote_ident(f"{database}.{STATE_TABLE}")
    else:
        database = None
        ident = MySQLDestination._quote_ident(STATE_TABLE)

    conn = MySQLDestination._connect(config)
    try:
        cur = conn.cursor()
        if database is not None:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = %s",
                (database, STATE_TABLE),
            )
        else:
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = DATABASE() AND table_name = %s",
                (STATE_TABLE,),
            )
        probe = cur.fetchone()
        # Same shape tolerance as ``_fetch_rows_mysql``: a DictCursor yields a
        # mapping rather than a tuple.
        count = (
            next(iter(probe.values()), 0) if isinstance(probe, dict) else probe[0]
        )
        if not count:
            return {}
        cur.execute(_STATE_SELECT.format(ident), (sync_name,))
        state: dict[str, str] = {}
        for r in cur.fetchall():
            if isinstance(r, dict):
                state[r["key_hash"]] = r["key_json"]
            else:
                state[r[0]] = r[1]
        return state
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# fetch_all_keys — read-only destination key read for the destination-strategy
# mirror delete preview (#693)
# ---------------------------------------------------------------------------


def _key_tuples_from_rows(rows: Any, key_cols: list[str]) -> list[tuple[Any, ...]]:
    """Normalise fetched rows to key tuples in ``key_cols`` order.

    Tolerates mapping rows the way ``_fetch_rows_mysql`` does — a pymysql
    ``DictCursor`` yields dicts rather than tuples.
    """
    keys: list[tuple[Any, ...]] = []
    for row in rows:
        if isinstance(row, dict):
            keys.append(tuple(row[c] for c in key_cols))
        else:
            keys.append(tuple(row))
    return keys


def fetch_all_keys(
    config: DestinationConfig,
    key_cols: list[str],
    scope_cols: list[str] | None = None,
    scopes: list[tuple[Any, ...]] | None = None,
) -> list[tuple[Any, ...]]:
    """Read the destination's own key set, **read-only** (#693).

    Issues ``SELECT <key_cols> FROM <table> [WHERE <scope> IN (…)]`` and returns
    one tuple per row, in ``key_cols`` order. Only the key columns are selected —
    the preview reports keys, so pulling the full rows would cost bandwidth for
    columns nobody renders.

    This is the read the *destination*-strategy mirror preview needs and #470's
    :func:`fetch_rows_by_keys` structurally cannot provide: the destination
    strategy DELETEs ``dest_keys - source_keys``, and a keyed fetch only ever
    returns rows whose key IS in the source set.

    ``scope_cols`` / ``scopes`` (mirror.scope, #687) narrow the read **in SQL**,
    with the same clause shape each dialect's ``_build_mirror_delete`` emits, so
    the preview and the real DELETE consider the same rows: Postgres binds one
    auto-expanded tuple per ``%s``, MySQL an explicit flattened placeholder list.
    As there, the clause is added only when both are non-empty — a scoped mirror
    that observed no scope values deletes nothing, and the caller is expected to
    skip the preview entirely in that case.

    Postgres / MySQL only — the destinations whose mirror DELETE comes from the
    shared ``BaseSqlDestination._finalize_mirror`` path. ClickHouse and Snowflake
    override mirror finalisation with dialect-specific statements, so a
    :class:`NotImplementedError` is raised rather than returning ``[]``: an empty
    list is indistinguishable from "this mirror deletes nothing", and quietly
    claiming that about a mirror which *will* delete rows is the one wrong answer
    a preview must not give. Callers degrade to "not previewed".

    Never writes: one ``SELECT``, no DDL/DML, no ``COMMIT``.
    """
    if isinstance(config, PostgresDestinationConfig):
        return _fetch_all_keys_postgres(config, key_cols, scope_cols, scopes)
    if isinstance(config, MySQLDestinationConfig):
        return _fetch_all_keys_mysql(config, key_cols, scope_cols, scopes)
    if isinstance(config, (ClickHouseDestinationConfig, SnowflakeDestinationConfig)):
        raise NotImplementedError(
            f"fetch_all_keys does not support {type(config).__name__} "
            "(mirror finalisation is dialect-specific there); "
            "caller should skip the destination-strategy delete preview."
        )
    raise TypeError(f"Cannot fetch keys from {type(config).__name__}")


def _fetch_all_keys_postgres(
    config: PostgresDestinationConfig,
    key_cols: list[str],
    scope_cols: list[str] | None,
    scopes: list[tuple[Any, ...]] | None,
) -> list[tuple[Any, ...]]:
    """Postgres leg — mirrors ``PostgresDestination._build_mirror_delete``'s
    scope clause (``<col> IN %s`` with psycopg2 tuple auto-expansion)."""
    from psycopg2 import sql as _pgsql

    from drt.destinations.postgres import PostgresDestination, _qualified_ident

    col_list = _pgsql.SQL(", ").join(_pgsql.Identifier(c) for c in key_cols)
    where = _pgsql.SQL("")
    params: tuple[Any, ...] = ()
    if scope_cols and scopes:
        if len(scope_cols) == 1:
            scope_expr: Any = _pgsql.Identifier(scope_cols[0])
            params = (tuple(s[0] for s in scopes),)
        else:
            scope_expr = _pgsql.SQL("({})").format(
                _pgsql.SQL(", ").join(_pgsql.Identifier(c) for c in scope_cols)
            )
            params = (tuple(tuple(s) for s in scopes),)
        where = _pgsql.SQL(" WHERE {} IN %s").format(scope_expr)

    stmt = _pgsql.SQL("SELECT {cols} FROM {table}{where}").format(
        cols=col_list,
        table=_qualified_ident(config.table),
        where=where,
    )

    conn = PostgresDestination._connect(config)
    try:
        cur = conn.cursor()
        if params:
            cur.execute(stmt, params)
        else:
            cur.execute(stmt)
        return _key_tuples_from_rows(cur.fetchall(), key_cols)
    finally:
        conn.close()


def _fetch_all_keys_mysql(
    config: MySQLDestinationConfig,
    key_cols: list[str],
    scope_cols: list[str] | None,
    scopes: list[tuple[Any, ...]] | None,
) -> list[tuple[Any, ...]]:
    """MySQL leg — mirrors ``MySQLDestination._build_mirror_delete``'s scope
    clause (explicit ``%s`` list, flattened params; pymysql has no
    tuple auto-expansion)."""
    from drt.destinations.mysql import MySQLDestination

    col_list = ", ".join(f"`{c}`" for c in key_cols)
    table_q = MySQLDestination._quote_ident(config.table)
    where = ""
    params: list[Any] = []
    if scope_cols and scopes:
        if len(scope_cols) == 1:
            placeholders = ", ".join(["%s"] * len(scopes))
            where = f" WHERE `{scope_cols[0]}` IN ({placeholders})"
            params = [s[0] for s in scopes]
        else:
            col_tuple = "(" + ", ".join(f"`{c}`" for c in scope_cols) + ")"
            row = "(" + ", ".join(["%s"] * len(scope_cols)) + ")"
            placeholders = ", ".join([row] * len(scopes))
            where = f" WHERE {col_tuple} IN ({placeholders})"
            params = [v for s in scopes for v in s]

    stmt = f"SELECT {col_list} FROM {table_q}{where}"  # noqa: S608 — idents quoted

    conn = MySQLDestination._connect(config)
    try:
        cur = conn.cursor()
        if params:
            cur.execute(stmt, params)
        else:
            cur.execute(stmt)
        return _key_tuples_from_rows(cur.fetchall(), key_cols)
    finally:
        conn.close()
