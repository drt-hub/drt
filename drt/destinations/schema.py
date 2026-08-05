"""Destination schema introspection for schema-aware serialization (#317, Layer 3).

The JSON-serialization design space has three layers:

- **Layer 1** (#311 / #315): per-destination patches — auto ``json.dumps`` for
  dict/list values, fix the "can't adapt type 'dict'" crash.
- **Layer 2** (#316): the ``json_columns`` config — the user declares which
  columns hold JSON, validated as an allowlist.
- **Layer 3** (this module): introspect the destination's ``INFORMATION_SCHEMA``
  at sync start and route each value by the column's *actual* type. Zero config
  burden, and it resolves the ambiguity Layer 2 can't — a Python ``list`` bound
  for a Postgres column could mean a JSONB array **or** a native ``ARRAY``, and
  only the real column type disambiguates.

``describe_columns`` is **best-effort**: any failure — a missing driver, a
locked-down ``information_schema``, or a table that doesn't exist yet on a
first run — returns ``None`` so the caller silently keeps its prior behaviour.

Implemented for Postgres, MySQL, Snowflake, and Databricks. For Snowflake,
VARIANT / OBJECT / ARRAY map to the ``json`` category — semi-structured columns
load via ``PARSE_JSON``, so the destination wraps those bind sites accordingly.
Databricks introspection lands here too (STRUCT / MAP / ARRAY / VARIANT →
``json``); wiring it into the Databricks write path (the ``from_json`` /
``parse_json`` bind sites) is the remaining step of the #317 Databricks phase.
ClickHouse and BigQuery defer complex-type encoding to their client libraries
(no gap today); extending introspection to them is tracked as later phases of #317.

Table-driven (#723 part 2): the four dialects share one execution pipeline
(``_describe``) — connect, run the query, fetch, categorize each row. Only
what genuinely differs per dialect is data in ``_DESCRIBE_SPECS``: the SQL/
params (``build_query`` — real per-dialect logic, e.g. schema-qualification
handling, not just a template string), the ``connect`` call (Postgres/MySQL
use a staticmethod, Snowflake/Databricks an instance method — #723 tracks
normalizing that separately; unchanged here), whether the cursor is used as
a context manager (Snowflake/Databricks) or bare (Postgres/MySQL — a real
behavioral difference the existing tests' mock wiring locks in), and the
``data_type`` → category mapping.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, NamedTuple

if TYPE_CHECKING:
    from collections.abc import Callable

    from drt.config.models import (
        DatabricksDestinationConfig,
        MySQLDestinationConfig,
        PostgresDestinationConfig,
        SnowflakeDestinationConfig,
    )

# Category vocabulary shared with the serializer.
JSON = "json"  # JSON / JSONB column — encode dict *and* list as JSON
ARRAY = "array"  # native array column — pass the list through to the driver
SCALAR = "scalar"  # everything else — a complex value here is unusual


class _DescribeSpec(NamedTuple):
    """One dialect's introspection recipe — only the genuinely per-dialect
    pieces; ``_describe`` below runs all four through the same pipeline.
    """

    connect: Callable[[Any], Any]
    build_query: Callable[[Any], tuple[str, list[Any]]]
    categorize: Callable[[str | None], str]
    cursor_is_context_manager: bool


def describe_columns(config: Any) -> dict[str, str] | None:
    """Return ``{column_name: category}`` for the destination table, or ``None``.

    ``category`` is one of :data:`JSON`, :data:`ARRAY`, :data:`SCALAR`.
    ``None`` means "introspection unavailable" — the caller must fall back to
    its prior serialization behaviour. Never raises: a best-effort read of a
    metadata table must not break a sync.
    """
    try:
        config_type = getattr(config, "type", None)
        spec = _DESCRIBE_SPECS.get(config_type) if isinstance(config_type, str) else None
        if spec is None:
            return None
        return _describe(config, spec)
    except Exception:
        # Locked-down information_schema, missing driver, transient connection
        # failure — degrade gracefully rather than fail the sync.
        return None


def _describe(config: Any, spec: _DescribeSpec) -> dict[str, str] | None:
    sql, params = spec.build_query(config)
    conn = spec.connect(config)
    try:
        if spec.cursor_is_context_manager:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        else:
            cur = conn.cursor()
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return None
    out: dict[str, str] = {}
    for row in rows:
        # pymysql may be a plain or a Dict cursor; both preserve SELECT order.
        # Other drivers always yield tuples, so this is a no-op for them.
        values = list(row.values()) if isinstance(row, dict) else list(row)
        col, data_type = values[0], values[1]
        out[str(col)] = spec.categorize(data_type)
    return out


def _split_qualified(name: str) -> tuple[str | None, str]:
    """Split ``"schema.table"`` → ``(schema, table)``; ``"table"`` → ``(None, table)``."""
    if "." in name:
        schema, _, table = name.rpartition(".")
        return (schema or None), table
    return None, name


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------


def _connect_postgres(config: PostgresDestinationConfig) -> Any:
    from drt.destinations.postgres import PostgresDestination

    return PostgresDestination._connect(config)


def _query_postgres(config: PostgresDestinationConfig) -> tuple[str, list[Any]]:
    schema, table = _split_qualified(config.table)
    sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s"
    params: list[Any] = [table]
    if schema is not None:
        sql += " AND table_schema = %s"
        params.append(schema)
    else:
        # Unqualified: avoid matching catalogs that re-use the name.
        sql += " AND table_schema NOT IN ('pg_catalog', 'information_schema')"
    return sql, params


def _categorize_postgres(data_type: str | None) -> str:
    dt = (data_type or "").lower()
    if dt in ("json", "jsonb"):
        return JSON
    if dt == "array":  # information_schema reports 'ARRAY' for array columns
        return ARRAY
    return SCALAR


# ---------------------------------------------------------------------------
# MySQL
# ---------------------------------------------------------------------------


def _connect_mysql(config: MySQLDestinationConfig) -> Any:
    from drt.destinations.mysql import MySQLDestination

    return MySQLDestination._connect(config)


def _query_mysql(config: MySQLDestinationConfig) -> tuple[str, list[Any]]:
    # ``table`` may be ``db.table``; otherwise the connection's database is the schema.
    schema, table = _split_qualified(config.table)
    sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s"
    params: list[Any] = [table]
    if schema is not None:
        sql += " AND table_schema = %s"
        params.append(schema)
    else:
        # Unqualified: scope to the connection's current database. Without this,
        # information_schema.columns spans every database the connection can see,
        # so a same-named table in another schema collides and can mislabel a
        # column's category (#317 review).
        sql += " AND table_schema = DATABASE()"
    return sql, params


def _categorize_mysql(data_type: str | None) -> str:
    dt = (data_type or "").lower()
    if dt == "json":
        return JSON
    return SCALAR  # MySQL has no native array type


# ---------------------------------------------------------------------------
# Snowflake
# ---------------------------------------------------------------------------


def _connect_snowflake(config: SnowflakeDestinationConfig) -> Any:
    from drt.destinations.snowflake import SnowflakeDestination

    return SnowflakeDestination._connect(config)


def _query_snowflake(config: SnowflakeDestinationConfig) -> tuple[str, list[Any]]:
    # Snowflake's INFORMATION_SCHEMA lives under each database. Unquoted
    # identifiers are stored upper-cased; quoted ones keep their case — match
    # case-insensitively so both styles resolve.
    sql = (
        f"SELECT column_name, data_type FROM {config.database}.information_schema.columns "
        "WHERE UPPER(table_schema) = UPPER(%s) AND UPPER(table_name) = UPPER(%s)"
    )
    params: list[Any] = [config.schema_, config.table]
    return sql, params


def _categorize_snowflake(data_type: str | None) -> str:
    # Snowflake's semi-structured types all load via PARSE_JSON, so VARIANT /
    # OBJECT / ARRAY map to the "json" category. (Unlike Postgres, Snowflake has
    # no driver-side typed-array adapter to pass a list through to.)
    dt = (data_type or "").upper()
    if dt in ("VARIANT", "OBJECT", "ARRAY"):
        return JSON
    return SCALAR


# ---------------------------------------------------------------------------
# Databricks
# ---------------------------------------------------------------------------


def _connect_databricks(config: DatabricksDestinationConfig) -> Any:
    from drt.destinations.databricks import DatabricksDestination

    return DatabricksDestination._connect(config)


def _query_databricks(config: DatabricksDestinationConfig) -> tuple[str, list[Any]]:
    # Unity Catalog exposes ``information_schema`` under each catalog. Identifiers
    # are case-insensitive (stored lower-cased), so match case-insensitively for
    # both quoted and unquoted table definitions. ``data_type`` reports the
    # top-level type name (e.g. ``ARRAY`` / ``STRUCT``); ``full_data_type`` would
    # carry the parameterised form, which we don't need for categorisation.
    # Native ``?`` paramstyle (#707): the Databricks connector binds server-side
    # with ``?`` markers, unlike the Postgres / MySQL / Snowflake queries in this
    # file which are pyformat (``%s``) — the two paramstyles coexist here on
    # purpose, one per driver's native style.
    sql = (
        f"SELECT column_name, data_type FROM {config.catalog}.information_schema.columns "
        "WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?)"
    )
    params: list[Any] = [config.schema_, config.table]
    return sql, params


def _categorize_databricks(data_type: str | None) -> str:
    # Databricks' complex types load via ``from_json`` / ``parse_json`` — the SQL
    # connector has no typed adapter to pass a Python list/dict straight through,
    # so STRUCT / MAP / ARRAY / VARIANT map to the "json" category (the write path
    # wraps those bind sites). Mirrors the Snowflake treatment above.
    dt = (data_type or "").upper()
    if dt in ("STRUCT", "MAP", "ARRAY", "VARIANT"):
        return JSON
    return SCALAR


# Keyed by the config's ``type`` discriminator (same convention as
# drt/connectors/registry.py) — equivalent to the original isinstance chain
# since every DestinationConfig's Pydantic-validated `type` literal always
# matches its class.
_DESCRIBE_SPECS: dict[str, _DescribeSpec] = {
    "postgres": _DescribeSpec(_connect_postgres, _query_postgres, _categorize_postgres, False),
    "mysql": _DescribeSpec(_connect_mysql, _query_mysql, _categorize_mysql, False),
    "snowflake": _DescribeSpec(
        _connect_snowflake, _query_snowflake, _categorize_snowflake, True
    ),
    "databricks": _DescribeSpec(
        _connect_databricks, _query_databricks, _categorize_databricks, True
    ),
}


def describe_databricks_ddls(
    config: DatabricksDestinationConfig,
) -> dict[str, str] | None:
    """Map each STRUCT / ARRAY / MAP column to its full type DDL (for ``from_json``).

    :func:`describe_columns` returns the *category* (``json`` / ``scalar``) — enough
    to decide *whether* a value needs JSON encoding. Databricks' ``from_json``
    additionally needs the target type's DDL (e.g. ``ARRAY<STRING>``,
    ``STRUCT<a: INT, b: STRING>``) to reconstruct the value, which
    ``information_schema`` exposes as ``full_data_type``.

    Only STRUCT / ARRAY / MAP columns are returned. VARIANT columns load via
    ``parse_json`` (no DDL needed), so a ``json``-category column *absent* from
    this map is a VARIANT — the write path should ``parse_json`` it. Best-effort
    like :func:`describe_columns`: returns ``None`` on any failure or no matches.
    """
    from drt.destinations.databricks import DatabricksDestination

    # Native ``?`` paramstyle (#707) — see the note in ``_query_databricks``.
    sql = (
        f"SELECT column_name, full_data_type FROM {config.catalog}.information_schema.columns "
        "WHERE lower(table_schema) = lower(?) AND lower(table_name) = lower(?)"
    )
    params: list[Any] = [config.schema_, config.table]
    try:
        conn = DatabricksDestination._connect(config)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        finally:
            conn.close()
    except Exception:
        # Best-effort: a locked-down information_schema or transient failure
        # must not break a sync — the write path falls back to its prior behaviour.
        return None
    out = {
        str(col): str(ddl)
        for col, ddl in rows
        if (ddl or "").strip().upper().startswith(("STRUCT", "ARRAY", "MAP"))
    }
    return out or None
