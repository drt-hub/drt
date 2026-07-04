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
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
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


def describe_columns(config: Any) -> dict[str, str] | None:
    """Return ``{column_name: category}`` for the destination table, or ``None``.

    ``category`` is one of :data:`JSON`, :data:`ARRAY`, :data:`SCALAR`.
    ``None`` means "introspection unavailable" — the caller must fall back to
    its prior serialization behaviour. Never raises: a best-effort read of a
    metadata table must not break a sync.
    """
    from drt.config.models import (
        DatabricksDestinationConfig,
        MySQLDestinationConfig,
        PostgresDestinationConfig,
        SnowflakeDestinationConfig,
    )

    try:
        if isinstance(config, PostgresDestinationConfig):
            return _describe_postgres(config)
        if isinstance(config, MySQLDestinationConfig):
            return _describe_mysql(config)
        if isinstance(config, SnowflakeDestinationConfig):
            return _describe_snowflake(config)
        if isinstance(config, DatabricksDestinationConfig):
            return _describe_databricks(config)
    except Exception:
        # Locked-down information_schema, missing driver, transient connection
        # failure — degrade gracefully rather than fail the sync.
        return None
    return None


def _split_qualified(name: str) -> tuple[str | None, str]:
    """Split ``"schema.table"`` → ``(schema, table)``; ``"table"`` → ``(None, table)``."""
    if "." in name:
        schema, _, table = name.rpartition(".")
        return (schema or None), table
    return None, name


def _describe_postgres(config: PostgresDestinationConfig) -> dict[str, str] | None:
    from drt.destinations.postgres import PostgresDestination

    schema, table = _split_qualified(config.table)
    sql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = %s"
    params: list[Any] = [table]
    if schema is not None:
        sql += " AND table_schema = %s"
        params.append(schema)
    else:
        # Unqualified: avoid matching catalogs that re-use the name.
        sql += " AND table_schema NOT IN ('pg_catalog', 'information_schema')"

    conn = PostgresDestination._connect(config)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return None
    return {str(col): _categorize_postgres(data_type) for col, data_type in rows}


def _categorize_postgres(data_type: str | None) -> str:
    dt = (data_type or "").lower()
    if dt in ("json", "jsonb"):
        return JSON
    if dt == "array":  # information_schema reports 'ARRAY' for array columns
        return ARRAY
    return SCALAR


def _describe_mysql(config: MySQLDestinationConfig) -> dict[str, str] | None:
    from drt.destinations.mysql import MySQLDestination

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

    conn = MySQLDestination._connect(config)
    try:
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
        values = list(row.values()) if isinstance(row, dict) else row
        col, data_type = values[0], values[1]
        out[str(col)] = _categorize_mysql(data_type)
    return out


def _categorize_mysql(data_type: str | None) -> str:
    dt = (data_type or "").lower()
    if dt == "json":
        return JSON
    return SCALAR  # MySQL has no native array type


def _describe_snowflake(config: SnowflakeDestinationConfig) -> dict[str, str] | None:
    from drt.destinations.snowflake import SnowflakeDestination

    # Snowflake's INFORMATION_SCHEMA lives under each database. Unquoted
    # identifiers are stored upper-cased; quoted ones keep their case — match
    # case-insensitively so both styles resolve.
    sql = (
        f"SELECT column_name, data_type FROM {config.database}.information_schema.columns "
        "WHERE UPPER(table_schema) = UPPER(%s) AND UPPER(table_name) = UPPER(%s)"
    )
    params: list[Any] = [config.schema_, config.table]

    # _connect is an instance method on the Snowflake destination.
    conn = SnowflakeDestination()._connect(config)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return None
    return {str(col): _categorize_snowflake(data_type) for col, data_type in rows}


def _categorize_snowflake(data_type: str | None) -> str:
    # Snowflake's semi-structured types all load via PARSE_JSON, so VARIANT /
    # OBJECT / ARRAY map to the "json" category. (Unlike Postgres, Snowflake has
    # no driver-side typed-array adapter to pass a list through to.)
    dt = (data_type or "").upper()
    if dt in ("VARIANT", "OBJECT", "ARRAY"):
        return JSON
    return SCALAR


def _describe_databricks(config: DatabricksDestinationConfig) -> dict[str, str] | None:
    from drt.destinations.databricks import DatabricksDestination

    # Unity Catalog exposes ``information_schema`` under each catalog. Identifiers
    # are case-insensitive (stored lower-cased), so match case-insensitively for
    # both quoted and unquoted table definitions. ``data_type`` reports the
    # top-level type name (e.g. ``ARRAY`` / ``STRUCT``); ``full_data_type`` would
    # carry the parameterised form, which we don't need for categorisation.
    sql = (
        f"SELECT column_name, data_type FROM {config.catalog}.information_schema.columns "
        "WHERE lower(table_schema) = lower(%s) AND lower(table_name) = lower(%s)"
    )
    params: list[Any] = [config.schema_, config.table]

    # ``_connect`` is an instance method on the Databricks destination.
    conn = DatabricksDestination()._connect(config)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
    finally:
        conn.close()
    if not rows:
        return None
    return {str(col): _categorize_databricks(data_type) for col, data_type in rows}


def _categorize_databricks(data_type: str | None) -> str:
    # Databricks' complex types load via ``from_json`` / ``parse_json`` — the SQL
    # connector has no typed adapter to pass a Python list/dict straight through,
    # so STRUCT / MAP / ARRAY / VARIANT map to the "json" category (the write path
    # wraps those bind sites). Mirrors the Snowflake treatment above.
    dt = (data_type or "").upper()
    if dt in ("STRUCT", "MAP", "ARRAY", "VARIANT"):
        return JSON
    return SCALAR


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

    sql = (
        f"SELECT column_name, full_data_type FROM {config.catalog}.information_schema.columns "
        "WHERE lower(table_schema) = lower(%s) AND lower(table_name) = lower(%s)"
    )
    params: list[Any] = [config.schema_, config.table]
    try:
        conn = DatabricksDestination()._connect(config)
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
