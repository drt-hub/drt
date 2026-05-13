"""PostgreSQL destination — upsert or replace rows into a PostgreSQL table.

Uses INSERT ... ON CONFLICT (upsert_key) DO UPDATE SET ... for idempotent writes.
Supports ``sync.mode: replace`` (TRUNCATE → INSERT within a single transaction).
Requires: pip install drt-core[postgres]

Example sync YAML:

    destination:
      type: postgres
      host_env: TARGET_PG_HOST
      dbname_env: TARGET_PG_DBNAME
      user_env: TARGET_PG_USER
      password_env: TARGET_PG_PASSWORD
      table: public.analytics_scores
      upsert_key: [id]
"""

from __future__ import annotations

import json
from typing import Any

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, PostgresDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError

try:
    from psycopg2.extras import Json as _Psycopg2Json
except ImportError:
    _Psycopg2Json = None  # type: ignore[assignment,misc]


def _serialize_value(
    value: Any,
    column: str | None = None,
    json_columns: list[str] | None = None,
) -> Any:
    """Wrap dict values with psycopg2.extras.Json for JSONB columns.

    psycopg2 has no default adapter for ``dict``, so any dict value
    bound for a JSONB column (e.g. from a BigQuery JSON source) causes
    ``ProgrammingError: can't adapt type 'dict'``. Wrapping with
    ``Json`` produces the correct wire format for PostgreSQL JSONB.

    When *json_columns* is specified, only columns in that list are wrapped
    with ``Json()`` — other dict columns raise an early :class:`ValueError`
    pointing at the missing column, rather than failing deep inside the driver
    with a confusing ``can't adapt type 'dict'`` error.
    When *json_columns* is ``None`` (backward compat), all dicts are wrapped.

    Other types (str, int, float, list, None) pass through unchanged —
    psycopg2's built-in adapters handle those correctly.

    Raises:
        ValueError: If *json_columns* is set and an unlisted column receives
            a dict or list value.
    """
    if isinstance(value, dict):
        if json_columns is not None:
            if column and column in json_columns:
                if _Psycopg2Json is not None:
                    return _Psycopg2Json(value)
                return json.dumps(value, ensure_ascii=False)
            # Unlisted dict column with explicit json_columns → fail early
            raise ValueError(
                f"Column '{column}' contains a dict value but "
                f"is not listed in json_columns={json_columns}. "
                f"Add '{column}' to json_columns or remove the value."
            )
        if _Psycopg2Json is not None:
            return _Psycopg2Json(value)
        return json.dumps(value, ensure_ascii=False)  # backward compat fallback
    if (
        isinstance(value, list)
        and json_columns is not None
        and column
        and column not in json_columns
    ):
        # Unlisted list column with explicit json_columns → fail early
        raise ValueError(
            f"Column '{column}' contains a list value but "
            f"is not listed in json_columns={json_columns}. "
            f"Add '{column}' to json_columns or remove the value."
        )
    return value


def _split_qualified(table: str) -> tuple[str | None, str]:
    """Split an optional ``schema.table`` name into schema and relation parts."""
    if "." not in table:
        return None, table
    schema, relation = table.split(".", 1)
    return schema or None, relation


def _join_qualified(schema: str | None, relation: str) -> str:
    if schema is None:
        return relation
    return f"{schema}.{relation}"


def _qualified_ident(table: str) -> Any:
    """Return a psycopg2 Identifier that quotes each qualified-name part."""
    from psycopg2 import sql as _pgsql

    schema, relation = _split_qualified(table)
    if schema is None:
        return _pgsql.Identifier(relation)
    return _pgsql.Identifier(schema, relation)


def _relation_name(table: str) -> str:
    return _split_qualified(table)[1]


def _with_relation_suffix(table: str, suffix: str) -> str:
    schema, relation = _split_qualified(table)
    return _join_qualified(schema, f"{relation}{suffix}")


class PostgresDestination:
    """Upsert or replace records into a PostgreSQL table."""

    def __init__(self) -> None:
        self._replace_truncated: bool = False
        self._swap_shadow_created: bool = False
        self._swap_table: str | None = None

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, PostgresDestinationConfig)
        if not records:
            return SyncResult()

        conn = self._connect(config)
        result = SyncResult()

        try:
            cur = conn.cursor()
            columns = list(records[0].keys())

            if sync_options.mode == "replace":
                if sync_options.replace_strategy == "swap":
                    result = self._load_replace_swap(
                        conn,
                        cur,
                        records,
                        columns,
                        config.table,
                        sync_options,
                        config,
                    )
                else:
                    result = self._load_replace(
                        conn,
                        cur,
                        records,
                        columns,
                        config.table,
                        sync_options,
                        config,
                    )
            else:
                result = self._load_upsert(
                    conn,
                    cur,
                    records,
                    columns,
                    config,
                    sync_options,
                )
        finally:
            conn.close()

        return result

    def get_row_count(self, config: DestinationConfig) -> int:
        """Get the current row count from the destination table.

        Args:
            config: Destination configuration (must be PostgresDestinationConfig).

        Returns:
            Row count as integer.

        Raises:
            Exception: If connection or query fails.
        """
        from psycopg2 import sql

        assert isinstance(config, PostgresDestinationConfig)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            query = sql.SQL("SELECT COUNT(*) FROM {}").format(
                _qualified_ident(config.table)
            )
            cur.execute(query)
            row = cur.fetchone()
            return row[0] if row else 0
        finally:
            conn.close()

    def test_connection(self, config: DestinationConfig) -> None:
        """Test connectivity by establishing a connection and running SELECT 1."""
        assert isinstance(config, PostgresDestinationConfig)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
        finally:
            conn.close()

    def _load_replace(
        self,
        conn: Any,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        table: str,
        sync_options: SyncOptions,
        config: PostgresDestinationConfig,
    ) -> SyncResult:
        """TRUNCATE (once) → INSERT within a transaction."""
        from psycopg2 import sql as _pgsql
        result = SyncResult()

        if not self._replace_truncated:
            cur.execute(_pgsql.SQL("TRUNCATE TABLE {}").format(_qualified_ident(table)))
            self._replace_truncated = True

        query = self._build_insert_sql(table, columns)

        for i, record in enumerate(records):
            try:
                values = [_serialize_value(record.get(c), c, config.json_columns) for c in columns]
                cur.execute(query, values)
                result.success += 1
            except Exception as e:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=i,
                        record_preview=json.dumps(record, default=str)[:200],
                        http_status=None,
                        error_message=str(e),
                    )
                )
                if sync_options.on_error == "fail":
                    conn.rollback()
                    return result
                conn.rollback()
                cur = conn.cursor()
                if not self._replace_truncated:
                    cur.execute(
                        _pgsql.SQL("TRUNCATE TABLE {}").format(
                            _qualified_ident(table)
                        )
                    )
                    self._replace_truncated = True
                continue

        conn.commit()
        return result

    def _load_replace_swap(
        self,
        conn: Any,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        table: str,
        sync_options: SyncOptions,
        config: PostgresDestinationConfig,
    ) -> SyncResult:
        """Build a shadow table per sync; atomic rename happens in finalize_sync."""
        from psycopg2 import sql as _pgsql
        result = SyncResult()
        shadow = _with_relation_suffix(table, "__drt_swap")

        if not self._swap_shadow_created:
            cur.execute(
                _pgsql.SQL("DROP TABLE IF EXISTS {}").format(_qualified_ident(shadow))
            )
            cur.execute(
                _pgsql.SQL("CREATE TABLE {} (LIKE {} INCLUDING ALL)").format(
                    _qualified_ident(shadow),
                    _qualified_ident(table),
                )
            )
            self._swap_shadow_created = True
            self._swap_table = table

        sql = self._build_insert_sql(shadow, columns)

        for i, record in enumerate(records):
            try:
                values = [_serialize_value(record.get(c), c, config.json_columns) for c in columns]
                cur.execute(sql, values)
                result.success += 1
            except Exception as e:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=i,
                        record_preview=json.dumps(record, default=str)[:200],
                        http_status=None,
                        error_message=str(e),
                    )
                )
                if sync_options.on_error == "fail":
                    conn.rollback()
                    # Cleanup shadow on hard fail
                    cur = conn.cursor()
                    cur.execute(
                        _pgsql.SQL("DROP TABLE IF EXISTS {}").format(
                            _qualified_ident(shadow)
                        )
                    )
                    conn.commit()
                    self._swap_shadow_created = False
                    self._swap_table = None
                    return result
                # on_error=skip: keep going

        conn.commit()
        return result

    def finalize_sync(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """Atomic rename: original->old, shadow->original, drop old."""
        from psycopg2 import sql as _pgsql

        if not self._swap_shadow_created or self._swap_table is None:
            return None

        assert isinstance(config, PostgresDestinationConfig)
        table = self._swap_table
        shadow = _with_relation_suffix(table, "__drt_swap")
        old = _with_relation_suffix(table, "__drt_old")

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            # Single transaction: original->old, shadow->original.
            # ALTER TABLE ... RENAME TO takes a bare relation name on the RHS;
            # the schema is preserved automatically.
            cur.execute(
                _pgsql.SQL("ALTER TABLE {} RENAME TO {}").format(
                    _qualified_ident(table),
                    _pgsql.Identifier(_relation_name(old)),
                )
            )
            cur.execute(
                _pgsql.SQL("ALTER TABLE {} RENAME TO {}").format(
                    _qualified_ident(shadow),
                    _pgsql.Identifier(_relation_name(table)),
                )
            )
            conn.commit()
            # DROP old in separate tx (failure here doesn't break the swap).
            cur.execute(_pgsql.SQL("DROP TABLE {}").format(_qualified_ident(old)))
            conn.commit()
        finally:
            conn.close()
            self._swap_shadow_created = False
            self._swap_table = None

        return SyncResult()

    @staticmethod
    def _load_upsert(
        conn: Any,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        config: PostgresDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        result = SyncResult()
        update_cols = [c for c in columns if c not in config.upsert_key]
        query = PostgresDestination._build_upsert_sql(
            config.table,
            columns,
            config.upsert_key,
            update_cols,
        )

        for i, record in enumerate(records):
            try:
                values = [_serialize_value(record.get(c), c, config.json_columns) for c in columns]
                cur.execute(query, values)
                result.success += 1
            except Exception as e:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=i,
                        record_preview=json.dumps(record, default=str)[:200],
                        http_status=None,
                        error_message=str(e),
                    )
                )
                if sync_options.on_error == "fail":
                    conn.rollback()
                    return result
                conn.rollback()
                cur = conn.cursor()
                continue

        conn.commit()
        return result

    @staticmethod
    def _build_insert_sql(table: str, columns: list[str]) -> Any:
        from psycopg2 import sql as _pgsql
        return _pgsql.SQL("INSERT INTO {} ({}) VALUES ({})").format(
            _qualified_ident(table),
            _pgsql.SQL(", ").join(_pgsql.Identifier(c) for c in columns),
            _pgsql.SQL(", ").join(_pgsql.Placeholder() for _ in columns),
        )

    @staticmethod
    def _build_upsert_sql(
        table: str,
        columns: list[str],
        upsert_key: list[str],
        update_cols: list[str],
    ) -> Any:
        from psycopg2 import sql as _pgsql
        if update_cols:
            set_clause = _pgsql.SQL(", ").join(
                _pgsql.SQL("{} = EXCLUDED.{}").format(
                    _pgsql.Identifier(c), _pgsql.Identifier(c)
                )
                for c in update_cols
            )
            conflict_action = _pgsql.SQL("DO UPDATE SET ") + set_clause
        else:
            conflict_action = _pgsql.SQL("DO NOTHING")

        return _pgsql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) {}"
        ).format(
            _qualified_ident(table),
            _pgsql.SQL(", ").join(_pgsql.Identifier(c) for c in columns),
            _pgsql.SQL(", ").join(_pgsql.Placeholder() for _ in columns),
            _pgsql.SQL(", ").join(_pgsql.Identifier(c) for c in upsert_key),
            conflict_action,
        )

    @staticmethod
    def _connect(config: PostgresDestinationConfig) -> Any:
        try:
            import psycopg2
        except ImportError as e:
            raise ImportError(
                "PostgreSQL destination requires: pip install drt-core[postgres]"
            ) from e

        # Connection string takes precedence
        conn_str = (
            resolve_env(None, config.connection_string_env)
            if config.connection_string_env
            else None
        )
        if conn_str:
            return psycopg2.connect(conn_str)

        # Fall back to individual parameters
        host = resolve_env(config.host, config.host_env)
        dbname = resolve_env(config.dbname, config.dbname_env)
        user = resolve_env(config.user, config.user_env)
        password = resolve_env(config.password, config.password_env)

        if not host:
            raise ValueError("PostgreSQL destination: host could not be resolved.")
        if not dbname:
            raise ValueError("PostgreSQL destination: dbname could not be resolved.")

        kwargs: dict[str, Any] = {
            "host": host,
            "port": config.port,
            "dbname": dbname,
            "user": user,
            "password": password,
        }

        if config.ssl and config.ssl.enabled:
            kwargs["sslmode"] = "require"
            ca = resolve_env(None, config.ssl.ca_env)
            if ca:
                kwargs["sslrootcert"] = ca
            cert = resolve_env(None, config.ssl.cert_env)
            if cert:
                kwargs["sslcert"] = cert
            key = resolve_env(None, config.ssl.key_env)
            if key:
                kwargs["sslkey"] = key

        return psycopg2.connect(**kwargs)
