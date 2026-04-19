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


def _serialize_value(value: Any, column: str | None = None, json_columns: list[str] | None = None) -> Any:
    """Wrap dict values with psycopg2.extras.Json for JSONB columns.

    psycopg2 has no default adapter for ``dict``, so any dict value
    bound for a JSONB column (e.g. from a BigQuery JSON source) causes
    ``ProgrammingError: can't adapt type 'dict'``. Wrapping with
    ``Json`` produces the correct wire format for PostgreSQL JSONB.

    When *json_columns* is specified, only columns in that list are wrapped
    with ``Json()`` — other dict columns pass through as plain dicts.
    When *json_columns* is ``None`` (backward compat), all dicts are wrapped.

    Other types (str, int, float, list, None) pass through unchanged —
    psycopg2's built-in adapters handle those correctly.
    """
    if isinstance(value, dict):
        if json_columns is not None:
            if column and column in json_columns:
                from psycopg2.extras import Json  # lazy: psycopg2 is optional
                return Json(value)
            return value  # not in json_columns → pass through as dict
        from psycopg2.extras import Json  # lazy: psycopg2 is optional
        return Json(value)  # backward compat: no config → always wrap
    return value


class PostgresDestination:
    """Upsert or replace records into a PostgreSQL table."""

    def __init__(self) -> None:
        self._replace_truncated: bool = False

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
                sql.Identifier(config.table)
            )
            cur.execute(query)
            row = cur.fetchone()
            return row[0] if row else 0
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
        result = SyncResult()

        if not self._replace_truncated:
            cur.execute(f"TRUNCATE TABLE {table}")
            self._replace_truncated = True

        sql = self._build_insert_sql(table, columns)

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
                    return result
                conn.rollback()
                cur = conn.cursor()
                if not self._replace_truncated:
                    cur.execute(f"TRUNCATE TABLE {table}")
                    self._replace_truncated = True
                continue

        conn.commit()
        return result

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
        sql = PostgresDestination._build_upsert_sql(
            config.table,
            columns,
            config.upsert_key,
            update_cols,
        )

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
                    return result
                conn.rollback()
                cur = conn.cursor()
                continue

        conn.commit()
        return result

    @staticmethod
    def _build_insert_sql(table: str, columns: list[str]) -> str:
        """Build plain INSERT SQL (no conflict handling)."""
        cols_str = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        return f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders})"

    @staticmethod
    def _build_upsert_sql(
        table: str,
        columns: list[str],
        upsert_key: list[str],
        update_cols: list[str],
    ) -> str:
        """Build INSERT ... ON CONFLICT DO UPDATE SQL."""
        cols_str = ", ".join(f'"{c}"' for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        conflict_str = ", ".join(f'"{c}"' for c in upsert_key)

        if update_cols:
            set_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
            return (
                f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) "
                f"ON CONFLICT ({conflict_str}) DO UPDATE SET {set_clause}"
            )
        # All columns are part of the key — just ignore duplicates
        return (
            f"INSERT INTO {table} ({cols_str}) VALUES ({placeholders}) "
            f"ON CONFLICT ({conflict_str}) DO NOTHING"
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
