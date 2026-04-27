"""MySQL destination — upsert or replace rows into a MySQL table.

Uses INSERT ... ON DUPLICATE KEY UPDATE for idempotent writes.
Supports ``sync.mode: replace`` (TRUNCATE → INSERT within a single transaction).
Requires: pip install drt-core[mysql]

Example sync YAML:

    destination:
      type: mysql
      host_env: TARGET_MYSQL_HOST
      dbname_env: TARGET_MYSQL_DBNAME
      user_env: TARGET_MYSQL_USER
      password_env: TARGET_MYSQL_PASSWORD
      table: interviewer_learning_profiles
      upsert_key: [user_id, company_id]
"""

from __future__ import annotations

import json
from typing import Any

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, MySQLDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError


def _serialize_value(value: Any, column: str | None = None, json_columns: list[str] | None = None) -> Any:
    """Serialize dict/list values to JSON strings for pymysql.

    If json_columns is specified, only columns in that list are JSON-serialized.
    This allows non-JSON columns to receive native Python types (e.g. list →
    ARRAY) when the driver supports it.

    When json_columns is None (backward compat), all dict/list values are
    serialized — matching the pre-#316 heuristic behavior.

    Raises:
        ValueError: If *json_columns* is set and an unlisted column receives
            a dict or list value.
    """
    if not isinstance(value, (dict, list)):  # noqa: UP038
        return value
    # Explicit config: only serialize listed columns
    if json_columns is not None:
        if column and column in json_columns:
            return json.dumps(value, ensure_ascii=False)
        # Unlisted dict/list column with explicit json_columns → fail early
        raise ValueError(
            f"Column '{column}' contains a {type(value).__name__} value but "
            f"is not listed in json_columns={json_columns}. "
            f"Add '{column}' to json_columns or remove the value."
        )
    # Backward compat: no config → serialize all complex types
    return json.dumps(value, ensure_ascii=False)


class MySQLDestination:
    """Upsert or replace records into a MySQL table."""

    def __init__(self) -> None:
        self._replace_truncated: bool = False

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, MySQLDestinationConfig)
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
            config: Destination configuration (must be MySQLDestinationConfig).

        Returns:
            Row count as integer.

        Raises:
            Exception: If connection or query fails.
        """
        assert isinstance(config, MySQLDestinationConfig)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            # Escape table name with backticks for safety
            escaped_table = (
                "`.`".join(config.table.split("."))
                if "." in config.table
                else config.table
            )
            cur.execute(f"SELECT COUNT(*) FROM `{escaped_table}`")
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
        config: MySQLDestinationConfig,
    ) -> SyncResult:
        """TRUNCATE (once) → INSERT within a transaction."""
        result = SyncResult()

        if not self._replace_truncated:
            cur.execute(f"TRUNCATE TABLE `{table}`")
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
                continue

        conn.commit()
        return result

    @staticmethod
    def _load_upsert(
        conn: Any,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        config: MySQLDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        result = SyncResult()
        update_cols = [c for c in columns if c not in config.upsert_key]
        sql = MySQLDestination._build_upsert_sql(config.table, columns, update_cols)

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
        cols_str = ", ".join(f"`{c}`" for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        return f"INSERT INTO `{table}` ({cols_str}) VALUES ({placeholders})"

    @staticmethod
    def _build_upsert_sql(
        table: str,
        columns: list[str],
        update_cols: list[str],
    ) -> str:
        """Build INSERT ... ON DUPLICATE KEY UPDATE SQL."""
        cols_str = ", ".join(f"`{c}`" for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))

        if update_cols:
            set_clause = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)
            return (
                f"INSERT INTO `{table}` ({cols_str}) VALUES ({placeholders}) "
                f"ON DUPLICATE KEY UPDATE {set_clause}"
            )
        # All columns are part of the key — just ignore duplicates
        return f"INSERT IGNORE INTO `{table}` ({cols_str}) VALUES ({placeholders})"

    @staticmethod
    def _connect(config: MySQLDestinationConfig) -> Any:
        try:
            import pymysql
        except ImportError as e:
            raise ImportError("MySQL destination requires: pip install drt-core[mysql]") from e

        # Connection string takes precedence
        conn_str = (
            resolve_env(None, config.connection_string_env)
            if config.connection_string_env
            else None
        )
        if conn_str:
            from urllib.parse import urlparse

            parsed = urlparse(conn_str)
            kwargs: dict[str, Any] = {
                "host": parsed.hostname,
                "port": parsed.port or config.port,
                "database": parsed.path.lstrip("/"),
                "charset": "utf8mb4",
                "autocommit": False,
            }
            if parsed.username:
                kwargs["user"] = parsed.username
            if parsed.password:
                kwargs["password"] = parsed.password
            return pymysql.connect(**kwargs)

        # Fall back to individual parameters
        host = resolve_env(config.host, config.host_env)
        dbname = resolve_env(config.dbname, config.dbname_env)
        user = resolve_env(config.user, config.user_env)
        password = resolve_env(config.password, config.password_env)

        if not host:
            raise ValueError("MySQL destination: host could not be resolved.")
        if not dbname:
            raise ValueError("MySQL destination: dbname could not be resolved.")

        kwargs_individual: dict[str, Any] = {
            "host": host,
            "port": config.port,
            "database": dbname,
            "charset": "utf8mb4",
            "autocommit": False,
        }
        if user:
            kwargs_individual["user"] = user
        if password:
            kwargs_individual["password"] = password

        if config.ssl and config.ssl.enabled:
            ssl_dict: dict[str, Any] = {}
            ca = resolve_env(None, config.ssl.ca_env)
            if ca:
                ssl_dict["ca"] = ca
            cert = resolve_env(None, config.config.cert_env)
            if cert:
                ssl_dict["cert"] = cert
            key = resolve_env(None, config.ssl.key_env)
            if key:
                ssl_dict["key"] = key
            kwargs_individual["ssl"] = ssl_dict

        return pymysql.connect(**kwargs_individual)
