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


def _serialize_value(value: Any) -> Any:
    """Serialize dict/list values to JSON strings for pymysql.

    pymysql does not auto-serialize complex Python types, so values
    bound for JSON columns (common when sourcing from BigQuery) must
    be converted to strings before execute().
    """
    if isinstance(value, dict | list):
        return json.dumps(value, ensure_ascii=False)
    return value



class MySQLDestination:
    """Upsert or replace records into a MySQL table."""

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
        assert isinstance(config, MySQLDestinationConfig)
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
                    )
                else:
                    result = self._load_replace(
                        conn,
                        cur,
                        records,
                        columns,
                        config.table,
                        sync_options,
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

    @staticmethod
    def _quote_ident(table: str) -> str:
        """Backtick-quote a (possibly schema-qualified) identifier.

        ``mydb.scores`` -> ``\\`mydb\\`.\\`scores\\```
        ``scores``      -> ``\\`scores\\```
        """
        if "." in table:
            return "`" + "`.`".join(table.split(".")) + "`"
        return f"`{table}`"

    def _load_replace(
        self,
        conn: Any,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        table: str,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """TRUNCATE (once) → INSERT within a transaction."""
        result = SyncResult()

        if not self._replace_truncated:
            cur.execute(f"TRUNCATE TABLE `{table}`")
            self._replace_truncated = True

        sql = self._build_insert_sql(table, columns)

        for i, record in enumerate(records):
            try:
                values = [_serialize_value(record.get(c)) for c in columns]
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

    def _load_replace_swap(
        self,
        conn: Any,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        table: str,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Build a shadow table per sync; atomic rename happens in finalize_sync."""
        result = SyncResult()
        shadow = f"{table}__drt_swap"
        shadow_q = self._quote_ident(shadow)
        table_q = self._quote_ident(table)

        if not self._swap_shadow_created:
            cur.execute(f"DROP TABLE IF EXISTS {shadow_q}")
            # MySQL: CREATE TABLE ... LIKE ... copies columns + indexes
            # + AUTO_INCREMENT default — no extra clause needed.
            cur.execute(f"CREATE TABLE {shadow_q} LIKE {table_q}")
            self._swap_shadow_created = True
            self._swap_table = table

        sql = self._build_insert_sql(shadow, columns)

        for i, record in enumerate(records):
            try:
                values = [_serialize_value(record.get(c)) for c in columns]
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
                    cur.execute(f"DROP TABLE IF EXISTS {shadow_q}")
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
        """Atomic single-statement RENAME: original->old, shadow->original; drop old."""
        if not self._swap_shadow_created or self._swap_table is None:
            return None

        assert isinstance(config, MySQLDestinationConfig)
        table = self._swap_table
        shadow = f"{table}__drt_swap"
        old = f"{table}__drt_old"
        table_q = self._quote_ident(table)
        shadow_q = self._quote_ident(shadow)
        old_q = self._quote_ident(old)

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            # MySQL's multi-table RENAME is atomic in a single statement.
            cur.execute(
                f"RENAME TABLE {table_q} TO {old_q}, {shadow_q} TO {table_q}"
            )
            conn.commit()
            # DROP old in separate tx (failure here doesn't break the swap).
            cur.execute(f"DROP TABLE {old_q}")
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
        config: MySQLDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        result = SyncResult()
        update_cols = [c for c in columns if c not in config.upsert_key]
        sql = MySQLDestination._build_upsert_sql(config.table, columns, update_cols)

        for i, record in enumerate(records):
            try:
                values = [_serialize_value(record.get(c)) for c in columns]
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
            cert = resolve_env(None, config.ssl.cert_env)
            if cert:
                ssl_dict["cert"] = cert
            key = resolve_env(None, config.ssl.key_env)
            if key:
                ssl_dict["key"] = key
            kwargs_individual["ssl"] = ssl_dict

        return pymysql.connect(**kwargs_individual)
