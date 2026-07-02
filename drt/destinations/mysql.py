"""MySQL destination — upsert / replace / mirror rows into a MySQL table.

Uses INSERT ... ON DUPLICATE KEY UPDATE for idempotent writes.
Supports ``sync.mode: replace`` (TRUNCATE → INSERT within a single transaction)
and ``sync.mode: mirror`` (upsert all source rows, then DELETE destination
rows whose ``upsert_key`` was not observed across the run — application-side
diff strategy, see #340).
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
from drt.destinations._serializer import serialize_complex_value
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError


def _mysql_json_encoder(value: Any) -> str:
    """Wire-format a dict/list as a JSON string for pymysql.

    pymysql has no native JSON adapter, so both dicts and lists get
    serialized the same way and the column type (``JSON``, ``TEXT``,
    etc.) determines the storage.
    """
    return json.dumps(value, ensure_ascii=False)


def _serialize_value(
    value: Any,
    column: str | None = None,
    json_columns: list[str] | None = None,
    schema: dict[str, str] | None = None,
) -> Any:
    """Serialize dict/list values to JSON strings for pymysql.

    If json_columns is specified, only columns in that list are
    JSON-serialized — other complex values raise early so the user gets
    a pointing error instead of a deep driver failure. When json_columns
    is ``None`` (back-compat with pre-#316), all dict/list values are
    serialized.

    Delegates the decision logic to
    :func:`drt.destinations._serializer.serialize_complex_value`; only
    the MySQL-specific JSON encoder lives here.

    Raises:
        ValueError: If *json_columns* is set and an unlisted column receives
            a dict or list value.
    """
    return serialize_complex_value(
        value,
        column,
        json_columns,
        dict_encoder=_mysql_json_encoder,
        list_encoder=_mysql_json_encoder,  # MySQL encodes both dicts and lists
        schema=schema,
    )


class MySQLDestination:
    """Upsert or replace records into a MySQL table.

    Implements ConnectionTestable via test_connection().
    """

    def __init__(self) -> None:
        self._replace_truncated: bool = False
        self._swap_shadow_created: bool = False
        self._swap_table: str | None = None
        # sync.mode: mirror (#340 Step 2) — accumulates upsert_key tuples seen
        # across batches so finalize_sync can DELETE missing rows.
        # ``None`` means mirror mode hasn't engaged yet (no batch with
        # records); finalize_sync treats that as "skip DELETE" — safety
        # against deleting everything when the source produced no data.
        self._mirror_keys: list[tuple[Any, ...]] | None = None
        # mirror.scope (#687) — distinct scope-column value tuples observed
        # across batches; the destination-strategy DELETE is restricted to
        # rows whose scope values are in this set.
        self._mirror_scopes: set[tuple[Any, ...]] | None = None
        # Layer 3 (#317): INFORMATION_SCHEMA map, fetched once per table per sync.
        self._schema_cache: dict[str, dict[str, str] | None] = {}

    def _resolve_schema(self, config: MySQLDestinationConfig) -> dict[str, str] | None:
        """Column → type-category map for the target table, cached per sync.

        Returns ``None`` (Layer 3 inactive) when ``introspect_schema`` is off,
        ``json_columns`` is set (Layer 2 wins), or introspection isn't available.
        """
        if not config.introspect_schema or config.json_columns is not None:
            return None
        if config.table not in self._schema_cache:
            from drt.destinations.schema import describe_columns

            self._schema_cache[config.table] = describe_columns(config)
        return self._schema_cache[config.table]

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, MySQLDestinationConfig)
        if not records:
            return SyncResult()

        # mirror.scope (#687) — a scope column absent from the model output
        # is a config error; fail fast before any row is written.
        if (
            sync_options.mode == "mirror"
            and sync_options.mirror is not None
            and sync_options.mirror.scope
        ):
            missing = [
                c for c in sync_options.mirror.scope if c not in records[0]
            ]
            if missing:
                raise ValueError(
                    "mirror.scope columns missing from the model output: "
                    f"{missing} (available: {sorted(records[0].keys())})"
                )

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
                # sync.mode: mirror (#340 Step 2) — accumulate upsert_key
                # tuples for the finalize_sync DELETE pass. Only keys from
                # successfully-loaded records are tracked (failed records
                # don't count as "source state").
                if sync_options.mode == "mirror":
                    if not config.upsert_key:
                        raise ValueError(
                            "sync.mode: mirror requires destination.upsert_key "
                            "(needed to identify which rows to DELETE)."
                        )
                    if self._mirror_keys is None:
                        self._mirror_keys = []
                    failed_indices = {
                        re.batch_index for re in result.row_errors
                    }
                    # mirror.scope (#687) — also collect distinct scope
                    # value tuples so the DELETE can be pruned to parents
                    # observed in this run.
                    scope_cols = (
                        sync_options.mirror.scope
                        if sync_options.mirror is not None
                        else None
                    )
                    if scope_cols and self._mirror_scopes is None:
                        self._mirror_scopes = set()
                    for idx, record in enumerate(records):
                        if idx in failed_indices:
                            continue
                        self._mirror_keys.append(
                            tuple(record.get(k) for k in config.upsert_key)
                        )
                        if scope_cols:
                            assert self._mirror_scopes is not None
                            self._mirror_scopes.add(
                                tuple(record.get(c) for c in scope_cols)
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
            cur.execute(f"SELECT COUNT(*) FROM {self._quote_ident(config.table)}")
            row = cur.fetchone()
            return row[0] if row else 0
        finally:
            conn.close()

    def test_connection(self, config: DestinationConfig) -> None:
        """Test connectivity by establishing a connection and running SELECT 1."""
        assert isinstance(config, MySQLDestinationConfig)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute("SELECT 1")
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
        config: MySQLDestinationConfig,
    ) -> SyncResult:
        """TRUNCATE (once) → INSERT within a transaction."""
        result = SyncResult()

        if not self._replace_truncated:
            cur.execute(f"TRUNCATE TABLE {self._quote_ident(table)}")
            self._replace_truncated = True

        sql = self._build_insert_sql(table, columns)
        schema_map = self._resolve_schema(config)

        for i, record in enumerate(records):
            try:
                values = [
                    _serialize_value(record.get(c), c, config.json_columns, schema_map)
                    for c in columns
                ]
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
        config: MySQLDestinationConfig,
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
        schema_map = self._resolve_schema(config)

        for i, record in enumerate(records):
            try:
                values = [
                    _serialize_value(record.get(c), c, config.json_columns, schema_map)
                    for c in columns
                ]
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
        """End-of-sync hook: swap-finalize for replace, DELETE-missing for mirror.

        - ``mode=replace, replace_strategy=swap``: atomic multi-table RENAME of
          the shadow table over the original (existing behaviour).
        - ``mode=mirror`` (#340 Step 2): DELETE rows from the destination whose
          ``upsert_key`` tuple is not in the set seen across all batches.
          Skipped if the source produced no batches with records —
          treats "no observation" as "don't delete anything" for safety.
        """
        if sync_options.mode == "mirror":
            result = self._finalize_mirror(config, sync_options)
            # Reset mirror state regardless of result so a re-run starts fresh.
            self._mirror_keys = None
            self._mirror_scopes = None
            return result

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

    def _finalize_mirror(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """``sync.mode: mirror`` end-of-sync DELETE pass (#340 Step 2).

        Deletes destination rows whose ``upsert_key`` tuple is not in the
        set of keys observed across all batches.

        pymysql does not auto-expand tuple-of-tuples like psycopg2 does,
        so we build the ``NOT IN`` placeholder list explicitly:

        - single-column form: ``DELETE FROM `t` WHERE `c` NOT IN (%s, %s, ...)``
        - composite form:     ``DELETE FROM `t` WHERE (`c1`,`c2`) NOT IN ((%s,%s),(%s,%s),...)``

        Memory-bound to the source key cardinality; for tables larger than
        a few million keys, a temp-table strategy (#340 follow-up) will be
        more appropriate.

        Returns ``None`` when ``_mirror_keys`` is empty or ``None`` —
        treats "no batch with records was ever observed" as a signal to
        skip the DELETE entirely, so a transient empty source doesn't
        wipe the destination.
        """
        assert isinstance(config, MySQLDestinationConfig)
        if not self._mirror_keys:
            return None

        # mirror.strategy: tracked (#686) — state-based diff instead of the
        # destination-table diff below. Shares the empty-source guard above,
        # so a transient empty source also keeps the tracked baseline intact.
        if (
            sync_options.mirror is not None
            and sync_options.mirror.strategy == "tracked"
        ):
            return self._finalize_mirror_tracked(config, sync_options)

        # Dedupe to keep the IN list compact when batches overlap.
        keys = list({tuple(k) for k in self._mirror_keys})
        upsert_cols = config.upsert_key
        table_q = self._quote_ident(config.table)

        # mirror.scope (#687) — prepend "scope IN (observed)" so the diff
        # only touches rows under parents this run actually saw. Rows under
        # unobserved parents (other pipelines / the application) stay put.
        scope_cols = (
            sync_options.mirror.scope if sync_options.mirror is not None else None
        )
        # list(), not sorted() — scope values may include None (unorderable).
        scopes = list(self._mirror_scopes or set()) if scope_cols else None

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            scope_clause = ""
            scope_params: list[Any] = []
            if scope_cols and scopes:
                if len(scope_cols) == 1:
                    s_placeholders = ", ".join(["%s"] * len(scopes))
                    scope_clause = (
                        f"`{scope_cols[0]}` IN ({s_placeholders}) AND "
                    )
                    scope_params = [sc[0] for sc in scopes]
                else:
                    s_col_tuple = (
                        "(" + ", ".join(f"`{c}`" for c in scope_cols) + ")"
                    )
                    s_row = "(" + ", ".join(["%s"] * len(scope_cols)) + ")"
                    s_placeholders = ", ".join([s_row] * len(scopes))
                    scope_clause = (
                        f"{s_col_tuple} IN ({s_placeholders}) AND "
                    )
                    scope_params = [v for sc in scopes for v in sc]
            if len(upsert_cols) == 1:
                placeholders = ", ".join(["%s"] * len(keys))
                col_q = f"`{upsert_cols[0]}`"
                stmt = (
                    f"DELETE FROM {table_q} WHERE {scope_clause}{col_q} "
                    f"NOT IN ({placeholders})"
                )
                params: list[Any] = scope_params + [k[0] for k in keys]
            else:
                col_tuple = "(" + ", ".join(f"`{c}`" for c in upsert_cols) + ")"
                row_placeholder = "(" + ", ".join(["%s"] * len(upsert_cols)) + ")"
                placeholders = ", ".join([row_placeholder] * len(keys))
                stmt = (
                    f"DELETE FROM {table_q} WHERE {scope_clause}{col_tuple} "
                    f"NOT IN ({placeholders})"
                )
                params = scope_params + [v for key in keys for v in key]
            cur.execute(stmt, params)
            conn.commit()
        finally:
            conn.close()

        # SyncResult has no dedicated `deleted` field; future work tracks
        # this separately. Returning a bare SyncResult signals "finalize
        # ran successfully" to the engine without inflating success/failed.
        return SyncResult()

    def _finalize_mirror_tracked(
        self,
        config: MySQLDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """``mirror.strategy: tracked`` (#686) — delete only rows drt synced.

        MySQL counterpart of the Postgres implementation: reads the
        previously-synced key set for this sync from the drt-managed
        ``_drt_synced_keys`` table (created lazily in the target's database),
        deletes ``previous - current`` from the target, and rewrites the
        state — one transaction. First run / lost state baselines with a
        WARN and no deletes. pymysql does not auto-expand tuple-of-tuples,
        so IN placeholder lists are built explicitly like the destination
        strategy above.
        """
        import logging

        from drt.destinations._mirror_state import (
            STATE_TABLE,
            diff_keys,
            key_hash,
            key_json,
        )

        sync_name = sync_options._sync_name or config.table
        current = list({tuple(k) for k in self._mirror_keys or []})
        upsert_cols = config.upsert_key
        table_q = self._quote_ident(config.table)
        if "." in config.table:
            database = config.table.rsplit(".", 1)[0]
            state_q = self._quote_ident(f"{database}.{STATE_TABLE}")
        else:
            state_q = self._quote_ident(STATE_TABLE)

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {state_q} ("
                "sync_name VARCHAR(255) NOT NULL, "
                "key_hash CHAR(64) NOT NULL, "
                "key_json TEXT NOT NULL, "
                "PRIMARY KEY (sync_name, key_hash))"
            )
            cur.execute(
                f"SELECT key_hash, key_json FROM {state_q} WHERE sync_name = %s",
                (sync_name,),
            )
            previous = {row[0]: row[1] for row in cur.fetchall()}

            if previous:
                to_delete = diff_keys(previous, current)
                if to_delete:
                    if len(upsert_cols) == 1:
                        placeholders = ", ".join(["%s"] * len(to_delete))
                        col_q = f"`{upsert_cols[0]}`"
                        stmt = (
                            f"DELETE FROM {table_q} WHERE {col_q} "
                            f"IN ({placeholders})"
                        )
                        params: list[Any] = [k[0] for k in to_delete]
                    else:
                        col_tuple = (
                            "(" + ", ".join(f"`{c}`" for c in upsert_cols) + ")"
                        )
                        row_placeholder = (
                            "(" + ", ".join(["%s"] * len(upsert_cols)) + ")"
                        )
                        placeholders = ", ".join(
                            [row_placeholder] * len(to_delete)
                        )
                        stmt = (
                            f"DELETE FROM {table_q} WHERE {col_tuple} "
                            f"IN ({placeholders})"
                        )
                        params = [v for key in to_delete for v in key]
                    cur.execute(stmt, params)
            else:
                logging.getLogger(__name__).warning(
                    "tracked mirror: no prior state for sync %r in %s — "
                    "baselining this run's %d key(s); no deletes this run.",
                    sync_name,
                    STATE_TABLE,
                    len(current),
                )

            # Rewrite this sync's state to the current key set.
            cur.execute(
                f"DELETE FROM {state_q} WHERE sync_name = %s",
                (sync_name,),
            )
            cur.executemany(
                f"INSERT INTO {state_q} (sync_name, key_hash, key_json) "
                "VALUES (%s, %s, %s)",
                [(sync_name, key_hash(k), key_json(k)) for k in current],
            )
            conn.commit()
        finally:
            conn.close()

        return SyncResult()

    def _load_upsert(
        self,
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
        schema_map = self._resolve_schema(config)

        for i, record in enumerate(records):
            try:
                values = [
                    _serialize_value(record.get(c), c, config.json_columns, schema_map)
                    for c in columns
                ]
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
        table_q = MySQLDestination._quote_ident(table)
        return f"INSERT INTO {table_q} ({cols_str}) VALUES ({placeholders})"

    @staticmethod
    def _build_upsert_sql(
        table: str,
        columns: list[str],
        update_cols: list[str],
    ) -> str:
        """Build INSERT ... ON DUPLICATE KEY UPDATE SQL."""
        cols_str = ", ".join(f"`{c}`" for c in columns)
        placeholders = ", ".join(["%s"] * len(columns))
        table_q = MySQLDestination._quote_ident(table)

        if update_cols:
            set_clause = ", ".join(f"`{c}` = VALUES(`{c}`)" for c in update_cols)
            return (
                f"INSERT INTO {table_q} ({cols_str}) VALUES ({placeholders}) "
                f"ON DUPLICATE KEY UPDATE {set_clause}"
            )
        # All columns are part of the key — just ignore duplicates
        return f"INSERT IGNORE INTO {table_q} ({cols_str}) VALUES ({placeholders})"

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
