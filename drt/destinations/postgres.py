"""PostgreSQL destination — upsert / replace / mirror rows into a PostgreSQL table.

Uses INSERT ... ON CONFLICT (upsert_key) DO UPDATE SET ... for idempotent writes.
Supports ``sync.mode: replace`` (TRUNCATE → INSERT within a single transaction)
and ``sync.mode: mirror`` (upsert all source rows, then DELETE destination
rows whose ``upsert_key`` is not present in the observed source set — #340).
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
import logging
from datetime import timedelta
from typing import Any

from drt.config.credentials import resolve_env
from drt.config.models import (
    DestinationConfig,
    PostgresDestinationConfig,
    SyncOptions,
)
from drt.destinations._serializer import serialize_complex_value
from drt.destinations.base import SyncResult
from drt.destinations.sql_base import BaseSqlDestination

try:
    from psycopg2.extras import Json as _Psycopg2Json
except ImportError:  # pragma: no cover — fires only when drt-core[postgres] is not installed
    _Psycopg2Json = None  # type: ignore[assignment,misc]

# Prefer to import sql at module import time if available; fall back to None
try:
    from psycopg2 import sql  # type: ignore
except Exception:  # pragma: no cover — fires only when drt-core[postgres] is not installed
    sql = None  # type: ignore


def _pg_dict_encoder(value: dict[str, Any]) -> Any:
    """Wire-format a dict for PostgreSQL JSONB.

    Prefer psycopg2's native ``Json`` adapter; fall back to ``json.dumps``
    so the destination keeps working when psycopg2.extras is unavailable
    (e.g. behind an import-shim during tests).
    """
    if _Psycopg2Json is not None:
        return _Psycopg2Json(value)
    return json.dumps(value, ensure_ascii=False)


def _serialize_value(
    value: Any,
    column: str | None = None,
    json_columns: list[str] | None = None,
    schema: dict[str, str] | None = None,
) -> Any:
    """Wrap dict values with psycopg2.extras.Json for JSONB columns.

    psycopg2 has no default adapter for ``dict``, so any dict value bound
    for a JSONB column (e.g. from a BigQuery JSON source) would otherwise
    raise ``ProgrammingError: can't adapt type 'dict'``. Wrapping with
    ``Json`` produces the correct wire format for PostgreSQL JSONB.

    Lists pass through to psycopg2's ARRAY adapter unchanged — the driver
    handles them when the destination column is a typed ARRAY. Lists
    routed to a JSON column should be listed in ``json_columns`` so this
    function knows to allow them; unlisted complex values raise early.

    Delegates the decision logic to
    :func:`drt.destinations._serializer.serialize_complex_value`; only the
    Postgres-specific dict encoder lives here.

    Raises:
        ValueError: If *json_columns* is set and an unlisted column receives
            a dict or list value.
    """
    return serialize_complex_value(
        value,
        column,
        json_columns,
        dict_encoder=_pg_dict_encoder,
        list_encoder=None,  # pass-through — psycopg2's ARRAY adapter takes over
        schema=schema,
    )


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


class PostgresDestination(BaseSqlDestination):
    """Upsert or replace records into a PostgreSQL table.

    Implements ConnectionTestable via test_connection(). Per-sync state, schema
    resolution, and mirror bookkeeping come from ``BaseSqlDestination``.
    """

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

    def get_table_name(self, config: DestinationConfig) -> str:
        """Implements ``QueryableDestination`` (#469)."""
        assert isinstance(config, PostgresDestinationConfig)
        return config.table

    def execute_test_query(self, config: DestinationConfig, query: str) -> int:
        """Implements ``QueryableDestination`` (#469).

        Raises:
            Exception: If connection or query fails.
        """
        assert isinstance(config, PostgresDestinationConfig)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(query)
            result: Any = cur.fetchone()[0]
            return int(result)
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
        schema_map = self._resolve_schema(config)

        for i, record in enumerate(records):
            try:
                values = [
                    _serialize_value(record.get(c), c, config.json_columns, schema_map)
                    for c in columns
                ]
                cur.execute(query, values)
                result.success += 1
            except Exception as e:
                self._record_row_error(result, i, record, e)
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
                self._record_row_error(result, i, record, e)
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

    def _shadow_name(self, table: str) -> str:
        return _with_relation_suffix(table, "__drt_swap")

    def _old_name(self, table: str) -> str:
        return _with_relation_suffix(table, "__drt_old")

    def _rename_swap(
        self, conn: Any, cur: Any, table: str, shadow: str, old: str
    ) -> None:
        """PG swap rename: two ``ALTER TABLE ... RENAME TO`` under one commit,
        then a separate DROP+commit for the old table."""
        from psycopg2 import sql as _pgsql

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

    def _build_mirror_delete(
        self,
        table: str,
        upsert_cols: list[str],
        keys: list[tuple[Any, ...]],
        scope_cols: list[str] | None = None,
        scopes: list[tuple[Any, ...]] | None = None,
        negate: bool = True,
    ) -> tuple[Any, Any]:
        """Postgres mirror ``DELETE`` builder (#719 phase 2b) — returns a
        ``(psycopg2.sql.Composed, tuple)`` pair.

        Strategy: composite ``[NOT] IN`` with a *single* server-side
        parameter per clause — psycopg2 expands a ``tuple`` (single-column
        key) or a ``tuple of tuples`` (composite key) into the right SQL, so
        no placeholder list is built here. ``negate=True`` gives the
        destination-strategy ``NOT IN`` form, ``negate=False`` the
        tracked-strategy ``IN`` form (#686). ``scope_cols``/``scopes``
        prepend the mirror.scope restriction (#687).
        """
        from psycopg2 import sql as _pgsql

        op = "NOT IN" if negate else "IN"
        scope_clause = _pgsql.SQL("")
        scope_params: tuple[Any, ...] = ()
        if scope_cols and scopes:
            if len(scope_cols) == 1:
                scope_clause = _pgsql.SQL("{} IN %s AND ").format(
                    _pgsql.Identifier(scope_cols[0])
                )
                scope_params = (tuple(s[0] for s in scopes),)
            else:
                scope_tuple = _pgsql.SQL("({})").format(
                    _pgsql.SQL(", ").join(_pgsql.Identifier(c) for c in scope_cols)
                )
                scope_clause = _pgsql.SQL("{} IN %s AND ").format(scope_tuple)
                scope_params = (tuple(tuple(s) for s in scopes),)
        if len(upsert_cols) == 1:
            # Single-column form: DELETE WHERE [scope IN %s AND] col [NOT] IN %s
            stmt = _pgsql.SQL("DELETE FROM {} WHERE {}{} " + op + " %s").format(
                _qualified_ident(table),
                scope_clause,
                _pgsql.Identifier(upsert_cols[0]),
            )
            params: tuple[Any, ...] = (
                *scope_params,
                tuple(k[0] for k in keys),
            )
        else:
            # Composite form: DELETE WHERE [scope IN %s AND] (c1, c2) [NOT] IN %s
            col_tuple = _pgsql.SQL("({})").format(
                _pgsql.SQL(", ").join(_pgsql.Identifier(c) for c in upsert_cols)
            )
            stmt = _pgsql.SQL("DELETE FROM {} WHERE {}{} " + op + " %s").format(
                _qualified_ident(table),
                scope_clause,
                col_tuple,
            )
            params = (*scope_params, tuple(keys))
        return stmt, params

    # --- tracked-mirror state hooks (#686 / #695, #719 phase 2b) -----------
    def _state_table_ident(self, config: Any) -> tuple[Any, Any, Any]:
        """State table lives in the target table's schema; ``to_regclass``
        takes the *unquoted* qualified name as a bound parameter."""
        from drt.destinations._mirror_state import STATE_TABLE

        schema, _relation = _split_qualified(config.table)
        qualified = _join_qualified(schema, STATE_TABLE)
        return _qualified_ident(qualified), schema, qualified

    def _state_table_exists(self, cur: Any, scope: Any, raw: str) -> bool:
        cur.execute("SELECT to_regclass(%s)", (raw,))
        return cur.fetchone()[0] is not None

    def _create_state_table(self, cur: Any, ident: Any) -> None:
        from psycopg2 import sql as _pgsql

        cur.execute(
            _pgsql.SQL(
                "CREATE TABLE IF NOT EXISTS {} ("
                "sync_name VARCHAR(255) NOT NULL, "
                "key_hash CHAR(64) NOT NULL, "
                "key_json TEXT NOT NULL, "
                "scope_spec TEXT, "
                "scope_key TEXT, "
                "PRIMARY KEY (sync_name, key_hash))"
            ).format(ident)
        )

    def _state_scope_columns_exist(self, cur: Any, scope: Any, raw: str) -> bool:
        cur.execute(
            "SELECT COUNT(*) FROM pg_attribute "
            "WHERE attrelid = to_regclass(%s) "
            "AND attname IN ('scope_spec', 'scope_key') "
            "AND NOT attisdropped",
            (raw,),
        )
        row = cur.fetchone()
        return bool(row is not None and row[0] == 2)

    def _add_state_scope_columns(self, cur: Any, ident: Any) -> None:
        from psycopg2 import sql as _pgsql

        cur.execute(
            _pgsql.SQL(
                "ALTER TABLE {} ADD COLUMN IF NOT EXISTS scope_spec TEXT, "
                "ADD COLUMN IF NOT EXISTS scope_key TEXT"
            ).format(ident)
        )

    def _state_sql(self, template: str, ident: Any) -> Any:
        from psycopg2 import sql as _pgsql

        return _pgsql.SQL(template).format(ident)

    def list_orphan_swap_tables(
        self,
        config: DestinationConfig,
        base_table: str,
        older_than: timedelta | None = None,
    ) -> list[str]:
        """Detect orphan swap tables for the current sync's base table.

        PostgreSQL does not expose a reliable creation timestamp for tables in
        standard catalogs, so *older_than* is best-effort and currently only
        affects logging. The lookup is scoped to the current sync's base table
        so one sync never sees another sync's shadow tables.

        Args:
            config: Postgres destination configuration.
            base_table: The current sync's base table name. May be schema-
                qualified (e.g. ``public.users``); only the table component is
                used to derive the shadow name.
            older_than: Optional age filter in hours.

        Returns:
            Fully qualified ``schema.table`` names for orphan swap tables.

        Raises:
            Exception: If the catalog query fails.
        """
        assert isinstance(config, PostgresDestinationConfig)

        shadow_name = f"{base_table.rsplit('.', 1)[-1]}__drt_swap"
        schema_name = config.table.rsplit('.', 1)[0] if "." in config.table else None

        if older_than is not None:
            # Best-effort: PostgreSQL doesn't store table creation timestamp
            logging.getLogger(__name__).info(
                "older_than filter requested but not supported for Postgres; "
                "returning all matches"
            )

        conn = self._connect(config)
        try:
            cur = conn.cursor()
            query = [
                "SELECT table_schema, table_name",
                "FROM information_schema.tables",
                "WHERE table_type = 'BASE TABLE'",
                "  AND table_name = %s",
                "  AND table_schema NOT IN ('pg_catalog', 'information_schema')",
            ]
            params: list[Any] = [shadow_name]
            if schema_name:
                query.append("  AND table_schema = %s")
                params.append(schema_name)
            cur.execute("\n".join(query), tuple(params))
            rows = cur.fetchall()
            result: list[str] = []
            for schema, name in rows:
                # Defensive: ensure exact shadow name matches the current sync.
                if name == shadow_name:
                    result.append(f"{schema}.{name}")
            return result
        finally:
            conn.close()

    def drop_orphan_swap_tables(
        self, config: DestinationConfig, tables: list[str]
    ) -> tuple[list[str], list[str]]:
        """Drop the given orphan swap tables and return (dropped, failed).

        This enforces safety checks (only drop tables ending with
        ``__drt_swap``) and performs the DROP using the destination's
        connection. Returns two lists: successfully dropped tables and
        tables that failed to drop.

        Each table drop is independently committed to ensure that failure
        of one table does not rollback successful drops of others.
        """
        assert isinstance(config, PostgresDestinationConfig)

        dropped: list[str] = []
        failed: list[str] = []

        conn = self._connect(config)
        try:
            for full_name in tables:
                # Validate format: must be exactly schema.table
                if not full_name or full_name.count(".") != 1:
                    failed.append(full_name)
                    continue

                schema, table = full_name.split(".", 1)
                if not schema or not table:
                    failed.append(full_name)
                    continue

                # Validate suffix: must end with __drt_swap
                if not table.endswith("__drt_swap"):
                    failed.append(full_name)
                    continue

                try:
                    cur = conn.cursor()
                    # Ensure we have psycopg2.sql available; import lazily if needed
                    _sql = sql
                    if _sql is None:
                        from psycopg2 import sql as _sql  # type: ignore

                    cur.execute(
                        _sql.SQL("DROP TABLE IF EXISTS {}.{}").format(
                            _sql.Identifier(schema), _sql.Identifier(table)
                        )
                    )
                    conn.commit()
                    dropped.append(full_name)
                except Exception:
                    # Try to rollback this attempt and record failure
                    try:
                        conn.rollback()
                    except Exception:
                        pass
                    failed.append(full_name)
        finally:
            conn.close()

        return dropped, failed

    def _load_upsert(
        self,
        conn: Any,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        config: PostgresDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        result = SyncResult()
        policy = sync_options.match_policy
        update_cols = [c for c in columns if c not in config.upsert_key]
        schema_map = self._resolve_schema(config)

        # match_policy (#757) picks the write shape and, for the narrowed
        # policies, the parameter order. Postgres has clean rowcount semantics:
        # ON CONFLICT DO NOTHING reports rows *inserted* (0 == already existed),
        # and UPDATE reports rows *matched* (0 == no such row) regardless of
        # whether any value actually changed — so cur.rowcount == 0 is an exact
        # "skipped, no match" signal for both narrowed policies.
        if policy == "create_only":
            query = PostgresDestination._build_create_only_sql(
                config.table, columns, config.upsert_key
            )
            value_cols = columns
        elif policy == "update_only":
            if not update_cols:
                raise ValueError(
                    "sync.match_policy: update_only needs at least one non-key "
                    "column to update, but every column is in upsert_key."
                )
            query = PostgresDestination._build_update_only_sql(
                config.table, update_cols, config.upsert_key
            )
            # UPDATE ... SET <update_cols> WHERE <upsert_key>: SET params first,
            # then the WHERE key params.
            value_cols = update_cols + config.upsert_key
        else:
            query = PostgresDestination._build_upsert_sql(
                config.table,
                columns,
                config.upsert_key,
                update_cols,
            )
            value_cols = columns

        for i, record in enumerate(records):
            try:
                values = [
                    _serialize_value(record.get(c), c, config.json_columns, schema_map)
                    for c in value_cols
                ]
                cur.execute(query, values)
                if policy in ("create_only", "update_only") and cur.rowcount == 0:
                    result.skipped += 1
                    result.skipped_no_match += 1  # #757 — no create/update target
                else:
                    result.success += 1
            except Exception as e:
                self._record_row_error(result, i, record, e)
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
    def _build_create_only_sql(
        table: str, columns: list[str], upsert_key: list[str]
    ) -> Any:
        """``match_policy: create_only`` (#757) — insert only rows not yet present.

        ``ON CONFLICT (key) DO NOTHING`` so existing rows are left untouched;
        ``cur.rowcount`` is then the inserted count (0 == the row already
        existed → skipped).
        """
        from psycopg2 import sql as _pgsql

        return _pgsql.SQL(
            "INSERT INTO {} ({}) VALUES ({}) ON CONFLICT ({}) DO NOTHING"
        ).format(
            _qualified_ident(table),
            _pgsql.SQL(", ").join(_pgsql.Identifier(c) for c in columns),
            _pgsql.SQL(", ").join(_pgsql.Placeholder() for _ in columns),
            _pgsql.SQL(", ").join(_pgsql.Identifier(c) for c in upsert_key),
        )

    @staticmethod
    def _build_update_only_sql(
        table: str, update_cols: list[str], upsert_key: list[str]
    ) -> Any:
        """``match_policy: update_only`` (#757) — update only rows that exist.

        A plain ``UPDATE ... SET <cols> WHERE <key>`` never inserts, so rows
        with no destination match are left alone; ``cur.rowcount`` is the
        matched-row count (0 == no such row → skipped). Params bind SET columns
        first, then the WHERE key columns.
        """
        from psycopg2 import sql as _pgsql

        set_clause = _pgsql.SQL(", ").join(
            _pgsql.SQL("{} = {}").format(_pgsql.Identifier(c), _pgsql.Placeholder())
            for c in update_cols
        )
        where_clause = _pgsql.SQL(" AND ").join(
            _pgsql.SQL("{} = {}").format(_pgsql.Identifier(c), _pgsql.Placeholder())
            for c in upsert_key
        )
        return _pgsql.SQL("UPDATE {} SET {} WHERE {}").format(
            _qualified_ident(table), set_clause, where_clause
        )

    def supported_match_policies(self) -> frozenset[str]:
        """Postgres honours all three ``match_policy`` values (#757)."""
        return frozenset({"upsert", "update_only", "create_only"})

    # --- dialect hooks (#719) ---------------------------------------------
    def _dialect_connect(self, config: Any) -> Any:
        return self._connect(config)  # _connect is @classmethod(config) — #723

    def _qualify_ident(self, name: str) -> Any:
        return _qualified_ident(name)  # module-level fn

    @classmethod
    def _connect(cls, config: PostgresDestinationConfig) -> Any:
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
