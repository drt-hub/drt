"""Snowflake destination — write data back to Snowflake tables.

Supports:
- INSERT (append, ``config.mode: insert``)
- MERGE (upsert using key columns, ``config.mode: merge``)
- ``sync.mode: replace`` (#434) — full table replace, two strategies:
  - ``replace_strategy: truncate`` (default) — ``TRUNCATE`` (once) then INSERT.
  - ``replace_strategy: swap`` — write to a shadow ``<table>__drt_swap``
    built with ``CREATE OR REPLACE TABLE ... LIKE`` (copies clustering keys,
    but NOT masking / row-access policies or tags), then an atomic
    ``ALTER TABLE ... SWAP WITH`` in :meth:`finalize_sync`. ``SWAP WITH``
    preserves grants (role privileges) on the original name; tables that rely
    on column policies should re-apply them or front the table with a
    policy-bearing view. First run (target absent) falls through to a
    direct write.
- ``sync.mode: mirror`` (#340 Step 4) — MERGE upsert, then end-of-sync
  ``DELETE FROM ... WHERE upsert_key NOT IN (observed)`` from
  :meth:`finalize_sync`. Mirror mode forces the MERGE write path
  regardless of ``config.mode``, so users only need to set
  ``destination.upsert_key`` and ``sync.mode: mirror``.

Install: snowflake-connector-python.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, SnowflakeDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import record_row_error
from drt.destinations.sql_base import BaseSqlDestination
from drt.destinations.sql_utils import check_mirror_supported, tagged_cursor

_SWAP_SUFFIX = "__drt_swap"


def _value_clause(columns: list[str], schema_map: dict[str, str] | None) -> tuple[str, list[str]]:
    """Build the per-row value clause for an INSERT and the JSON column list.

    Layer 3 (#317): VARIANT / OBJECT / ARRAY columns (category ``json``) must be
    loaded via ``PARSE_JSON``. Snowflake **disallows functions in a ``VALUES``
    clause**, so when any column needs wrapping we switch to the ``SELECT`` form
    (``INSERT INTO t (cols) SELECT %s, PARSE_JSON(%s)``). With no JSON columns
    the clause is the unchanged ``VALUES (%s, %s, ...)`` — byte-identical to
    pre-#317 behaviour.

    Returns ``(clause, json_columns)`` where ``clause`` already includes the
    ``VALUES (...)`` / ``SELECT ...`` keyword.
    """
    exprs: list[str] = []
    json_columns: list[str] = []
    # Snowflake's INFORMATION_SCHEMA returns column names in their stored case
    # (UPPERCASE for unquoted DDL) while source record keys are usually
    # lowercase — fold both sides so PARSE_JSON wrapping actually fires for the
    # common unquoted-table + lowercase-key pipeline (#317 review).
    folded = {str(k).lower(): v for k, v in schema_map.items()} if schema_map is not None else None
    for col in columns:
        if folded is not None and folded.get(str(col).lower()) == "json":
            exprs.append("PARSE_JSON(%s)")
            json_columns.append(col)
        else:
            exprs.append("%s")
    if json_columns:
        return "SELECT " + ", ".join(exprs), json_columns
    return "VALUES (" + ", ".join(exprs) + ")", json_columns


def _bind_row(row: dict[str, Any], columns: list[str], json_columns: list[str]) -> list[Any]:
    """Order a row's values to ``columns``; ``json.dumps`` the JSON columns so
    ``PARSE_JSON`` receives a JSON string."""
    js = set(json_columns)
    return [json.dumps(row.get(c), default=str) if c in js else row.get(c) for c in columns]


# #988: a MERGE ... USING (VALUES ...) statement replaces the old CREATE TEMP
# TABLE staging step, so `sync.mode: mirror` no longer needs schema-level
# CREATE TABLE on Snowflake at all (staging via a physical temp table was the
# one remaining DDL requirement after #987 fixed the finalize-side diff).
# Verified live against a real account: 1000 rows x 3 columns (3000 scalar
# bind params) executed in well under a second with no degradation across
# 10/25/50/100/250/500/1000-row checkpoints; an unbounded single statement
# (tried up to 32000 rows in an earlier, cruder probe) became impractically
# slow. This budget stays comfortably under the verified-safe ceiling while
# scaling down automatically for wide tables, the same shape as
# databricks.py's `_rows_per_chunk` (driven by a different, driver-imposed
# limit there — Snowflake's own ceiling isn't documented, see #988).
_MERGE_PARAM_BUDGET = 2000


def _rows_per_merge_chunk(n_cols: int) -> int:
    """How many rows fit in one VALUES-sourced MERGE under the verified budget."""
    return max(1, _MERGE_PARAM_BUDGET // max(1, n_cols))


def _merge_using_subquery(
    columns: list[str], schema_map: dict[str, str] | None, n_rows: int
) -> str:
    """Build the ``(SELECT ... FROM (VALUES ...) AS t(...))`` subquery that
    sources a MERGE's ``USING`` clause from ``n_rows`` worth of scalar-bound
    values, instead of a physical staging table.

    PARSE_JSON wraps JSON-category columns in the outer SELECT rather than
    inside the VALUES clause itself — Snowflake disallows functions inside
    ``VALUES()`` (the same reason ``_value_clause`` switches a plain INSERT
    to ``SELECT`` form for #317's Layer 3 JSON columns; applying PARSE_JSON
    to the *projection* of a literal VALUES table hits neither restriction).
    Generic ``v0..vN`` aliases on the VALUES table (rather than the real
    column names) sidestep any edge case where a column name collides with a
    SQL keyword when used as a bare alias.
    """
    folded = {str(k).lower(): v for k, v in schema_map.items()} if schema_map is not None else None
    alias_names = [f"v{i}" for i in range(len(columns))]
    select_parts = [
        f"PARSE_JSON({alias_names[i]}) AS {col}"
        if folded is not None and folded.get(str(col).lower()) == "json"
        else f"{alias_names[i]} AS {col}"
        for i, col in enumerate(columns)
    ]
    row_placeholder = "(" + ", ".join(["%s"] * len(columns)) + ")"
    values_sql = ", ".join([row_placeholder] * n_rows)
    return (
        f"SELECT {', '.join(select_parts)} FROM (VALUES {values_sql}) "
        f"AS t({', '.join(alias_names)})"
    )


def _build_merge_sql(
    table_fq: str, columns: list[str], upsert_key: list[str], using_subquery: str
) -> str:
    """Build the full ``MERGE INTO ... USING (<subquery>) AS source`` statement."""
    key_clause = " AND ".join([f"target.{k} = source.{k}" for k in upsert_key])
    update_cols = [c for c in columns if c not in upsert_key]
    update_clause = ", ".join([f"{c} = source.{c}" for c in update_cols])
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"source.{c}" for c in columns])
    matched_clause = f"WHEN MATCHED THEN UPDATE SET {update_clause}" if update_cols else ""
    return f"""
        MERGE INTO {table_fq} target
        USING ({using_subquery}) AS source
        ON {key_clause}
        {matched_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols})
        VALUES ({insert_vals})
    """


class SnowflakeDestination(BaseSqlDestination):
    """Write records into Snowflake tables."""

    def __init__(self) -> None:
        super().__init__()
        # Snowflake-only first-run fall-through: target table doesn't exist
        # yet, so replace-swap writes straight to it and skips the swap.
        self._swap_direct_write: bool = False

    def _resolve_schema(self, config: SnowflakeDestinationConfig) -> dict[str, str] | None:
        """Column → type-category map for the target table, cached per sync.

        Returns ``None`` (Layer 3 inactive) when ``introspect_schema`` is off or
        introspection is unavailable.
        """
        if not config.introspect_schema:
            return None
        if config.table not in self._schema_cache:
            from drt.destinations.schema import describe_columns

            self._schema_cache[config.table] = describe_columns(config)
        return self._schema_cache[config.table]

    def _validate_mirror_scope(
        self,
        records: list[dict[str, Any]],
        config: Any,
        sync_options: SyncOptions,
    ) -> None:
        assert isinstance(config, SnowflakeDestinationConfig)
        check_mirror_supported(
            config,
            sync_options,
            "snowflake",
            supports_tracked_scope=True,
        )
        if (
            sync_options.mode == "mirror"
            and sync_options.mirror is not None
            and sync_options.mirror.scope
        ):
            missing = [c for c in sync_options.mirror.scope if c not in records[0]]
            if missing:
                raise ValueError(
                    "mirror.scope columns missing from the model output: "
                    f"{missing} (available: {sorted(records[0].keys())})"
                )

    def _load_replace(
        self,
        conn: Any,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        table: str,
        sync_options: SyncOptions,
        config: SnowflakeDestinationConfig,
    ) -> SyncResult:
        """``replace_strategy: truncate`` — TRUNCATE once, then INSERT rows."""
        del conn
        assert isinstance(config, SnowflakeDestinationConfig)
        result = SyncResult()
        table_fq = f"{config.database}.{config.schema_}.{table}"
        schema_map = self._resolve_schema(config)

        if not self._replace_truncated:
            cur.execute(f"TRUNCATE TABLE {table_fq}")
            self._replace_truncated = True

        col_list = ", ".join(columns)
        value_clause, json_cols = _value_clause(columns, schema_map)
        sql = f"INSERT INTO {table_fq} ({col_list}) {value_clause}"
        self._insert_rows(cur, sql, records, sync_options, result, columns, json_cols)
        return result

    def _load_replace_swap(
        self,
        conn: Any,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        table: str,
        sync_options: SyncOptions,
        config: SnowflakeDestinationConfig,
    ) -> SyncResult:
        """``replace_strategy: swap`` — write to a shadow table; SWAP in finalize.

        First batch: if the target table doesn't exist yet, fall through to a
        direct write (no shadow, no swap). Otherwise build the shadow with
        ``CREATE OR REPLACE TABLE ... LIKE`` (carries clustering keys).
        """
        del conn
        assert isinstance(config, SnowflakeDestinationConfig)
        result = SyncResult()
        table_fq = f"{config.database}.{config.schema_}.{table}"
        schema_map = self._resolve_schema(config)
        shadow_fq = f"{table_fq}{_SWAP_SUFFIX}"

        if not self._swap_shadow_created and not self._swap_direct_write:
            if self._target_exists(cur, config):
                cur.execute(f"CREATE OR REPLACE TABLE {shadow_fq} LIKE {table_fq}")
                self._swap_shadow_created = True
                self._swap_table = table_fq
            else:
                # First run: nothing to swap against — write straight to target.
                self._swap_direct_write = True

        write_fq = table_fq if self._swap_direct_write else shadow_fq
        col_list = ", ".join(columns)
        value_clause, json_cols = _value_clause(columns, schema_map)
        sql = f"INSERT INTO {write_fq} ({col_list}) {value_clause}"

        try:
            self._insert_rows(cur, sql, records, sync_options, result, columns, json_cols)
        except Exception:
            # on_error=fail mid-swap: drop the half-built shadow and reset so a
            # re-run starts clean. (Direct-write path has no shadow to drop.)
            if self._swap_shadow_created:
                cur.execute(f"DROP TABLE IF EXISTS {shadow_fq}")
                self._swap_shadow_created = False
                self._swap_table = None
            raise
        return result

    def _load_upsert(
        self,
        conn: Any,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        config: SnowflakeDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        del conn
        assert isinstance(config, SnowflakeDestinationConfig)
        result = SyncResult()
        table_fq = f"{config.database}.{config.schema_}.{config.table}"
        schema_map = self._resolve_schema(config)
        effective_mode = "merge" if sync_options.mode == "mirror" else config.mode
        col_list = ", ".join(columns)
        value_clause, json_cols = _value_clause(columns, schema_map)

        if effective_mode == "insert":
            sql = f"""
                INSERT INTO {table_fq} ({col_list})
                {value_clause}
            """

            for i, row in enumerate(records):
                try:
                    cur.execute(sql, _bind_row(row, columns, json_cols))
                    result.success += 1
                except Exception as e:
                    record_row_error(
                        result,
                        i,
                        str(row)[:200],
                        e,
                    )
                    if sync_options.on_error == "fail":
                        raise

        elif effective_mode == "merge":
            if not config.upsert_key:
                raise ValueError("upsert_key is required for merge mode")

            # #988: chunked MERGE ... USING (VALUES ...) replaces the old
            # CREATE TEMP TABLE staging step — no DDL privilege needed at all
            # now. A chunk-level failure falls back to one MERGE per row.
            chunk_size = _rows_per_merge_chunk(len(columns))
            for chunk_start in range(0, len(records), chunk_size):
                chunk = records[chunk_start : chunk_start + chunk_size]
                try:
                    using_sql = _merge_using_subquery(columns, schema_map, len(chunk))
                    merge_sql = _build_merge_sql(
                        table_fq, columns, config.upsert_key, using_sql
                    )
                    flat_params: list[Any] = [
                        v for row in chunk for v in _bind_row(row, columns, json_cols)
                    ]
                    cur.execute(merge_sql, flat_params)
                    result.success += len(chunk)
                except Exception:
                    for offset, row in enumerate(chunk):
                        idx = chunk_start + offset
                        try:
                            using_sql = _merge_using_subquery(columns, schema_map, 1)
                            merge_sql = _build_merge_sql(
                                table_fq, columns, config.upsert_key, using_sql
                            )
                            cur.execute(merge_sql, _bind_row(row, columns, json_cols))
                            result.success += 1
                        except Exception as e:
                            record_row_error(
                                result,
                                idx,
                                str(row)[:200],
                                e,
                            )
                            if sync_options.on_error == "fail":
                                raise

        else:
            raise ValueError(f"Unsupported mode: {config.mode}")

        return result

    def _insert_rows(
        self,
        cur: Any,
        sql: str,
        records: list[dict[str, Any]],
        sync_options: SyncOptions,
        result: SyncResult,
        columns: list[str],
        json_cols: list[str],
    ) -> None:
        """Execute a parameterised INSERT per row, honouring ``on_error``.

        Values are ordered to ``columns`` and JSON columns (``json_cols``) are
        ``json.dumps``'d to feed the ``PARSE_JSON`` bind sites. Ordering by
        ``columns`` (via ``row.get`` in ``_bind_row``) keeps binds correct even
        when a source yields rows with a varying key order/set — so
        ``columns``/``json_cols`` are required, not an optional
        ``list(row.values())`` fallback (#699).
        """
        for i, row in enumerate(records):
            try:
                cur.execute(sql, _bind_row(row, columns, json_cols))
                result.success += 1
            except Exception as e:
                record_row_error(
                    result,
                    i,
                    str(row)[:200],
                    e,
                )
                if sync_options.on_error == "fail":
                    raise

    def _target_exists(self, cur: Any, config: SnowflakeDestinationConfig) -> bool:
        """Return True if the target table exists (``SHOW TABLES LIKE``)."""
        cur.execute(
            f"SHOW TABLES LIKE '{config.table}' IN SCHEMA {config.database}.{config.schema_}"
        )
        return bool(cur.fetchall())

    def _shadow_name(self, table: str) -> str:
        return f"{table}{_SWAP_SUFFIX}"

    def _swap_cursor_context(self, conn: Any, sync_options: SyncOptions) -> Any:
        return tagged_cursor(conn.cursor(), sync_options)

    def _complete_swap(
        self, conn: Any, cur: Any, table: str, shadow: str
    ) -> None:
        """Atomically exchange target/shadow, then drop the old shadow."""
        # Atomic exchange — preserves grants on the original name.
        # Snowflake autocommits, so the SWAP commits before the DROP
        # (mirrors the separate-transaction split in postgres.py).
        cur.execute(f"ALTER TABLE {table} SWAP WITH {shadow}")
        # SWAP succeeded — the replace is committed. Reset in-memory state
        # only now: a failed SWAP leaves it intact so the shadow stays
        # recoverable (`drt clean --orphans`) and a retry is still possible.
        self._reset_swap_state()
        # Best-effort cleanup of the now-old shadow.
        cur.execute(f"DROP TABLE {shadow}")

    def _reset_swap_state(self) -> None:
        super()._reset_swap_state()
        self._swap_direct_write = False

    def _reset_swap_state_after_completion(self) -> None:
        # A failed SWAP must retain state; successful SWAP resets inside
        # _complete_swap before the cleanup DROP.
        pass

    def _reset_swap_state_after_noop(self) -> None:
        self._swap_direct_write = False

    def _build_mirror_delete(
        self,
        table_fq: str,
        upsert_cols: list[str],
        keys: list[tuple[Any, ...]],
        scope_cols: list[str] | None = None,
        scopes: list[tuple[Any, ...]] | None = None,
        negate: bool = True,
    ) -> tuple[str, list[Any]]:
        """Build a mirror ``DELETE`` statement (#340 Step 4 / #687 / #692).

        The connector uses ``%s`` placeholders (same family as psycopg2 /
        pymysql), but Snowflake SQL does not auto-expand a tuple-of-tuples —
        so, mirroring the MySQL destination's approach (rather than
        Postgres's tuple auto-expansion), the placeholder list is always
        built explicitly:

        - single-column form: ``WHERE col [NOT] IN (%s, %s, ...)`` with a
          flat values list
        - composite form: ``WHERE (c1, c2) [NOT] IN ((%s, %s), (%s, %s), ...)``
          with the values flattened in row-major order

        ``scope_cols``/``scopes`` (#692, mirroring Postgres/MySQL's #687
        handling) prepend a ``scope IN (observed) AND`` clause in the same
        shape. ``negate`` selects destination-strategy (``NOT IN``, delete
        what's absent) vs. tracked-strategy (``IN``, delete exactly these
        keys).
        """
        op = "NOT IN" if negate else "IN"
        scope_clause = ""
        scope_params: list[Any] = []
        if scope_cols and scopes:
            if len(scope_cols) == 1:
                scope_placeholders = ", ".join(["%s"] * len(scopes))
                scope_clause = f"{scope_cols[0]} IN ({scope_placeholders}) AND "
                scope_params = [s[0] for s in scopes]
            else:
                scope_col_tuple = "(" + ", ".join(scope_cols) + ")"
                scope_row_placeholder = "(" + ", ".join(["%s"] * len(scope_cols)) + ")"
                scope_placeholders = ", ".join([scope_row_placeholder] * len(scopes))
                scope_clause = f"{scope_col_tuple} IN ({scope_placeholders}) AND "
                scope_params = [v for s in scopes for v in s]

        if len(upsert_cols) == 1:
            placeholders = ", ".join(["%s"] * len(keys))
            stmt = (
                f"DELETE FROM {table_fq} WHERE {scope_clause}{upsert_cols[0]} "
                f"{op} ({placeholders})"
            )
            params = [*scope_params, *(k[0] for k in keys)]
        else:
            col_tuple = "(" + ", ".join(upsert_cols) + ")"
            row_placeholder = "(" + ", ".join(["%s"] * len(upsert_cols)) + ")"
            placeholders = ", ".join([row_placeholder] * len(keys))
            stmt = f"DELETE FROM {table_fq} WHERE {scope_clause}{col_tuple} {op} ({placeholders})"
            params = [*scope_params, *(v for key in keys for v in key)]
        return stmt, params

    # --- mirror-template hooks (#720 phase 3) ----------------------------
    def _mirror_table_ident(self, config: Any) -> str:
        assert isinstance(config, SnowflakeDestinationConfig)
        return f"{config.database}.{config.schema_}.{config.table}"

    def _commit_mirror(self, conn: Any) -> None:
        # Snowflake's existing mirror path relies on connection autocommit.
        del conn

    def _state_table_ident(self, config: Any) -> tuple[Any, Any, Any]:
        from drt.destinations._mirror_state import STATE_TABLE

        assert isinstance(config, SnowflakeDestinationConfig)
        ident = f"{config.database}.{config.schema_}.{STATE_TABLE}"
        return ident, (config.database, config.schema_), ident

    def _state_table_exists(self, cur: Any, scope: Any, raw: str) -> bool:
        from drt.destinations._mirror_state import STATE_TABLE

        del raw
        database, schema = scope
        cur.execute(f"SHOW TABLES LIKE '{STATE_TABLE}' IN SCHEMA {database}.{schema}")
        return bool(cur.fetchall())

    def _create_state_table(self, cur: Any, ident: Any) -> None:
        cur.execute(
            f"CREATE TABLE IF NOT EXISTS {ident} ("
            "sync_name VARCHAR(255) NOT NULL, "
            "key_hash CHAR(64) NOT NULL, "
            "key_json VARCHAR NOT NULL, "
            "scope_spec VARCHAR, "
            "scope_key VARCHAR, "
            "PRIMARY KEY (sync_name, key_hash))"
        )

    def _state_scope_columns_exist(self, cur: Any, scope: Any, raw: str) -> bool:
        from drt.destinations._mirror_state import STATE_TABLE

        del raw
        database, schema = scope
        cur.execute(
            f"SELECT COUNT(*) FROM {database}.information_schema.columns "
            "WHERE table_schema = %s AND table_name = %s "
            "AND column_name IN ('SCOPE_SPEC', 'SCOPE_KEY')",
            [schema.upper(), STATE_TABLE.upper()],
        )
        row = cur.fetchone()
        return bool(row is not None and row[0] == 2)

    def _add_state_scope_columns(self, cur: Any, ident: Any) -> None:
        cur.execute(
            f"ALTER TABLE {ident} ADD COLUMN scope_spec VARCHAR, scope_key VARCHAR"
        )

    def _state_sql(self, template: str, ident: Any) -> Any:
        return template.format(ident)

    def _state_params(self, *values: Any) -> Any:
        return list(values)

    def _try_add_state_scope_columns(self, cur: Any, ident: Any) -> bool:
        # Snowflake autocommits and has no savepoints. A refused ALTER fails
        # independently and leaves the connection usable.
        try:
            self._add_state_scope_columns(cur, ident)
        except Exception:  # noqa: BLE001 — no ALTER privilege is a supported state
            return False
        return True

    def _stage_mirror_keys(
        self,
        cur: Any,
        config: Any,
        rows: list[tuple[str, str]],
    ) -> tuple[bool, str]:
        from drt.destinations._mirror_state import DIFF_STAGING_TABLE

        assert isinstance(config, SnowflakeDestinationConfig)
        ident = f"{config.database}.{config.schema_}.{DIFF_STAGING_TABLE}"
        try:
            cur.execute(
                f"CREATE TEMPORARY TABLE {ident} "
                "(key_hash VARCHAR(64), key_json VARCHAR)"
            )
            if rows:
                cur.executemany(
                    f"INSERT INTO {ident} (key_hash, key_json) VALUES (%s, %s)",
                    rows,
                )
        except Exception:  # noqa: BLE001 — no temporary-table privilege is supported
            return False, ident
        return True, ident

    def test_connection(self, config: DestinationConfig) -> None:
        """Test connectivity by establishing a connection and running SELECT 1."""
        assert isinstance(config, SnowflakeDestinationConfig)
        conn = self._connect(config)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
        finally:
            conn.close()

    # --- dialect hooks (#720 phase 1) -------------------------------------
    def _dialect_connect(
        self, config: Any, query_tags: dict[str, str] | None = None
    ) -> Any:
        assert isinstance(config, SnowflakeDestinationConfig)
        return self._connect(config, query_tags=query_tags)

    def _qualify_ident(self, name: str) -> str:
        # Snowflake's existing path uses unquoted, fully-qualified strings.
        return name

    def get_table_name(self, config: DestinationConfig) -> str:
        """Implements ``QueryableDestination`` (#469).

        Fully-qualified — the connection sets the database/schema context,
        but the FQN matches how this destination writes and is unambiguous
        for test / diff queries.
        """
        assert isinstance(config, SnowflakeDestinationConfig)
        return f"{config.database}.{config.schema_}.{config.table}"

    def execute_test_query(self, config: DestinationConfig, query: str) -> int:
        """Implements ``QueryableDestination`` (#469).

        Raises:
            Exception: If connection or query fails.
        """
        assert isinstance(config, SnowflakeDestinationConfig)
        conn = self._connect(config)
        try:
            with conn.cursor() as cur:
                cur.execute(query)
                result: Any = cur.fetchone()[0]
                return int(result)
        finally:
            conn.close()

    def list_orphan_swap_tables(
        self,
        config: DestinationConfig,
        base_table: str,
        older_than: timedelta | None = None,
    ) -> list[str]:
        """List leftover ``<table>__drt_swap`` shadow tables for ``base_table``.

        Used by ``drt clean --orphans``. Snowflake exposes no portable table
        creation timestamp, so ``older_than`` is accepted for Protocol
        compatibility but not applied. Scoped to the current sync's table so one
        sync never sees another sync's shadow.
        """
        assert isinstance(config, SnowflakeDestinationConfig)
        shadow_name = f"{base_table.rsplit('.', 1)[-1]}{_SWAP_SUFFIX}"
        conn = self._connect(config)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SHOW TABLES LIKE '{shadow_name}' IN SCHEMA {config.database}.{config.schema_}"
                )
                rows = cur.fetchall()
        finally:
            conn.close()
        if not rows:
            return []
        return [f"{config.database}.{config.schema_}.{shadow_name}"]

    def drop_orphan_swap_tables(
        self, config: DestinationConfig, tables: list[str]
    ) -> tuple[list[str], list[str]]:
        """Drop the given orphan swap tables; returns ``(dropped, failed)``.

        Safety: only names whose final component ends with ``__drt_swap`` are
        dropped; anything else is reported as failed without being touched.
        """
        assert isinstance(config, SnowflakeDestinationConfig)
        dropped: list[str] = []
        failed: list[str] = []
        conn = self._connect(config)
        try:
            with conn.cursor() as cur:
                for name in tables:
                    if not name or not name.split(".")[-1].endswith(_SWAP_SUFFIX):
                        failed.append(name)
                        continue
                    try:
                        cur.execute(f"DROP TABLE {name}")
                        dropped.append(name)
                    except Exception:
                        failed.append(name)
        finally:
            conn.close()
        return dropped, failed

    @classmethod
    def _connect(
        cls, config: SnowflakeDestinationConfig, *, query_tags: dict[str, str] | None = None
    ) -> Any:
        """Establish a connection to Snowflake.

        ``query_tags`` (#768) sets the session's ``QUERY_TAG`` — Snowflake's
        native cost-attribution mechanism, applied to every query the session
        runs. Same approach as the Snowflake source's ``_connect``.
        """
        try:
            import snowflake.connector
        except ImportError as e:
            raise ImportError(
                "Snowflake destination requires: pip install drt-core[snowflake]"
            ) from e

        account = resolve_env(None, config.account_env)
        user = resolve_env(None, config.user_env)
        private_key_pem = resolve_env(None, config.private_key_env)
        password = resolve_env(None, config.password_env)

        if not account or not user or not (private_key_pem or password):
            raise ValueError(
                "Missing Snowflake credentials. Check environment variables or secrets.toml."
            )

        # Key-pair auth (#737) wins over password — the SERVICE-user path for
        # accounts that enforce MFA on password sign-ins.
        auth: dict[str, Any] = {}
        if private_key_pem:
            from drt.config.credentials import load_snowflake_private_key

            auth["private_key"] = load_snowflake_private_key(
                private_key_pem,
                resolve_env(None, config.private_key_passphrase_env),
            )
        else:
            auth["password"] = password

        if query_tags:
            import json

            auth["session_parameters"] = {"QUERY_TAG": json.dumps(query_tags, sort_keys=True)}

        return snowflake.connector.connect(
            account=account,
            user=user,
            warehouse=config.warehouse,
            database=config.database,
            schema=config.schema_,
            **auth,
        )
