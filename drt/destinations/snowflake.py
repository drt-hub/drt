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
from drt.destinations.row_errors import RowError
from drt.destinations.sql_utils import tagged_cursor

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


class SnowflakeDestination:
    """Write records into Snowflake tables."""

    def __init__(self) -> None:
        # sync.mode: mirror (#340 Step 4) — accumulates upsert_key tuples seen
        # across batches so finalize_sync can DELETE missing rows.
        # ``None`` means mirror mode hasn't engaged yet (no batch with
        # records); finalize_sync treats that as "skip DELETE" — safety
        # against deleting everything when the source produced no data.
        self._mirror_keys: list[tuple[Any, ...]] | None = None
        # mirror.scope (#692, mirroring #687) — distinct scope-column value
        # tuples observed across batches; the finalize DELETE (destination or
        # tracked strategy) is restricted to rows whose scope values are in
        # this set.
        self._mirror_scopes: set[tuple[Any, ...]] | None = None

        # sync.mode: replace (#434) — per-sync state, reused across batches.
        # ``_replace_truncated`` ensures TRUNCATE runs once for the truncate
        # strategy. ``_swap_shadow_created`` / ``_swap_table`` track the swap
        # shadow so finalize_sync can do the atomic SWAP. ``_swap_direct_write``
        # is the first-run fall-through: target table doesn't exist yet, so we
        # write straight to it and skip the swap.
        self._replace_truncated: bool = False
        self._swap_shadow_created: bool = False
        self._swap_table: str | None = None  # fully-qualified target name
        self._swap_direct_write: bool = False

        # Layer 3 (#317): INFORMATION_SCHEMA map, fetched once per table per sync.
        self._schema_cache: dict[str, dict[str, str] | None] = {}

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

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, SnowflakeDestinationConfig)
        if not records:
            return SyncResult()
        conn = self._connect(config, query_tags=sync_options._query_tags)
        result = SyncResult()

        # sync.mode: mirror forces the MERGE write path regardless of
        # config.mode — mirror semantics require upsert. Validate
        # upsert_key here so the misconfiguration is surfaced before any
        # row touches Snowflake.
        is_mirror = sync_options.mode == "mirror"
        # Reject an unserveable mirror config (missing upsert_key, or a
        # scope+tracked composition where scope isn't a subset of
        # upsert_key, #694) before writing; close the connection we just
        # opened before surfacing the error. tracked/scope themselves are
        # supported on Snowflake since #692.
        from drt.destinations.sql_utils import check_mirror_supported

        try:
            check_mirror_supported(config, sync_options, "snowflake", supports_tracked_scope=True)
        except ValueError:
            conn.close()
            raise
        if (
            is_mirror
            and sync_options.mirror is not None
            and sync_options.mirror.scope
        ):
            missing = [c for c in sync_options.mirror.scope if c not in records[0]]
            if missing:
                conn.close()
                raise ValueError(
                    "mirror.scope columns missing from the model output: "
                    f"{missing} (available: {sorted(records[0].keys())})"
                )
        try:
            with tagged_cursor(conn.cursor(), sync_options) as cur:
                columns = list(records[0].keys())
                table_fq = f"{config.database}.{config.schema_}.{config.table}"
                # Layer 3 (#317): map columns to type categories once per sync.
                schema_map = self._resolve_schema(config)

                # sync.mode: replace (#434) — full-table replace, dispatched
                # before the insert/merge/mirror write paths.
                if sync_options.mode == "replace":
                    if sync_options.replace_strategy == "swap":
                        self._load_replace_swap(
                            cur,
                            records,
                            columns,
                            config,
                            table_fq,
                            sync_options,
                            result,
                            schema_map,
                        )
                    else:
                        self._load_replace_truncate(
                            cur, records, columns, table_fq, sync_options, result, schema_map
                        )
                    return result

                effective_mode = "merge" if is_mirror else config.mode
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
                            result.failed += 1
                            result.row_errors.append(
                                RowError(
                                    batch_index=i,
                                    record_preview=str(row)[:200],
                                    http_status=None,
                                    error_message=str(e),
                                )
                            )
                            if sync_options.on_error == "fail":
                                raise

                elif effective_mode == "merge":
                    if not config.upsert_key:
                        raise ValueError("upsert_key is required for merge mode")

                    key_clause = " AND ".join(
                        [f"target.{k} = source.{k}" for k in config.upsert_key]
                    )

                    update_cols = [c for c in columns if c not in config.upsert_key]
                    update_clause = ", ".join([f"{c} = source.{c}" for c in update_cols])

                    insert_cols = col_list
                    insert_vals = ", ".join([f"source.{c}" for c in columns])

                    staging_table = f"TMP_{config.table.upper()}"

                    cur.execute(f"CREATE TEMP TABLE {staging_table} LIKE {table_fq}")

                    for i, row in enumerate(records):
                        try:
                            cur.execute(
                                f"""
                                INSERT INTO {staging_table} ({col_list})
                                {value_clause}
                                """,
                                _bind_row(row, columns, json_cols),
                            )
                        except Exception as e:
                            result.failed += 1
                            result.row_errors.append(
                                RowError(
                                    batch_index=i,
                                    record_preview=str(row)[:200],
                                    http_status=None,
                                    error_message=str(e),
                                )
                            )
                            if sync_options.on_error == "fail":
                                raise

                    matched_clause = (
                        f"WHEN MATCHED THEN UPDATE SET {update_clause}" if update_cols else ""
                    )

                    merge_sql = f"""
                        MERGE INTO {table_fq} target
                        USING {staging_table} source
                        ON {key_clause}
                        {matched_clause}
                        WHEN NOT MATCHED THEN INSERT ({insert_cols})
                        VALUES ({insert_vals})
                    """

                    cur.execute(merge_sql)
                    result.success += len(records) - result.failed

                    # sync.mode: mirror (#340 Step 4) — accumulate upsert_key
                    # tuples for the finalize_sync DELETE pass. Only keys from
                    # records that survived the staging INSERT count as
                    # "source state" — records whose batch_index landed in
                    # row_errors are skipped.
                    if is_mirror:
                        assert config.upsert_key  # guarded above
                        if self._mirror_keys is None:
                            self._mirror_keys = []
                        scope_cols = (
                            sync_options.mirror.scope
                            if sync_options.mirror is not None
                            else None
                        )
                        if scope_cols and self._mirror_scopes is None:
                            self._mirror_scopes = set()
                        failed_indices = {re.batch_index for re in result.row_errors}
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

                else:
                    raise ValueError(f"Unsupported mode: {config.mode}")

        finally:
            conn.close()

        return result

    def _load_replace_truncate(
        self,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        table_fq: str,
        sync_options: SyncOptions,
        result: SyncResult,
        schema_map: dict[str, str] | None = None,
    ) -> None:
        """``replace_strategy: truncate`` — TRUNCATE once, then INSERT rows."""
        if not self._replace_truncated:
            cur.execute(f"TRUNCATE TABLE {table_fq}")
            self._replace_truncated = True

        col_list = ", ".join(columns)
        value_clause, json_cols = _value_clause(columns, schema_map)
        sql = f"INSERT INTO {table_fq} ({col_list}) {value_clause}"
        self._insert_rows(cur, sql, records, sync_options, result, columns, json_cols)

    def _load_replace_swap(
        self,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        config: SnowflakeDestinationConfig,
        table_fq: str,
        sync_options: SyncOptions,
        result: SyncResult,
        schema_map: dict[str, str] | None = None,
    ) -> None:
        """``replace_strategy: swap`` — write to a shadow table; SWAP in finalize.

        First batch: if the target table doesn't exist yet, fall through to a
        direct write (no shadow, no swap). Otherwise build the shadow with
        ``CREATE OR REPLACE TABLE ... LIKE`` (carries clustering keys).
        """
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
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=i,
                        record_preview=str(row)[:200],
                        http_status=None,
                        error_message=str(e),
                    )
                )
                if sync_options.on_error == "fail":
                    raise

    def _target_exists(self, cur: Any, config: SnowflakeDestinationConfig) -> bool:
        """Return True if the target table exists (``SHOW TABLES LIKE``)."""
        cur.execute(
            f"SHOW TABLES LIKE '{config.table}' IN SCHEMA {config.database}.{config.schema_}"
        )
        return bool(cur.fetchall())

    def finalize_sync(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """End-of-sync hook: atomic SWAP for ``replace_strategy: swap`` (#434),
        DELETE-missing for ``sync.mode: mirror`` (#340 Step 4).

        - ``mode=mirror``: DELETE rows whose ``upsert_key`` wasn't observed.
        - ``mode=replace, replace_strategy=swap``: ``ALTER TABLE ... SWAP WITH``
          the shadow, then DROP the now-old shadow. Skipped when the first run
          wrote directly to the target (no shadow was built).

        Resets per-sync state after dispatch so a re-run starts fresh.
        """
        if sync_options.mode == "mirror":
            result = self._finalize_mirror(config, sync_options)
            self._mirror_keys = None
            self._mirror_scopes = None
            return result

        if not self._swap_shadow_created or self._swap_table is None:
            # truncate-replace / insert / merge / swap-first-run — nothing to do.
            self._swap_direct_write = False
            return None

        assert isinstance(config, SnowflakeDestinationConfig)
        table_fq = self._swap_table
        shadow_fq = f"{table_fq}{_SWAP_SUFFIX}"
        conn = self._connect(config, query_tags=sync_options._query_tags)
        try:
            with tagged_cursor(conn.cursor(), sync_options) as cur:
                # Atomic exchange — preserves grants on the original name.
                # Snowflake autocommits, so the SWAP commits before the DROP
                # (mirrors the separate-transaction split in postgres.py).
                cur.execute(f"ALTER TABLE {table_fq} SWAP WITH {shadow_fq}")
                # SWAP succeeded — the replace is committed. Reset in-memory
                # state only now: a failed SWAP leaves it intact so the shadow
                # stays recoverable (`drt clean --orphans`) and a retry is
                # still possible.
                self._swap_shadow_created = False
                self._swap_table = None
                self._swap_direct_write = False
                # Best-effort cleanup of the now-old shadow.
                cur.execute(f"DROP TABLE {shadow_fq}")
        finally:
            conn.close()
        return SyncResult()

    def _build_mirror_delete(
        self,
        table_fq: str,
        upsert_cols: list[str],
        keys: list[tuple[Any, ...]],
        scope_cols: list[str] | None,
        scopes: list[tuple[Any, ...]] | None,
        negate: bool,
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

    def _finalize_mirror(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """``sync.mode: mirror`` end-of-sync DELETE pass (#340 Step 4 / #687).

        Issues ``DELETE FROM <db>.<schema>.<table> WHERE key NOT IN
        (<observed>)`` against Snowflake, via :meth:`_build_mirror_delete`.

        ``mirror.strategy: tracked`` (#692) dispatches to
        :meth:`_finalize_mirror_tracked` instead — state-based diff rather
        than the whole-table diff below. Shares the empty-source guard, so
        a transient empty source also keeps a tracked baseline intact.

        Returns ``None`` when ``_mirror_keys`` is empty or ``None`` —
        treats "no batch with records was ever observed" as a signal to
        skip the DELETE entirely, so a transient empty source doesn't
        wipe the destination.
        """
        assert isinstance(config, SnowflakeDestinationConfig)
        if not self._mirror_keys:
            return None

        if sync_options.mirror is not None and sync_options.mirror.strategy == "tracked":
            return self._finalize_mirror_tracked(config, sync_options)

        upsert_cols = config.upsert_key
        assert upsert_cols  # guarded in load()

        # Dedupe to keep the IN list compact when batches overlap.
        keys = list({tuple(k) for k in self._mirror_keys})
        table_fq = f"{config.database}.{config.schema_}.{config.table}"

        # mirror.scope (#687/#692) — restrict the diff to rows under parents
        # this run actually observed. list(), not sorted() — scope values
        # may include None (unorderable).
        scope_cols = sync_options.mirror.scope if sync_options.mirror is not None else None
        scopes = list(self._mirror_scopes or set()) if scope_cols else None

        conn = self._connect(config, query_tags=sync_options._query_tags)
        try:
            with tagged_cursor(conn.cursor(), sync_options) as cur:
                stmt, params = self._build_mirror_delete(
                    table_fq, upsert_cols, keys, scope_cols, scopes, negate=True
                )
                cur.execute(stmt, params)
        finally:
            conn.close()

        # SyncResult has no dedicated `deleted` field; future work tracks
        # this separately. Returning a bare SyncResult signals "finalize
        # ran successfully" to the engine without inflating success/failed.
        return SyncResult()

    def _finalize_mirror_tracked(
        self, config: Any, sync_options: SyncOptions
    ) -> SyncResult | None:
        """``mirror.strategy: tracked`` (#692) — delete only rows drt synced.

        Snowflake counterpart of ``BaseSqlDestination._finalize_mirror_tracked``
        (Postgres/MySQL, #686/#694) — same algorithm, own connection/cursor
        and ``SHOW TABLES`` existence probe (Snowflake has no
        ``to_regclass``/``information_schema.tables`` equivalent as cheap as
        the one already used by ``_target_exists`` for the replace-swap path).

        Reads the previously-synced key set for this sync from the
        drt-managed ``_drt_synced_keys`` table (created lazily in the
        target's database/schema), deletes ``previous - current`` from the
        target, and rewrites the state to the current key set.

        First run (or lost state) baselines: record keys, delete nothing,
        WARN. Rows the application wrote are never candidates for deletion
        because they were never in the tracked set.

        ``mirror.scope`` + ``strategy: tracked`` (#694 part 1) prunes both
        the state read and the state rewrite to the observed scope — see
        ``BaseSqlDestination._finalize_mirror_tracked`` for the full
        rationale; the algorithm here is identical, just against Snowflake's
        own connection/cursor and explicit-placeholder DELETE shape.

        SQL-side diff (#694 part 2, same rationale/proof as the Postgres/
        MySQL implementation): this run's keys are staged into a scratch
        table and ``previous - current`` is computed with a ``NOT EXISTS``
        join against ``_drt_synced_keys`` in SQL, so a state table with
        millions of rows never gets read into Python just to compute a
        typically-small diff. Scope-filtering happens in Python afterward,
        on the (small) diff — mathematically equivalent to filtering the
        full previous set by scope first, since scope membership and
        current-membership are independent conditions. The old
        "read every untouched row so it can be reinserted unchanged" step
        is gone: untouched rows are simply never selected by either the
        diff query or the new-keys query.
        """
        import logging

        from drt.destinations._mirror_state import (
            DIFF_STAGING_TABLE,
            STATE_TABLE,
            decode_key,
            key_hash,
            key_json,
        )

        assert isinstance(config, SnowflakeDestinationConfig)
        sync_name = sync_options._sync_name or config.table
        current = list({tuple(k) for k in self._mirror_keys or []})
        upsert_cols = config.upsert_key
        assert upsert_cols  # guarded in load()
        table_fq = f"{config.database}.{config.schema_}.{config.table}"
        state_fq = f"{config.database}.{config.schema_}.{STATE_TABLE}"
        diff_table = f"{config.database}.{config.schema_}.{DIFF_STAGING_TABLE}"

        scope_cols = sync_options.mirror.scope if sync_options.mirror is not None else None
        scope_positions = [upsert_cols.index(c) for c in scope_cols] if scope_cols else None
        observed_scopes = set(self._mirror_scopes or set()) if scope_positions else None

        conn = self._connect(config, query_tags=sync_options._query_tags)
        try:
            with tagged_cursor(conn.cursor(), sync_options) as cur:
                # Pre-provisioning (mirrors #695): only CREATE when the state
                # table is genuinely absent, so a locked-down destination
                # user can run against one an admin created ahead of time.
                cur.execute(
                    f"SHOW TABLES LIKE '{STATE_TABLE}' IN SCHEMA {config.database}.{config.schema_}"
                )
                if not cur.fetchall():
                    cur.execute(
                        f"CREATE TABLE IF NOT EXISTS {state_fq} ("
                        "sync_name VARCHAR(255) NOT NULL, "
                        "key_hash CHAR(64) NOT NULL, "
                        "key_json VARCHAR NOT NULL, "
                        "PRIMARY KEY (sync_name, key_hash))"
                    )

                # Baseline check: a cheap existence probe, never a full read.
                cur.execute(
                    f"SELECT 1 FROM {state_fq} WHERE sync_name = %s LIMIT 1", [sync_name]
                )
                previous_exists = cur.fetchone() is not None

                # Snowflake has session-scoped TEMPORARY tables, same as
                # Postgres/MySQL — no manual DROP is strictly required, but
                # one is issued anyway for clarity.
                cur.execute(
                    f"CREATE TEMPORARY TABLE {diff_table} "
                    "(key_hash VARCHAR(64), key_json VARCHAR)"
                )
                if current:
                    cur.executemany(
                        f"INSERT INTO {diff_table} (key_hash, key_json) VALUES (%s, %s)",
                        [(key_hash(k), key_json(k)) for k in current],
                    )

                cur.execute(
                    f"SELECT s.key_hash, s.key_json FROM {state_fq} s WHERE s.sync_name = %s "
                    f"AND NOT EXISTS (SELECT 1 FROM {diff_table} c WHERE c.key_hash = s.key_hash)",
                    [sync_name],
                )
                raw_diff = cur.fetchall()

                if scope_positions is not None and observed_scopes is not None:
                    to_delete = [
                        decode_key(kj)
                        for _h, kj in raw_diff
                        if tuple(decode_key(kj)[p] for p in scope_positions) in observed_scopes
                    ]
                else:
                    to_delete = [decode_key(kj) for _h, kj in raw_diff]

                if to_delete:
                    stmt, params = self._build_mirror_delete(
                        table_fq, upsert_cols, to_delete, None, None, negate=False
                    )
                    cur.execute(stmt, params)
                elif not previous_exists:
                    logging.getLogger(__name__).warning(
                        "tracked mirror: no prior state for sync %r in %s — "
                        "baselining this run's %d key(s); no deletes this run.",
                        sync_name,
                        STATE_TABLE,
                        len(current),
                    )

                if to_delete:
                    cur.executemany(
                        f"DELETE FROM {state_fq} WHERE sync_name = %s AND key_hash = %s",
                        [(sync_name, key_hash(k)) for k in to_delete],
                    )
                cur.execute(
                    f"SELECT c.key_hash, c.key_json FROM {diff_table} c "
                    f"WHERE NOT EXISTS (SELECT 1 FROM {state_fq} s "
                    "WHERE s.sync_name = %s AND s.key_hash = c.key_hash)",
                    [sync_name],
                )
                to_insert = cur.fetchall()
                if to_insert:
                    cur.executemany(
                        f"INSERT INTO {state_fq} (sync_name, key_hash, key_json) "
                        "VALUES (%s, %s, %s)",
                        [(sync_name, h, kj) for h, kj in to_insert],
                    )
                cur.execute(f"DROP TABLE {diff_table}")
        finally:
            conn.close()

        return SyncResult()

    def test_connection(self, config: DestinationConfig) -> None:
        """Test connectivity by establishing a connection and running SELECT 1."""
        assert isinstance(config, SnowflakeDestinationConfig)
        conn = self._connect(config)
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
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
