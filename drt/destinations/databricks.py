"""Databricks Delta Lake destination — write data back to Databricks tables.

Supports:

- INSERT (append, ``config.mode: insert``)
- MERGE (upsert via Delta Lake's native ``MERGE INTO``, ``config.mode: merge``)
- ``sync.mode: replace`` (#643) — full table replace, two strategies:
  - ``replace_strategy: truncate`` (default) — ``TRUNCATE`` (once) then INSERT.
  - ``replace_strategy: swap`` — stage into a shadow ``<table>__drt_swap``
    (``CREATE OR REPLACE TABLE ... AS SELECT * ... WHERE 1=0``), then an
    atomic ``INSERT OVERWRITE <target> SELECT * FROM <shadow>`` in
    :meth:`finalize_sync`. Delta has no ``ALTER TABLE ... SWAP WITH``;
    ``INSERT OVERWRITE`` is atomic via snapshot isolation and preserves the
    target table object (grants / properties / clustering). First run
    (target absent) falls through to a direct write.
- ``sync.mode: mirror`` (#340 family — Databricks leg) — MERGE upsert,
  then end-of-sync ``DELETE FROM ... WHERE upsert_key NOT IN (observed)``
  from :meth:`finalize_sync`. Mirror mode forces the MERGE write path
  regardless of ``config.mode``, so users only need to set
  ``destination.upsert_key`` and ``sync.mode: mirror``.

Naming: Unity Catalog three-part name ``catalog.schema.table``. For
workspaces still on Hive Metastore, set ``catalog: hive_metastore``.

Auth: Databricks SQL Connector — ``host_env`` / ``http_path_env`` /
``token_env`` resolved at runtime. The token-bearing principal needs
USAGE on the catalog and schema plus ``MODIFY`` on the target table
(plus ``CREATE`` on the schema for the merge-path Delta scratch table).

Install: ``pip install drt-core[databricks]`` (depends on
``databricks-sql-connector>=3.0``). The target table MUST be a Delta
Lake table for MERGE / mirror to work — non-Delta tables will fail at
``MERGE INTO`` time with a Databricks error.

Example sync YAML:

    destination:
      type: databricks
      host_env: DATABRICKS_HOST
      http_path_env: DATABRICKS_HTTP_PATH
      token_env: DATABRICKS_TOKEN
      catalog: main
      schema: default
      table: user_scores
      mode: merge
      upsert_key: [user_id]
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from drt.config.credentials import resolve_env
from drt.config.models import DatabricksDestinationConfig, DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.destinations.sql_utils import tagged_cursor

_SWAP_SUFFIX = "__drt_swap"


def _value_clause(
    columns: list[str],
    category_map: dict[str, str] | None,
    ddls: dict[str, str] | None,
) -> tuple[str, list[str]]:
    """Build the per-row value clause for an INSERT and the JSON column list.

    Layer 3 (#317): json-category columns are wrapped so Databricks reconstructs
    the complex value from a JSON string —

    - STRUCT / ARRAY / MAP -> ``from_json(?, '<ddl>')`` (DDL from ``information_schema``)
    - VARIANT -> ``parse_json(?)`` (no DDL form)

    Scalars pass straight through (``?``). Databricks — like Snowflake — won't
    accept these functions in a ``VALUES`` clause, so any wrapping switches the
    statement to the ``SELECT`` form (``INSERT INTO t (cols) SELECT ?,
    from_json(?, '<ddl>')``). With no json columns the clause is the unchanged
    ``VALUES (?, ?, ...)``.

    Markers are native ``?`` placeholders (databricks-sql-connector's default,
    server-side binding — #707). The DDL is interpolated as a literal (it comes
    verbatim from ``information_schema``, so there is no injection surface); the
    value itself stays a ``?`` bind. Returns ``(clause, json_columns)`` where
    ``clause`` already includes the ``VALUES (...)`` / ``SELECT ...`` keyword.
    """
    exprs: list[str] = []
    json_columns: list[str] = []
    # information_schema reports column names lower-cased; source record keys may
    # differ in case — fold both sides so wrapping fires for the common pipeline.
    cats = (
        {str(k).lower(): v for k, v in category_map.items()} if category_map is not None else None
    )
    ddl_map = {str(k).lower(): v for k, v in ddls.items()} if ddls is not None else None
    for col in columns:
        key = str(col).lower()
        if cats is not None and cats.get(key) == "json":
            json_columns.append(col)
            ddl = ddl_map.get(key) if ddl_map is not None else None
            if ddl:
                # STRUCT / ARRAY / MAP — reconstruct via the target DDL.
                # Escape single quotes so a pathological column DDL can't break
                # out of the string literal (defence-in-depth — the DDL already
                # comes verbatim from information_schema).
                safe_ddl = ddl.replace("'", "''")
                exprs.append(f"from_json(?, '{safe_ddl}')")
            else:
                # VARIANT — no DDL form.
                exprs.append("parse_json(?)")
        else:
            exprs.append("?")
    if json_columns:
        return "SELECT " + ", ".join(exprs), json_columns
    return "VALUES (" + ", ".join(exprs) + ")", json_columns


def _bind_row(row: dict[str, Any], columns: list[str], json_columns: list[str]) -> list[Any]:
    """Order a row's values to ``columns``; ``json.dumps`` the json columns so the
    ``from_json`` / ``parse_json`` bind receives a JSON string."""
    js = set(json_columns)
    return [json.dumps(row.get(c), default=str) if c in js else row.get(c) for c in columns]


# databricks-sql-connector's native paramstyle allows at most 255 parameter
# markers per statement — multi-row INSERT chunks must stay under it (#734).
_NATIVE_PARAM_LIMIT = 255


def _rows_per_chunk(n_cols: int) -> int:
    """How many rows fit in one multi-row INSERT under the native marker limit."""
    return max(1, _NATIVE_PARAM_LIMIT // max(1, n_cols))


class DatabricksDestination:
    """Write records into Databricks Delta Lake tables."""

    def __init__(self) -> None:
        # sync.mode: mirror — accumulates upsert_key tuples seen across
        # batches so finalize_sync can DELETE missing rows. ``None`` means
        # mirror mode hasn't engaged yet (no batch with records); finalize
        # treats that as "skip DELETE" — safety against deleting
        # everything when the source produced no data.
        self._mirror_keys: list[tuple[Any, ...]] | None = None
        # mirror.scope (#692, mirroring #687) — distinct scope-column value
        # tuples observed across batches; the finalize DELETE (destination or
        # tracked strategy) is restricted to rows whose scope values are in
        # this set.
        self._mirror_scopes: set[tuple[Any, ...]] | None = None

        # sync.mode: replace (#643) — per-sync state, reused across batches.
        # ``_replace_truncated`` ensures TRUNCATE runs once for the truncate
        # strategy. ``_swap_shadow_created`` / ``_swap_table`` track the swap
        # shadow so finalize_sync can do the atomic INSERT OVERWRITE.
        # ``_swap_direct_write`` is the first-run fall-through: target table
        # doesn't exist yet, so we write straight to it and skip the swap.
        self._replace_truncated: bool = False
        self._swap_shadow_created: bool = False
        self._swap_table: str | None = None  # fully-qualified target name
        self._swap_direct_write: bool = False

        # Layer 3 (#317): information_schema maps, fetched once per table per sync.
        # ``_schema_cache`` -> column category (json / scalar);
        # ``_ddl_cache`` -> STRUCT/ARRAY/MAP column -> full type DDL for from_json.
        self._schema_cache: dict[str, dict[str, str] | None] = {}
        self._ddl_cache: dict[str, dict[str, str] | None] = {}

    def _resolve_schema(self, config: DatabricksDestinationConfig) -> dict[str, str] | None:
        """Column -> type-category map for the target table, cached per sync.

        Returns ``None`` (Layer 3 inactive) when ``introspect_schema`` is off or
        introspection is unavailable.
        """
        if not config.introspect_schema:
            return None
        if config.table not in self._schema_cache:
            from drt.destinations.schema import describe_columns

            self._schema_cache[config.table] = describe_columns(config)
        return self._schema_cache[config.table]

    def _resolve_ddls(self, config: DatabricksDestinationConfig) -> dict[str, str] | None:
        """STRUCT/ARRAY/MAP column -> full type DDL, cached per sync.

        Used to build ``from_json(?, '<ddl>')``. VARIANT columns are absent (they
        load via ``parse_json``). ``None`` when introspection is off/unavailable.
        """
        if not config.introspect_schema:
            return None
        if config.table not in self._ddl_cache:
            from drt.destinations.schema import describe_databricks_ddls

            self._ddl_cache[config.table] = describe_databricks_ddls(config)
        return self._ddl_cache[config.table]

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, DatabricksDestinationConfig)
        if not records:
            # Empty-source short-circuit — no databricks import, no
            # warehouse call. Same shape as the other registered
            # destinations (empty-batch contract suite, #604-#606).
            return SyncResult()

        result = SyncResult()
        conn = self._connect(config, query_tags=sync_options._query_tags)

        # sync.mode: mirror forces the MERGE write path regardless of
        # config.mode — mirror semantics require upsert. Validate
        # upsert_key here so the misconfiguration is surfaced before any
        # row touches Databricks.
        is_mirror = sync_options.mode == "mirror"
        # Reject an unserveable mirror config (missing upsert_key, or a
        # scope+tracked composition where scope isn't a subset of
        # upsert_key, #694) before writing; close the connection we just
        # opened before surfacing the error. tracked/scope themselves are
        # supported on Databricks since #692.
        from drt.destinations.sql_utils import check_mirror_supported

        try:
            check_mirror_supported(config, sync_options, "databricks", supports_tracked_scope=True)
        except ValueError:
            conn.close()
            raise
        if is_mirror and sync_options.mirror is not None and sync_options.mirror.scope:
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
                table_fq = f"{config.catalog}.{config.schema_}.{config.table}"
                # Layer 3 (#317): map columns to type categories + json DDLs once
                # per sync (cached), then wrap json-category binds accordingly.
                category_map = self._resolve_schema(config)
                ddls = self._resolve_ddls(config)

                # sync.mode: replace (#643) — full-table replace, dispatched
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
                            category_map,
                            ddls,
                        )
                    else:
                        self._load_replace_truncate(
                            cur,
                            records,
                            columns,
                            table_fq,
                            sync_options,
                            result,
                            category_map,
                            ddls,
                        )
                    return result

                effective_mode = "merge" if is_mirror else config.mode
                col_list = ", ".join(columns)
                value_clause, json_cols = _value_clause(columns, category_map, ddls)

                if effective_mode == "insert":
                    sql = f"INSERT INTO {table_fq} ({col_list}) {value_clause}"
                    self._insert_rows(cur, sql, records, sync_options, result, columns, json_cols)

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

                    # Databricks Delta needs a relation on the USING
                    # side of MERGE. Delta doesn't have session-local
                    # temp tables (no `CREATE TEMP TABLE`), so we stage
                    # into a uniquely-named Delta scratch table in the
                    # target catalog.schema, then DROP it at the end.
                    # The token-bearing principal needs ``CREATE`` on
                    # the schema in addition to ``MODIFY`` on the
                    # target.
                    staging_table = (
                        f"{config.catalog}.{config.schema_}.__drt_staging_{config.table}"
                    )

                    cur.execute(
                        f"CREATE OR REPLACE TABLE {staging_table} "
                        f"AS SELECT * FROM {table_fq} WHERE 1=0"
                    )

                    staging_sql = f"INSERT INTO {staging_table} ({col_list}) {value_clause}"
                    # Staging success is accounted after the MERGE (success =
                    # len(records) - failed), so skip per-row success counting.
                    self._insert_rows(
                        cur,
                        staging_sql,
                        records,
                        sync_options,
                        result,
                        columns,
                        json_cols,
                        count_success=False,
                    )

                    matched_clause = (
                        f"WHEN MATCHED THEN UPDATE SET {update_clause}" if update_cols else ""
                    )

                    merge_sql = (
                        f"MERGE INTO {table_fq} target "
                        f"USING {staging_table} source "
                        f"ON {key_clause} "
                        f"{matched_clause} "
                        f"WHEN NOT MATCHED THEN INSERT ({insert_cols}) "
                        f"VALUES ({insert_vals})"
                    )
                    cur.execute(merge_sql)
                    result.success += len(records) - result.failed

                    # Clean up the staging Delta table so subsequent
                    # syncs don't trip over it (and so storage doesn't
                    # accumulate).
                    cur.execute(f"DROP TABLE IF EXISTS {staging_table}")

                    # sync.mode: mirror — accumulate upsert_key tuples
                    # for the finalize_sync DELETE pass. Only keys from
                    # records that survived the staging INSERT count as
                    # "source state" — failed records are skipped.
                    if is_mirror:
                        assert config.upsert_key
                        if self._mirror_keys is None:
                            self._mirror_keys = []
                        scope_cols = (
                            sync_options.mirror.scope if sync_options.mirror is not None else None
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
                                self._mirror_scopes.add(tuple(record.get(c) for c in scope_cols))

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
        category_map: dict[str, str] | None = None,
        ddls: dict[str, str] | None = None,
    ) -> None:
        """``replace_strategy: truncate`` — TRUNCATE once, then INSERT rows."""
        if not self._replace_truncated:
            cur.execute(f"TRUNCATE TABLE {table_fq}")
            self._replace_truncated = True

        col_list = ", ".join(columns)
        value_clause, json_cols = _value_clause(columns, category_map, ddls)
        sql = f"INSERT INTO {table_fq} ({col_list}) {value_clause}"
        self._insert_rows(cur, sql, records, sync_options, result, columns, json_cols)

    def _load_replace_swap(
        self,
        cur: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        config: DatabricksDestinationConfig,
        table_fq: str,
        sync_options: SyncOptions,
        result: SyncResult,
        category_map: dict[str, str] | None = None,
        ddls: dict[str, str] | None = None,
    ) -> None:
        """``replace_strategy: swap`` — stage to a shadow; INSERT OVERWRITE in finalize.

        First batch: if the target table doesn't exist yet, fall through to a
        direct write (no shadow, no swap). Otherwise build the shadow by cloning
        the target's schema into an empty Delta table.
        """
        shadow_fq = f"{table_fq}{_SWAP_SUFFIX}"

        if not self._swap_shadow_created and not self._swap_direct_write:
            if self._target_exists(cur, config):
                cur.execute(
                    f"CREATE OR REPLACE TABLE {shadow_fq} AS SELECT * FROM {table_fq} WHERE 1=0"
                )
                self._swap_shadow_created = True
                self._swap_table = table_fq
            else:
                # First run: nothing to swap against — write straight to target.
                self._swap_direct_write = True

        write_fq = table_fq if self._swap_direct_write else shadow_fq
        col_list = ", ".join(columns)
        value_clause, json_cols = _value_clause(columns, category_map, ddls)
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
        *,
        count_success: bool = True,
    ) -> None:
        """Execute a parameterised INSERT for ``records``, honouring ``on_error``.

        Values are ordered to ``columns`` and json columns (``json_cols``) are
        ``json.dumps``'d so the ``from_json`` / ``parse_json`` bind receives a
        JSON string (Layer 3, #317). Ordering by ``columns`` (via ``row.get`` in
        ``_bind_row``) keeps binds correct even when a source yields rows with a
        varying key order/set — so ``columns``/``json_cols`` are required, not an
        optional ``list(row.values())`` fallback (#699).

        Scalar-only loads (no json columns) are batched into multi-row
        ``VALUES (…), (…)`` statements sized to the native 255-marker limit —
        one warehouse round trip per chunk instead of per row (#734). A failed
        chunk lands nothing (a multi-row INSERT is atomic), so it is replayed
        row by row to keep exact per-row ``RowError`` attribution and
        ``on_error`` semantics. The ``SELECT``-form json inserts don't compose
        into multi-row ``VALUES`` and stay one statement per row.

        ``count_success=False`` skips ``result.success`` accounting — the MERGE
        staging path computes success after the merge instead.
        """
        if json_cols:
            self._insert_rows_one_by_one(
                cur,
                sql,
                records,
                sync_options,
                result,
                columns,
                json_cols,
                base_index=0,
                count_success=count_success,
            )
            return

        row_marker = "(" + ", ".join(["?"] * len(columns)) + ")"
        rows_per = _rows_per_chunk(len(columns))
        for start in range(0, len(records), rows_per):
            chunk = records[start : start + rows_per]
            if len(chunk) == 1:
                self._insert_rows_one_by_one(
                    cur,
                    sql,
                    chunk,
                    sync_options,
                    result,
                    columns,
                    json_cols,
                    base_index=start,
                    count_success=count_success,
                )
                continue
            # The single-row statement ends in ``VALUES {row_marker}`` — extend
            # it with one marker group per additional row in the chunk.
            chunk_sql = sql + ", " + ", ".join([row_marker] * (len(chunk) - 1))
            params: list[Any] = []
            for row in chunk:
                params.extend(_bind_row(row, columns, json_cols))
            try:
                cur.execute(chunk_sql, params)
                if count_success:
                    result.success += len(chunk)
            except Exception:
                self._insert_rows_one_by_one(
                    cur,
                    sql,
                    chunk,
                    sync_options,
                    result,
                    columns,
                    json_cols,
                    base_index=start,
                    count_success=count_success,
                )

    def _insert_rows_one_by_one(
        self,
        cur: Any,
        sql: str,
        records: list[dict[str, Any]],
        sync_options: SyncOptions,
        result: SyncResult,
        columns: list[str],
        json_cols: list[str],
        *,
        base_index: int,
        count_success: bool,
    ) -> None:
        """One INSERT per row — json ``SELECT``-form loads and chunk replay (#734)."""
        for i, row in enumerate(records):
            try:
                cur.execute(sql, _bind_row(row, columns, json_cols))
                if count_success:
                    result.success += 1
            except Exception as e:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=base_index + i,
                        record_preview=str(row)[:200],
                        http_status=None,
                        error_message=str(e),
                    )
                )
                if sync_options.on_error == "fail":
                    raise

    def _target_exists(self, cur: Any, config: DatabricksDestinationConfig) -> bool:
        """Return True if the target table exists (``SHOW TABLES ... LIKE``)."""
        cur.execute(f"SHOW TABLES IN {config.catalog}.{config.schema_} LIKE '{config.table}'")
        return bool(cur.fetchall())

    def finalize_sync(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """End-of-sync hook: atomic INSERT OVERWRITE for ``replace_strategy: swap``
        (#643), DELETE-missing for ``sync.mode: mirror``.

        - ``mode=mirror``: DELETE rows whose ``upsert_key`` wasn't observed.
        - ``mode=replace, replace_strategy=swap``: ``INSERT OVERWRITE`` the
          target from the shadow (atomic via Delta snapshot isolation; the
          target table object — grants / properties / clustering — is
          preserved), then DROP the shadow. Skipped when the first run wrote
          directly to the target (no shadow was built).

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

        assert isinstance(config, DatabricksDestinationConfig)
        table_fq = self._swap_table
        shadow_fq = f"{table_fq}{_SWAP_SUFFIX}"
        conn = self._connect(config, query_tags=sync_options._query_tags)
        try:
            with tagged_cursor(conn.cursor(), sync_options) as cur:
                # Atomic data overwrite — Delta snapshot isolation; the target
                # table object (grants / properties / clustering) is preserved.
                cur.execute(f"INSERT OVERWRITE {table_fq} SELECT * FROM {shadow_fq}")
                cur.execute(f"DROP TABLE IF EXISTS {shadow_fq}")
        finally:
            conn.close()
            self._swap_shadow_created = False
            self._swap_table = None
            self._swap_direct_write = False
        return SyncResult()

    def _delete_via_staged_keys(
        self,
        cur: Any,
        table_fq: str,
        upsert_cols: list[str],
        keys: list[tuple[Any, ...]],
        keys_table: str,
        scope_cols: list[str] | None,
        scopes: list[tuple[Any, ...]] | None,
        negate: bool,
    ) -> None:
        """Stage ``keys`` into a scratch Delta table, then DELETE from
        ``table_fq`` by set membership against it (#340 family / #687 / #692).

        Under native ``?`` binding (#707) the per-statement parameter limit
        rules out inlining every observed key into a single ``NOT IN (?, ?,
        ...)``/``IN (?, ?, ...)``, so the keys are staged (chunked multi-row
        INSERTs sized to the marker limit, #734) and removed via a subquery —
        ``DELETE ... WHERE key [NOT] IN (SELECT key FROM staging)`` binds
        *no* key parameters in the DELETE itself, so it scales past the limit
        regardless of how many keys were observed. Delta has no session-local
        temp tables, so the scratch table lives in the target's own
        catalog/schema (the principal needs ``CREATE`` there in addition to
        ``MODIFY`` on the target) and is dropped at the end.

        ``scope_cols``/``scopes`` (#692, mirroring the other three dialects'
        #687 handling) are inlined directly as an ``IN (?, ...)`` clause
        rather than staged — scope cardinality (distinct parents) is
        expected to stay well under the marker limit even when the key
        cardinality it restricts does not.
        """
        key_cols = ", ".join(upsert_cols)
        where_cols = upsert_cols[0] if len(upsert_cols) == 1 else f"({key_cols})"
        row_marker = "(" + ", ".join(["?"] * len(upsert_cols)) + ")"

        cur.execute(
            f"CREATE OR REPLACE TABLE {keys_table} AS SELECT {key_cols} FROM {table_fq} WHERE 1=0"
        )
        insert_key_prefix = f"INSERT INTO {keys_table} ({key_cols}) VALUES "
        rows_per = _rows_per_chunk(len(upsert_cols))
        for start in range(0, len(keys), rows_per):
            chunk = keys[start : start + rows_per]
            cur.execute(
                insert_key_prefix + ", ".join([row_marker] * len(chunk)),
                [v for key in chunk for v in key],
            )

        op = "NOT IN" if negate else "IN"

        # Scope restriction (#692). A *composite* scope cannot be written as
        # `(a, b) IN ((?, ?), ...)` on Delta — the same multi-column `IN`
        # restriction that breaks the key predicate below (#908) — so it is
        # expanded to OR-of-ANDs, which every dialect accepts. The
        # single-column form is left byte-identical: it works, it ships, and a
        # patch release is the wrong place to rewrite a working path.
        def _scope_cond(prefix: str) -> str:
            """The scope restriction, optionally column-qualified for MERGE."""
            if len(scope_cols or []) == 1:
                markers = ", ".join(["?"] * len(scopes or []))
                return f"{prefix}{(scope_cols or [''])[0]} IN ({markers})"
            one = "(" + " AND ".join(f"{prefix}{c} = ?" for c in scope_cols or []) + ")"
            return "(" + " OR ".join([one] * len(scopes or [])) + ")"

        scope_clause = ""
        scope_params: list[Any] = []
        if scope_cols and scopes:
            scope_params = (
                [s[0] for s in scopes] if len(scope_cols) == 1 else [v for s in scopes for v in s]
            )
            scope_clause = f"{_scope_cond('')} AND "

        if len(upsert_cols) == 1:
            delete_sql = (
                f"DELETE FROM {table_fq} WHERE {scope_clause}{where_cols} "
                f"{op} (SELECT {key_cols} FROM {keys_table})"
            )
        else:
            # #908: Delta rejects `(a, b) IN (SELECT a, b FROM …)` with
            # DELTA_UNSUPPORTED_MULTI_COL_IN_PREDICATE, so every mirror with a
            # composite upsert_key failed at the DELETE — including *every*
            # tracked+scoped run, since `scope` must be a subset of
            # `upsert_key` and a scope covering the whole key is degenerate.
            # MERGE is Delta's supported way to express a multi-column
            # anti-join delete: WHEN MATCHED for "delete exactly these keys"
            # (tracked), WHEN NOT MATCHED BY SOURCE for "delete what this run
            # did not observe" (plain/scoped mirror).
            #
            # Only the composite branch moves. The single-column path above is
            # the one that has been running in production since #707 and its
            # tests assert it binds no key parameters at all; leaving it
            # untouched keeps this fix to the shape that is actually broken.
            on_clause = " AND ".join(f"t.{c} = s.{c}" for c in upsert_cols)
            clause = "WHEN NOT MATCHED BY SOURCE" if negate else "WHEN MATCHED"
            extra = f" AND {_scope_cond('t.')}" if scope_cols and scopes else ""
            delete_sql = (
                f"MERGE INTO {table_fq} AS t USING {keys_table} AS s "
                f"ON {on_clause} {clause}{extra} THEN DELETE"
            )
        # No params arg at all when unscoped — byte-identical to the
        # pre-#692 call shape (existing tests assert the anti-join binds no
        # parameters), not just an empty list.
        if scope_params:
            cur.execute(delete_sql, scope_params)
        else:
            cur.execute(delete_sql)
        cur.execute(f"DROP TABLE IF EXISTS {keys_table}")

    def _finalize_mirror(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """``sync.mode: mirror`` end-of-sync DELETE pass (#687 scope added #692).

        Deletes rows whose ``upsert_key`` was not observed in the source, via
        :meth:`_delete_via_staged_keys`.

        ``mirror.strategy: tracked`` (#692) dispatches to
        :meth:`_finalize_mirror_tracked` instead — state-based diff rather
        than the whole-table diff below. Shares the empty-source guard, so
        a transient empty source also keeps a tracked baseline intact.

        Returns ``None`` when ``_mirror_keys`` is empty or ``None`` — treats "no
        batch with records was ever observed" as a signal to skip the DELETE
        entirely, so a transient empty source doesn't wipe the destination.
        """
        assert isinstance(config, DatabricksDestinationConfig)
        if not self._mirror_keys:
            return None

        if sync_options.mirror is not None and sync_options.mirror.strategy == "tracked":
            return self._finalize_mirror_tracked(config, sync_options)

        upsert_cols = config.upsert_key
        assert upsert_cols  # guarded in load()

        # Dedupe the observed keys.
        keys = list({tuple(k) for k in self._mirror_keys})
        table_fq = f"{config.catalog}.{config.schema_}.{config.table}"
        keys_table = f"{config.catalog}.{config.schema_}.__drt_mirror_keys_{config.table}"

        # mirror.scope (#687/#692) — restrict the diff to rows under parents
        # this run actually observed. list(), not sorted() — scope values
        # may include None (unorderable).
        scope_cols = sync_options.mirror.scope if sync_options.mirror is not None else None
        scopes = list(self._mirror_scopes or set()) if scope_cols else None

        conn = self._connect(config, query_tags=sync_options._query_tags)
        try:
            with tagged_cursor(conn.cursor(), sync_options) as cur:
                self._delete_via_staged_keys(
                    cur, table_fq, upsert_cols, keys, keys_table, scope_cols, scopes, negate=True
                )
        finally:
            conn.close()

        return SyncResult()

    def _finalize_mirror_tracked(self, config: Any, sync_options: SyncOptions) -> SyncResult | None:
        """``mirror.strategy: tracked`` (#692) — delete only rows drt synced.

        Same Census-style algorithm as ``BaseSqlDestination._finalize_mirror_tracked``
        (Postgres/MySQL, #686/#694) and the Snowflake/ClickHouse destinations'
        versions: reads the previously-synced key set for this sync from a
        drt-managed ``_drt_synced_keys`` table, deletes ``previous - current``
        from the target, and rewrites the state to the current key set.
        First run (or lost state) baselines: record keys, delete nothing, WARN.

        The target DELETE reuses :meth:`_delete_via_staged_keys` with
        ``negate=False`` — the same 255-marker-limit reasoning that already
        forced the destination-strategy path onto a staged anti-join applies
        identically here; a tracked table's stale-key list can just as
        easily exceed the limit.

        No cross-statement transaction — this is a Delta Lake platform
        limitation (each DML statement is its own Delta commit; nothing in
        this codebase wraps multiple statements into one transaction for
        Databricks), not an oversight this method could fix on its own.
        Unlike ClickHouse's version of this method (single, non-chunked
        state DELETE/INSERT statements, so a mid-sequence failure has one
        clean boundary), the state rewrite here is chunked (255-marker
        limit) into several independently-committed ``cur.execute()``
        calls. A failure partway through the state DELETE loop is harmless
        (a stale key deleted from state a second time next run is a
        no-op); a failure partway through the state INSERT loop silently
        and permanently loses tracking for whichever keys were in
        not-yet-executed chunks — the next run's "no prior state" baseline
        safety net does not catch this, since ``previous`` still reads back
        non-empty. Caught in review; not fixed here (would need Databricks
        to support genuine multi-statement transactions, which Delta
        doesn't).

        ``mirror.scope`` + ``strategy: tracked`` (#694 part 1) prunes both
        the state read and the state rewrite to the observed scope — see
        ``BaseSqlDestination._finalize_mirror_tracked`` for the full
        rationale; the algorithm here is identical, just against
        Databricks' own cursor and staged-delete primitives.

        SQL-side diff (#694 part 2, same rationale/proof as the other three
        dialects): this run's keys are staged into a second scratch table
        (``_mirror_state.DIFF_STAGING_TABLE``, distinct from
        :meth:`_delete_via_staged_keys`'s ``keys_table`` — that one's schema
        matches ``upsert_cols``' types for the target-table anti-join,
        this one is always ``(key_hash, key_json)`` for the state-table
        diff) and ``previous - current`` is computed with a ``NOT EXISTS``
        join against ``_drt_synced_keys`` in SQL, so a state table with
        millions of rows never gets read into Python just to compute a
        typically-small diff — **for unscoped tracked mirror**. Chunked
        inserts reuse ``_rows_per_chunk`` — the native ``?`` paramstyle's
        255-marker limit applies here exactly as it does to
        ``_delete_via_staged_keys``'s own staging inserts. Scope-filtering
        the diff in Python afterward is mathematically equivalent to
        filtering the full previous set by scope first (same proof as the
        base implementation) — but the diff query itself has no scope
        predicate, so a scoped run touching one of many historically-
        tracked scopes doesn't get the same memory win (#890). The old
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
            scope_key_json,
            scope_spec_json,
        )

        assert isinstance(config, DatabricksDestinationConfig)
        sync_name = sync_options._sync_name or config.table
        current = list({tuple(k) for k in self._mirror_keys or []})
        upsert_cols = config.upsert_key
        assert upsert_cols  # guarded in load()
        table_fq = f"{config.catalog}.{config.schema_}.{config.table}"
        state_fq = f"{config.catalog}.{config.schema_}.{STATE_TABLE}"
        keys_table = f"{config.catalog}.{config.schema_}.__drt_mirror_keys_{config.table}"
        # Suffixed by config.table, same as keys_table above — Databricks has
        # no session-local temp tables (see _delete_via_staged_keys), so this
        # is a real, shared, persistent object per catalog.schema. Without
        # the suffix, two tracked-mirror syncs targeting different tables in
        # the same catalog.schema running concurrently (`drt run --threads
        # N>1`) would share one scratch table and race on it — one thread's
        # CREATE OR REPLACE/INSERT could be clobbered mid-flight by another's,
        # corrupting raw_diff/to_insert for both (caught in review, #890
        # follow-up territory but fixed directly since it's a one-line fix).
        diff_table = f"{config.catalog}.{config.schema_}.{DIFF_STAGING_TABLE}_{config.table}"

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
                    f"SHOW TABLES IN {config.catalog}.{config.schema_} LIKE '{STATE_TABLE}'"
                )
                fresh_table = not cur.fetchall()
                if fresh_table:
                    cur.execute(
                        f"CREATE TABLE IF NOT EXISTS {state_fq} ("
                        "sync_name STRING, key_hash STRING, key_json STRING, "
                        "scope_spec STRING, scope_key STRING"
                        ") USING DELTA"
                    )

                # #890: scope columns let the diff be narrowed server-side.
                # Probed only for a scoped run — an unscoped sync gains nothing
                # and must not pay a probe, let alone DDL, for it.
                scope_spec = scope_spec_json(list(scope_cols)) if scope_cols else None
                scope_sql = False
                if scope_positions is not None:
                    if fresh_table:
                        scope_sql = True  # created with the columns
                    else:
                        # Unity Catalog exposes information_schema per catalog,
                        # the same shape drt/destinations/schema.py already uses
                        # for this dialect.
                        cur.execute(
                            f"SELECT COUNT(*) FROM {config.catalog}.information_schema.columns "
                            "WHERE table_schema = ? AND table_name = ? "
                            "AND column_name IN ('scope_spec', 'scope_key')",
                            [config.schema_, STATE_TABLE],
                        )
                        row = cur.fetchone()
                        if row is not None and row[0] == 2:
                            scope_sql = True
                        else:
                            # No SAVEPOINT, as on Snowflake/ClickHouse and
                            # unlike Postgres/MySQL — nothing here to poison.
                            # Delta spells this ADD COLUMNS (...), plural and
                            # parenthesised, unlike every other leg's ADD COLUMN.
                            try:
                                cur.execute(
                                    f"ALTER TABLE {state_fq} "
                                    "ADD COLUMNS (scope_spec STRING, scope_key STRING)"
                                )
                            except Exception:  # noqa: BLE001 — a supported state (#695)
                                pass
                            else:
                                scope_sql = True

                # Baseline check: a cheap existence probe, never a full read.
                cur.execute(f"SELECT 1 FROM {state_fq} WHERE sync_name = ? LIMIT 1", [sync_name])
                previous_exists = cur.fetchone() is not None

                cur.execute(
                    f"CREATE OR REPLACE TABLE {diff_table} "
                    "(key_hash STRING, key_json STRING) USING DELTA"
                )
                if current:
                    insert_prefix = f"INSERT INTO {diff_table} (key_hash, key_json) VALUES "
                    rows_per = _rows_per_chunk(2)
                    diff_rows = [[key_hash(k), key_json(k)] for k in current]
                    for start in range(0, len(diff_rows), rows_per):
                        chunk = diff_rows[start : start + rows_per]
                        cur.execute(
                            insert_prefix + ", ".join(["(?, ?)"] * len(chunk)),
                            [v for row in chunk for v in row],
                        )

                # #890, mirroring the other legs. The first two branches keep
                # this a purely *coarse* filter — every row they let through is
                # re-checked exactly by the Python filter below, so it can only
                # ever return too many rows, never too few:
                #   scope_key IS NULL → written before the columns existed
                #   scope_spec <> ... → written under a different mirror.scope
                diff_sql = (
                    f"SELECT s.key_hash, s.key_json FROM {state_fq} s WHERE s.sync_name = ? "
                    f"AND NOT EXISTS (SELECT 1 FROM {diff_table} c WHERE c.key_hash = s.key_hash)"
                )
                diff_params: list[Any] = [sync_name]
                if scope_sql and observed_scopes:
                    observed_json = sorted(key_json(sc) for sc in observed_scopes)
                    placeholders = ", ".join(["?"] * len(observed_json))
                    diff_sql += (
                        " AND (s.scope_key IS NULL OR s.scope_spec <> ? "
                        f"OR s.scope_key IN ({placeholders}))"
                    )
                    diff_params = [sync_name, scope_spec, *observed_json]
                cur.execute(diff_sql, diff_params)
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
                    self._delete_via_staged_keys(
                        cur,
                        table_fq,
                        upsert_cols,
                        to_delete,
                        keys_table,
                        None,
                        None,
                        negate=False,
                    )
                elif not previous_exists:
                    logging.getLogger(__name__).warning(
                        "tracked mirror: no prior state for sync %r in %s — "
                        "baselining this run's %d key(s); no deletes this run.",
                        sync_name,
                        STATE_TABLE,
                        len(current),
                    )

                if to_delete:
                    delete_prefix = f"DELETE FROM {state_fq} WHERE sync_name = ? AND key_hash IN ("
                    # -1 reserves the marker `sync_name` itself binds, on top
                    # of the chunk's hash markers, under the 255 limit.
                    rows_per = _rows_per_chunk(1) - 1
                    to_delete_hashes = [key_hash(k) for k in to_delete]
                    for start in range(0, len(to_delete_hashes), rows_per):
                        hash_chunk = to_delete_hashes[start : start + rows_per]
                        cur.execute(
                            delete_prefix + ", ".join(["?"] * len(hash_chunk)) + ")",
                            [sync_name, *hash_chunk],
                        )

                cur.execute(
                    f"SELECT c.key_hash, c.key_json FROM {diff_table} c "
                    f"WHERE NOT EXISTS (SELECT 1 FROM {state_fq} s "
                    "WHERE s.sync_name = ? AND s.key_hash = c.key_hash)",
                    [sync_name],
                )
                to_insert = cur.fetchall()
                if to_insert:
                    # Scoped runs record the scope alongside the key; unscoped
                    # ones (or a state table without the columns) leave them
                    # NULL, which the predicate above always lets through.
                    if scope_sql and scope_positions is not None:
                        cols, width = "sync_name, key_hash, key_json, scope_spec, scope_key", 5
                        state_rows = [
                            [
                                sync_name,
                                h,
                                kj,
                                scope_spec,
                                scope_key_json(decode_key(kj), scope_positions),
                            ]
                            for h, kj in to_insert
                        ]
                    else:
                        cols, width = "sync_name, key_hash, key_json", 3
                        state_rows = [[sync_name, h, kj] for h, kj in to_insert]
                    insert_prefix = f"INSERT INTO {state_fq} ({cols}) VALUES "
                    placeholder = "(" + ", ".join(["?"] * width) + ")"
                    rows_per = _rows_per_chunk(width)
                    for start in range(0, len(state_rows), rows_per):
                        chunk = state_rows[start : start + rows_per]
                        cur.execute(
                            insert_prefix + ", ".join([placeholder] * len(chunk)),
                            [v for row in chunk for v in row],
                        )

                cur.execute(f"DROP TABLE IF EXISTS {diff_table}")
        finally:
            conn.close()

        return SyncResult()

    def test_connection(self, config: DestinationConfig) -> None:
        """Test connectivity by establishing a connection and running ``SELECT 1``."""
        assert isinstance(config, DatabricksDestinationConfig)
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

        Used by ``drt clean --orphans``. ``older_than`` is accepted for Protocol
        compatibility but not applied. Scoped to the current sync's table so one
        sync never sees another sync's shadow.
        """
        assert isinstance(config, DatabricksDestinationConfig)
        shadow_name = f"{base_table.rsplit('.', 1)[-1]}{_SWAP_SUFFIX}"
        conn = self._connect(config)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SHOW TABLES IN {config.catalog}.{config.schema_} LIKE '{shadow_name}'"
                )
                rows = cur.fetchall()
        finally:
            conn.close()
        if not rows:
            return []
        return [f"{config.catalog}.{config.schema_}.{shadow_name}"]

    def drop_orphan_swap_tables(
        self, config: DestinationConfig, tables: list[str]
    ) -> tuple[list[str], list[str]]:
        """Drop the given orphan swap tables; returns ``(dropped, failed)``.

        Safety: only names whose final component ends with ``__drt_swap`` are
        dropped; anything else is reported as failed without being touched.
        """
        assert isinstance(config, DatabricksDestinationConfig)
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
        cls, config: DatabricksDestinationConfig, *, query_tags: dict[str, str] | None = None
    ) -> Any:
        """Establish a connection to Databricks via SQL Connector.

        ``query_tags`` (#768) passes straight through to the driver's native
        ``query_tags`` connect kwarg (serialized into a ``QUERY_TAGS`` session
        config, applied to every query the session runs). Same approach as
        the Databricks source's ``_connect``.
        """
        try:
            from databricks import sql  # type: ignore[import-untyped]
        except ImportError as e:
            raise ImportError(
                "Databricks destination requires: pip install drt-core[databricks]"
            ) from e

        host = resolve_env(None, config.host_env)
        http_path = resolve_env(None, config.http_path_env)
        token = resolve_env(None, config.token_env)

        if not host or not http_path or not token:
            raise ValueError(
                "Missing Databricks credentials. Check environment variables "
                f"({config.host_env}, {config.http_path_env}, {config.token_env})."
            )

        # This destination binds with native `?` placeholders (#707) —
        # databricks-sql-connector >=3.0's default paramstyle (server-side
        # binding). No `use_inline_params` opt-in: its client-side inline
        # rendering is deprecated upstream and carries an escaping-based
        # injection surface that native binding removes.
        connect_args: dict[str, Any] = {
            "server_hostname": host,
            "http_path": http_path,
            "access_token": token,
        }
        if query_tags:
            connect_args["query_tags"] = query_tags

        return sql.connect(**connect_args)
