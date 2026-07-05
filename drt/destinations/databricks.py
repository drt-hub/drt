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

_SWAP_SUFFIX = "__drt_swap"


def _value_clause(
    columns: list[str],
    category_map: dict[str, str] | None,
    ddls: dict[str, str] | None,
) -> tuple[str, list[str]]:
    """Build the per-row value clause for an INSERT and the JSON column list.

    Layer 3 (#317): json-category columns are wrapped so Databricks reconstructs
    the complex value from a JSON string —

    - STRUCT / ARRAY / MAP -> ``from_json(%s, '<ddl>')`` (DDL from ``information_schema``)
    - VARIANT -> ``parse_json(%s)`` (no DDL form)

    Scalars pass straight through (``%s``). Databricks — like Snowflake — won't
    accept these functions in a ``VALUES`` clause, so any wrapping switches the
    statement to the ``SELECT`` form (``INSERT INTO t (cols) SELECT %s,
    from_json(%s, '<ddl>')``). With no json columns the clause is the unchanged
    ``VALUES (%s, %s, ...)`` — byte-identical to pre-#317 behaviour.

    The DDL is interpolated as a literal (it comes verbatim from
    ``information_schema``, so there is no injection surface); the value itself
    stays a ``%s`` bind. Returns ``(clause, json_columns)`` where ``clause``
    already includes the ``VALUES (...)`` / ``SELECT ...`` keyword.
    """
    exprs: list[str] = []
    json_columns: list[str] = []
    # information_schema reports column names lower-cased; source record keys may
    # differ in case — fold both sides so wrapping fires for the common pipeline.
    cats = (
        {str(k).lower(): v for k, v in category_map.items()}
        if category_map is not None
        else None
    )
    ddl_map = (
        {str(k).lower(): v for k, v in ddls.items()} if ddls is not None else None
    )
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
                exprs.append(f"from_json(%s, '{safe_ddl}')")
            else:
                # VARIANT — no DDL form.
                exprs.append("parse_json(%s)")
        else:
            exprs.append("%s")
    if json_columns:
        return "SELECT " + ", ".join(exprs), json_columns
    return "VALUES (" + ", ".join(exprs) + ")", json_columns


def _bind_row(row: dict[str, Any], columns: list[str], json_columns: list[str]) -> list[Any]:
    """Order a row's values to ``columns``; ``json.dumps`` the json columns so the
    ``from_json`` / ``parse_json`` bind receives a JSON string."""
    js = set(json_columns)
    return [
        json.dumps(row.get(c), default=str) if c in js else row.get(c) for c in columns
    ]


class DatabricksDestination:
    """Write records into Databricks Delta Lake tables."""

    def __init__(self) -> None:
        # sync.mode: mirror — accumulates upsert_key tuples seen across
        # batches so finalize_sync can DELETE missing rows. ``None`` means
        # mirror mode hasn't engaged yet (no batch with records); finalize
        # treats that as "skip DELETE" — safety against deleting
        # everything when the source produced no data.
        self._mirror_keys: list[tuple[Any, ...]] | None = None

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

        Used to build ``from_json(%s, '<ddl>')``. VARIANT columns are absent (they
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
        conn = self._connect(config)

        # sync.mode: mirror forces the MERGE write path regardless of
        # config.mode — mirror semantics require upsert. Validate
        # upsert_key here so the misconfiguration is surfaced before any
        # row touches Databricks.
        is_mirror = sync_options.mode == "mirror"
        if is_mirror and not config.upsert_key:
            conn.close()
            raise ValueError(
                "sync.mode: mirror requires destination.upsert_key "
                "(needed to identify which rows to DELETE)."
            )
        # mirror.strategy: tracked (#686) is Postgres/MySQL-only for now —
        # fail fast rather than silently falling back to the destination
        # diff, which has different (co-writer-unsafe) delete semantics.
        if (
            is_mirror
            and sync_options.mirror is not None
            and (sync_options.mirror.strategy == "tracked" or sync_options.mirror.scope)
        ):
            conn.close()
            raise ValueError(
                "mirror.strategy: tracked / mirror.scope are not yet supported on databricks "
                "(supported: postgres, mysql — see #686 follow-ups)."
            )
        try:
            with conn.cursor() as cur:
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
                            cur, records, columns, config, table_fq, sync_options,
                            result, category_map, ddls,
                        )
                    else:
                        self._load_replace_truncate(
                            cur, records, columns, table_fq, sync_options, result,
                            category_map, ddls,
                        )
                    return result

                effective_mode = "merge" if is_mirror else config.mode
                col_list = ", ".join(columns)
                value_clause, json_cols = _value_clause(columns, category_map, ddls)

                if effective_mode == "insert":
                    sql = f"INSERT INTO {table_fq} ({col_list}) {value_clause}"

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

                    staging_sql = (
                        f"INSERT INTO {staging_table} ({col_list}) {value_clause}"
                    )
                    for i, row in enumerate(records):
                        try:
                            cur.execute(staging_sql, _bind_row(row, columns, json_cols))
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
                        failed_indices = {re.batch_index for re in result.row_errors}
                        for idx, record in enumerate(records):
                            if idx in failed_indices:
                                continue
                            self._mirror_keys.append(
                                tuple(record.get(k) for k in config.upsert_key)
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
                    f"CREATE OR REPLACE TABLE {shadow_fq} "
                    f"AS SELECT * FROM {table_fq} WHERE 1=0"
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
        columns: list[str] | None = None,
        json_cols: list[str] | None = None,
    ) -> None:
        """Execute a parameterised INSERT per row, honouring ``on_error``.

        When ``columns`` is given, values are ordered to it and json columns
        (``json_cols``) are ``json.dumps``'d so the ``from_json`` / ``parse_json``
        bind receives a JSON string (Layer 3, #317).
        """
        for i, row in enumerate(records):
            try:
                values = (
                    _bind_row(row, columns, json_cols or [])
                    if columns is not None
                    else list(row.values())
                )
                cur.execute(sql, values)
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

    def _target_exists(self, cur: Any, config: DatabricksDestinationConfig) -> bool:
        """Return True if the target table exists (``SHOW TABLES ... LIKE``)."""
        cur.execute(
            f"SHOW TABLES IN {config.catalog}.{config.schema_} "
            f"LIKE '{config.table}'"
        )
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
            return result

        if not self._swap_shadow_created or self._swap_table is None:
            # truncate-replace / insert / merge / swap-first-run — nothing to do.
            self._swap_direct_write = False
            return None

        assert isinstance(config, DatabricksDestinationConfig)
        table_fq = self._swap_table
        shadow_fq = f"{table_fq}{_SWAP_SUFFIX}"
        conn = self._connect(config)
        try:
            with conn.cursor() as cur:
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

    def _finalize_mirror(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """``sync.mode: mirror`` end-of-sync DELETE pass.

        Issues ``DELETE FROM <catalog>.<schema>.<table> WHERE key NOT IN
        (<observed>)`` against Databricks Delta. The connector uses
        ``pyformat`` placeholders, but Databricks SQL does not auto-expand
        a tuple-of-tuples — so the placeholder list is built explicitly,
        mirroring the Snowflake leg of #340.

        Returns ``None`` when ``_mirror_keys`` is empty or ``None`` —
        treats "no batch with records was ever observed" as a signal to
        skip the DELETE entirely, so a transient empty source doesn't
        wipe the destination.
        """
        assert isinstance(config, DatabricksDestinationConfig)
        if not self._mirror_keys:
            return None

        upsert_cols = config.upsert_key
        assert upsert_cols  # guarded in load()

        # Dedupe to keep the IN list compact when batches overlap.
        keys = list({tuple(k) for k in self._mirror_keys})
        table_fq = f"{config.catalog}.{config.schema_}.{config.table}"

        conn = self._connect(config)
        try:
            with conn.cursor() as cur:
                if len(upsert_cols) == 1:
                    placeholders = ", ".join(["%s"] * len(keys))
                    stmt = f"DELETE FROM {table_fq} WHERE {upsert_cols[0]} NOT IN ({placeholders})"
                    params: list[Any] = [k[0] for k in keys]
                else:
                    col_tuple = "(" + ", ".join(upsert_cols) + ")"
                    row_placeholder = "(" + ", ".join(["%s"] * len(upsert_cols)) + ")"
                    placeholders = ", ".join([row_placeholder] * len(keys))
                    stmt = f"DELETE FROM {table_fq} WHERE {col_tuple} NOT IN ({placeholders})"
                    params = [v for key in keys for v in key]
                cur.execute(stmt, params)
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
                    f"SHOW TABLES IN {config.catalog}.{config.schema_} "
                    f"LIKE '{shadow_name}'"
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

    def _connect(self, config: DatabricksDestinationConfig) -> Any:
        """Establish a connection to Databricks via SQL Connector."""
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

        return sql.connect(
            server_hostname=host,
            http_path=http_path,
            access_token=token,
        )
