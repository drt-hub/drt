"""Databricks Delta Lake destination — write data back to Databricks tables.

Supports:

- INSERT (append, ``config.mode: insert``)
- MERGE (upsert via Delta Lake's native ``MERGE INTO``, ``config.mode: merge``)
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

from typing import Any

from drt.config.credentials import resolve_env
from drt.config.models import DatabricksDestinationConfig, DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError


class DatabricksDestination:
    """Write records into Databricks Delta Lake tables."""

    def __init__(self) -> None:
        # sync.mode: mirror — accumulates upsert_key tuples seen across
        # batches so finalize_sync can DELETE missing rows. ``None`` means
        # mirror mode hasn't engaged yet (no batch with records); finalize
        # treats that as "skip DELETE" — safety against deleting
        # everything when the source produced no data.
        self._mirror_keys: list[tuple[Any, ...]] | None = None

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
        effective_mode = "merge" if is_mirror else config.mode

        try:
            with conn.cursor() as cur:
                columns = list(records[0].keys())
                col_list = ", ".join(columns)
                placeholders = ", ".join(["%s"] * len(columns))
                table_fq = f"{config.catalog}.{config.schema_}.{config.table}"

                if effective_mode == "insert":
                    sql = f"INSERT INTO {table_fq} ({col_list}) VALUES ({placeholders})"

                    for i, row in enumerate(records):
                        try:
                            cur.execute(sql, list(row.values()))
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

                    for i, row in enumerate(records):
                        try:
                            cur.execute(
                                f"INSERT INTO {staging_table} ({col_list}) VALUES ({placeholders})",
                                list(row.values()),
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

    def finalize_sync(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """End-of-sync hook: DELETE-missing for ``sync.mode: mirror``.

        Databricks has no swap-replace finalize path in this destination
        (no shadow tables), so this hook is mirror-only. Resets
        ``_mirror_keys`` after dispatch so a re-run starts fresh.
        """
        if sync_options.mode != "mirror":
            return None
        result = self._finalize_mirror(config, sync_options)
        self._mirror_keys = None
        return result

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
