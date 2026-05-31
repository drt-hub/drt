"""Snowflake destination — write data back to Snowflake tables.

Supports:
- INSERT (append, ``config.mode: insert``)
- MERGE (upsert using key columns, ``config.mode: merge``)
- ``sync.mode: mirror`` (#340 Step 4) — MERGE upsert, then end-of-sync
  ``DELETE FROM ... WHERE upsert_key NOT IN (observed)`` from
  :meth:`finalize_sync`. Mirror mode forces the MERGE write path
  regardless of ``config.mode``, so users only need to set
  ``destination.upsert_key`` and ``sync.mode: mirror``.

Install: snowflake-connector-python.
"""

from __future__ import annotations

from typing import Any

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, SnowflakeDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError


class SnowflakeDestination:
    """Write records into Snowflake tables."""

    def __init__(self) -> None:
        # sync.mode: mirror (#340 Step 4) — accumulates upsert_key tuples seen
        # across batches so finalize_sync can DELETE missing rows.
        # ``None`` means mirror mode hasn't engaged yet (no batch with
        # records); finalize_sync treats that as "skip DELETE" — safety
        # against deleting everything when the source produced no data.
        self._mirror_keys: list[tuple[Any, ...]] | None = None

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, SnowflakeDestinationConfig)
        if not records:
            return SyncResult()
        conn = self._connect(config)
        result = SyncResult()

        # sync.mode: mirror forces the MERGE write path regardless of
        # config.mode — mirror semantics require upsert. Validate
        # upsert_key here so the misconfiguration is surfaced before any
        # row touches Snowflake.
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

                table_fq = f"{config.database}.{config.schema_}.{config.table}"

                if effective_mode == "insert":
                    sql = f"""
                        INSERT INTO {table_fq} ({col_list})
                        VALUES ({placeholders})
                    """

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
                    update_clause = ", ".join(
                        [f"{c} = source.{c}" for c in update_cols]
                    )

                    insert_cols = col_list
                    insert_vals = ", ".join([f"source.{c}" for c in columns])

                    staging_table = f"TMP_{config.table.upper()}"

                    cur.execute(f"CREATE TEMP TABLE {staging_table} LIKE {table_fq}")

                    for i, row in enumerate(records):
                        try:
                            cur.execute(
                                f"""
                                INSERT INTO {staging_table} ({col_list})
                                VALUES ({placeholders})
                                """,
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
                        f"WHEN MATCHED THEN UPDATE SET {update_clause}"
                        if update_cols
                        else ""
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
                        failed_indices = {
                            re.batch_index for re in result.row_errors
                        }
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
        """End-of-sync hook: DELETE-missing for ``sync.mode: mirror`` (#340 Step 4).

        Snowflake has no swap-replace finalize path (no shadow tables in
        the current destination), so this hook is mirror-only. Resets
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
        """``sync.mode: mirror`` end-of-sync DELETE pass (#340 Step 4).

        Issues ``DELETE FROM <db>.<schema>.<table> WHERE key NOT IN
        (<observed>)`` against Snowflake. The connector uses ``%s``
        placeholders (same family as psycopg2 / pymysql), but Snowflake
        SQL does not auto-expand a tuple-of-tuples — so the placeholder
        list is built explicitly:

        - single-column form:
          ``WHERE col NOT IN (%s, %s, ...)`` with a flat values list
        - composite form:
          ``WHERE (c1, c2) NOT IN ((%s, %s), (%s, %s), ...)`` with the
          values flattened in row-major order

        Returns ``None`` when ``_mirror_keys`` is empty or ``None`` —
        treats "no batch with records was ever observed" as a signal to
        skip the DELETE entirely, so a transient empty source doesn't
        wipe the destination.
        """
        assert isinstance(config, SnowflakeDestinationConfig)
        if not self._mirror_keys:
            return None

        upsert_cols = config.upsert_key
        assert upsert_cols  # guarded in load()

        # Dedupe to keep the IN list compact when batches overlap.
        keys = list({tuple(k) for k in self._mirror_keys})
        table_fq = f"{config.database}.{config.schema_}.{config.table}"

        conn = self._connect(config)
        try:
            with conn.cursor() as cur:
                if len(upsert_cols) == 1:
                    placeholders = ", ".join(["%s"] * len(keys))
                    stmt = (
                        f"DELETE FROM {table_fq} "
                        f"WHERE {upsert_cols[0]} NOT IN ({placeholders})"
                    )
                    params: list[Any] = [k[0] for k in keys]
                else:
                    col_tuple = "(" + ", ".join(upsert_cols) + ")"
                    row_placeholder = "(" + ", ".join(["%s"] * len(upsert_cols)) + ")"
                    placeholders = ", ".join([row_placeholder] * len(keys))
                    stmt = (
                        f"DELETE FROM {table_fq} WHERE {col_tuple} "
                        f"NOT IN ({placeholders})"
                    )
                    params = [v for key in keys for v in key]
                cur.execute(stmt, params)
        finally:
            conn.close()

        # SyncResult has no dedicated `deleted` field; future work tracks
        # this separately. Returning a bare SyncResult signals "finalize
        # ran successfully" to the engine without inflating success/failed.
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

    def _connect(self, config: SnowflakeDestinationConfig) -> Any:
        """Establish a connection to Snowflake."""
        try:
            import snowflake.connector
        except ImportError as e:
            raise ImportError(
                "Snowflake destination requires: pip install drt-core[snowflake]"
            ) from e

        account = resolve_env(None, config.account_env)
        user = resolve_env(None, config.user_env)
        password = resolve_env(None, config.password_env)

        if not account or not user or not password:
            raise ValueError(
                "Missing Snowflake credentials. Check environment variables or secrets.toml."
            )

        return snowflake.connector.connect(
            account=account,
            user=user,
            password=password,
            warehouse=config.warehouse,
            database=config.database,
            schema=config.schema_,
        )