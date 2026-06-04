"""ClickHouse destination — insert rows into a ClickHouse table.

Uses clickhouse-connect for HTTP-based inserts. Each record is inserted
individually to enable row-level error tracking (consistent with the
PostgreSQL and MySQL destination pattern).

Deduplication is handled by ClickHouse's ReplacingMergeTree engine at merge
time — the destination performs simple INSERTs.

Supports ``sync.mode: replace`` (TRUNCATE TABLE → INSERT) and
``replace_strategy: swap`` (zero-downtime: build a shadow table via
``CREATE TABLE ... AS ...``, INSERT into the shadow, then atomically
``EXCHANGE TABLES`` in :meth:`finalize_sync`).

Also supports ``sync.mode: mirror`` (#340 Step 3): INSERT every source
row, then in :meth:`finalize_sync` issue a single ``ALTER TABLE ...
DELETE WHERE <upsert_key> NOT IN (<observed>)`` mutation that removes
destination rows whose key was not in the source. The mutation runs
with ``mutations_sync=1`` so it completes before the call returns.
Mutations rewrite affected parts and are expensive — mirror mode is
appropriate for small/medium reference tables, not for high-volume
fact tables.

Requires: pip install drt-core[clickhouse]

Example sync YAML:

    destination:
      type: clickhouse
      host_env: TARGET_CH_HOST
      database_env: TARGET_CH_DATABASE
      user_env: TARGET_CH_USER
      password_env: TARGET_CH_PASSWORD
      table: analytics_scores
"""

from __future__ import annotations

import json
from typing import Any

from drt.config.credentials import resolve_env
from drt.config.models import ClickHouseDestinationConfig, DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError


class ClickHouseDestination:
    """Insert records into a ClickHouse table.

    Implements ConnectionTestable via test_connection().
    """

    def __init__(self) -> None:
        self._replace_truncated: bool = False
        self._swap_shadow_created: bool = False
        self._swap_table: str | None = None
        # sync.mode: mirror (#340 Step 3) — accumulates upsert_key tuples seen
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
        assert isinstance(config, ClickHouseDestinationConfig)
        if not records:
            return SyncResult()

        client = self._connect(config)
        result = SyncResult()

        try:
            columns = list(records[0].keys())

            if (
                sync_options.mode == "replace"
                and sync_options.replace_strategy == "swap"
            ):
                result = self._load_replace_swap(
                    client,
                    records,
                    columns,
                    config.table,
                    sync_options,
                )
            else:
                if sync_options.mode == "replace" and not self._replace_truncated:
                    client.command(
                        f"TRUNCATE TABLE {self._quote_ident(config.table)}"
                    )
                    self._replace_truncated = True

                # sync.mode: mirror (#340 Step 3) — validate upsert_key
                # before any INSERT so a misconfigured sync fails fast
                # rather than after partially populating the table.
                if sync_options.mode == "mirror" and not config.upsert_key:
                    raise ValueError(
                        "sync.mode: mirror requires destination.upsert_key "
                        "(needed to identify which rows to DELETE)."
                    )

                # clickhouse-connect's client.insert(table=...) interpolates
                # the table raw into "INSERT INTO {table} ..." with no quoting
                # (see clickhouse_connect/driver/insert.py), so pre-quote here.
                table_q = self._quote_ident(config.table)
                # TODO: batch insert with fallback to row-by-row on error
                for i, record in enumerate(records):
                    try:
                        row = [[record.get(c) for c in columns]]
                        client.insert(table_q, row, column_names=columns)
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
                            return result
                        continue

                # sync.mode: mirror (#340 Step 3) — accumulate upsert_key
                # tuples for the finalize_sync DELETE pass. Only keys from
                # successfully-loaded records are tracked (failed records
                # don't count as "source state").
                if sync_options.mode == "mirror":
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
        finally:
            client.close()

        return result

    def _load_replace_swap(
        self,
        client: Any,
        records: list[dict[str, Any]],
        columns: list[str],
        table: str,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Build a shadow table per sync; atomic EXCHANGE happens in finalize_sync.

        ClickHouse's ``CREATE TABLE shadow AS original`` clones the engine,
        partitioning, ORDER BY, and column definitions. INSERTs go to the shadow
        until :meth:`finalize_sync` runs ``EXCHANGE TABLES`` (atomic since 21.8).
        """
        result = SyncResult()
        shadow = f"{table}__drt_swap"
        shadow_q = self._quote_ident(shadow)
        table_q = self._quote_ident(table)

        if not self._swap_shadow_created:
            client.command(f"DROP TABLE IF EXISTS {shadow_q}")
            client.command(f"CREATE TABLE {shadow_q} AS {table_q}")
            self._swap_shadow_created = True
            self._swap_table = table

        for i, record in enumerate(records):
            try:
                row = [[record.get(c) for c in columns]]
                client.insert(shadow_q, row, column_names=columns)
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
                    # Drop the partial shadow + reset state so finalize_sync()
                    # cannot EXCHANGE partial data into the live table.
                    # try/finally guarantees state reset even if DROP fails;
                    # at worst we leave an orphan shadow (tracked by #433).
                    try:
                        client.command(f"DROP TABLE IF EXISTS {shadow_q}")
                    finally:
                        self._swap_shadow_created = False
                        self._swap_table = None
                    return result
                continue

        return result

    def finalize_sync(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """End-of-sync hook: EXCHANGE for swap-replace, ALTER DELETE for mirror.

        - ``mode=replace, replace_strategy=swap``: atomic ``EXCHANGE TABLES``
          (existing behaviour). After the exchange the shadow table holds the
          OLD data, so we drop it. ``EXCHANGE TABLES`` is atomic in
          ClickHouse 21.8+.
        - ``mode=mirror`` (#340 Step 3): ``ALTER TABLE ... DELETE WHERE
          <upsert_key> NOT IN (<observed>)`` mutation that removes
          destination rows whose key was not in the source. Skipped if the
          source produced no batches with records — treats "no observation"
          as "don't delete anything" for safety.
        """
        if sync_options.mode == "mirror":
            result = self._finalize_mirror(config, sync_options)
            # Reset mirror state regardless of result so a re-run starts fresh.
            self._mirror_keys = None
            return result

        if not self._swap_shadow_created or self._swap_table is None:
            return None

        assert isinstance(config, ClickHouseDestinationConfig)
        table = self._swap_table
        shadow = f"{table}__drt_swap"
        table_q = self._quote_ident(table)
        shadow_q = self._quote_ident(shadow)

        client = self._connect(config)
        try:
            client.command(f"EXCHANGE TABLES {table_q} AND {shadow_q}")
            # Shadow now contains the OLD data — drop it.
            client.command(f"DROP TABLE {shadow_q}")
        finally:
            client.close()
            self._swap_shadow_created = False
            self._swap_table = None

        return SyncResult()

    def _finalize_mirror(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """``sync.mode: mirror`` end-of-sync DELETE pass (#340 Step 3).

        Deletes destination rows whose ``upsert_key`` tuple is not in the
        set of keys observed across all batches via an ``ALTER TABLE ...
        DELETE`` mutation. Runs with ``mutations_sync=1`` so the call
        blocks until the mutation finishes.

        Uses clickhouse_connect's native ``{name:Type}`` parameter
        substitution with ``Array(String)`` (single column) or
        ``Array(Tuple(String, ...))`` (composite). Both column references
        and parameter values are coerced with ``toString()`` so the
        comparison works regardless of the source column type — at the
        cost of skipping any index on the upsert_key column. Mirror mode
        is intended for small/medium reference tables where this is
        acceptable; the temp-table strategy (#340 follow-up) targets the
        high-cardinality case.

        Returns ``None`` when ``_mirror_keys`` is empty or ``None`` —
        treats "no batch with records was ever observed" as a signal to
        skip the DELETE entirely, so a transient empty source doesn't
        wipe the destination.
        """
        assert isinstance(config, ClickHouseDestinationConfig)
        if not self._mirror_keys:
            return None

        upsert_cols = config.upsert_key
        assert upsert_cols  # guarded in load()

        # Dedupe to keep the IN list compact when batches overlap.
        keys = list({tuple(k) for k in self._mirror_keys})
        table_q = self._quote_ident(config.table)

        client = self._connect(config)
        try:
            if len(upsert_cols) == 1:
                col_q = f"`{upsert_cols[0]}`"
                sql = (
                    f"ALTER TABLE {table_q} DELETE "
                    f"WHERE toString({col_q}) NOT IN {{keys:Array(String)}}"
                )
                params: dict[str, Any] = {
                    "keys": [str(k[0]) for k in keys]
                }
            else:
                col_tuple = (
                    "(" + ", ".join(f"toString(`{c}`)" for c in upsert_cols) + ")"
                )
                tuple_type = "Tuple(" + ", ".join(["String"] * len(upsert_cols)) + ")"
                sql = (
                    f"ALTER TABLE {table_q} DELETE "
                    f"WHERE {col_tuple} NOT IN {{keys:Array({tuple_type})}}"
                )
                params = {
                    "keys": [tuple(str(v) for v in k) for k in keys]
                }
            client.command(sql, parameters=params, settings={"mutations_sync": 1})
        finally:
            client.close()

        # SyncResult has no dedicated `deleted` field; future work tracks
        # this separately. Returning a bare SyncResult signals "finalize
        # ran successfully" to the engine without inflating success/failed.
        return SyncResult()

    @staticmethod
    def _quote_ident(table: str) -> str:
        """Backtick-quote a (possibly database-qualified) identifier.

        ``mydb.scores`` -> ``\\`mydb\\`.\\`scores\\```
        ``scores``      -> ``\\`scores\\```
        """
        if "." in table:
            return "`" + "`.`".join(table.split(".")) + "`"
        return f"`{table}`"

    def get_row_count(self, config: DestinationConfig) -> int:
        """Get the current row count from the destination table.

        Args:
            config: Destination configuration (must be ClickHouseDestinationConfig).

        Returns:
            Row count as integer.

        Raises:
            Exception: If connection or query fails.
        """
        assert isinstance(config, ClickHouseDestinationConfig)
        client = self._connect(config)
        try:
            result = client.query(
                f"SELECT COUNT(*) FROM {self._quote_ident(config.table)}"
            )
            # clickhouse_connect returns a QueryResult object
            # result.result_rows is a list of tuples
            if result.result_rows:
                return int(result.result_rows[0][0])
            return 0
        finally:
            client.close()

    def test_connection(self, config: DestinationConfig) -> None:
        """Test connectivity by establishing a connection and running SELECT 1."""
        assert isinstance(config, ClickHouseDestinationConfig)
        client = self._connect(config)
        try:
            client.command("SELECT 1")
        finally:
            client.close()

    @staticmethod
    def _connect(config: ClickHouseDestinationConfig) -> Any:
        try:
            import clickhouse_connect  # type: ignore[import-untyped]
        except ImportError as e:
            raise ImportError(
                "ClickHouse destination requires: pip install drt-core[clickhouse]"
            ) from e

        # Connection string takes precedence
        conn_str = (
            resolve_env(None, config.connection_string_env)
            if config.connection_string_env
            else None
        )
        if conn_str:
            return clickhouse_connect.get_client(dsn=conn_str)

        # Fall back to individual parameters
        host = resolve_env(config.host, config.host_env)
        database = resolve_env(config.database, config.database_env)
        user = resolve_env(config.user, config.user_env)
        password = resolve_env(config.password, config.password_env) or ""

        if not host:
            raise ValueError("ClickHouse destination: host could not be resolved.")
        if not database:
            raise ValueError("ClickHouse destination: database could not be resolved.")

        return clickhouse_connect.get_client(
            host=host,
            port=config.port,
            database=database,
            username=user or "default",
            password=password,
            secure=config.secure,
        )
