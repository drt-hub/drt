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
    """Insert records into a ClickHouse table."""

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
                    client.command(f"TRUNCATE TABLE {config.table}")
                    self._replace_truncated = True

                # TODO: batch insert with fallback to row-by-row on error
                for i, record in enumerate(records):
                    try:
                        row = [[record.get(c) for c in columns]]
                        client.insert(config.table, row, column_names=columns)
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

        if not self._swap_shadow_created:
            client.command(f"DROP TABLE IF EXISTS {shadow}")
            client.command(f"CREATE TABLE {shadow} AS {table}")
            self._swap_shadow_created = True
            self._swap_table = table

        for i, record in enumerate(records):
            try:
                row = [[record.get(c) for c in columns]]
                client.insert(shadow, row, column_names=columns)
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

        return result

    def finalize_sync(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult | None:
        """Atomic EXCHANGE: shadow contents become live; old data dropped.

        After ``EXCHANGE TABLES original AND shadow``, the shadow table now
        holds the OLD data, so we drop it. ``EXCHANGE TABLES`` is atomic in
        ClickHouse 21.8+.
        """
        if not self._swap_shadow_created or self._swap_table is None:
            return None

        assert isinstance(config, ClickHouseDestinationConfig)
        table = self._swap_table
        shadow = f"{table}__drt_swap"

        client = self._connect(config)
        try:
            client.command(f"EXCHANGE TABLES {table} AND {shadow}")
            # Shadow now contains the OLD data — drop it.
            client.command(f"DROP TABLE {shadow}")
        finally:
            client.close()
            self._swap_shadow_created = False
            self._swap_table = None

        return SyncResult()

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
            # Use backtick quoting for ClickHouse table identifiers
            escaped_table = (
                ".`".join(config.table.split("."))
                if "." in config.table
                else config.table
            )
            result = client.query(f"SELECT COUNT(*) FROM `{escaped_table}`")
            # clickhouse_connect returns a QueryResult object
            # result.result_rows is a list of tuples
            if result.result_rows:
                return int(result.result_rows[0][0])
            return 0
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
