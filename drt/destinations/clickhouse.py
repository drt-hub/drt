"""ClickHouse destination — insert rows into a ClickHouse table.

Uses clickhouse-connect for HTTP-based inserts. Each record is inserted
individually to enable row-level error tracking (consistent with the
PostgreSQL and MySQL destination pattern).

Deduplication is handled by ClickHouse's ReplacingMergeTree engine at merge
time — the destination performs simple INSERTs.

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
