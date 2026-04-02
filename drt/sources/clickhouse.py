"""ClickHouse source implementation.

Requires: pip install drt-core[clickhouse]

Example ~/.drt/profiles.yml:
    ch:
      type: clickhouse
      host: localhost
      port: 8123
      database: default
      user: default
      password_env: CLICKHOUSE_PASSWORD   # export CLICKHOUSE_PASSWORD=secret
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

from drt.config.credentials import ClickHouseProfile, ProfileConfig


class ClickHouseSource:
    """Extract records from a ClickHouse database."""

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        assert isinstance(config, ClickHouseProfile)
        client = self._connect(config)
        try:
            result = client.query(query)
            # clickhouse_connect puts column names in result.column_names
            # and rows in result.result_rows
            columns = result.column_names
            for row in result.result_rows:
                yield dict(zip(columns, row))
        finally:
            client.close()

    def test_connection(self, config: ProfileConfig) -> bool:
        assert isinstance(config, ClickHouseProfile)
        try:
            client = self._connect(config)
            client.query("SELECT 1")
            client.close()
            return True
        except Exception:
            return False

    def _connect(self, config: ClickHouseProfile) -> Any:
        try:
            import clickhouse_connect
        except ImportError as e:
            raise ImportError(
                "ClickHouse support requires: pip install drt-core[clickhouse]"
            ) from e

        from drt.config.credentials import resolve_env

        password = resolve_env(config.password, config.password_env) or ""

        return clickhouse_connect.get_client(
            host=config.host,
            port=config.port,
            database=config.database,
            username=config.user,
            password=password,
        )
