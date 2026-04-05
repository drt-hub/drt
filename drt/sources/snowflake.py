"""Snowflake source implementation.

Requires: pip install drt-core[snowflake]

Example ~/.drt/profiles.yml:
    snowflake_prod:
      type: snowflake
      account: xy12345.us-east-1
      user: analyst
      password_env: SNOWFLAKE_PASSWORD
      database: ANALYTICS
      schema: PUBLIC
      warehouse: COMPUTE_WH
      role: ANALYST_ROLE   # optional
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from drt.config.credentials import ProfileConfig, SnowflakeProfile, resolve_env


class SnowflakeSource:
    """Extract records from a Snowflake data warehouse."""

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        assert isinstance(config, SnowflakeProfile)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            try:
                cur.execute(query)
                columns = [desc[0] for desc in cur.description]
                for row in cur.fetchall():
                    yield dict(zip(columns, row))
            finally:
                cur.close()
        finally:
            conn.close()

    def test_connection(self, config: ProfileConfig) -> bool:
        assert isinstance(config, SnowflakeProfile)
        conn = None
        try:
            conn = self._connect(config)
            cur = conn.cursor()
            try:
                cur.execute("SELECT 1")
                return True
            finally:
                cur.close()
        except Exception:
            return False
        finally:
            if conn:
                conn.close()

    def _connect(self, config: SnowflakeProfile) -> Any:
        try:
            import snowflake.connector
        except ImportError as e:
            raise ImportError(
                "Snowflake support requires: pip install drt-core[snowflake]"
            ) from e

        password = resolve_env(config.password, config.password_env) or ""

        connect_args: dict[str, Any] = {
            "account": config.account,
            "user": config.user,
            "password": password,
            "database": config.database,
            "schema": config.schema,
            "warehouse": config.warehouse,
        }
        if config.role:
            connect_args["role"] = config.role

        return snowflake.connector.connect(**connect_args)
