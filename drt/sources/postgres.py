"""PostgreSQL source implementation.

Requires: pip install drt-core[postgres]

Example ~/.drt/profiles.yml:
    pg:
      type: postgres
      host: localhost
      port: 5432
      dbname: analytics
      user: analyst
      password_env: PG_PASSWORD   # export PG_PASSWORD=secret
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

from drt.config.credentials import PostgresProfile, ProfileConfig


class PostgresSource:
    """Extract records from a PostgreSQL database."""

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        assert isinstance(config, PostgresProfile)
        conn = self._connect(config)
        try:
            cur = conn.cursor()
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            for row in cur.fetchall():
                yield dict(zip(columns, row))
        finally:
            conn.close()

    def test_connection(self, config: ProfileConfig) -> bool:
        assert isinstance(config, PostgresProfile)
        try:
            conn = self._connect(config)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.close()
            return True
        except Exception:
            return False

    def _connect(self, config: PostgresProfile) -> Any:
        try:
            import psycopg2
        except ImportError as e:
            raise ImportError("PostgreSQL support requires: pip install drt-core[postgres]") from e

        password = config.password or (
            os.environ.get(config.password_env) if config.password_env else None
        )
        return psycopg2.connect(
            host=config.host,
            port=config.port,
            dbname=config.dbname,
            user=config.user,
            password=password,
        )
