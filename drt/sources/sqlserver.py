"""SQL Server source using pymssql.

Requires: pip install drt-core[sqlserver]

Example ~/.drt/profiles.yml:
    sqlserver_prod:
      type: sqlserver
      host: db.example.com
      port: 1433
      database: analytics
      user: drt_reader
      password_env: SQLSERVER_PASSWORD
      schema: dbo
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from drt.config.credentials import ProfileConfig, SQLServerProfile, resolve_env


class SQLServerSource:
    """Extract records from a SQL Server database."""

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        assert isinstance(config, SQLServerProfile)
        conn = self._connect(config)
        try:
            cur = conn.cursor(as_dict=True)
            try:
                cur.execute(query)
                for row in cur.fetchall():
                    yield dict(row)
            finally:
                cur.close()
        finally:
            conn.close()

    def test_connection(self, config: ProfileConfig) -> bool:
        assert isinstance(config, SQLServerProfile)
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

    def _connect(self, config: SQLServerProfile) -> Any:
        password = resolve_env(config.password, config.password_env) or ""

        try:
            import pymssql
        except ImportError as e:
            raise ImportError("SQL Server support requires: pip install drt-core[sqlserver]") from e

        return pymssql.connect(
            server=config.host,
            port=config.port,
            user=config.user,
            password=password,
            database=config.database,
        )
