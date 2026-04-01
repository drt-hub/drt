"""MySQL source implementation.

Requires: pip install drt-core[mysql]

Example ~/.drt/profiles.yml:
    mysql:
      type: mysql
      host: localhost
      port: 3306
      dbname: analytics
      user: analyst
      password_env: MYSQL_PASSWORD   # export MYSQL_PASSWORD=secret
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

from drt.config.credentials import MySQLProfile, ProfileConfig


class MySQLSource:
    """Extract records from a MySQL database."""

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        assert isinstance(config, MySQLProfile)
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
        assert isinstance(config, MySQLProfile)
        try:
            conn = self._connect(config)
            cur = conn.cursor()
            cur.execute("SELECT 1")
            conn.close()
            return True
        except Exception:
            return False

    def _connect(self, config: MySQLProfile) -> Any:
        try:
            import pymysql
        except ImportError as e:
            raise ImportError("MySQL support requires: pip install drt-core[mysql]") from e

        password = config.password or (
            os.environ.get(config.password_env) if config.password_env else None
        )
        return pymysql.connect(
            host=config.host,
            port=config.port,
            database=config.dbname,
            user=config.user,
            password=password,
            charset="utf8mb4",
        )