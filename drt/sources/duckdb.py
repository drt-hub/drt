"""DuckDB source implementation.

Great for local development and testing — no cloud credentials needed.
Works with local .duckdb files or in-memory databases.

Requires: pip install drt-core[duckdb]

Example ~/.drt/profiles.yml:
    local:
      type: duckdb
      database: ./data/warehouse.duckdb   # or :memory:
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from drt.config.credentials import DuckDBProfile, ProfileConfig


class DuckDBSource:
    """Extract records from a DuckDB database."""

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        assert isinstance(config, DuckDBProfile)
        try:
            import duckdb
        except ImportError as e:
            raise ImportError("DuckDB support requires: pip install drt-core[duckdb]") from e

        conn = duckdb.connect(config.database)
        try:
            result = conn.execute(query)
            columns = [desc[0] for desc in result.description]
            for row in result.fetchall():
                yield dict(zip(columns, row))
        finally:
            conn.close()

    def test_connection(self, config: ProfileConfig) -> bool:
        assert isinstance(config, DuckDBProfile)
        try:
            import duckdb

            conn = duckdb.connect(config.database)
            conn.execute("SELECT 1").fetchall()
            conn.close()
            return True
        except Exception:
            return False
