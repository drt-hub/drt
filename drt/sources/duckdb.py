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
            # Streaming (#765): fetch in batches rather than materialising the
            # whole result set. "It's a local file" is not the same as "it's
            # free" — the cost being removed is holding every row as a Python
            # object, which a local file incurs just as readily as a remote
            # warehouse. Measured figures: docs/research/extraction-memory.md.
            #
            # An explicit fetchmany loop rather than iterating: DuckDB's result
            # object has no __iter__, unlike sqlite3/pymssql/snowflake.
            while True:
                batch = result.fetchmany(config.fetch_size)
                if not batch:
                    break
                for row in batch:
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
