"""SQLite source implementation.

A SQLite source connector using Python's built-in sqlite3.
No extra dependencies required — ideal for:
testing, prototyping, and local development.
Works with local .sqlite files or in-memory databases.

Example ~/.drt/profiles.yml:
    local:
      type: sqlite
      database: ./data/warehouse.sqlite   # or :memory:
"""

from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from typing import Any

from drt.config.credentials import ProfileConfig, SQLiteProfile


class SQLiteSource:
    """Extract records from a SQLite database."""

    def extract(
        self,
        query: str,
        config: ProfileConfig,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        # query_tags unused: local SQLite has no session/job tagging
        # primitive; the SQL comment already prepended to `query` by the
        # engine is this connector's only attribution (#768).
        if not isinstance(config, SQLiteProfile):
            raise TypeError("Expected SQLiteProfile")

        conn = sqlite3.connect(config.database)
        try:
            result = conn.execute(query)
            columns = [desc[0] for desc in result.description]
            # Streaming (#765): iterate the cursor in ``fetch_size`` batches
            # rather than materialising the result set. "It's a local file" is
            # not the same as "it's free" — the cost being removed is holding
            # every row as a Python object, which a local file incurs just as
            # readily as a remote warehouse. Measured figures:
            # docs/research/extraction-memory.md.
            result.arraysize = config.fetch_size
            for row in result:
                yield dict(zip(columns, row))
        finally:
            conn.close()

    def test_connection(self, config: ProfileConfig) -> bool:
        if not isinstance(config, SQLiteProfile):
            raise TypeError("Expected SQLiteProfile")
        try:
            conn = sqlite3.connect(config.database)
            conn.execute("SELECT 1").fetchall()
            conn.close()
            return True
        except Exception:
            return False
