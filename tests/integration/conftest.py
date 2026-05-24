"""Shared fixtures for integration tests."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from drt.config.credentials import BigQueryProfile, DuckDBProfile, ProfileConfig


class FakeSource:
    """A Source that yields pre-defined rows — no BigQuery required."""

    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict]:
        yield from self._rows

    def test_connection(self, config: ProfileConfig) -> bool:
        return True


@pytest.fixture
def profile() -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="test_project", dataset="test_dataset")


@pytest.fixture
def fake_source() -> FakeSource:
    return FakeSource(
        [
            {"id": 1, "name": "Alice", "email": "alice@example.com"},
            {"id": 2, "name": "Bob", "email": "bob@example.com"},
            {"id": 3, "name": "Carol", "email": "carol@example.com"},
        ]
    )


@pytest.fixture
def duckdb_with_users(tmp_path: Path) -> tuple[object, DuckDBProfile]:
    """Seed a DuckDB file with a `users` table and return (Source, Profile).

    Uses a file path under tmp_path (not `:memory:`) because each
    `DuckDBSource.extract()` call opens a fresh connection, and `:memory:`
    databases are not shared across connections.

    The seeded table is `users (id INTEGER, name VARCHAR, email VARCHAR)`
    with three rows. To drive a sync through the engine, use
    ``SyncConfig(model="ref('users')", ...)`` — the resolver turns that into
    ``SELECT * FROM users``.
    """
    duckdb = pytest.importorskip("duckdb")

    from drt.sources.duckdb import DuckDBSource

    db_path = str(tmp_path / "test.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR, email VARCHAR)")
        conn.executemany(
            "INSERT INTO users VALUES (?, ?, ?)",
            [
                (1, "Alice", "alice@example.com"),
                (2, "Bob", "bob@example.com"),
                (3, "Carol", "carol@example.com"),
            ],
        )
    finally:
        conn.close()

    return DuckDBSource(), DuckDBProfile(type="duckdb", database=db_path)
