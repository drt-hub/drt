"""Shared fixtures + credential gating for the DWH smoke harness (#674, part of #654).

These tests drive a *real* end-to-end sync into a live cloud warehouse
(Snowflake / Databricks / BigQuery) and read the rows back to confirm they
landed. They are the real-service complement to the mock-injected unit suites.

Gating contract (so this is a safe no-op for forks / contributors):
  * The connector driver is gated with ``pytest.importorskip`` — environments
    without ``drt-core[snowflake|databricks|bigquery]`` skip at collection.
  * Credentials come from ``DRT_SMOKE_*`` env vars. When they're absent the
    module skips, so a normal ``pytest`` run (and every fork) is green without
    ever touching a warehouse. The dwh-smoke workflow injects them from repo
    secrets so the checks run for real only on the upstream repo.

The source side is the same seeded DuckDB ``users`` table used by the rest of
the integration harness, so only the destination leg differs per warehouse.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest

from drt.config.credentials import DuckDBProfile
from drt.sources.duckdb import DuckDBSource

# Rows every smoke test seeds + expects to read back. Kept identical to the
# rest of tests/integration so the per-warehouse bodies stay copy-paste close.
SEED_ROWS = [
    (1, "Alice", "alice@example.com"),
    (2, "Bob", "bob@example.com"),
    (3, "Carol", "carol@example.com"),
]


def require_env(*names: str) -> dict[str, str]:
    """Return the requested env vars, or skip the test if any are unset.

    This is the credential gate: no ``DRT_SMOKE_*`` secrets -> graceful skip,
    so forks and the default ``pytest`` run never attempt a real connection.
    """
    missing = [n for n in names if not os.environ.get(n)]
    if missing:
        pytest.skip(
            "DWH smoke credentials not set: "
            + ", ".join(missing)
            + " (expected for forks / local runs without cloud secrets)."
        )
    return {n: os.environ[n] for n in names}


def unique_table(base: str) -> str:
    """A collision-free target table name so parallel/nightly runs don't clash."""
    return f"{base}_{uuid.uuid4().hex[:10]}"


def seed_duckdb_users(tmp_path: Path) -> tuple[DuckDBSource, DuckDBProfile]:
    """Seed a DuckDB ``users`` table and return ``(Source, Profile)``.

    Mirrors the ``duckdb_with_users`` fixture in the parent conftest, but lives
    here so the smoke package is self-contained.
    """
    duckdb = pytest.importorskip("duckdb")
    db_path = str(tmp_path / "smoke_source.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE TABLE users (id INTEGER, name VARCHAR, email VARCHAR)")
        conn.executemany("INSERT INTO users VALUES (?, ?, ?)", SEED_ROWS)
    finally:
        conn.close()
    return DuckDBSource(), DuckDBProfile(type="duckdb", database=db_path)


@pytest.fixture
def duckdb_users(tmp_path: Path) -> Iterator[tuple[DuckDBSource, DuckDBProfile]]:
    yield seed_duckdb_users(tmp_path)
