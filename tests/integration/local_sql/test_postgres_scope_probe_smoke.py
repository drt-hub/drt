"""Live Postgres regressions that a mock cursor cannot prove (#939)."""

from __future__ import annotations

import pytest

from drt.destinations.postgres import PostgresDestination

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")


def test_state_scope_columns_probe_follows_search_path() -> None:
    """The unqualified probe must inspect the table Postgres actually resolves."""
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer

    with postgres_container("postgres:16-alpine", driver=None) as postgres:
        conn = psycopg2.connect(postgres.get_connection_url())
        try:
            with conn.cursor() as cur:
                cur.execute("CREATE SCHEMA legacy")
                cur.execute("CREATE SCHEMA migrated")
                cur.execute(
                    "CREATE TABLE legacy._drt_synced_keys ("
                    "sync_name VARCHAR(255) NOT NULL, "
                    "key_hash CHAR(64) NOT NULL, "
                    "key_json TEXT NOT NULL, "
                    "PRIMARY KEY (sync_name, key_hash))"
                )
                cur.execute(
                    "CREATE TABLE migrated._drt_synced_keys "
                    "(LIKE legacy._drt_synced_keys INCLUDING ALL)"
                )
                cur.execute(
                    "ALTER TABLE migrated._drt_synced_keys "
                    "ADD COLUMN scope_spec TEXT, ADD COLUMN scope_key TEXT"
                )

                destination = PostgresDestination()
                raw = "_drt_synced_keys"

                cur.execute("SET search_path TO legacy, migrated")
                assert destination._state_scope_columns_exist(cur, None, raw) is False

                cur.execute("SET search_path TO migrated, legacy")
                assert destination._state_scope_columns_exist(cur, None, raw) is True
        finally:
            conn.close()
