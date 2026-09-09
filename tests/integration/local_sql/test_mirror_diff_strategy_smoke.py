"""End-to-end: mode: mirror + mirror.strategy: diff (#1110) against a real
Postgres source and destination.

Exercises the full path a mock cannot: the engine's private-attr smuggling
of removed keys from source-side extraction to destination-side
finalize_sync (drt/engine/sync.py -> drt/destinations/sql_base.py), and the
actual DELETE issued against a live table.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from drt.config.credentials import PostgresProfile
from drt.config.models import SyncConfig, SyncOptions
from drt.destinations.postgres import PostgresDestination
from drt.engine.sync import run_sync
from drt.sources.postgres import PostgresSource

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")


def test_mirror_diff_strategy_deletes_exactly_the_removed_rows(tmp_path: Path) -> None:
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer

    with postgres_container(
        "postgres:16-alpine",
        username="admin",
        password="adminpass",
        dbname="testdb",
        driver=None,
    ) as postgres:
        host = postgres.get_container_host_ip()
        port = int(postgres.get_exposed_port(5432))
        profile = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            with admin.cursor() as cur:
                cur.execute(
                    "CREATE TABLE source_users (id INTEGER, email TEXT); "
                    "INSERT INTO source_users VALUES "
                    "(1, 'a@x.com'), (2, 'b@x.com'), (3, 'c@x.com'); "
                    "CREATE TABLE dest_users (id INTEGER PRIMARY KEY, email TEXT)"
                )
            admin.commit()

            sync = SyncConfig(
                name="mirror_diff_sync",
                model="SELECT id, email FROM source_users",
                destination={
                    "type": "postgres",
                    "host": host,
                    "port": port,
                    "dbname": "testdb",
                    "user": "admin",
                    "password": "adminpass",
                    "table": "dest_users",
                    "upsert_key": ["id"],
                },
                sync=SyncOptions(
                    mode="mirror",
                    incremental_strategy="diff",
                    mirror={"strategy": "diff"},
                ),
            )

            # --- Run 1: first diff run, nothing to delete yet. ---
            result = run_sync(sync, PostgresSource(), PostgresDestination(), profile, tmp_path)
            assert result.success == 3
            assert result.diff_removed_keys == []

            with admin.cursor() as cur:
                cur.execute("SELECT id FROM dest_users ORDER BY id")
                assert [r[0] for r in cur.fetchall()] == [1, 2, 3]

            # --- Edit: row 2 is deleted source-side. ---
            with admin.cursor() as cur:
                cur.execute("DELETE FROM source_users WHERE id = 2")
            admin.commit()

            # --- Run 2: mirror.strategy: diff deletes exactly row 2. ---
            result = run_sync(sync, PostgresSource(), PostgresDestination(), profile, tmp_path)
            assert result.diff_removed_keys == [{"id": 2}]

            with admin.cursor() as cur:
                cur.execute("SELECT id FROM dest_users ORDER BY id")
                assert [r[0] for r in cur.fetchall()] == [1, 3]
        finally:
            admin.close()
