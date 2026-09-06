"""SnapshotDiffSource against a real Postgres (#755, ADR 0005 step 5).

A mock cursor accepts any SQL string — it cannot prove the JOIN/anti-join
structure is correct, nor catch the documented ``ROW(...)::text`` NULL-vs-
empty-string ambiguity this module's hash expression was deliberately built
to avoid (see ``_diff_hash_expr`` in ``drt/sources/postgres.py``). Both need
a live engine, same discipline as #960's ``test_managed_table_primitive_smoke.py``.
"""

from __future__ import annotations

import pytest

from drt.config.credentials import PostgresProfile
from drt.sources.postgres import PostgresSource

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")


def _seed(admin: object, sql: str) -> None:
    with admin.cursor() as cur:  # type: ignore[attr-defined]
        cur.execute(sql)
    admin.commit()  # type: ignore[attr-defined]


def test_diff_incremental_round_trip() -> None:
    """First run sends everything as added; a second run after edits
    classifies added / changed / removed correctly; a third run with no
    edits sees nothing."""
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
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        source = PostgresSource()
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            _seed(
                admin,
                "CREATE TABLE users (id INTEGER, email TEXT, plan TEXT); "
                "INSERT INTO users VALUES (1, 'a@x.com', 'free'), "
                "(2, 'b@x.com', 'free'), (3, 'c@x.com', 'pro')",
            )
            query = "SELECT id, email, plan FROM users"

            # --- Run 1: first run, nothing to diff against yet. ---
            result = source.extract_snapshot_diff(
                query,
                config,
                sync_name="users_sync",
                key_columns=["id"],
                hash_columns="all",
            )
            added = list(result.added)
            assert result.is_first_run is True
            assert list(result.changed) == []
            assert list(result.removed_keys) == []
            assert {r["id"] for r in added} == {1, 2, 3}
            source.commit_snapshot_diff(config, "users_sync")

            # --- Edit: row 1 changes plan, row 2 is deleted, row 4 is added. ---
            _seed(
                admin,
                "UPDATE users SET plan = 'pro' WHERE id = 1; "
                "DELETE FROM users WHERE id = 2; "
                "INSERT INTO users VALUES (4, 'd@x.com', 'free')",
            )

            # --- Run 2: classify against run 1's committed baseline. ---
            result = source.extract_snapshot_diff(
                query,
                config,
                sync_name="users_sync",
                key_columns=["id"],
                hash_columns="all",
            )
            assert result.is_first_run is False
            added = list(result.added)
            changed = list(result.changed)
            removed = list(result.removed_keys)
            assert [r["id"] for r in added] == [4]
            assert [r["id"] for r in changed] == [1]
            assert changed[0]["plan"] == "pro"
            assert removed == [{"id": 2}]
            source.commit_snapshot_diff(config, "users_sync")

            # --- Run 3: no edits since the last commit — nothing to report. ---
            result = source.extract_snapshot_diff(
                query,
                config,
                sync_name="users_sync",
                key_columns=["id"],
                hash_columns="all",
            )
            assert list(result.added) == []
            assert list(result.changed) == []
            assert list(result.removed_keys) == []
        finally:
            admin.close()


def test_uncommitted_run_rediffs_against_the_same_stale_baseline() -> None:
    """The engine only commits after a run with zero row failures (#755's
    conservative reconcile-on-next-run posture, mirroring #920/#955). Prove
    the source side actually supports that: calling extract twice with no
    commit between must produce the identical classification both times."""
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
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        source = PostgresSource()
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            _seed(
                admin,
                "CREATE TABLE users (id INTEGER, email TEXT); "
                "INSERT INTO users VALUES (1, 'a@x.com')",
            )
            query = "SELECT id, email FROM users"
            result = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns="all"
            )
            list(result.added)
            source.commit_snapshot_diff(config, "s")

            _seed(admin, "UPDATE users SET email = 'a2@x.com' WHERE id = 1")

            first = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns="all"
            )
            first_changed = [r["id"] for r in first.changed]
            list(first.added)
            list(first.removed_keys)
            # Simulate a failed run: no commit.

            second = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns="all"
            )
            second_changed = [r["id"] for r in second.changed]
            assert first_changed == second_changed == [1]
        finally:
            admin.close()


def test_null_to_empty_string_transition_is_detected_as_changed() -> None:
    """Postgres's ``ROW(...)::text`` cast renders a NULL column and an empty
    string identically for some column types — a naive row-hash built that
    way would silently miss this transition as 'unchanged'. The hash
    expression this module actually uses (coalesce+sentinel+separator, not
    ROW()::text) must not have that blind spot."""
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
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        source = PostgresSource()
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            _seed(
                admin,
                "CREATE TABLE users (id INTEGER, note TEXT); INSERT INTO users VALUES (1, NULL)",
            )
            query = "SELECT id, note FROM users"
            result = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns="all"
            )
            list(result.added)
            source.commit_snapshot_diff(config, "s")

            _seed(admin, "UPDATE users SET note = '' WHERE id = 1")

            result = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns="all"
            )
            changed = list(result.changed)
            assert [r["id"] for r in changed] == [1]
            assert changed[0]["note"] == ""
        finally:
            admin.close()


def test_explicit_hash_columns_ignores_columns_outside_the_list() -> None:
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
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        source = PostgresSource()
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            _seed(
                admin,
                "CREATE TABLE users (id INTEGER, email TEXT, last_seen TEXT); "
                "INSERT INTO users VALUES (1, 'a@x.com', 'monday')",
            )
            query = "SELECT id, email, last_seen FROM users"
            result = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns=["email"]
            )
            list(result.added)
            source.commit_snapshot_diff(config, "s")

            # last_seen changes every run but is not a hash column — must
            # NOT register as changed.
            _seed(admin, "UPDATE users SET last_seen = 'tuesday' WHERE id = 1")
            result = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns=["email"]
            )
            assert list(result.changed) == []
            source.commit_snapshot_diff(config, "s")

            # email IS a hash column — must register.
            _seed(admin, "UPDATE users SET email = 'new@x.com' WHERE id = 1")
            result = source.extract_snapshot_diff(
                query, config, sync_name="s", key_columns=["id"], hash_columns=["email"]
            )
            assert [r["id"] for r in result.changed] == [1]
        finally:
            admin.close()


def test_hash_columns_typo_raises_loudly() -> None:
    """A typo'd hash_columns entry must fail loudly, not silently narrow the
    hash and hide real changes as unchanged rows (the worst failure class:
    silent data staleness)."""
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
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        source = PostgresSource()
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            _seed(admin, "CREATE TABLE users (id INTEGER, email TEXT)")
            with pytest.raises(ValueError, match="check for a typo"):
                source.extract_snapshot_diff(
                    "SELECT id, email FROM users",
                    config,
                    sync_name="s",
                    key_columns=["id"],
                    hash_columns=["emial"],
                )
        finally:
            admin.close()


def test_missing_key_column_raises_loudly() -> None:
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
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        source = PostgresSource()
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            _seed(admin, "CREATE TABLE users (id INTEGER, email TEXT)")
            with pytest.raises(ValueError, match="upsert_key column"):
                source.extract_snapshot_diff(
                    "SELECT id, email FROM users",
                    config,
                    sync_name="s",
                    key_columns=["user_id"],
                    hash_columns="all",
                )
        finally:
            admin.close()


def test_commit_without_extract_is_a_noop() -> None:
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
        config = PostgresProfile(
            type="postgres",
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        source = PostgresSource()
        # Never called extract_snapshot_diff for this sync_name — must not raise.
        source.commit_snapshot_diff(config, "never_run")
