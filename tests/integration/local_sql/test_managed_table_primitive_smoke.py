"""ManagedTableCapable against a real Postgres (#960, ADR 0005 step 3).

A mock cursor accepts any string — it can prove the orchestration calls the
right methods in the right order, but not that the DDL it issues is valid
against a real engine, or that a locked-down role genuinely never sees a
CREATE statement it has no privilege for. Both properties are exactly what
tracked mirror's own `local_sql_smoke` coverage (`test_tracked_mirror_no_ddl_smoke.py`,
#986) exists to catch that a unit test cannot.
"""

from __future__ import annotations

import pytest

from drt.config.credentials import PostgresProfile
from drt.sources.postgres import PostgresSource

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")


def test_ensure_managed_schema_with_no_create_privilege_and_preprovisioned_schema() -> None:
    """The escape hatch, live: an admin pre-creates the managed schema and
    grants the sync role no CREATE privilege on the database at all —
    `ensure_managed_schema()` must still succeed by detecting the schema
    already exists, never attempting the CREATE."""
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
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            with admin.cursor() as cur:
                cur.execute("CREATE SCHEMA _drt")
                cur.execute("CREATE USER retl_user WITH PASSWORD 'retlpass'")
                cur.execute("REVOKE CREATE ON DATABASE testdb FROM PUBLIC")
                cur.execute("GRANT USAGE ON SCHEMA _drt TO retl_user")
                # Prove the privilege really is absent, not just untested.
                cur.execute("SELECT has_database_privilege('retl_user', 'testdb', 'CREATE')")
                assert cur.fetchone() == (False,)
            admin.commit()

            config = PostgresProfile(
                type="postgres",
                host=host,
                port=port,
                dbname="testdb",
                user="retl_user",
                password="retlpass",
            )
            # Would raise InsufficientPrivilege if the CREATE SCHEMA
            # statement were ever actually issued against this role.
            PostgresSource().ensure_managed_schema(config)
        finally:
            admin.close()


def test_create_use_drop_is_a_clean_reversible_cycle() -> None:
    """ADR 0005 Decision 4: turning a warehouse-backed feature off must be a
    clean, symmetric undo — create the schema, create a table in it, drop
    the table, and confirm the schema (and the database as a whole) is left
    exactly as if the feature had never been enabled for that table."""
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

        # Off -> on.
        assert source.managed_table_exists(config, "_drt_probe") is False
        source.ensure_managed_schema(config)

        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            with admin.cursor() as cur:
                cur.execute("CREATE TABLE _drt._drt_probe (id INTEGER PRIMARY KEY)")
            admin.commit()
        finally:
            admin.close()

        assert source.managed_table_exists(config, "_drt_probe") is True

        # On -> off: the reversible half.
        source.drop_managed_table(config, "_drt_probe")
        assert source.managed_table_exists(config, "_drt_probe") is False

        # Dropping again (no consumer left) must not error, and must not
        # touch the schema itself — a second #960 consumer sharing the
        # schema must be unaffected by this one's cleanup.
        source.drop_managed_table(config, "_drt_probe")
        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            with admin.cursor() as cur:
                cur.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name = '_drt'")
                assert cur.fetchone() is not None
        finally:
            admin.close()


def test_managed_table_exists_is_case_sensitive_for_a_mixed_case_schema() -> None:
    """ensure_managed_schema() creates the schema case-sensitively via
    Identifier() — managed_table_exists() must resolve a mixed-case
    managed_schema the same way, not silently fold it to lowercase.

    A regression guard: an earlier implementation probed via
    ``to_regclass('schema.table')``, which parses its argument as an
    identifier and lowercases the unquoted schema name, so it never found a
    table in a mixed-case schema that had genuinely been created."""
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
            managed_schema="DrtManaged",
        )
        source = PostgresSource()
        source.ensure_managed_schema(config)

        admin = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            with admin.cursor() as cur:
                cur.execute('CREATE TABLE "DrtManaged"._drt_probe (id INTEGER PRIMARY KEY)')
            admin.commit()
        finally:
            admin.close()

        assert source.managed_table_exists(config, "_drt_probe") is True
