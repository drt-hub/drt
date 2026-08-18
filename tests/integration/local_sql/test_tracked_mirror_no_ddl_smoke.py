"""Tracked mirror with pre-provisioned state and no temporary-table DDL (#986)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from drt.config.credentials import BigQueryProfile
from drt.config.models import (
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    SyncConfig,
    SyncOptions,
)
from drt.destinations.mysql import MySQLDestination
from drt.destinations.postgres import PostgresDestination
from drt.engine.sync import run_sync
from drt.sources.fake import FakeSource

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
pymysql = pytest.importorskip("pymysql")
testcontainers_mysql = pytest.importorskip("testcontainers.mysql")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")


def _sync(destination: Any) -> SyncConfig:
    return SyncConfig(
        name="tracked_no_ddl",
        model="SELECT 1",
        destination=destination,
        sync=SyncOptions(mode="mirror", mirror={"strategy": "tracked"}),
    )


def _profile() -> BigQueryProfile:
    """FakeSource ignores its profile; use the smallest ubiquitous profile."""
    return BigQueryProfile(type="bigquery", project="test", dataset="test")


def _run_two_generations(
    destination: Any,
    implementation: Any,
    tmp_path: Path,
) -> None:
    first = run_sync(
        _sync(destination),
        FakeSource(
            [{"id": 1, "value": "one"}, {"id": 2, "value": "two"}, {"id": 3, "value": "three"}]
        ),
        implementation,
        _profile(),
        tmp_path,
    )
    assert first.success == 3

    second = run_sync(
        _sync(destination),
        FakeSource([{"id": 1, "value": "one"}, {"id": 2, "value": "two"}]),
        type(implementation)(),
        _profile(),
        tmp_path,
    )
    assert second.success == 2


def test_mysql_tracked_mirror_without_create_temporary_tables(tmp_path: Path) -> None:
    """Table-scoped grants are sufficient for baseline plus stale-row delete."""
    require_docker()
    mysql_container = testcontainers_mysql.MySqlContainer

    mysql = mysql_container(
        "mysql:8.4",
        username="root",
        password="rootpass",
        dbname="testdb",
        dialect="pymysql",
    ).with_env("MYSQL_ROOT_HOST", "%")
    with mysql:
        host = mysql.get_container_host_ip()
        port = int(mysql.get_exposed_port(3306))
        admin = pymysql.connect(
            host=host,
            port=port,
            user="root",
            password="rootpass",
            database="testdb",
            autocommit=True,
        )
        try:
            with admin.cursor() as cur:
                cur.execute("CREATE TABLE scores (id INT PRIMARY KEY, value VARCHAR(255))")
                cur.execute(
                    "CREATE TABLE _drt_synced_keys ("
                    "sync_name VARCHAR(255) NOT NULL, key_hash CHAR(64) NOT NULL, "
                    "key_json TEXT NOT NULL, PRIMARY KEY (sync_name, key_hash))"
                )
                cur.execute("CREATE USER 'retl_user'@'%' IDENTIFIED BY 'retlpass'")
                cur.execute(
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON testdb.scores TO 'retl_user'@'%'"
                )
                cur.execute(
                    "GRANT SELECT, INSERT, DELETE ON testdb._drt_synced_keys TO 'retl_user'@'%'"
                )
                cur.execute("SHOW GRANTS FOR 'retl_user'@'%'")
                grants = " ".join(row[0] for row in cur.fetchall())
                assert "CREATE TEMPORARY TABLES" not in grants

            config = MySQLDestinationConfig(
                type="mysql",
                host=host,
                port=port,
                dbname="testdb",
                user="retl_user",
                password="retlpass",
                table="scores",
                upsert_key=["id"],
                introspect_schema=False,
            )
            _run_two_generations(config, MySQLDestination(), tmp_path)

            with admin.cursor() as cur:
                cur.execute("SELECT id FROM scores ORDER BY id")
                assert cur.fetchall() == ((1,), (2,))
        finally:
            admin.close()


def test_postgres_tracked_mirror_without_temp_privilege(tmp_path: Path) -> None:
    """The savepoint recovers the transaction after PostgreSQL denies CREATE."""
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
            host=host,
            port=port,
            dbname="testdb",
            user="admin",
            password="adminpass",
        )
        try:
            with admin.cursor() as cur:
                cur.execute("CREATE TABLE public.scores (id INTEGER PRIMARY KEY, value TEXT)")
                cur.execute(
                    "CREATE TABLE public._drt_synced_keys ("
                    "sync_name VARCHAR(255) NOT NULL, key_hash CHAR(64) NOT NULL, "
                    "key_json TEXT NOT NULL, PRIMARY KEY (sync_name, key_hash))"
                )
                cur.execute("CREATE USER retl_user WITH PASSWORD 'retlpass'")
                cur.execute("GRANT SELECT, INSERT, UPDATE, DELETE ON public.scores TO retl_user")
                cur.execute("GRANT SELECT, INSERT, DELETE ON public._drt_synced_keys TO retl_user")
                cur.execute("REVOKE TEMPORARY ON DATABASE testdb FROM PUBLIC")
                cur.execute("REVOKE TEMPORARY ON DATABASE testdb FROM retl_user")
                cur.execute("SELECT has_database_privilege('retl_user', 'testdb', 'TEMPORARY')")
                assert cur.fetchone() == (False,)
            admin.commit()

            config = PostgresDestinationConfig(
                type="postgres",
                host=host,
                port=port,
                dbname="testdb",
                user="retl_user",
                password="retlpass",
                table="public.scores",
                upsert_key=["id"],
                introspect_schema=False,
            )
            _run_two_generations(config, PostgresDestination(), tmp_path)

            with admin.cursor() as cur:
                cur.execute("SELECT id FROM public.scores ORDER BY id")
                assert cur.fetchall() == [(1,), (2,)]
        finally:
            admin.close()
