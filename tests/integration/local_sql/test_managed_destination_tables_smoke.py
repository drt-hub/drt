"""Live SQL validity for first-release managed destination tables (#760)."""

from __future__ import annotations

import pytest

from drt.config.models import (
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    SyncConfig,
    SyncOptions,
)
from drt.destinations.mysql import MySQLDestination
from drt.destinations.postgres import PostgresDestination
from drt.engine.contract import declared_destination_types

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
pymysql = pytest.importorskip("pymysql")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")
testcontainers_mysql = pytest.importorskip("testcontainers.mysql")


def test_postgres_create_append_and_hostile_identifier_are_live_valid() -> None:
    require_docker()
    hostile = 'profile.name"; DROP TABLE audit; --'
    with testcontainers_postgres.PostgresContainer(
        "postgres:16-alpine",
        username="admin",
        password="adminpass",
        dbname="testdb",
        driver=None,
    ) as pg:
        host, port = pg.get_container_host_ip(), int(pg.get_exposed_port(5432))
        common = {
            "type": "postgres",
            "host": host,
            "port": port,
            "dbname": "testdb",
            "user": "admin",
            "password": "adminpass",
            "table": "managed_users",
            "upsert_key": ["id"],
        }
        create_options = SyncOptions()
        declared = SyncConfig.model_validate(
            {
                "name": "typed_postgres",
                "model": "select 1",
                "destination": {**common, "type": "postgres"},
                "columns": [
                    {"name": "id", "type": "integer"},
                    {"name": hostile, "type": "string"},
                ],
            }
        )
        create_options._source_column_types = declared_destination_types(
            declared.columns, declared.sync.field_mappings
        )
        created = PostgresDestination().load(
            [{"id": "1", hostile: "Ada"}],
            PostgresDestinationConfig(**common, manage_table=True),
            create_options,
        )
        assert created.success == 1
        assert created.ddl == [
            'CREATE TABLE IF NOT EXISTS "managed_users" '
            '("id" BIGINT NOT NULL, "profile.name""; DROP TABLE audit; --" TEXT, '
            'PRIMARY KEY ("id"))'
        ]

        append_options = SyncOptions()
        append_options._source_column_types = {"score": "number"}
        appended = PostgresDestination().load(
            [{"id": "1", hostile: "Ada Lovelace", "score": "10"}],
            PostgresDestinationConfig(
                **common,
                manage_table=False,
                on_schema_change="append_new_columns",
            ),
            append_options,
        )
        assert appended.success == 1
        assert appended.ddl == [
            'ALTER TABLE "managed_users" ADD COLUMN IF NOT EXISTS "score" NUMERIC NULL'
        ]

        conn = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            with conn.cursor() as cur:
                from psycopg2 import sql

                cur.execute(
                    sql.SQL("SELECT id, {}, score FROM managed_users").format(
                        sql.Identifier(hostile)
                    )
                )
                assert cur.fetchall() == [(1, "Ada Lovelace", 10)]
        finally:
            conn.close()


def test_mysql_create_append_and_hostile_identifier_are_live_valid() -> None:
    require_docker()
    hostile = "profile.name`; DROP TABLE audit; --"
    with testcontainers_mysql.MySqlContainer("mysql:8.4") as mysql:
        host, port = mysql.get_container_host_ip(), int(mysql.get_exposed_port(3306))
        common = {
            "type": "mysql",
            "host": host,
            "port": port,
            "dbname": mysql.dbname,
            "user": mysql.username,
            "password": mysql.password,
            "table": "managed_users",
            "upsert_key": ["id"],
        }
        create_options = SyncOptions()
        declared = SyncConfig.model_validate(
            {
                "name": "typed_mysql",
                "model": "select 1",
                "destination": {**common, "type": "mysql"},
                "columns": [
                    {"name": "id", "type": "integer"},
                    {"name": hostile, "type": "string"},
                ],
            }
        )
        create_options._source_column_types = declared_destination_types(
            declared.columns, declared.sync.field_mappings
        )
        created = MySQLDestination().load(
            [{"id": "1", hostile: "Ada"}],
            MySQLDestinationConfig(**common, manage_table=True),
            create_options,
        )
        assert created.success == 1
        assert created.ddl == [
            "CREATE TABLE IF NOT EXISTS `managed_users` "
            "(`id` BIGINT NOT NULL, `profile.name``; DROP TABLE audit; --` TEXT NULL, "
            "PRIMARY KEY (`id`))"
        ]

        append_options = SyncOptions()
        append_options._source_column_types = {"score": "number"}
        appended = MySQLDestination().load(
            [{"id": "1", hostile: "Ada Lovelace", "score": "10"}],
            MySQLDestinationConfig(
                **common,
                manage_table=False,
                on_schema_change="append_new_columns",
            ),
            append_options,
        )
        assert appended.success == 1
        assert appended.ddl == [
            "ALTER TABLE `managed_users` ADD COLUMN `score` DECIMAL(65, 30) NULL"
        ]

        conn = pymysql.connect(
            host=host,
            port=port,
            user=mysql.username,
            password=mysql.password,
            database=mysql.dbname,
        )
        try:
            with conn.cursor() as cur:
                escaped = hostile.replace("`", "``")
                cur.execute(f"SELECT id, `{escaped}`, score FROM managed_users")  # noqa: S608
                assert cur.fetchall() == ((1, "Ada Lovelace", 10),)
        finally:
            conn.close()
