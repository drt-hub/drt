"""Live SQL validity for first-release managed destination tables (#760)."""

from __future__ import annotations

from collections.abc import Iterator
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
from drt.engine.contract import declared_destination_types
from drt.engine.sync import run_sync

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
pymysql = pytest.importorskip("pymysql")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")
testcontainers_mysql = pytest.importorskip("testcontainers.mysql")


def test_postgres_create_append_and_hostile_identifier_are_live_valid(tmp_path: Path) -> None:
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

        class IntegerSource:
            def extract(
                self,
                query: str,
                config: Any,
                *,
                query_tags: dict[str, str] | None = None,
            ) -> Iterator[dict[str, Any]]:
                del query, config, query_tags
                yield {"id": 1, "raw_score": 10}

        masked_sync = SyncConfig.model_validate(
            {
                "name": "masked_score",
                "model": "select 1",
                "destination": {
                    **common,
                    "table": "masked_users",
                    "manage_table": True,
                },
                "columns": [
                    {"name": "id", "type": "integer"},
                    {"name": "raw_score", "type": "integer"},
                ],
                "sync": {
                    "field_mappings": {"raw_score": "score_hash"},
                    "mask": {"score_hash": "hash"},
                },
            }
        )
        masked = run_sync(
            masked_sync,
            IntegerSource(),
            PostgresDestination(),
            BigQueryProfile(type="bigquery", project="p", dataset="d"),
            tmp_path,
        )
        assert masked.success == 1
        assert masked.ddl == [
            'CREATE TABLE IF NOT EXISTS "masked_users" '
            '("id" BIGINT NOT NULL, "score_hash" TEXT, PRIMARY KEY ("id"))'
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
                cur.execute(
                    "CREATE TABLE sparse_users (id BIGINT PRIMARY KEY, tenant TEXT NOT NULL)"
                )
                cur.execute(
                    "CREATE TABLE generated_users ("
                    "id BIGINT PRIMARY KEY, "
                    "generated_id BIGINT GENERATED ALWAYS AS IDENTITY, "
                    "doubled BIGINT GENERATED ALWAYS AS (id * 2) STORED)"
                )
            conn.commit()

            sparse_destination = PostgresDestination()
            sparse_config = PostgresDestinationConfig(
                **{**common, "table": "sparse_users"},
                manage_table=False,
                on_schema_change="ignore",
            )
            assert (
                sparse_destination.load(
                    [{"id": 1, "tenant": "a"}], sparse_config, SyncOptions()
                ).success
                == 1
            )
            with pytest.raises(ValueError, match=r"required column\(s\).*tenant"):
                sparse_destination.load([{"id": 2}], sparse_config, SyncOptions())

            generated_config = PostgresDestinationConfig(
                **{**common, "table": "generated_users"},
                manage_table=False,
            )
            assert (
                PostgresDestination().load([{"id": 7}], generated_config, SyncOptions()).success
                == 1
            )
            with conn.cursor() as cur:
                cur.execute("SELECT id, tenant FROM sparse_users ORDER BY id")
                assert cur.fetchall() == [(1, "a")]
                cur.execute("SELECT id, generated_id, doubled FROM generated_users")
                assert cur.fetchall() == [(7, 1, 14)]
                cur.execute(
                    "SELECT data_type FROM information_schema.columns "
                    "WHERE table_name = 'masked_users' AND column_name = 'score_hash'"
                )
                assert cur.fetchone() == ("text",)
                cur.execute("SELECT id, score_hash FROM masked_users")
                assert cur.fetchall() == [
                    (
                        1,
                        "4a44dc15364204a80fe80e9039455cc1608281820fe2b24f1e5233ade6af1dd5",
                    )
                ]
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
                cur.execute(
                    "CREATE TABLE sparse_users "
                    "(id BIGINT PRIMARY KEY, tenant VARCHAR(255) NOT NULL)"
                )
                cur.execute(
                    "CREATE TABLE prefix_users "
                    "(email VARCHAR(255) NOT NULL, UNIQUE KEY uq_email (email(10)))"
                )
                cur.execute(
                    "CREATE TABLE generated_users ("
                    "id BIGINT PRIMARY KEY, "
                    "doubled BIGINT GENERATED ALWAYS AS (id * 2) STORED)"
                )
            conn.commit()

            sparse_destination = MySQLDestination()
            sparse_config = MySQLDestinationConfig(
                **{**common, "table": "sparse_users"},
                manage_table=False,
                on_schema_change="ignore",
            )
            assert (
                sparse_destination.load(
                    [{"id": 1, "tenant": "a"}], sparse_config, SyncOptions()
                ).success
                == 1
            )
            with pytest.raises(ValueError, match=r"required column\(s\).*tenant"):
                sparse_destination.load([{"id": 2}], sparse_config, SyncOptions())

            prefix_config = MySQLDestinationConfig(
                **{**common, "table": "prefix_users", "upsert_key": ["email"]},
                manage_table=False,
            )
            with pytest.raises(ValueError, match="full-column.*Prefix indexes"):
                MySQLDestination().load(
                    [{"email": "same-prefix-a@example.test"}],
                    prefix_config,
                    SyncOptions(),
                )

            generated_config = MySQLDestinationConfig(
                **{**common, "table": "generated_users"},
                manage_table=False,
            )
            generated = MySQLDestination().load([{"id": 7}], generated_config, SyncOptions())
            assert generated.success == 1

            with conn.cursor() as cur:
                cur.execute("SELECT id, tenant FROM sparse_users ORDER BY id")
                assert cur.fetchall() == ((1, "a"),)
                cur.execute("SELECT COUNT(*) FROM prefix_users")
                assert cur.fetchone() == (0,)
                cur.execute("SELECT id, doubled FROM generated_users")
                assert cur.fetchall() == ((7, 14),)
        finally:
            conn.close()
