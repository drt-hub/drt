"""Live Postgres and MySQL proof for ``sync.write_policy: fill_empty`` (#1238).

A mock cursor accepts any SQL string; only a real server proves that the
``CASE`` / ``IF`` expression is valid for text *and* integer columns, treats NULL
and an empty string as empty, and keeps a stored 0 or a non-empty string.
"""

from __future__ import annotations

from typing import Any

import pytest

from drt.config.models import MySQLDestinationConfig, PostgresDestinationConfig, SyncOptions
from drt.destinations.mysql import MySQLDestination
from drt.destinations.postgres import PostgresDestination
from drt.engine.diff import compute_diff

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
pymysql = pytest.importorskip("pymysql")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")
testcontainers_mysql = pytest.importorskip("testcontainers.mysql")

# Existing destination rows: (id, industry, score, employees, notes)
_EXISTING = [
    (1, "Retail", 5, None, ""),  # industry kept; employees NULL and notes '' are filled
    (2, None, None, None, None),  # everything empty: everything is filled
    (3, "", 1, 10, "kept"),  # industry '' is filled; employees 10 and notes are kept
    (4, "Bank", 0, 0, "x"),  # a stored 0 is a value, not empty
]

_INCOMING = [
    {"id": 1, "industry": "Software", "score": 9, "employees": 100, "notes": "n"},
    {"id": 2, "industry": "Software", "score": 9, "employees": 100, "notes": "n"},
    {"id": 3, "industry": "Software", "score": 9, "employees": 100, "notes": "n"},
    {"id": 4, "industry": "Software", "score": 9, "employees": 100, "notes": "n"},
    {"id": 5, "industry": "Software", "score": 9, "employees": 100, "notes": "n"},  # new row
]

# score is always overwritten (override); everything else is fill-only.
_OPTIONS = {"write_policy": "fill_empty", "write_policy_overrides": {"score": "overwrite"}}

_EXPECTED = [
    (1, "Retail", 9, 100, "n"),
    (2, "Software", 9, 100, "n"),
    (3, "Software", 9, 10, "kept"),
    (4, "Bank", 9, 0, "x"),
    (5, "Software", 9, 100, "n"),  # inserted in full
]


def test_postgres_fill_empty() -> None:
    require_docker()
    with testcontainers_postgres.PostgresContainer(
        "postgres:16-alpine", username="admin", password="adminpass", dbname="testdb", driver=None
    ) as pg:
        host, port = pg.get_container_host_ip(), int(pg.get_exposed_port(5432))
        conn = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TABLE contacts (id INTEGER PRIMARY KEY, industry TEXT, "
                    "score INTEGER, employees INTEGER, notes TEXT)"
                )
                cur.executemany("INSERT INTO contacts VALUES (%s, %s, %s, %s, %s)", _EXISTING)
            conn.commit()

            config = PostgresDestinationConfig(
                type="postgres",
                host=host,
                port=port,
                dbname="testdb",
                user="admin",
                password="adminpass",
                table="contacts",
                upsert_key=["id"],
            )
            options = SyncOptions(**_OPTIONS)  # type: ignore[arg-type]

            diff = compute_diff(_INCOMING, config, options, limit=100)
            # r1: industry; r3: employees, notes; r4: industry, employees (a stored 0), notes
            assert diff.kept_values == 6

            result = PostgresDestination().load(_INCOMING, config, options)
            assert result.success == 5 and result.failed == 0

            with conn.cursor() as cur:
                cur.execute("SELECT * FROM contacts ORDER BY id")
                assert cur.fetchall() == _EXPECTED
        finally:
            conn.close()


def test_postgres_update_only_with_fill_empty_never_creates_and_keeps_values() -> None:
    require_docker()
    with testcontainers_postgres.PostgresContainer(
        "postgres:16-alpine", username="admin", password="adminpass", dbname="testdb", driver=None
    ) as pg:
        host, port = pg.get_container_host_ip(), int(pg.get_exposed_port(5432))
        conn = psycopg2.connect(
            host=host, port=port, dbname="testdb", user="admin", password="adminpass"
        )
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TABLE contacts (id INTEGER PRIMARY KEY, industry TEXT, score INTEGER)"
                )
                cur.execute("INSERT INTO contacts VALUES (1, 'Retail', NULL), (2, NULL, NULL)")
            conn.commit()
            config = PostgresDestinationConfig(
                type="postgres",
                host=host,
                port=port,
                dbname="testdb",
                user="admin",
                password="adminpass",
                table="contacts",
                upsert_key=["id"],
            )
            options = SyncOptions(match_policy="update_only", write_policy="fill_empty")

            result = PostgresDestination().load(
                [
                    {"id": 1, "industry": "Software", "score": 7},
                    {"id": 2, "industry": "Software", "score": 7},
                    {"id": 99, "industry": "Software", "score": 7},
                ],
                config,
                options,
            )

            assert result.success == 2 and result.skipped_no_match == 1
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM contacts ORDER BY id")
                assert cur.fetchall() == [(1, "Retail", 7), (2, "Software", 7)]
        finally:
            conn.close()


def test_mysql_fill_empty() -> None:
    require_docker()
    with testcontainers_mysql.MySqlContainer("mysql:8.0") as mysql:
        host, port = mysql.get_container_host_ip(), int(mysql.get_exposed_port(3306))

        def connect() -> Any:
            return pymysql.connect(
                host=host,
                port=port,
                user=mysql.username,
                password=mysql.password,
                database=mysql.dbname,
            )

        conn = connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "CREATE TABLE contacts (id INT PRIMARY KEY, industry VARCHAR(40), "
                    "score INT, employees INT, notes VARCHAR(40))"
                )
                cur.executemany("INSERT INTO contacts VALUES (%s, %s, %s, %s, %s)", _EXISTING)
            conn.commit()

            config = MySQLDestinationConfig(
                type="mysql",
                host=host,
                port=port,
                dbname=mysql.dbname,
                user=mysql.username,
                password=mysql.password,
                table="contacts",
                upsert_key=["id"],
                introspect_schema=False,
            )
            options = SyncOptions(**_OPTIONS)  # type: ignore[arg-type]

            result = MySQLDestination().load(_INCOMING, config, options)
            assert result.success == 5 and result.failed == 0

            with conn.cursor() as cur:
                cur.execute("SELECT * FROM contacts ORDER BY id")
                assert list(cur.fetchall()) == _EXPECTED
        finally:
            conn.close()
