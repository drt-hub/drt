"""Live Postgres/MySQL proof that on_error: skip no longer discards an
earlier successful row in the same batch when a later row fails (#1136) — a
mock connection's rollback()/SAVEPOINT handling is a no-op, which is exactly
why the unit test suite never caught this (#908 lesson).
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest

from drt.config.models import MySQLDestinationConfig, PostgresDestinationConfig, SyncOptions
from drt.destinations.mysql import MySQLDestination
from drt.destinations.postgres import PostgresDestination

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
pymysql = pytest.importorskip("pymysql")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")
testcontainers_mysql = pytest.importorskip("testcontainers.mysql")


def _pg_config(**overrides: object) -> PostgresDestinationConfig:
    defaults: dict[str, object] = {
        "type": "postgres",
        "host": "localhost",
        "dbname": "testdb",
        "user": "testuser",
        "password": "testpass",
        "table": "scores",
        "upsert_key": ["id"],
        "introspect_schema": False,
    }
    defaults.update(overrides)
    return PostgresDestinationConfig(**defaults)


def _mysql_config(**overrides: object) -> MySQLDestinationConfig:
    defaults: dict[str, object] = {
        "type": "mysql",
        "host": "localhost",
        "dbname": "testdb",
        "user": "testuser",
        "password": "testpass",
        "table": "scores",
        "upsert_key": ["id"],
        "introspect_schema": False,
    }
    defaults.update(overrides)
    return MySQLDestinationConfig(**defaults)


def _mysql_connect(mysql: Any) -> Any:
    """A fresh pymysql connection to the running container.

    ``load()``/``finalize_sync()`` always close the connection they're
    handed (matching normal call sites, which open a fresh connection per
    call) — every stage of these tests needs its own.
    """
    return pymysql.connect(
        host=mysql.get_container_host_ip(),
        port=int(mysql.get_exposed_port(3306)),
        user=mysql.username,
        password=mysql.password,
        database=mysql.dbname,
    )


def test_postgres_upsert_skip_keeps_earlier_successful_row() -> None:
    """id=1 succeeds, id=2 fails (bad type for `score`), id=3 succeeds.
    Before #1136: id=2's failure forced a full conn.rollback() to recover
    the aborted Postgres transaction, discarding id=1's uncommitted INSERT
    even though result.success already counted it. The SAVEPOINT-based
    recovery must leave id=1 (and id=3) in place."""
    require_docker()
    with testcontainers_postgres.PostgresContainer("postgres:16-alpine", driver=None) as postgres:
        setup_conn = psycopg2.connect(postgres.get_connection_url())
        try:
            with setup_conn.cursor() as cur:
                cur.execute("CREATE TABLE scores (id INT PRIMARY KEY, score FLOAT)")
            setup_conn.commit()
        finally:
            setup_conn.close()

        load_conn = psycopg2.connect(postgres.get_connection_url())
        destination = PostgresDestination()

        records = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": "not-a-float-oops"},
            {"id": 3, "score": 0.9},
        ]
        with patch.object(PostgresDestination, "_connect", return_value=load_conn):
            result = destination.load(records, _pg_config(), SyncOptions(on_error="skip"))

        assert result.success == 2
        assert result.failed == 1

        verify_conn = psycopg2.connect(postgres.get_connection_url())
        try:
            with verify_conn.cursor() as cur:
                cur.execute("SELECT id, score FROM scores ORDER BY id")
                rows = cur.fetchall()
        finally:
            verify_conn.close()

        assert rows == [(1, 0.5), (3, 0.9)]


def test_mysql_upsert_skip_keeps_earlier_successful_row() -> None:
    """MySQL/InnoDB doesn't abort the whole transaction on an ordinary
    statement error (unlike Postgres) — before #1136, mysql.py's skip path
    still called a full conn.rollback() anyway (mirroring the Postgres
    code), which was gratuitous on MySQL and equally discarded id=1's
    already-successful row for no compensating benefit."""
    require_docker()
    with testcontainers_mysql.MySqlContainer("mysql:8.0") as mysql:
        setup_conn = _mysql_connect(mysql)
        try:
            with setup_conn.cursor() as cur:
                cur.execute("CREATE TABLE scores (id INT PRIMARY KEY, score FLOAT)")
            setup_conn.commit()
        finally:
            setup_conn.close()

        load_conn = _mysql_connect(mysql)
        destination = MySQLDestination()
        records = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": "not-a-float-oops"},
            {"id": 3, "score": 0.9},
        ]
        with patch.object(MySQLDestination, "_connect", return_value=load_conn):
            result = destination.load(records, _mysql_config(), SyncOptions(on_error="skip"))

        assert result.success == 2
        assert result.failed == 1

        verify_conn = _mysql_connect(mysql)
        try:
            with verify_conn.cursor() as cur:
                cur.execute("SELECT id, score FROM scores ORDER BY id")
                rows = cur.fetchall()
        finally:
            verify_conn.close()

        assert rows == ((1, 0.5), (3, 0.9))


def test_postgres_replace_swap_skip_does_not_cascade_fail_every_later_row() -> None:
    """A second, related bug found while investigating #1136:
    _load_replace_swap's on_error: skip path had NO recovery at all after a
    row failure (unlike _load_upsert/_load_replace's own conn.rollback()) —
    every row after the first failure raised InFailedSqlTransaction, since
    Postgres leaves the transaction aborted until an explicit ROLLBACK. The
    SAVEPOINT fix must let id=3 land despite id=2's failure."""
    require_docker()
    with testcontainers_postgres.PostgresContainer("postgres:16-alpine", driver=None) as postgres:
        setup_conn = psycopg2.connect(postgres.get_connection_url())
        try:
            with setup_conn.cursor() as cur:
                cur.execute("CREATE TABLE scores (id INT PRIMARY KEY, score FLOAT)")
            setup_conn.commit()
        finally:
            setup_conn.close()

        load_conn = psycopg2.connect(postgres.get_connection_url())
        destination = PostgresDestination()

        records = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": "not-a-float-oops"},
            {"id": 3, "score": 0.9},
        ]
        with patch.object(PostgresDestination, "_connect", return_value=load_conn):
            result = destination.load(
                records,
                _pg_config(),
                SyncOptions(mode="replace", replace_strategy="swap", on_error="skip"),
            )

        assert result.success == 2
        assert result.failed == 1

        finalize_conn = psycopg2.connect(postgres.get_connection_url())
        with patch.object(PostgresDestination, "_connect", return_value=finalize_conn):
            destination.finalize_sync(
                _pg_config(), SyncOptions(mode="replace", replace_strategy="swap")
            )

        verify_conn = psycopg2.connect(postgres.get_connection_url())
        try:
            with verify_conn.cursor() as cur:
                cur.execute("SELECT id, score FROM scores ORDER BY id")
                rows = cur.fetchall()
        finally:
            verify_conn.close()

        assert rows == [(1, 0.5), (3, 0.9)]


def test_mysql_replace_swap_skip_records_failure_and_keeps_going() -> None:
    """MySQL counterpart: _load_replace_swap's skip path had no recovery
    call at all. MySQL doesn't need one for an ordinary statement error
    (verified empirically for #1136), but the SAVEPOINT fix makes the
    recovery explicit and dialect-uniform rather than relying on that."""
    require_docker()
    with testcontainers_mysql.MySqlContainer("mysql:8.0") as mysql:
        setup_conn = _mysql_connect(mysql)
        try:
            with setup_conn.cursor() as cur:
                cur.execute("CREATE TABLE scores (id INT PRIMARY KEY, score FLOAT)")
            setup_conn.commit()
        finally:
            setup_conn.close()

        load_conn = _mysql_connect(mysql)
        destination = MySQLDestination()
        records = [
            {"id": 1, "score": 0.5},
            {"id": 2, "score": "not-a-float-oops"},
            {"id": 3, "score": 0.9},
        ]
        with patch.object(MySQLDestination, "_connect", return_value=load_conn):
            result = destination.load(
                records,
                _mysql_config(),
                SyncOptions(mode="replace", replace_strategy="swap", on_error="skip"),
            )

        assert result.success == 2
        assert result.failed == 1

        finalize_conn = _mysql_connect(mysql)
        with patch.object(MySQLDestination, "_connect", return_value=finalize_conn):
            destination.finalize_sync(
                _mysql_config(), SyncOptions(mode="replace", replace_strategy="swap")
            )

        verify_conn = _mysql_connect(mysql)
        try:
            with verify_conn.cursor() as cur:
                cur.execute("SELECT id, score FROM scores ORDER BY id")
                rows = cur.fetchall()
        finally:
            verify_conn.close()

        assert rows == ((1, 0.5), (3, 0.9))
