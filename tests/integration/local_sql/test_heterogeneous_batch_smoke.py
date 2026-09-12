"""Live Postgres proof that a heterogeneous batch's real write reaches the
warehouse correctly (#1091) — a mock cursor can't validate the SQL itself
(#908 lesson): it accepts any string, so a bug here would need a real
INSERT/ON CONFLICT round trip to surface.
"""

from __future__ import annotations

import pytest

from drt.config.models import PostgresDestinationConfig, SyncOptions
from drt.destinations.postgres import PostgresDestination

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")


def _config(**overrides: object) -> PostgresDestinationConfig:
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


def test_field_first_appearing_in_a_later_record_actually_lands() -> None:
    """A field absent from the first record of a batch but present in a
    later one (a legitimately heterogeneous/optional-field source) used to
    be silently dropped for the *whole* batch — columns were derived from
    the first record alone. It must now reach the destination for the
    records that actually have it, and must not null-fill it onto — or
    otherwise disturb — the records that don't."""
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer

    with postgres_container("postgres:16-alpine", driver=None) as postgres:
        setup_conn = psycopg2.connect(postgres.get_connection_url())
        try:
            with setup_conn.cursor() as cur:
                cur.execute("CREATE TABLE scores (id INT PRIMARY KEY, score FLOAT, note TEXT)")
                # Pre-existing row for id=1, with a note already set — proves
                # the record lacking "note" in this run's batch does not
                # clobber it (the bug the rejected naive "widen to union"
                # fix would have introduced).
                cur.execute("INSERT INTO scores (id, score, note) VALUES (1, 0.10, 'pre-existing')")
            setup_conn.commit()
        finally:
            setup_conn.close()

        # load() always closes the connection it's handed (matches normal
        # call sites, which open a fresh connection per load()), so it gets
        # its own.
        load_conn = psycopg2.connect(postgres.get_connection_url())

        def _fake_connect(_config: PostgresDestinationConfig) -> object:
            return load_conn

        destination = PostgresDestination()
        destination._connect = _fake_connect  # type: ignore[method-assign]

        records = [
            {"id": 1, "score": 0.95},
            {"id": 2, "score": 0.80, "note": "flagged"},
        ]
        result = destination.load(records, _config(), SyncOptions())
        assert result.success == 2
        assert result.failed == 0

        verify_conn = psycopg2.connect(postgres.get_connection_url())
        try:
            with verify_conn.cursor() as cur:
                cur.execute("SELECT id, score, note FROM scores ORDER BY id")
                rows = cur.fetchall()
        finally:
            verify_conn.close()

        assert rows == [
            (1, 0.95, "pre-existing"),
            (2, 0.80, "flagged"),
        ]
