"""Live Postgres proof that a heterogeneous batch's real write reaches the
warehouse correctly (#1091) — a mock cursor can't validate the SQL itself
(#908 lesson): it accepts any string, so a bug here would need a real
INSERT/ON CONFLICT round trip to surface.
"""

from __future__ import annotations

from unittest.mock import patch

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
        destination = PostgresDestination()

        records = [
            {"id": 1, "score": 0.95},
            {"id": 2, "score": 0.80, "note": "flagged"},
        ]
        with patch.object(PostgresDestination, "_connect", return_value=load_conn):
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


def test_column_default_applies_when_a_run_omits_it() -> None:
    """Codex review on PR #1135 caught a second bug in the first attempted
    fix: widening the write's column list to the batch-wide union means a
    record lacking a column binds an explicit NULL for it, overriding that
    column's own DEFAULT (or failing a NOT NULL column outright) instead of
    letting the DEFAULT apply. Building the INSERT per contiguous
    key-signature run instead means a run lacking "note" never mentions it
    at all, so Postgres applies the column's DEFAULT."""
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer

    with postgres_container("postgres:16-alpine", driver=None) as postgres:
        setup_conn = psycopg2.connect(postgres.get_connection_url())
        try:
            with setup_conn.cursor() as cur:
                cur.execute(
                    "CREATE TABLE scores (id INT PRIMARY KEY, score FLOAT, "
                    "note TEXT NOT NULL DEFAULT 'unknown')"
                )
            setup_conn.commit()
        finally:
            setup_conn.close()

        load_conn = psycopg2.connect(postgres.get_connection_url())
        destination = PostgresDestination()

        records = [
            {"id": 1, "score": 0.95},
            {"id": 2, "score": 0.80, "note": "flagged"},
        ]
        with patch.object(PostgresDestination, "_connect", return_value=load_conn):
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
            (1, 0.95, "unknown"),  # DEFAULT applied, not an explicit NULL
            (2, 0.80, "flagged"),
        ]


def test_on_error_fail_rolls_back_every_run_in_the_batch() -> None:
    """Codex review on PR #1135 caught a third bug: an earlier design
    dispatched one _load_upsert call per key-signature group, so an earlier
    group's successful writes could already be committed by the time a
    later group failed under on_error: fail -- breaking the existing
    all-or-nothing guarantee. The corrected design calls _load_upsert
    exactly once per load() (one transaction), so a later run's failure
    still rolls back an earlier run's work within the same call."""
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer

    with postgres_container("postgres:16-alpine", driver=None) as postgres:
        setup_conn = psycopg2.connect(postgres.get_connection_url())
        try:
            with setup_conn.cursor() as cur:
                cur.execute("CREATE TABLE scores (id INT PRIMARY KEY, score FLOAT NOT NULL)")
            setup_conn.commit()
        finally:
            setup_conn.close()

        load_conn = psycopg2.connect(postgres.get_connection_url())
        destination = PostgresDestination()

        # Run 1 (signature {id, score}) succeeds; run 2 (signature {id,
        # score, note} -- "note" isn't a real column) fails on every row.
        records = [
            {"id": 1, "score": 0.95},
            {"id": 2, "score": 0.80, "note": "flagged"},
        ]
        with patch.object(PostgresDestination, "_connect", return_value=load_conn):
            result = destination.load(records, _config(), SyncOptions(on_error="fail"))
        assert result.failed == 1
        # #1136: result.success must reflect the post-rollback reality (run
        # 1's row did NOT survive), not the pre-rollback per-row count.
        assert result.success == 0

        verify_conn = psycopg2.connect(postgres.get_connection_url())
        try:
            with verify_conn.cursor() as cur:
                cur.execute("SELECT id, score FROM scores ORDER BY id")
                rows = cur.fetchall()
        finally:
            verify_conn.close()

        # Run 1's row (id=1) must not have survived -- rolled back along
        # with run 2's failure, matching on_error: fail's pre-#1091
        # all-or-nothing contract for the whole batch.
        assert rows == []


def test_upsert_key_repeated_under_different_signatures_preserves_last_write() -> None:
    """Codex review on PR #1135 caught a first bug: an earlier design
    grouped records by signature globally, which could dispatch records
    sharing an upsert key out of their original relative order -- changing
    which write ends up "last" for upsert's implicit last-write-wins
    semantics. Splitting into runs only at a signature *change* preserves
    original order even when the same key reappears under a different
    signature later in the batch."""
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer

    with postgres_container("postgres:16-alpine", driver=None) as postgres:
        setup_conn = psycopg2.connect(postgres.get_connection_url())
        try:
            with setup_conn.cursor() as cur:
                cur.execute("CREATE TABLE scores (id INT PRIMARY KEY, score FLOAT, note TEXT)")
            setup_conn.commit()
        finally:
            setup_conn.close()

        load_conn = psycopg2.connect(postgres.get_connection_url())
        destination = PostgresDestination()

        # Same id=1 appears three times under two different signatures.
        # Original order says the LAST write (score=2, no note) should win.
        records = [
            {"id": 1, "score": 0},
            {"id": 1, "score": 1, "note": "mid"},
            {"id": 1, "score": 2},
        ]
        with patch.object(PostgresDestination, "_connect", return_value=load_conn):
            result = destination.load(records, _config(), SyncOptions())
        assert result.success == 3

        verify_conn = psycopg2.connect(postgres.get_connection_url())
        try:
            with verify_conn.cursor() as cur:
                cur.execute("SELECT id, score, note FROM scores")
                rows = cur.fetchall()
        finally:
            verify_conn.close()

        assert rows == [(1, 2, "mid")]  # score from the last write; note untouched (upsert)
