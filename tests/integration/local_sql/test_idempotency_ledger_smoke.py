"""Warehouse-backed idempotency ledger against a real Postgres (#1099, ADR 0005 step 5).

A mock cursor can prove the right SQL text is *issued*, not that
`= ANY(%s)` genuinely binds a Python list, that `executemany(...
ON CONFLICT DO NOTHING)` behaves as expected, or that two overlapping
`mark_delivered` calls for the *same* key don't raise instead of silently
coalescing. Same lesson tests/unit/test_state_warehouse.py cannot cover on
its own — established by #960/#920's own smoke tests for the primitives
this ledger is built on.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from drt.config.credentials import PostgresProfile
from drt.state.warehouse import PostgresWarehouseIdempotencyLedger

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")


@pytest.fixture
def pg_profile():
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer
    with postgres_container(
        "postgres:16-alpine",
        username="admin",
        password="adminpass",
        dbname="testdb",
        driver=None,
    ) as postgres:
        yield PostgresProfile(
            type="postgres",
            host=postgres.get_container_host_ip(),
            port=int(postgres.get_exposed_port(5432)),
            dbname="testdb",
            user="admin",
            password="adminpass",
        )


def test_already_delivered_is_empty_before_any_mark(pg_profile: PostgresProfile) -> None:
    ledger = PostgresWarehouseIdempotencyLedger(pg_profile)
    assert ledger.already_delivered("orders", ["k1", "k2"]) == set()


def test_mark_then_check_round_trips(pg_profile: PostgresProfile) -> None:
    ledger = PostgresWarehouseIdempotencyLedger(pg_profile)

    ledger.mark_delivered("orders", ["k1", "k2"], "2026-09-07T00:00:00+00:00")

    assert ledger.already_delivered("orders", ["k1", "k2", "k3"]) == {"k1", "k2"}
    # A different sync_name must not see another sync's marks -- the table
    # is keyed by (sync_name, idempotency_key), not idempotency_key alone.
    assert ledger.already_delivered("other_sync", ["k1"]) == set()


def test_mark_delivered_is_idempotent_for_a_repeated_key(pg_profile: PostgresProfile) -> None:
    """The Protocol promises a duplicate mark is a no-op, not an error --
    proves the `ON CONFLICT (sync_name, idempotency_key) DO NOTHING` clause
    actually has that effect against a real PRIMARY KEY constraint, not
    just that the SQL text contains the phrase."""
    ledger = PostgresWarehouseIdempotencyLedger(pg_profile)

    ledger.mark_delivered("orders", ["k1"], "2026-09-07T00:00:00+00:00")
    ledger.mark_delivered("orders", ["k1"], "2026-09-07T01:00:00+00:00")  # must not raise

    assert ledger.already_delivered("orders", ["k1"]) == {"k1"}


def test_prune_removes_only_rows_older_than_retention(pg_profile: PostgresProfile) -> None:
    ledger = PostgresWarehouseIdempotencyLedger(pg_profile)
    now = datetime.now(timezone.utc)
    old = (now - timedelta(days=10)).isoformat()
    recent = (now - timedelta(hours=1)).isoformat()

    ledger.mark_delivered("orders", ["old-key"], old)
    ledger.mark_delivered("orders", ["recent-key"], recent)

    removed = ledger.prune("orders", retention_days=7)

    assert removed == 1
    assert ledger.already_delivered("orders", ["old-key", "recent-key"]) == {"recent-key"}


def test_concurrent_first_marks_survive_table_creation_race(
    pg_profile: PostgresProfile,
) -> None:
    """Same race #920 found and fixed in the shared _ensure_table_exists
    helper (CREATE TABLE IF NOT EXISTS is not atomic across Postgres
    sessions), reproduced here for _drt_idempotency specifically since it's
    a new, independent first-use path through that same helper."""
    import concurrent.futures

    def write(i: int) -> None:
        PostgresWarehouseIdempotencyLedger(pg_profile).mark_delivered(
            "orders", [f"key_{i}"], "2026-09-07T00:00:00+00:00"
        )

    errors: list[BaseException] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(write, i) for i in range(8)]
        for future in concurrent.futures.as_completed(futures):
            exc = future.exception()
            if exc is not None:
                errors.append(exc)

    assert not errors, f"concurrent first marks raised: {errors}"

    ledger = PostgresWarehouseIdempotencyLedger(pg_profile)
    all_keys = [f"key_{i}" for i in range(8)]
    assert ledger.already_delivered("orders", all_keys) == set(all_keys)


def test_concurrent_marks_of_the_same_key_do_not_raise(pg_profile: PostgresProfile) -> None:
    """The write-path scenario the ledger is actually meant to survive:
    two overlapping runs both decide (from an already-stale
    already_delivered() read) that the same key is safe to send, and both
    then call mark_delivered() for it -- ON CONFLICT DO NOTHING must
    coalesce this into one row, not a UniqueViolation on the loser."""
    import concurrent.futures

    def write() -> None:
        PostgresWarehouseIdempotencyLedger(pg_profile).mark_delivered(
            "orders", ["shared-key"], "2026-09-07T00:00:00+00:00"
        )

    errors: list[BaseException] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(write) for _ in range(8)]
        for future in concurrent.futures.as_completed(futures):
            exc = future.exception()
            if exc is not None:
                errors.append(exc)

    assert not errors, f"concurrent marks of the same key raised: {errors}"

    ledger = PostgresWarehouseIdempotencyLedger(pg_profile)
    assert ledger.already_delivered("orders", ["shared-key"]) == {"shared-key"}
