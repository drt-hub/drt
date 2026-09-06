"""Warehouse-backed state/history/DLQ stores against a real Postgres (#920, ADR 0005 step 4).

A mock cursor can prove the right SQL text is *issued*, not that it is
*valid* against a real engine, or that concurrent upserts genuinely
serialize instead of racing. Both are exactly what this file exists to
prove that tests/unit/test_state_warehouse.py cannot — same lesson
tests/integration/local_sql/test_managed_table_primitive_smoke.py (#960)
already established for the primitive this backend is built on.
"""

from __future__ import annotations

import pytest

from drt.config.credentials import PostgresProfile
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState
from drt.state.warehouse import (
    PostgresWarehouseDlqBackend,
    PostgresWarehouseHistoryStore,
    PostgresWarehouseStateStore,
)

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


def test_state_store_upsert_read_and_reset_round_trip(pg_profile: PostgresProfile) -> None:
    store = PostgresWarehouseStateStore(pg_profile)

    assert store.get_last_sync("orders") is None
    assert store.get_all() == {}

    store.save_sync(
        SyncState(
            sync_name="orders",
            last_run_at="2026-09-06T00:00:00+00:00",
            records_synced=100,
            status="success",
            last_cursor_value="42",
        )
    )
    first = store.get_last_sync("orders")
    assert first == SyncState(
        sync_name="orders",
        last_run_at="2026-09-06T00:00:00+00:00",
        records_synced=100,
        status="success",
        error=None,
        last_cursor_value="42",
    )

    # Same sync_name again -- UPSERT, not a second row.
    store.save_sync(
        SyncState(
            sync_name="orders",
            last_run_at="2026-09-06T01:00:00+00:00",
            records_synced=150,
            status="partial",
            error="rate limited",
            last_cursor_value="99",
        )
    )
    updated = store.get_last_sync("orders")
    assert updated is not None
    assert updated.records_synced == 150
    assert updated.status == "partial"
    assert updated.error == "rate limited"

    store.save_sync(
        SyncState(sync_name="customers", last_run_at="t", records_synced=1, status="success")
    )
    assert set(store.get_all().keys()) == {"orders", "customers"}

    assert store.reset("orders") is True
    assert store.get_last_sync("orders") is None
    assert store.reset("orders") is False  # already gone -- no-op, not an error
    assert "customers" in store.get_all()


def test_history_store_append_read_and_prune(pg_profile: PostgresProfile) -> None:
    history = PostgresWarehouseHistoryStore(pg_profile)

    assert history.read() == []

    history.append(
        HistoryEntry(
            sync_name="orders",
            started_at="2020-01-01T00:00:00+00:00",
            completed_at="2020-01-01T00:01:00+00:00",
            duration_seconds=60.0,
            status="success",
            records_synced=10,
            records_failed=0,
            errors=[],
        )
    )
    history.append(
        HistoryEntry(
            sync_name="orders",
            started_at="2026-09-06T00:00:00+00:00",
            completed_at="2026-09-06T00:01:00+00:00",
            duration_seconds=30.0,
            status="failed",
            records_synced=0,
            records_failed=5,
            errors=["boom"],
            run_id="run-1",
        )
    )
    history.append(
        HistoryEntry(
            sync_name="customers",
            started_at="2026-09-06T00:00:00+00:00",
            completed_at="2026-09-06T00:01:00+00:00",
            duration_seconds=5.0,
            status="success",
            records_synced=2,
            records_failed=0,
        )
    )

    only_orders = history.read("orders")
    assert [e.status for e in only_orders] == ["failed", "success"]  # newest first
    assert only_orders[0].errors == ["boom"]
    assert only_orders[0].run_id == "run-1"

    merged = history.read()
    assert {e.sync_name for e in merged} == {"orders", "customers"}

    # 2020 entry is older than any real retention window -- gets pruned;
    # the 2026 entries survive.
    removed = history.prune("orders", retention_days=30)
    assert removed == 1
    assert [e.started_at for e in history.read("orders")] == ["2026-09-06T00:00:00+00:00"]


def test_dlq_backend_fifo_reconcile_and_clear(pg_profile: PostgresProfile) -> None:
    dlq = PostgresWarehouseDlqBackend(pg_profile)

    assert dlq.read("orders") == []
    assert dlq.depth("orders") == 0
    assert dlq.all_depths() == {}

    entries = [DeadLetter(record={"id": i}, error_message="boom", id=f"id-{i}") for i in range(5)]
    depth = dlq.append("orders", entries, max_records=3)
    assert depth == 3  # FIFO-capped: oldest 2 dropped

    remaining = dlq.read("orders")
    assert [e.record["id"] for e in remaining] == [2, 3, 4]
    assert dlq.all_depths() == {"orders": 3}

    # reconcile: remove one, update another's attempts/error, leave the rest.
    updated_entry = DeadLetter(
        record={"id": 3}, error_message="still failing", attempts=2, id="id-3"
    )
    result = dlq.reconcile("orders", remove_ids=["id-2"], updates={"id-3": updated_entry})
    assert {e.id for e in result} == {"id-3", "id-4"}
    reconciled = {e.id: e for e in result}
    assert reconciled["id-3"].attempts == 2
    assert reconciled["id-3"].error_message == "still failing"

    dlq.clear("orders")
    assert dlq.read("orders") == []
    assert dlq.all_depths() == {}


def test_dlq_and_state_share_the_same_managed_schema(pg_profile: PostgresProfile) -> None:
    """Both stores create their tables under PostgresProfile.managed_schema
    (#960) -- confirms they don't each invent their own schema."""
    state = PostgresWarehouseStateStore(pg_profile)
    dlq = PostgresWarehouseDlqBackend(pg_profile)

    state.save_sync(SyncState(sync_name="s", last_run_at="t", records_synced=1, status="success"))
    dlq.append("s", [DeadLetter(record={}, error_message="x", id="id-1")])

    admin = psycopg2.connect(
        host=pg_profile.host,
        port=pg_profile.port,
        dbname=pg_profile.dbname,
        user=pg_profile.user,
        password=pg_profile.password,
    )
    try:
        with admin.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s ORDER BY table_name",
                (pg_profile.managed_schema,),
            )
            tables = {row[0] for row in cur.fetchall()}
    finally:
        admin.close()

    assert {"_drt_runs", "_drt_dlq"}.issubset(tables)


def test_writes_succeed_with_preprovisioned_tables_and_no_create_privilege() -> None:
    """The escape hatch, live (#960/#695 discipline, caught missing in Codex
    review on this PR): an admin pre-creates the managed schema AND all three
    tables, then grants the sync role neither schema- nor table-level CREATE
    at all. Every write path must still succeed by detecting the table
    already exists via managed_table_exists(), never attempting CREATE
    TABLE — even the harmless-looking IF NOT EXISTS form."""
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
                cur.execute(
                    "CREATE TABLE _drt._drt_runs (sync_name TEXT PRIMARY KEY, "
                    "last_run_at TEXT NOT NULL, records_synced BIGINT NOT NULL, "
                    "status TEXT NOT NULL, error TEXT, last_cursor_value TEXT)"
                )
                cur.execute(
                    "CREATE TABLE _drt._drt_history ("
                    "sync_name TEXT NOT NULL, started_at TEXT NOT NULL, "
                    "completed_at TEXT NOT NULL, duration_seconds DOUBLE PRECISION NOT NULL, "
                    "status TEXT NOT NULL, records_synced BIGINT NOT NULL, "
                    "records_failed BIGINT NOT NULL, errors JSONB NOT NULL DEFAULT '[]', "
                    "cursor_value_used TEXT, dry_run BOOLEAN NOT NULL DEFAULT FALSE, "
                    "run_id TEXT, sync_run_id TEXT)"
                )
                cur.execute(
                    "CREATE TABLE _drt._drt_dlq (id TEXT PRIMARY KEY, "
                    "sync_name TEXT NOT NULL, record JSONB NOT NULL, "
                    "error_message TEXT NOT NULL, http_status INTEGER, ts TEXT NOT NULL, "
                    "attempts INTEGER NOT NULL, sync_run_id TEXT)"
                )
                cur.execute("CREATE USER retl_user WITH PASSWORD 'retlpass'")
                cur.execute("REVOKE CREATE ON DATABASE testdb FROM PUBLIC")
                cur.execute("GRANT USAGE ON SCHEMA _drt TO retl_user")
                cur.execute(
                    "GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA _drt TO retl_user"
                )
                # Prove the privilege really is absent, not just untested.
                cur.execute("SELECT has_database_privilege('retl_user', 'testdb', 'CREATE')")
                assert cur.fetchone() == (False,)
                cur.execute("SELECT has_schema_privilege('retl_user', '_drt', 'CREATE')")
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

            # Would raise InsufficientPrivilege if any CREATE statement were
            # ever actually issued against this role.
            PostgresWarehouseStateStore(config).save_sync(
                SyncState(sync_name="s", last_run_at="t", records_synced=1, status="success")
            )
            assert PostgresWarehouseStateStore(config).get_last_sync("s") is not None

            # append() is best-effort and swallows failures (per the
            # HistoryStore Protocol) -- a broken escape hatch would NOT
            # raise here, so read the row back to prove it actually landed.
            PostgresWarehouseHistoryStore(config).append(
                HistoryEntry(
                    sync_name="s",
                    started_at="t0",
                    completed_at="t1",
                    duration_seconds=1.0,
                    status="success",
                    records_synced=1,
                    records_failed=0,
                )
            )
            assert PostgresWarehouseHistoryStore(config).read("s") != []

            PostgresWarehouseDlqBackend(config).append(
                "s", [DeadLetter(record={}, error_message="x", id="id-1")]
            )
            assert PostgresWarehouseDlqBackend(config).depth("s") == 1
        finally:
            admin.close()


def test_dlq_replace_is_atomic_and_leaves_old_queue_intact_on_failure(
    pg_profile: PostgresProfile,
) -> None:
    """Codex review: an earlier version deleted on one connection, then
    called append() on a second one -- a failure between the two would
    permanently erase the queue. Proves the fix (one transaction, one
    commit) with a duplicate id in the replacement batch: a plain INSERT
    (no ON CONFLICT, unlike append()'s upsert) genuinely violates the
    PRIMARY KEY, and the surrounding transaction must roll the DELETE
    back with it -- the OLD queue must survive completely untouched,
    exactly as if replace() were never called."""
    dlq = PostgresWarehouseDlqBackend(pg_profile)

    original = [DeadLetter(record={"n": 1}, error_message="orig", id="keep-1")]
    dlq.append("orders", original)
    assert dlq.depth("orders") == 1

    broken_replacement = [
        DeadLetter(record={"n": 2}, error_message="new", id="new-1"),
        DeadLetter(record={"n": 3}, error_message="new", id="new-1"),  # duplicate id
    ]
    with pytest.raises(psycopg2.errors.UniqueViolation):
        dlq.replace("orders", broken_replacement)

    # The DELETE half of the failed transaction must have rolled back too --
    # the original entry is still exactly there, not gone and not doubled.
    survivors = dlq.read("orders")
    assert [e.id for e in survivors] == ["keep-1"]
    assert survivors[0].error_message == "orig"
