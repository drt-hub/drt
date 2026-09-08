"""Warehouse-backed state/history/DLQ stores against a real Databricks account (#1108).

A mock cursor can prove the right SQL text is *issued*, not that MERGE
actually upserts correctly, that parse_json round-trips a VARIANT column, or
that the staged-MERGE `replace()` genuinely leaves the original queue intact
on a failed merge — exactly what tests/unit/test_state_warehouse_databricks.py
cannot prove, same lesson tests/integration/dwh/test_snowflake_warehouse_state_smoke.py
already established for the Snowflake leg this mirrors.

Runs only when ``DRT_SMOKE_DATABRICKS_*`` secrets are present. Most tests use
the smoke principal's already-granted ``drt_smoke.smoke`` schema as
``managed_schema`` — no new grant needed, since these tables land inside an
already-usable schema. Only the concurrent-first-write-race test needs a
brand-new schema (the #1108 ``CREATE SCHEMA`` grant), gated the same way as
``test_databricks_managed_table_smoke.py``.
"""

from __future__ import annotations

import concurrent.futures
import os
import uuid

import pytest

from drt.config.credentials import DatabricksProfile
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState
from drt.state.warehouse_databricks import (
    DatabricksWarehouseDlqBackend,
    DatabricksWarehouseHistoryStore,
    DatabricksWarehouseStateStore,
)

from .conftest import require_env

pytestmark = pytest.mark.dwh_smoke

dbsql = pytest.importorskip("databricks.sql")

HOST_ENV = "DRT_SMOKE_DATABRICKS_HOST"
HTTP_PATH_ENV = "DRT_SMOKE_DATABRICKS_HTTP_PATH"
TOKEN_ENV = "DRT_SMOKE_DATABRICKS_TOKEN"
CATALOG_ENV = "DRT_SMOKE_DATABRICKS_CATALOG"
SCHEMA_ENV = "DRT_SMOKE_DATABRICKS_SCHEMA"
_HAS_CREATE_SCHEMA_GRANT_ENV = "DRT_SMOKE_DATABRICKS_HAS_CREATE_SCHEMA_GRANT"


def _require_creds() -> dict[str, str]:
    return require_env(HOST_ENV, HTTP_PATH_ENV, TOKEN_ENV, CATALOG_ENV, SCHEMA_ENV)


def _profile(creds: dict[str, str], **overrides: object) -> DatabricksProfile:
    return DatabricksProfile(
        type="databricks",
        server_hostname=creds[HOST_ENV],
        http_path=creds[HTTP_PATH_ENV],
        access_token=creds[TOKEN_ENV],
        catalog=creds[CATALOG_ENV],
        # Reuses the principal's already-granted schema as managed_schema --
        # no new CREATE SCHEMA grant needed for the round-trip tests below.
        managed_schema=creds[SCHEMA_ENV],
        **overrides,  # type: ignore[arg-type]
    )


def _admin_connect(creds: dict[str, str]):
    return dbsql.connect(
        server_hostname=creds[HOST_ENV],
        http_path=creds[HTTP_PATH_ENV],
        access_token=creds[TOKEN_ENV],
    )


def test_state_store_upsert_read_and_reset_round_trip() -> None:
    creds = _require_creds()
    profile = _profile(creds)
    sync_a = f"orders_{uuid.uuid4().hex[:8]}"
    sync_b = f"customers_{uuid.uuid4().hex[:8]}"
    store = DatabricksWarehouseStateStore(profile)
    try:
        assert store.get_last_sync(sync_a) is None
        assert store.get_all().get(sync_a) is None

        store.save_sync(
            SyncState(
                sync_name=sync_a,
                last_run_at="2026-09-06T00:00:00+00:00",
                records_synced=100,
                status="success",
                last_cursor_value="42",
            )
        )
        first = store.get_last_sync(sync_a)
        assert first == SyncState(
            sync_name=sync_a,
            last_run_at="2026-09-06T00:00:00+00:00",
            records_synced=100,
            status="success",
            error=None,
            last_cursor_value="42",
        )

        # Same sync_name again -- MERGE upsert, not a second row.
        store.save_sync(
            SyncState(
                sync_name=sync_a,
                last_run_at="2026-09-06T01:00:00+00:00",
                records_synced=150,
                status="partial",
                error="rate limited",
                last_cursor_value="99",
            )
        )
        updated = store.get_last_sync(sync_a)
        assert updated is not None
        assert updated.records_synced == 150
        assert updated.status == "partial"
        assert updated.error == "rate limited"

        store.save_sync(
            SyncState(sync_name=sync_b, last_run_at="t", records_synced=1, status="success")
        )
        assert {sync_a, sync_b}.issubset(store.get_all().keys())

        assert store.reset(sync_a) is True
        assert store.get_last_sync(sync_a) is None
        assert store.reset(sync_a) is False  # already gone -- no-op, not an error
        assert sync_b in store.get_all()
    finally:
        store.reset(sync_a)
        store.reset(sync_b)


def test_history_store_append_read_and_prune() -> None:
    from datetime import datetime, timezone

    creds = _require_creds()
    profile = _profile(creds)
    history = DatabricksWarehouseHistoryStore(profile)
    sync_a = f"orders_{uuid.uuid4().hex[:8]}"
    sync_b = f"customers_{uuid.uuid4().hex[:8]}"
    recent = datetime.now(timezone.utc).isoformat()

    history.append(
        HistoryEntry(
            sync_name=sync_a,
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
            sync_name=sync_a,
            started_at=recent,
            completed_at=recent,
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
            sync_name=sync_b,
            started_at=recent,
            completed_at=recent,
            duration_seconds=5.0,
            status="success",
            records_synced=2,
            records_failed=0,
        )
    )

    only_orders = history.read(sync_a)
    assert [e.status for e in only_orders] == ["failed", "success"]  # newest first
    # Proves parse_json/VARIANT round-tripped a real list, not a raw string.
    assert only_orders[0].errors == ["boom"]
    assert isinstance(only_orders[0].errors, list)
    assert only_orders[0].run_id == "run-1"

    # 2020 entry is older than any real retention window -- gets pruned;
    # the just-appended entry survives.
    removed = history.prune(sync_a, retention_days=30)
    assert removed == 1
    assert [e.started_at for e in history.read(sync_a)] == [recent]


def test_dlq_backend_fifo_reconcile_and_clear() -> None:
    """Per-run-unique ids sidestep a leftover row from a prior run against
    this same persistent account silently attributing to the wrong
    sync_name — same reasoning as the Snowflake leg's equivalent test."""
    creds = _require_creds()
    profile = _profile(creds)
    dlq = DatabricksWarehouseDlqBackend(profile)
    sync_name = f"orders_{uuid.uuid4().hex[:8]}"
    run = uuid.uuid4().hex[:8]

    def _id(i: int) -> str:
        return f"id-{run}-{i}"

    try:
        assert dlq.read(sync_name) == []
        assert dlq.depth(sync_name) == 0

        entries = [DeadLetter(record={"id": i}, error_message="boom", id=_id(i)) for i in range(5)]
        depth = dlq.append(sync_name, entries, max_records=3)
        assert depth == 3  # FIFO-capped: oldest 2 dropped

        remaining = dlq.read(sync_name)
        assert [e.record["id"] for e in remaining] == [2, 3, 4]
        # Proves parse_json/VARIANT round-tripped a real dict, not a raw string.
        assert isinstance(remaining[0].record, dict)
        assert dlq.all_depths().get(sync_name) == 3

        # reconcile: remove one, update another's attempts/error, leave the rest.
        updated_entry = DeadLetter(
            record={"id": 3}, error_message="still failing", attempts=2, id=_id(3)
        )
        result = dlq.reconcile(sync_name, remove_ids=[_id(2)], updates={_id(3): updated_entry})
        assert {e.id for e in result} == {_id(3), _id(4)}
        reconciled = {e.id: e for e in result}
        assert reconciled[_id(3)].attempts == 2
        assert reconciled[_id(3)].error_message == "still failing"
    finally:
        dlq.clear(sync_name)
    assert dlq.read(sync_name) == []


def test_dlq_replace_is_atomic_and_leaves_old_queue_intact_on_failure() -> None:
    """The #955 failure class this backend's staged single-``MERGE`` (see
    warehouse_databricks.py's module docstring) exists to prevent — closed
    without an explicit transaction, since Delta has none. A deliberately
    null ``error_message`` violates the real table's ``NOT NULL`` column
    only at the final MERGE step (Delta's ``CREATE TABLE ... AS SELECT``
    does not carry the constraint onto the scratch staging table), so the
    staging phase succeeds and the MERGE itself is what must fail atomically
    — proving the original queue survives untouched rather than partially
    replaced."""
    creds = _require_creds()
    profile = _profile(creds)
    dlq = DatabricksWarehouseDlqBackend(profile)
    sync_name = f"orders_{uuid.uuid4().hex[:8]}"
    run = uuid.uuid4().hex[:8]
    keep_id, new_id_1, new_id_2 = f"keep-{run}", f"new-{run}-1", f"new-{run}-2"

    try:
        original = [DeadLetter(record={"n": 1}, error_message="orig", id=keep_id)]
        dlq.append(sync_name, original)
        assert dlq.depth(sync_name) == 1

        broken_replacement = [
            DeadLetter(record={"n": 2}, error_message="new", id=new_id_1),
            DeadLetter(record={"n": 3}, error_message=None, id=new_id_2),  # type: ignore[arg-type]
        ]
        with pytest.raises(Exception, match="(?i)null"):
            dlq.replace(sync_name, broken_replacement)

        # The MERGE must have failed as a whole -- the original entry is
        # still exactly there, not gone and not doubled.
        survivors = dlq.read(sync_name)
        assert [e.id for e in survivors] == [keep_id]
        assert survivors[0].error_message == "orig"
    finally:
        dlq.clear(sync_name)


def test_dlq_replace_with_empty_list_clears_the_queue() -> None:
    """clear()/replace(sync_name, []) goes through a plain DELETE rather than
    an empty-source MERGE (see module docstring) -- proves that path
    actually empties the queue live, not just that it avoids the MERGE."""
    creds = _require_creds()
    profile = _profile(creds)
    dlq = DatabricksWarehouseDlqBackend(profile)
    sync_name = f"orders_{uuid.uuid4().hex[:8]}"

    dlq.append(sync_name, [DeadLetter(record={"n": 1}, error_message="x", id="id-1")])
    assert dlq.depth(sync_name) == 1

    dlq.replace(sync_name, [])
    assert dlq.read(sync_name) == []
    assert dlq.depth(sync_name) == 0


def test_writes_succeed_with_a_preexisting_schema_and_no_create_schema_privilege() -> None:
    """The escape hatch, live: managed_schema points at the principal's
    already-granted smoke schema (no CREATE SCHEMA grant needed — see
    test_databricks_managed_table_smoke.py's equivalent test for why). Every
    write path must succeed without ever needing to CREATE the schema, only
    its own tables inside it."""
    creds = _require_creds()
    profile = _profile(creds)
    sync_name = f"escape_hatch_{uuid.uuid4().hex[:8]}"

    DatabricksWarehouseStateStore(profile).save_sync(
        SyncState(sync_name=sync_name, last_run_at="t", records_synced=1, status="success")
    )
    assert DatabricksWarehouseStateStore(profile).get_last_sync(sync_name) is not None

    DatabricksWarehouseHistoryStore(profile).append(
        HistoryEntry(
            sync_name=sync_name,
            started_at="t0",
            completed_at="t1",
            duration_seconds=1.0,
            status="success",
            records_synced=1,
            records_failed=0,
        )
    )
    assert DatabricksWarehouseHistoryStore(profile).read(sync_name) != []

    dlq = DatabricksWarehouseDlqBackend(profile)
    try:
        dlq.append(sync_name, [DeadLetter(record={}, error_message="x", id=f"id-{sync_name}")])
        assert dlq.depth(sync_name) == 1
    finally:
        dlq.clear(sync_name)


def _require_create_schema_grant() -> None:
    if not os.environ.get(_HAS_CREATE_SCHEMA_GRANT_ENV):
        pytest.skip(
            f"{_HAS_CREATE_SCHEMA_GRANT_ENV} not set — run provisioning/databricks.sql's "
            "GRANT CREATE SCHEMA ON CATALOG against the real smoke account first, then "
            "set this env var to enable this test."
        )


def test_concurrent_first_writes_survive_table_creation_race() -> None:
    """Mirrors the Postgres/Snowflake legs' concurrent-first-use finding:
    CREATE TABLE IF NOT EXISTS is not guaranteed atomic across sessions. 8
    threads race save_sync() for a never-before-seen managed schema; none
    may raise."""
    creds = _require_creds()
    _require_create_schema_grant()
    schema_name = f"drt_smoke_{uuid.uuid4().hex[:10]}"
    profile = _profile(creds, managed_schema=schema_name)

    def write(i: int) -> None:
        DatabricksWarehouseStateStore(profile).save_sync(
            SyncState(sync_name=f"sync_{i}", last_run_at="t", records_synced=1, status="success")
        )

    errors: list[BaseException] = []
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(write, i) for i in range(8)]
            for future in concurrent.futures.as_completed(futures):
                exc = future.exception()
                if exc is not None:
                    errors.append(exc)

        assert not errors, f"concurrent first writes raised: {errors}"

        store = DatabricksWarehouseStateStore(profile)
        assert set(store.get_all().keys()) == {f"sync_{i}" for i in range(8)}
    finally:
        conn = _admin_connect(creds)
        try:
            cur = conn.cursor()
            cur.execute(f"DROP SCHEMA IF EXISTS {creds[CATALOG_ENV]}.{schema_name} CASCADE")
        finally:
            conn.close()
