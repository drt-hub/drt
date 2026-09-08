"""Warehouse-backed state/history/DLQ stores against a real Snowflake account (#1106).

A mock cursor can prove the right SQL text is *issued*, not that MERGE
actually upserts correctly, that PARSE_JSON round-trips a VARIANT column, or
that an explicit transaction genuinely rolls back a partial write — exactly
what tests/unit/test_state_warehouse_snowflake.py cannot prove, same lesson
tests/integration/local_sql/test_warehouse_state_backend_smoke.py already
established for the Postgres leg this mirrors.

Runs only when ``DRT_SMOKE_SNOWFLAKE_*`` secrets are present. Most tests use
the smoke role's already-granted ``DRT_SMOKE.PUBLIC`` schema as
``managed_schema`` — no new grant needed, since these tables land inside an
already-usable schema. Only the concurrent-first-write-race test needs a
brand-new schema (the #1106 ``CREATE SCHEMA`` grant), gated the same way as
``test_snowflake_managed_table_smoke.py``.
"""

from __future__ import annotations

import concurrent.futures
import os
import uuid

import pytest

from drt.config.credentials import SnowflakeProfile
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState
from drt.state.warehouse_snowflake import (
    SnowflakeWarehouseDlqBackend,
    SnowflakeWarehouseHistoryStore,
    SnowflakeWarehouseStateStore,
)

from .conftest import require_env

pytestmark = pytest.mark.dwh_smoke

snowflake_connector = pytest.importorskip("snowflake.connector")

ACCOUNT_ENV = "DRT_SMOKE_SNOWFLAKE_ACCOUNT"
USER_ENV = "DRT_SMOKE_SNOWFLAKE_USER"
PASSWORD_ENV = "DRT_SMOKE_SNOWFLAKE_PASSWORD"
KEY_ENV = "DRT_SMOKE_SNOWFLAKE_PRIVATE_KEY"
_HAS_CREATE_SCHEMA_GRANT_ENV = "DRT_SMOKE_SNOWFLAKE_HAS_CREATE_SCHEMA_GRANT"


def _require_creds() -> dict[str, str]:
    if not os.environ.get(KEY_ENV) and not os.environ.get(PASSWORD_ENV):
        pytest.skip(
            "Snowflake smoke auth not set: need DRT_SMOKE_SNOWFLAKE_PRIVATE_KEY "
            "(preferred) or DRT_SMOKE_SNOWFLAKE_PASSWORD."
        )
    return require_env(
        ACCOUNT_ENV,
        USER_ENV,
        "DRT_SMOKE_SNOWFLAKE_DATABASE",
        "DRT_SMOKE_SNOWFLAKE_SCHEMA",
        "DRT_SMOKE_SNOWFLAKE_WAREHOUSE",
    )


def _profile(creds: dict[str, str], **overrides: object) -> SnowflakeProfile:
    """``overrides`` (e.g. the concurrent-race test's ``managed_schema=
    schema_name``) must win over the defaults below — merged via a plain
    dict update rather than passed alongside them as separate kwargs, which
    silently dropped every caller's override until caught in review (the
    nightly workflow never actually ran this suite until #1108 wired it in,
    so nothing had exercised this path before)."""
    defaults: dict[str, object] = {
        "type": "snowflake",
        "account": creds[ACCOUNT_ENV],
        "user": creds[USER_ENV],
        "database": creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
        "warehouse": creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
        # Reuses the role's already-granted schema as managed_schema -- no
        # new CREATE SCHEMA grant needed for the round-trip tests below.
        "managed_schema": creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
    }
    if os.environ.get(KEY_ENV):
        defaults["private_key_env"] = KEY_ENV
    else:
        defaults["password_env"] = PASSWORD_ENV
    defaults.update(overrides)
    return SnowflakeProfile(**defaults)  # type: ignore[arg-type]


def _admin_connect(creds: dict[str, str]):
    auth: dict[str, object] = {}
    pem = os.environ.get(KEY_ENV)
    if pem:
        from drt.config.credentials import load_snowflake_private_key

        auth["private_key"] = load_snowflake_private_key(pem)
    else:
        auth["password"] = os.environ[PASSWORD_ENV]
    return snowflake_connector.connect(
        account=creds[ACCOUNT_ENV],
        user=creds[USER_ENV],
        warehouse=creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
        database=creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
        schema=creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
        **auth,
    )


def test_state_store_upsert_read_and_reset_round_trip() -> None:
    creds = _require_creds()
    profile = _profile(creds, managed_schema=f"{creds['DRT_SMOKE_SNOWFLAKE_SCHEMA']}")
    sync_a = f"orders_{uuid.uuid4().hex[:8]}"
    sync_b = f"customers_{uuid.uuid4().hex[:8]}"
    store = SnowflakeWarehouseStateStore(profile)
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
    """Codex review: a hard-coded "recent" timestamp eventually ages past
    any fixed retention_days, pruning both entries instead of one. The
    surviving entry's timestamp is derived from the test's own current UTC
    time instead, so this stays correct indefinitely."""
    from datetime import datetime, timezone

    creds = _require_creds()
    profile = _profile(creds)
    history = SnowflakeWarehouseHistoryStore(profile)
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
    # Proves PARSE_JSON/VARIANT round-tripped a real list, not a raw string.
    assert only_orders[0].errors == ["boom"]
    assert isinstance(only_orders[0].errors, list)
    assert only_orders[0].run_id == "run-1"

    # 2020 entry is older than any real retention window -- gets pruned;
    # the just-appended entry survives.
    removed = history.prune(sync_a, retention_days=30)
    assert removed == 1
    assert [e.started_at for e in history.read(sync_a)] == [recent]


def test_dlq_backend_fifo_reconcile_and_clear() -> None:
    """Codex review: the DLQ MERGEs globally on ``id`` and does not update
    ``sync_name`` on a match (same as Postgres's own ``ON CONFLICT (id) DO
    UPDATE`` — this is a shared, pre-existing property, not new here). A
    fixed literal id would collide with a leftover row from a prior run
    against this same persistent account and silently attribute to the
    wrong sync_name. Per-run-unique ids sidestep that regardless."""
    creds = _require_creds()
    profile = _profile(creds)
    dlq = SnowflakeWarehouseDlqBackend(profile)
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
        # Proves PARSE_JSON/VARIANT round-tripped a real dict, not a raw string.
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
    """The #955 failure class this backend's explicit transaction (see
    warehouse_snowflake.py's module docstring) exists to prevent: a crash
    partway through DELETE-then-INSERT under Snowflake's default
    per-statement autocommit would permanently erase the queue. Snowflake
    doesn't enforce PRIMARY KEY (unlike Postgres, where the equivalent test
    forces a duplicate-id UniqueViolation) -- NOT NULL *is* enforced, so a
    deliberately-null error_message forces a real mid-transaction failure
    instead."""
    creds = _require_creds()
    profile = _profile(creds)
    dlq = SnowflakeWarehouseDlqBackend(profile)
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

        # The DELETE half of the failed transaction must have rolled back
        # too -- the original entry is still exactly there, not gone and
        # not doubled.
        survivors = dlq.read(sync_name)
        assert [e.id for e in survivors] == [keep_id]
        assert survivors[0].error_message == "orig"
    finally:
        dlq.clear(sync_name)


def test_writes_succeed_with_a_preexisting_schema_and_no_create_schema_privilege() -> None:
    """The escape hatch, live: managed_schema points at the role's
    already-granted DRT_SMOKE.PUBLIC schema (no CREATE SCHEMA grant needed —
    see test_snowflake_managed_table_smoke.py's equivalent test for why).
    Every write path must succeed without ever needing to CREATE the
    schema, only its own tables inside it."""
    creds = _require_creds()
    profile = _profile(creds)
    sync_name = f"escape_hatch_{uuid.uuid4().hex[:8]}"

    SnowflakeWarehouseStateStore(profile).save_sync(
        SyncState(sync_name=sync_name, last_run_at="t", records_synced=1, status="success")
    )
    assert SnowflakeWarehouseStateStore(profile).get_last_sync(sync_name) is not None

    SnowflakeWarehouseHistoryStore(profile).append(
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
    assert SnowflakeWarehouseHistoryStore(profile).read(sync_name) != []

    dlq = SnowflakeWarehouseDlqBackend(profile)
    try:
        dlq.append(sync_name, [DeadLetter(record={}, error_message="x", id=f"id-{sync_name}")])
        assert dlq.depth(sync_name) == 1
    finally:
        dlq.clear(sync_name)


def _require_create_schema_grant() -> None:
    if not os.environ.get(_HAS_CREATE_SCHEMA_GRANT_ENV):
        pytest.skip(
            f"{_HAS_CREATE_SCHEMA_GRANT_ENV} not set — run provisioning/snowflake.sql "
            "section 4b (GRANT CREATE SCHEMA ON DATABASE) against the real smoke "
            "account first, then set this env var to enable this test."
        )


def test_concurrent_first_writes_survive_table_creation_race() -> None:
    """Mirrors the Postgres leg's concurrent-first-use finding: CREATE TABLE
    IF NOT EXISTS is not guaranteed atomic across sessions. 8 threads race
    save_sync() for a never-before-seen managed schema; none may raise."""
    creds = _require_creds()
    _require_create_schema_grant()
    schema_name = f"drt_smoke_{uuid.uuid4().hex[:10]}"
    profile = _profile(creds, managed_schema=schema_name)

    def write(i: int) -> None:
        SnowflakeWarehouseStateStore(profile).save_sync(
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

        store = SnowflakeWarehouseStateStore(profile)
        assert set(store.get_all().keys()) == {f"sync_{i}" for i in range(8)}
    finally:
        conn = _admin_connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"DROP SCHEMA IF EXISTS {creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}.{schema_name}"
                )
        finally:
            conn.close()
