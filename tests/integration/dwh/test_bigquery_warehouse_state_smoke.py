"""Warehouse state/history/DLQ stores against a real BigQuery project (#1107).

Each test creates its own throwaway managed dataset and always deletes it in
fixture cleanup. The smoke service account's project-level create-only role is
enough: a principal owns datasets it creates, while the backend itself still
supports the pre-provisioned-dataset/table escape hatch for production roles.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest

from drt.config.credentials import BigQueryProfile
from drt.state.dlq import DeadLetter
from drt.state.history import HistoryEntry
from drt.state.manager import SyncState
from drt.state.warehouse_bigquery import (
    BigQueryWarehouseDlqBackend,
    BigQueryWarehouseHistoryStore,
    BigQueryWarehouseStateStore,
)

from .conftest import require_env

pytestmark = pytest.mark.dwh_smoke

bigquery = pytest.importorskip("google.cloud.bigquery")


@pytest.fixture
def warehouse_profile() -> Iterator[BigQueryProfile]:
    creds = require_env(
        "DRT_SMOKE_BIGQUERY_PROJECT",
        "DRT_SMOKE_BIGQUERY_DATASET",
        "DRT_SMOKE_BIGQUERY_KEYFILE",
    )
    project = creds["DRT_SMOKE_BIGQUERY_PROJECT"]
    keyfile = creds["DRT_SMOKE_BIGQUERY_KEYFILE"]
    existing_dataset = f"{project}.{creds['DRT_SMOKE_BIGQUERY_DATASET']}"
    client = bigquery.Client.from_service_account_json(keyfile, project=project)
    location = client.get_dataset(existing_dataset).location
    managed_schema = f"drt_state_{uuid.uuid4().hex[:16]}"
    profile = BigQueryProfile(
        type="bigquery",
        project=project,
        dataset=creds["DRT_SMOKE_BIGQUERY_DATASET"],
        method="keyfile",
        keyfile=keyfile,
        location=location,
        managed_schema=managed_schema,
    )
    try:
        yield profile
    finally:
        client.delete_dataset(
            f"{project}.{managed_schema}",
            delete_contents=True,
            not_found_ok=True,
        )


def test_state_store_upsert_read_and_reset_round_trip(
    warehouse_profile: BigQueryProfile,
) -> None:
    store = BigQueryWarehouseStateStore(warehouse_profile)
    sync_a = f"orders_{uuid.uuid4().hex[:8]}"
    sync_b = f"customers_{uuid.uuid4().hex[:8]}"

    assert store.get_last_sync(sync_a) is None
    store.save_sync(
        SyncState(
            sync_name=sync_a,
            last_run_at="2026-10-04T00:00:00+00:00",
            records_synced=100,
            status="success",
            last_cursor_value="42",
        )
    )
    assert store.get_last_sync(sync_a) == SyncState(
        sync_name=sync_a,
        last_run_at="2026-10-04T00:00:00+00:00",
        records_synced=100,
        status="success",
        error=None,
        last_cursor_value="42",
    )

    store.save_sync(
        SyncState(
            sync_name=sync_a,
            last_run_at="2026-10-04T01:00:00+00:00",
            records_synced=150,
            status="partial",
            error="quoted ' error; still data",
            last_cursor_value="99",
        )
    )
    updated = store.get_last_sync(sync_a)
    assert updated is not None
    assert updated.records_synced == 150
    assert updated.error == "quoted ' error; still data"

    store.save_sync(
        SyncState(sync_name=sync_b, last_run_at="t", records_synced=1, status="success")
    )
    assert {sync_a, sync_b} == set(store.get_all())
    assert store.reset(sync_a) is True
    assert store.reset(sync_a) is False
    assert store.get_last_sync(sync_a) is None


def test_history_store_append_read_and_prune(
    warehouse_profile: BigQueryProfile,
) -> None:
    from datetime import datetime, timezone

    history = BigQueryWarehouseHistoryStore(warehouse_profile)
    sync_name = f"orders_{uuid.uuid4().hex[:8]}"
    recent = datetime.now(timezone.utc).isoformat()
    history.append(
        HistoryEntry(
            sync_name=sync_name,
            started_at="2020-01-01T00:00:00+00:00",
            completed_at="2020-01-01T00:01:00+00:00",
            duration_seconds=60.0,
            status="success",
            records_synced=10,
            records_failed=0,
        )
    )
    history.append(
        HistoryEntry(
            sync_name=sync_name,
            started_at=recent,
            completed_at=recent,
            duration_seconds=1.0,
            status="failed",
            records_synced=0,
            records_failed=1,
            errors=["quoted ' error"],
            run_id="run-1",
            sync_run_id="sync-run-1",
        )
    )

    entries = history.read(sync_name)
    assert [entry.status for entry in entries] == ["failed", "success"]
    assert entries[0].errors == ["quoted ' error"]
    assert entries[0].sync_run_id == "sync-run-1"
    assert history.prune(sync_name, retention_days=30) == 1
    assert [entry.started_at for entry in history.read(sync_name)] == [recent]


def test_dlq_fifo_reconcile_and_clear_round_trip(
    warehouse_profile: BigQueryProfile,
) -> None:
    dlq = BigQueryWarehouseDlqBackend(warehouse_profile)
    sync_name = f"orders_{uuid.uuid4().hex[:8]}"
    run = uuid.uuid4().hex[:8]

    def entry(i: int) -> DeadLetter:
        return DeadLetter(
            record={"id": i, "text": "quoted ' payload"},
            error_message="boom ' still data",
            timestamp=f"2026-10-04T00:00:0{i}+00:00",
            id=f"id-{run}-{i}",
        )

    assert dlq.read(sync_name) == []
    assert dlq.append(sync_name, [entry(i) for i in range(5)], max_records=3) == 3
    remaining = dlq.read(sync_name)
    assert [item.record["id"] for item in remaining] == [2, 3, 4]
    assert dlq.all_depths()[sync_name] == 3

    updated = entry(3)
    updated.attempts = 2
    updated.error_message = "still failing"
    result = dlq.reconcile(
        sync_name,
        remove_ids=[entry(2).id],
        updates={updated.id: updated},
    )
    assert {item.id for item in result} == {entry(3).id, entry(4).id}
    assert {item.id: item for item in result}[updated.id].attempts == 2

    dlq.clear(sync_name)
    assert dlq.depth(sync_name) == 0


def test_dlq_replace_upserts_before_removing_stale_entries(
    warehouse_profile: BigQueryProfile,
) -> None:
    dlq = BigQueryWarehouseDlqBackend(warehouse_profile)
    sync_name = f"replace_{uuid.uuid4().hex[:8]}"
    run = uuid.uuid4().hex[:8]
    keep_id, stale_id, new_id = f"keep-{run}", f"stale-{run}", f"new-{run}"
    dlq.append(
        sync_name,
        [
            DeadLetter(record={"n": 1}, error_message="old", id=keep_id),
            DeadLetter(record={"n": 2}, error_message="old", id=stale_id),
        ],
    )

    dlq.replace(
        sync_name,
        [
            DeadLetter(record={"n": 1}, error_message="updated", id=keep_id),
            DeadLetter(record={"n": 3}, error_message="new", id=new_id),
        ],
    )
    survivors = {entry.id: entry for entry in dlq.read(sync_name)}
    assert set(survivors) == {keep_id, new_id}
    assert survivors[keep_id].error_message == "updated"

    dlq.replace(sync_name, [])
    assert dlq.read(sync_name) == []


def test_dlq_same_id_is_isolated_between_syncs_across_append_and_replace(
    warehouse_profile: BigQueryProfile,
) -> None:
    dlq = BigQueryWarehouseDlqBackend(warehouse_profile)
    suffix = uuid.uuid4().hex[:8]
    sync_a = f"same_id_a_{suffix}"
    sync_b = f"same_id_b_{suffix}"
    shared_id = f"shared-{suffix}"

    dlq.append(
        sync_a,
        [DeadLetter(record={"owner": "a"}, error_message="a-old", id=shared_id)],
    )
    dlq.append(
        sync_b,
        [DeadLetter(record={"owner": "b"}, error_message="b-old", id=shared_id)],
    )
    assert [(item.id, item.record["owner"]) for item in dlq.read(sync_a)] == [(shared_id, "a")]
    assert [(item.id, item.record["owner"]) for item in dlq.read(sync_b)] == [(shared_id, "b")]

    dlq.replace(
        sync_a,
        [DeadLetter(record={"owner": "a-updated"}, error_message="a-new", id=shared_id)],
    )
    assert dlq.read(sync_a)[0].record["owner"] == "a-updated"
    assert dlq.read(sync_b)[0].record["owner"] == "b"
    assert dlq.read(sync_b)[0].error_message == "b-old"


def test_dlq_append_larger_than_one_merge_chunk_round_trips(
    warehouse_profile: BigQueryProfile,
) -> None:
    dlq = BigQueryWarehouseDlqBackend(warehouse_profile)
    sync_name = f"multi_chunk_{uuid.uuid4().hex[:8]}"
    entries = [
        DeadLetter(
            record={"index": index},
            error_message="chunked",
            timestamp=f"2026-10-04T00:00:00+00:00-{index:04d}",
            id=f"chunk-{index:04d}",
        )
        for index in range(501)
    ]

    assert dlq.append(sync_name, entries, max_records=0) == len(entries)
    round_tripped = dlq.read(sync_name)
    assert len(round_tripped) == len(entries)
    assert {item.record["index"] for item in round_tripped} == set(range(len(entries)))
