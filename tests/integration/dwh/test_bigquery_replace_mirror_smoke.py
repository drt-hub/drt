"""Live BigQuery replace/mirror coverage (#1055).

Mocks can pin generated SQL and client calls, but cannot prove GoogleSQL DML or
copy-job behavior. These tests exercise both replace strategies and a two-run
mirror deletion against unique throwaway tables in the configured smoke
dataset. Every target and scratch table is cleaned up in ``finally``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

import pytest

from drt.config.models import BigQueryDestinationConfig, SyncConfig, SyncOptions
from drt.destinations.bigquery import BigQueryDestination
from drt.engine.sync import run_sync

from .conftest import require_env, seed_duckdb_users, unique_table

pytestmark = pytest.mark.dwh_smoke

bigquery = pytest.importorskip("google.cloud.bigquery")


def _credentials() -> dict[str, str]:
    return require_env(
        "DRT_SMOKE_BIGQUERY_PROJECT",
        "DRT_SMOKE_BIGQUERY_DATASET",
        "DRT_SMOKE_BIGQUERY_KEYFILE",
    )


def _destination(
    creds: dict[str, str], table: str, *, upsert: bool = False
) -> BigQueryDestinationConfig:
    return BigQueryDestinationConfig(
        type="bigquery",
        project=creds["DRT_SMOKE_BIGQUERY_PROJECT"],
        dataset=creds["DRT_SMOKE_BIGQUERY_DATASET"],
        table=table,
        # Mirror must force MERGE even when the ordinary write path is
        # configured as append-only insert.
        mode="insert",
        upsert_key=["id"] if upsert else None,
        method="keyfile",
        keyfile=creds["DRT_SMOKE_BIGQUERY_KEYFILE"],
    )


def _client(creds: dict[str, str]) -> Any:
    return bigquery.Client.from_service_account_json(
        creds["DRT_SMOKE_BIGQUERY_KEYFILE"],
        project=creds["DRT_SMOKE_BIGQUERY_PROJECT"],
    )


def _table_id(creds: dict[str, str], table: str) -> str:
    return f"{creds['DRT_SMOKE_BIGQUERY_PROJECT']}.{creds['DRT_SMOKE_BIGQUERY_DATASET']}.{table}"


def _drop_scratch(client: Any, table_id: str) -> None:
    dataset_id, table = table_id.rsplit(".", 1)
    client.delete_table(table_id, not_found_ok=True)
    client.delete_table(f"{table_id}_drt_tmp", not_found_ok=True)
    scratch_prefixes = (f"{table}__drt_swap_", f"{table}__drt_mirror_keys_")
    for scratch in client.list_tables(dataset_id):
        if scratch.table_id.startswith(scratch_prefixes):
            client.delete_table(f"{dataset_id}.{scratch.table_id}", not_found_ok=True)


@pytest.mark.parametrize("strategy", ["truncate", "swap"])
def test_bigquery_replace_strategies(strategy: Literal["truncate", "swap"], tmp_path: Path) -> None:
    creds = _credentials()
    source, profile = seed_duckdb_users(tmp_path)
    table = unique_table(f"drt_replace_{strategy}")
    table_id = _table_id(creds, table)
    target = f"`{table_id}`"
    client = _client(creds)
    sync = SyncConfig(
        name=unique_table(f"bigquery_replace_{strategy}"),
        model="ref('users')",
        destination=_destination(creds, table),
        # Two batches prove the first load truncates and the second appends.
        sync=SyncOptions(mode="replace", replace_strategy=strategy, batch_size=2),
    )

    try:
        client.query(f"CREATE TABLE {target} (id INT64, name STRING, email STRING)").result()
        client.query(f"INSERT INTO {target} VALUES (99, 'stale', 'stale@example.com')").result()

        result = run_sync(sync, source, BigQueryDestination(), profile, tmp_path)
        assert result.success == 3
        assert result.failed == 0
        rows = list(client.query(f"SELECT id, name FROM {target} ORDER BY id").result())
        assert [(row["id"], row["name"]) for row in rows] == [
            (1, "Alice"),
            (2, "Bob"),
            (3, "Carol"),
        ]
    finally:
        _drop_scratch(client, table_id)


def test_bigquery_mirror_deletes_removed_key(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    creds = _credentials()
    source, profile = seed_duckdb_users(tmp_path)
    table = unique_table("drt_mirror")
    table_id = _table_id(creds, table)
    target = f"`{table_id}`"
    client = _client(creds)
    destination = _destination(creds, table, upsert=True)
    sync = SyncConfig(
        name=unique_table("bigquery_mirror"),
        model="ref('users')",
        destination=destination,
        sync=SyncOptions(mode="mirror", batch_size=2),
    )

    try:
        client.query(f"CREATE TABLE {target} (id INT64, name STRING, email STRING)").result()
        first = run_sync(sync, source, BigQueryDestination(), profile, tmp_path)
        assert first.success == 3
        assert first.failed == 0

        conn = duckdb.connect(profile.database)
        try:
            conn.execute("DELETE FROM users WHERE id = 3")
        finally:
            conn.close()

        second = run_sync(sync, source, BigQueryDestination(), profile, tmp_path)
        assert second.success == 2
        assert second.failed == 0
        rows = list(client.query(f"SELECT id, name FROM {target} ORDER BY id").result())
        assert [(row["id"], row["name"]) for row in rows] == [
            (1, "Alice"),
            (2, "Bob"),
        ]
    finally:
        _drop_scratch(client, table_id)


def test_bigquery_mirror_null_scope_deletes_only_within_null_scope(tmp_path: Path) -> None:
    duckdb = pytest.importorskip("duckdb")
    creds = _credentials()
    source, profile = seed_duckdb_users(tmp_path)
    conn = duckdb.connect(profile.database)
    try:
        conn.execute("ALTER TABLE users ADD COLUMN parent_id INTEGER")
        # ids 1/2 form an all-NULL first batch; id 3 carries INT64 in batch 2.
        # The mirror key table must retain the target's INT64 scope type.
        conn.execute("UPDATE users SET parent_id = 7 WHERE id = 3")
    finally:
        conn.close()

    table = unique_table("drt_mirror_null_scope")
    table_id = _table_id(creds, table)
    target = f"`{table_id}`"
    client = _client(creds)
    destination = _destination(creds, table, upsert=True)
    sync = SyncConfig(
        name=unique_table("bigquery_mirror_null_scope"),
        model="ref('users')",
        destination=destination,
        sync=SyncOptions(mode="mirror", mirror={"scope": ["parent_id"]}, batch_size=2),
    )

    try:
        client.query(
            f"CREATE TABLE {target} (id INT64, name STRING, email STRING, parent_id INT64)"
        ).result()
        first = run_sync(sync, source, BigQueryDestination(), profile, tmp_path)
        assert first.success == 3
        client.query(
            f"INSERT INTO {target} VALUES "
            "(98, 'stale-null', 'stale-null@example.com', NULL), "
            "(99, 'other-scope', 'other-scope@example.com', 8)"
        ).result()

        second = run_sync(sync, source, BigQueryDestination(), profile, tmp_path)
        assert second.success == 3
        assert second.failed == 0
        rows = list(client.query(f"SELECT id, parent_id FROM {target} ORDER BY id").result())
        assert [(row["id"], row["parent_id"]) for row in rows] == [
            (1, None),
            (2, None),
            (3, 7),
            (99, 8),
        ]
    finally:
        _drop_scratch(client, table_id)
