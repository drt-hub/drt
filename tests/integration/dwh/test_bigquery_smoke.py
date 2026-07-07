"""BigQuery DWH smoke test (#674 / #673) — mirrors test_snowflake_smoke.py.

seeded DuckDB ``users`` -> engine -> live BigQuery table -> read back.
Runs only when ``DRT_SMOKE_BIGQUERY_*`` secrets are present; skips otherwise.

Auth uses a service-account keyfile: the workflow writes the SA JSON secret to a
file and exposes its path as ``DRT_SMOKE_BIGQUERY_KEYFILE``.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from drt.config.models import BigQueryDestinationConfig, SyncConfig, SyncOptions
from drt.destinations.bigquery import BigQueryDestination
from drt.engine.sync import run_sync

from .conftest import require_env, seed_duckdb_users, unique_table

pytestmark = pytest.mark.dwh_smoke

bigquery = pytest.importorskip("google.cloud.bigquery")


def test_bigquery_insert_roundtrip(tmp_path: Path) -> None:
    creds = require_env(
        "DRT_SMOKE_BIGQUERY_PROJECT",
        "DRT_SMOKE_BIGQUERY_DATASET",
        "DRT_SMOKE_BIGQUERY_KEYFILE",
    )
    source, profile = seed_duckdb_users(tmp_path)
    table = unique_table("drt_smoke")
    project = creds["DRT_SMOKE_BIGQUERY_PROJECT"]
    dataset = creds["DRT_SMOKE_BIGQUERY_DATASET"]
    keyfile = creds["DRT_SMOKE_BIGQUERY_KEYFILE"]
    fqn = f"`{project}`.`{dataset}`.`{table}`"

    dest = BigQueryDestinationConfig(
        **{
            "type": "bigquery",
            "project": project,
            "dataset": dataset,
            "table": table,
            "mode": "insert",
            "method": "keyfile",
            "keyfile": keyfile,
        }
    )
    sync = SyncConfig(
        name="bigquery_smoke",
        model="ref('users')",
        destination=dest,
        sync=SyncOptions(batch_size=10),
    )

    client = bigquery.Client.from_service_account_json(keyfile, project=project)
    try:
        # drt's insert mode streams into an existing table (insert_rows_json);
        # it doesn't create one, so pre-create with the seed schema.
        client.query(f"CREATE TABLE {fqn} (id INT64, name STRING, email STRING)").result()

        result = run_sync(sync, source, BigQueryDestination(), profile, tmp_path)
        assert result.success == 3, f"expected 3 loaded rows, got {result.success}"
        assert result.failed == 0

        # Streaming inserts land in a buffer that isn't immediately visible to
        # SQL SELECT — poll briefly until all three rows are queryable.
        names: set[str] = set()
        for _ in range(12):
            rows = client.query(f"SELECT name FROM {fqn}").result()
            names = {row["name"] for row in rows}
            if names == {"Alice", "Bob", "Carol"}:
                break
            time.sleep(5)
        assert names == {"Alice", "Bob", "Carol"}
    finally:
        client.query(f"DROP TABLE IF EXISTS {fqn}").result()


def test_bigquery_merge_roundtrip(tmp_path: Path) -> None:
    """MERGE mode upserts via a temp table + cleans it up (#645).

    Drives the second BigQuery write path (``mode: merge``) end-to-end: the
    engine loads the batch into ``<table>_drt_tmp`` (``load_table_from_json``),
    runs one ``MERGE INTO target USING tmp`` (UPDATE matched / INSERT unmatched),
    then drops the temp table. The target is pre-seeded with a stale ``id=1`` row
    so the run exercises both MERGE branches (UPDATE id=1, INSERT id=2,3), and we
    assert the ``_drt_tmp`` staging table no longer exists afterwards.
    """
    from google.cloud.exceptions import NotFound

    creds = require_env(
        "DRT_SMOKE_BIGQUERY_PROJECT",
        "DRT_SMOKE_BIGQUERY_DATASET",
        "DRT_SMOKE_BIGQUERY_KEYFILE",
    )
    source, profile = seed_duckdb_users(tmp_path)
    table = unique_table("drt_smoke")
    project = creds["DRT_SMOKE_BIGQUERY_PROJECT"]
    dataset = creds["DRT_SMOKE_BIGQUERY_DATASET"]
    keyfile = creds["DRT_SMOKE_BIGQUERY_KEYFILE"]
    fqn = f"`{project}`.`{dataset}`.`{table}`"
    # Matches the destination's temp-table naming: f"{project}.{dataset}.{table}_drt_tmp".
    tmp_table_id = f"{project}.{dataset}.{table}_drt_tmp"

    dest = BigQueryDestinationConfig(
        **{
            "type": "bigquery",
            "project": project,
            "dataset": dataset,
            "table": table,
            "mode": "merge",
            "upsert_key": ["id"],
            "method": "keyfile",
            "keyfile": keyfile,
        }
    )
    sync = SyncConfig(
        name="bigquery_merge_smoke",
        model="ref('users')",
        destination=dest,
        sync=SyncOptions(batch_size=10),
    )

    client = bigquery.Client.from_service_account_json(keyfile, project=project)
    try:
        client.query(f"CREATE TABLE {fqn} (id INT64, name STRING, email STRING)").result()
        # Pre-seed a stale id=1 via DML INSERT (immediately queryable, unlike a
        # streaming insert) so the MERGE has a matched row to UPDATE.
        client.query(
            f"INSERT INTO {fqn} (id, name, email) VALUES (1, 'STALE', 'stale@example.com')"
        ).result()

        result = run_sync(sync, source, BigQueryDestination(), profile, tmp_path)
        assert result.success == 3, f"expected 3 upserted rows, got {result.success}"
        assert result.failed == 0

        # MERGE results are visible to SQL immediately (no streaming buffer).
        rows = list(client.query(f"SELECT id, name FROM {fqn}").result())
        by_id = {row["id"]: row["name"] for row in rows}
        assert by_id == {
            1: "Alice",
            2: "Bob",
            3: "Carol",
        }, f"expected id=1 UPDATED to Alice + id=2,3 INSERTed, got {by_id}"

        # Temp staging table must be dropped (#645).
        with pytest.raises(NotFound):
            client.get_table(tmp_table_id)
    finally:
        client.query(f"DROP TABLE IF EXISTS {fqn}").result()
        client.query(f"DROP TABLE IF EXISTS `{tmp_table_id}`").result()


def test_bigquery_connection() -> None:
    """`test_connection` succeeds against the real project (fast credential check).

    Mirrors `test_snowflake_connection` — runs the destination's `SELECT 1`
    connectivity probe, no table needed.
    """
    creds = require_env(
        "DRT_SMOKE_BIGQUERY_PROJECT",
        "DRT_SMOKE_BIGQUERY_DATASET",
        "DRT_SMOKE_BIGQUERY_KEYFILE",
    )
    dest = BigQueryDestinationConfig(
        **{
            "type": "bigquery",
            "project": creds["DRT_SMOKE_BIGQUERY_PROJECT"],
            "dataset": creds["DRT_SMOKE_BIGQUERY_DATASET"],
            "table": "drt_smoke_connection_check",
            "method": "keyfile",
            "keyfile": creds["DRT_SMOKE_BIGQUERY_KEYFILE"],
        }
    )
    BigQueryDestination().test_connection(dest)
