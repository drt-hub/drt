"""BigQuery DWH smoke test (#674 / #673) — mirrors test_snowflake_smoke.py.

seeded DuckDB ``users`` -> engine -> live BigQuery table -> read back.
Runs only when ``DRT_SMOKE_BIGQUERY_*`` secrets are present; skips otherwise.

Auth uses a service-account keyfile: the workflow writes the SA JSON secret to a
file and exposes its path as ``DRT_SMOKE_BIGQUERY_KEYFILE``.
"""

from __future__ import annotations

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
        result = run_sync(sync, source, BigQueryDestination(), profile, tmp_path)
        assert result.success == 3, f"expected 3 loaded rows, got {result.success}"
        assert result.failed == 0

        rows = client.query(f"SELECT name FROM {fqn}").result()
        names = {row["name"] for row in rows}
        assert names == {"Alice", "Bob", "Carol"}
    finally:
        client.query(f"DROP TABLE IF EXISTS {fqn}").result()
