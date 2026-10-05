"""Live BigQuery sparse-MERGE coverage for #1134.

Mocks can prove that drt generates one MERGE per contiguous key-signature run,
but only BigQuery can prove an omitted JSON key preserves the matched target
value while an explicit JSON null still writes SQL NULL.
"""

from __future__ import annotations

from typing import Any

import pytest

from drt.config.models import BigQueryDestinationConfig, SyncOptions
from drt.destinations.bigquery import BigQueryDestination

from .conftest import require_env, unique_table

pytestmark = pytest.mark.dwh_smoke

bigquery = pytest.importorskip("google.cloud.bigquery")


def _drop_target_and_scratch(client: Any, table_id: str) -> None:
    dataset_id, table = table_id.rsplit(".", 1)
    client.delete_table(table_id, not_found_ok=True)
    for candidate in client.list_tables(dataset_id):
        if candidate.table_id.startswith(f"{table}_drt_tmp_"):
            client.delete_table(f"{dataset_id}.{candidate.table_id}", not_found_ok=True)


def test_bigquery_sparse_merge_preserves_omission_and_writes_late_and_null_fields() -> None:
    creds = require_env(
        "DRT_SMOKE_BIGQUERY_PROJECT",
        "DRT_SMOKE_BIGQUERY_DATASET",
        "DRT_SMOKE_BIGQUERY_KEYFILE",
    )
    project = creds["DRT_SMOKE_BIGQUERY_PROJECT"]
    dataset = creds["DRT_SMOKE_BIGQUERY_DATASET"]
    keyfile = creds["DRT_SMOKE_BIGQUERY_KEYFILE"]
    table = unique_table("drt_sparse_merge")
    table_id = f"{project}.{dataset}.{table}"
    target = f"`{table_id}`"
    client = bigquery.Client.from_service_account_json(keyfile, project=project)
    config = BigQueryDestinationConfig(
        type="bigquery",
        project=project,
        dataset=dataset,
        table=table,
        mode="merge",
        upsert_key=["id"],
        method="keyfile",
        keyfile=keyfile,
    )
    records = [
        {"id": 1, "name": "updated-with-omission"},
        {"id": 2, "name": "inserted-with-late-field", "note": "late-value"},
        {"id": 3, "name": "updated-with-null", "note": None},
    ]

    try:
        client.query(f"CREATE TABLE {target} (id INT64, name STRING, note STRING)").result()
        client.query(
            f"INSERT INTO {target} (id, name, note) VALUES "
            "(1, 'stale-one', 'keep-me'), "
            "(3, 'stale-three', 'clear-me')"
        ).result()

        result = BigQueryDestination().load(records, config, SyncOptions(on_error="fail"))

        assert result.success == 3
        assert result.failed == 0
        rows = list(client.query(f"SELECT id, name, note FROM {target} ORDER BY id").result())
        assert [(row["id"], row["name"], row["note"]) for row in rows] == [
            (1, "updated-with-omission", "keep-me"),
            (2, "inserted-with-late-field", "late-value"),
            (3, "updated-with-null", None),
        ]
    finally:
        _drop_target_and_scratch(client, table_id)
