"""Live BigQuery ``write_policy: fill_empty`` coverage (#1238).

Mock clients can assert the generated MERGE text, but only BigQuery can prove
that TRIM is restricted to scalar STRING columns and that typed temp loads,
sparse signature runs, and semi-structured values compose correctly.
"""

from __future__ import annotations

import base64
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


def _encoded(value: str) -> str:
    """BigQuery JSON-load representation for a BYTES field."""
    return base64.b64encode(value.encode()).decode()


def test_bigquery_write_policy_fill_empty_merge_semantics() -> None:
    """Fill blank strings, keep typed values, override, and preserve omission."""
    creds = require_env(
        "DRT_SMOKE_BIGQUERY_PROJECT",
        "DRT_SMOKE_BIGQUERY_DATASET",
        "DRT_SMOKE_BIGQUERY_KEYFILE",
    )
    project = creds["DRT_SMOKE_BIGQUERY_PROJECT"]
    dataset = creds["DRT_SMOKE_BIGQUERY_DATASET"]
    keyfile = creds["DRT_SMOKE_BIGQUERY_KEYFILE"]
    table = unique_table("drt_write_policy")
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
    options = SyncOptions(
        write_policy="fill_empty",
        write_policy_overrides={"OVERWRITE_VALUE": "overwrite"},
    )
    records = [
        {
            "id": i,
            "string_value": f"source-{i}",
            "overwrite_value": f"new-{i}",
            "numeric_value": 99,
            "binary_value": _encoded(f"source-{i}"),
            "struct_value": {"value": f"source-{i}"},
        }
        for i in range(1, 6)
    ]
    # Omission, rather than explicit NULL, must leave this target column out of
    # the second signature-run MERGE altogether.
    records.append(
        {
            "id": 6,
            "overwrite_value": "new-6",
            "numeric_value": 99,
            "binary_value": _encoded("source-6"),
            "struct_value": {"value": "source-6"},
        }
    )

    try:
        client.query(
            f"CREATE TABLE {target} ("
            "id INT64, string_value STRING, overwrite_value STRING, "
            "numeric_value INT64, binary_value BYTES, "
            "struct_value STRUCT<value STRING>)"
        ).result()
        client.query(
            f"INSERT INTO {target} VALUES "
            "(1, NULL, 'old-1', NULL, NULL, NULL), "
            "(2, '', 'old-2', 2, b'', STRUCT('' AS value)), "
            "(3, '   ', 'old-3', 3, b'   ', STRUCT('   ' AS value)), "
            "(4, 'existing', 'old-4', 4, b'existing-4', "
            "STRUCT('existing-4' AS value)), "
            "(5, 'zero-row', 'old-5', 0, b'existing-5', "
            "STRUCT('existing-5' AS value)), "
            "(6, 'sparse-existing', 'old-6', 6, b'existing-6', "
            "STRUCT('existing-6' AS value))"
        ).result()

        result = BigQueryDestination().load(records, config, options)
        assert result.success == 6 and result.failed == 0

        rows = list(
            client.query(
                f"SELECT id, string_value, overwrite_value, numeric_value, "
                f"binary_value, struct_value.value AS struct_value FROM {target} ORDER BY id"
            ).result()
        )
        actual = [
            (
                row["id"],
                row["string_value"],
                row["overwrite_value"],
                row["numeric_value"],
                row["binary_value"],
                row["struct_value"],
            )
            for row in rows
        ]
        assert actual == [
            (1, "source-1", "new-1", 99, b"source-1", "source-1"),
            (2, "source-2", "new-2", 2, b"", ""),
            (3, "source-3", "new-3", 3, b"   ", "   "),
            (4, "existing", "new-4", 4, b"existing-4", "existing-4"),
            (5, "zero-row", "new-5", 0, b"existing-5", "existing-5"),
            (6, "sparse-existing", "new-6", 6, b"existing-6", "existing-6"),
        ]
    finally:
        _drop_target_and_scratch(client, table_id)
