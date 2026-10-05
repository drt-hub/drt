"""Live BigQuery diff-source + mirror-destination coverage.

The second run is removal-only: no added/changed row reaches ``load()``, yet
the destination must delete the exact composite key before the source promotes
its new snapshot baseline.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any

import pytest

from drt.config.credentials import BigQueryProfile
from drt.config.models import BigQueryDestinationConfig, SyncConfig, SyncOptions
from drt.destinations.bigquery import BigQueryDestination
from drt.engine.sync import run_sync
from drt.sources.bigquery import BigQuerySource

from .conftest import require_env

pytestmark = pytest.mark.dwh_smoke

bigquery = pytest.importorskip("google.cloud.bigquery")

PROJECT_ENV = "DRT_SMOKE_BIGQUERY_PROJECT"
DATASET_ENV = "DRT_SMOKE_BIGQUERY_DATASET"
KEYFILE_ENV = "DRT_SMOKE_BIGQUERY_KEYFILE"


def _credentials() -> dict[str, str]:
    return require_env(PROJECT_ENV, DATASET_ENV, KEYFILE_ENV)


def _rows(client: Any, table_fq: str) -> set[tuple[int, int, str]]:
    result = client.query(
        f"SELECT tenant_id, id, label FROM {table_fq} ORDER BY tenant_id, id"
    ).result()
    return {(row["tenant_id"], row["id"], row["label"]) for row in result}


def test_bigquery_mirror_diff_removal_only_deletes_and_advances_baseline(
    tmp_path: Path,
) -> None:
    creds = _credentials()
    project = creds[PROJECT_ENV]
    keyfile = creds[KEYFILE_ENV]
    client = bigquery.Client.from_service_account_json(keyfile, project=project)
    existing = client.get_dataset(f"{project}.{creds[DATASET_ENV]}")
    client = bigquery.Client.from_service_account_json(
        keyfile,
        project=project,
        location=existing.location,
    )
    dataset = f"drt_mirror_diff_{uuid.uuid4().hex[:16]}"
    source_table = "source_rows"
    destination_table = "destination_rows"
    source_id = f"{project}.{dataset}.{source_table}"
    destination_id = f"{project}.{dataset}.{destination_table}"
    source_fq = f"`{source_id}`"
    destination_fq = f"`{destination_id}`"
    sync_name = f"bigquery_mirror_diff_{uuid.uuid4().hex[:10]}"

    profile = BigQueryProfile(
        type="bigquery",
        project=project,
        dataset=dataset,
        managed_schema=dataset,
        method="keyfile",
        keyfile=keyfile,
        location=existing.location,
    )
    source = BigQuerySource()
    destination = BigQueryDestinationConfig(
        type="bigquery",
        project=project,
        dataset=dataset,
        table=destination_table,
        mode="merge",
        upsert_key=["tenant_id", "id"],
        method="keyfile",
        keyfile=keyfile,
        location=existing.location,
    )
    sync = SyncConfig(
        name=sync_name,
        model=(f"SELECT tenant_id, id, label FROM {source_fq} ORDER BY tenant_id, id"),
        destination=destination,
        sync=SyncOptions(
            mode="mirror",
            incremental_strategy="diff",
            mirror={"strategy": "diff"},
        ),
    )

    try:
        source.ensure_managed_schema(profile)
        schema = [
            bigquery.SchemaField("tenant_id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("label", "STRING"),
        ]
        client.create_table(bigquery.Table(source_id, schema=schema))
        client.create_table(bigquery.Table(destination_id, schema=schema))
        client.load_table_from_json(
            [
                {"tenant_id": 1, "id": 1, "label": "keep-a"},
                {"tenant_id": 1, "id": 2, "label": "remove"},
                {"tenant_id": 2, "id": 1, "label": "keep-b"},
            ],
            source_id,
        ).result()

        first = run_sync(sync, source, BigQueryDestination(), profile, tmp_path)
        assert first.success == 3
        assert first.failed == 0
        assert first.diff_removed_keys == []

        client.query(f"DELETE FROM {source_fq} WHERE tenant_id = 1 AND id = 2").result()

        # No row is added or changed. The only destination work is the
        # finalize-time exact-key DELETE fed by _diff_removed_keys.
        second = run_sync(sync, source, BigQueryDestination(), profile, tmp_path)
        assert second.success == 0
        assert second.failed == 0
        assert second.diff_removed_keys == [{"tenant_id": 1, "id": 2}]
        assert _rows(client, destination_fq) == {
            (1, 1, "keep-a"),
            (2, 1, "keep-b"),
        }

        # A third unchanged run proves the removal-only generation was
        # promoted only after its destination delete completed.
        third = run_sync(sync, source, BigQueryDestination(), profile, tmp_path)
        assert third.success == 0
        assert third.failed == 0
        assert third.diff_removed_keys == []
    finally:
        client.delete_dataset(
            f"{project}.{dataset}",
            delete_contents=True,
            not_found_ok=True,
        )
