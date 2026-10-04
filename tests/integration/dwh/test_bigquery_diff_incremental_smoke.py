"""Live BigQuery snapshot-diff incremental coverage (#1113/#755).

Mocks cannot validate GoogleSQL STRUCT hashing, table-label propagation, or
the atomic WRITE_TRUNCATE copy-job boundary. This suite runs only with the
existing DRT_SMOKE_BIGQUERY_* secrets and always removes its unique dataset.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator
from typing import Any

import pytest

from drt.config.credentials import BigQueryProfile
from drt.sources.bigquery import BigQuerySource

from .conftest import require_env

pytestmark = pytest.mark.dwh_smoke

bigquery = pytest.importorskip("google.cloud.bigquery")

PROJECT_ENV = "DRT_SMOKE_BIGQUERY_PROJECT"
DATASET_ENV = "DRT_SMOKE_BIGQUERY_DATASET"
KEYFILE_ENV = "DRT_SMOKE_BIGQUERY_KEYFILE"


@pytest.fixture
def bigquery_diff_profile() -> Iterator[tuple[BigQueryProfile, Any]]:
    creds = require_env(PROJECT_ENV, DATASET_ENV, KEYFILE_ENV)
    project = creds[PROJECT_ENV]
    keyfile = creds[KEYFILE_ENV]
    client = bigquery.Client.from_service_account_json(keyfile, project=project)
    existing = client.get_dataset(f"{project}.{creds[DATASET_ENV]}")
    managed_schema = f"drt_diff_{uuid.uuid4().hex[:16]}"
    profile = BigQueryProfile(
        type="bigquery",
        project=project,
        dataset=creds[DATASET_ENV],
        method="keyfile",
        keyfile=keyfile,
        location=existing.location,
        managed_schema=managed_schema,
    )
    try:
        BigQuerySource().ensure_managed_schema(profile)
        yield profile, client
    finally:
        client.delete_dataset(
            f"{project}.{managed_schema}",
            delete_contents=True,
            not_found_ok=True,
        )


def test_bigquery_diff_incremental_round_trip_and_crash_recovery(
    bigquery_diff_profile: tuple[BigQueryProfile, Any],
) -> None:
    """Classify two generations and prove an uncommitted extract repeats."""
    profile, client = bigquery_diff_profile
    source = BigQuerySource()
    suffix = uuid.uuid4().hex[:10]
    source_table = f"source_{suffix}"
    source_id = f"{profile.project}.{profile.managed_schema}.{source_table}"
    source_fq = f"`{source_id}`"
    sync_name = f"bigquery_diff_{suffix}"
    query = f"SELECT id AS ID, note, plan FROM {source_fq}"

    table = bigquery.Table(
        source_id,
        schema=[
            bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("note", "STRING"),
            bigquery.SchemaField("plan", "STRING"),
        ],
    )
    client.create_table(table)
    client.load_table_from_json(
        [
            {"id": 1, "note": None, "plan": "free"},
            {"id": 2, "note": "b", "plan": "free"},
            {"id": 3, "note": "c", "plan": "pro"},
        ],
        source_id,
    ).result()

    first = source.extract_snapshot_diff(
        query,
        profile,
        sync_name=sync_name,
        key_columns=["ID"],
        hash_columns=["note", "plan"],
    )
    assert first.is_first_run is True
    assert {row["ID"] for row in first.added} == {1, 2, 3}
    assert list(first.changed) == []
    assert list(first.removed_keys) == []
    source.commit_snapshot_diff(profile, sync_name)

    client.query(
        f"UPDATE {source_fq} SET note = @note WHERE id = @id",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("note", "STRING", ""),
                bigquery.ScalarQueryParameter("id", "INT64", 1),
            ]
        ),
    ).result()
    client.query(
        f"UPDATE {source_fq} SET plan = @plan WHERE id = @id",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("plan", "STRING", "pro"),
                bigquery.ScalarQueryParameter("id", "INT64", 2),
            ]
        ),
    ).result()
    client.query(
        f"DELETE FROM {source_fq} WHERE id = @id",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("id", "INT64", 3)]
        ),
    ).result()
    client.query(
        f"INSERT INTO {source_fq} (id, note, plan) VALUES (@id, @note, @plan)",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("id", "INT64", 4),
                bigquery.ScalarQueryParameter("note", "STRING", "d"),
                bigquery.ScalarQueryParameter("plan", "STRING", "free"),
            ]
        ),
    ).result()

    second = source.extract_snapshot_diff(
        query,
        profile,
        sync_name=sync_name,
        key_columns=["ID"],
        hash_columns=["note", "plan"],
    )
    assert second.is_first_run is False
    assert {row["ID"] for row in second.added} == {4}
    changed = list(second.changed)
    assert {row["ID"] for row in changed} == {1, 2}
    assert next(row for row in changed if row["ID"] == 1)["note"] == ""
    assert list(second.removed_keys) == [{"ID": 3}]

    # Simulate process death after extraction but before commit. A fresh
    # source instance atomically rebuilds the abandoned fixed-name scratch and
    # still diffs against the unchanged baseline.
    recovered = BigQuerySource()
    retry = recovered.extract_snapshot_diff(
        query,
        profile,
        sync_name=sync_name,
        key_columns=["ID"],
        hash_columns=["note", "plan"],
    )
    assert {row["ID"] for row in retry.added} == {4}
    assert {row["ID"] for row in retry.changed} == {1, 2}
    assert list(retry.removed_keys) == [{"ID": 3}]
    recovered.commit_snapshot_diff(profile, sync_name)

    unchanged = recovered.extract_snapshot_diff(
        query,
        profile,
        sync_name=sync_name,
        key_columns=["ID"],
        hash_columns=["note", "plan"],
    )
    assert list(unchanged.added) == []
    assert list(unchanged.changed) == []
    assert list(unchanged.removed_keys) == []


def test_bigquery_diff_detects_concurrent_scratch_replacement(
    bigquery_diff_profile: tuple[BigQueryProfile, Any],
) -> None:
    """A second run cannot be streamed or promoted by the first run."""
    profile, client = bigquery_diff_profile
    first_source = BigQuerySource()
    second_source = BigQuerySource()
    suffix = uuid.uuid4().hex[:10]
    source_table = f"race_source_{suffix}"
    source_id = f"{profile.project}.{profile.managed_schema}.{source_table}"
    sync_name = f"bigquery_diff_race_{suffix}"
    query = f"SELECT id, note FROM `{source_id}`"
    baseline, _ = first_source._snapshot_table_names(sync_name)

    table = bigquery.Table(
        source_id,
        schema=[
            bigquery.SchemaField("id", "INT64", mode="REQUIRED"),
            bigquery.SchemaField("note", "STRING"),
        ],
    )
    client.create_table(table)
    client.load_table_from_json([{"id": 1, "note": "first"}], source_id).result()

    first = first_source.extract_snapshot_diff(
        query,
        profile,
        sync_name=sync_name,
        key_columns=["id"],
        hash_columns="all",
    )
    second_source.extract_snapshot_diff(
        query,
        profile,
        sync_name=sync_name,
        key_columns=["id"],
        hash_columns="all",
    )

    yielded: list[dict[str, object]] = []
    with pytest.raises(RuntimeError, match=r"must not run concurrently"):
        for row in first.added:
            yielded.append(row)
    assert yielded == []

    with pytest.raises(RuntimeError, match=r"must not run concurrently"):
        first_source.commit_snapshot_diff(profile, sync_name)
    assert first_source.managed_table_exists(profile, baseline) is False
