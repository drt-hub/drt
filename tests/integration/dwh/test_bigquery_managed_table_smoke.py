"""BigQuery ManagedTableCapable smoke test (#960/#1107).

The unit suite proves client call shape; this live-gated test proves the
BigQuery resource semantics those mocks cannot: dataset creation in the
profile's project/location, idempotent repeated ensure, metadata-only table
existence checks, and symmetric table teardown without a SQL probe.

Runs only when ``DRT_SMOKE_BIGQUERY_*`` secrets are present. The smoke service
account has a project-level create-only custom role containing
``bigquery.datasets.create``; as creator it owns each unique throwaway dataset
this test creates. Cleanup always deletes that dataset and all of its contents.
"""

from __future__ import annotations

import threading
import uuid

import pytest

from drt.config.credentials import BigQueryProfile
from drt.sources.bigquery import BigQuerySource

from .conftest import require_env

pytestmark = pytest.mark.dwh_smoke

bigquery = pytest.importorskip("google.cloud.bigquery")

PROJECT_ENV = "DRT_SMOKE_BIGQUERY_PROJECT"
DATASET_ENV = "DRT_SMOKE_BIGQUERY_DATASET"
KEYFILE_ENV = "DRT_SMOKE_BIGQUERY_KEYFILE"


def _require_creds() -> dict[str, str]:
    return require_env(PROJECT_ENV, DATASET_ENV, KEYFILE_ENV)


def test_managed_schema_and_table_lifecycle() -> None:
    creds = _require_creds()
    project = creds[PROJECT_ENV]
    managed_schema = f"drt_smoke_managed_{uuid.uuid4().hex[:10]}"
    profile = BigQueryProfile(
        type="bigquery",
        project=project,
        dataset=creds[DATASET_ENV],
        method="keyfile",
        keyfile=creds[KEYFILE_ENV],
        managed_schema=managed_schema,
    )
    source = BigQuerySource()
    client = bigquery.Client.from_service_account_json(
        creds[KEYFILE_ENV], project=project, location=profile.location
    )
    dataset_id = f"{project}.{managed_schema}"
    table_name = "_drt_smoke_managed_table"
    table_id = f"{dataset_id}.{table_name}"

    try:
        source.ensure_managed_schema(profile)
        dataset = client.get_dataset(dataset_id)
        assert dataset.location == profile.location

        # Idempotent and escape-hatch safe: the second call finds the dataset
        # through get_dataset() and issues no create request.
        source.ensure_managed_schema(profile)

        assert source.managed_table_exists(profile, table_name) is False
        client.create_table(bigquery.Table(table_id, schema=[bigquery.SchemaField("id", "INT64")]))
        assert source.managed_table_exists(profile, table_name) is True

        source.drop_managed_table(profile, table_name)
        assert source.managed_table_exists(profile, table_name) is False
        source.drop_managed_table(profile, table_name)  # absent is a safe no-op
    finally:
        client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


def test_concurrent_first_use_is_idempotent() -> None:
    """Concurrent creators all succeed through ``exists_ok=True``."""
    creds = _require_creds()
    project = creds[PROJECT_ENV]
    managed_schema = f"drt_smoke_managed_{uuid.uuid4().hex[:10]}"
    profile = BigQueryProfile(
        type="bigquery",
        project=project,
        dataset=creds[DATASET_ENV],
        method="keyfile",
        keyfile=creds[KEYFILE_ENV],
        managed_schema=managed_schema,
    )
    dataset_id = f"{project}.{managed_schema}"
    client = bigquery.Client.from_service_account_json(
        creds[KEYFILE_ENV], project=project, location=profile.location
    )
    errors: list[BaseException] = []
    lock = threading.Lock()

    def _call() -> None:
        try:
            BigQuerySource().ensure_managed_schema(profile)
        except BaseException as exc:  # noqa: BLE001 - collected, not swallowed
            with lock:
                errors.append(exc)

    try:
        threads = [threading.Thread(target=_call) for _ in range(8)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert errors == [], f"concurrent ensure_managed_schema() raised: {errors!r}"
        assert client.get_dataset(dataset_id).location == profile.location
    finally:
        client.delete_dataset(dataset_id, delete_contents=True, not_found_ok=True)


def test_managed_table_capable_protocol_satisfied() -> None:
    from drt.sources.base import ManagedTableCapable

    assert isinstance(BigQuerySource(), ManagedTableCapable)
