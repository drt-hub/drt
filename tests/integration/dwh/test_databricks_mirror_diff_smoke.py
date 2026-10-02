"""Live Databricks mirror-diff coverage (#1177).

Mock cursors can pin the staged MERGE shape but cannot prove Delta accepts it.
This test exercises a removal-only second run with a composite upsert key.
"""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from drt.config.credentials import DatabricksProfile
from drt.config.models import DatabricksDestinationConfig, SyncConfig, SyncOptions
from drt.destinations.databricks import DatabricksDestination
from drt.engine.sync import run_sync
from drt.sources.databricks import DatabricksSource

from .conftest import require_env, unique_table

pytestmark = pytest.mark.dwh_smoke

dbsql = pytest.importorskip("databricks.sql")

HOST_ENV = "DRT_SMOKE_DATABRICKS_HOST"
HTTP_PATH_ENV = "DRT_SMOKE_DATABRICKS_HTTP_PATH"
TOKEN_ENV = "DRT_SMOKE_DATABRICKS_TOKEN"
CATALOG_ENV = "DRT_SMOKE_DATABRICKS_CATALOG"
SCHEMA_ENV = "DRT_SMOKE_DATABRICKS_SCHEMA"


def _connect(creds: dict[str, str]):
    return dbsql.connect(
        server_hostname=creds[HOST_ENV],
        http_path=creds[HTTP_PATH_ENV],
        access_token=creds[TOKEN_ENV],
    )


def _quoted_fqn(creds: dict[str, str], table: str) -> str:
    return f"`{creds[CATALOG_ENV]}`.`{creds[SCHEMA_ENV]}`.`{table}`"


def test_databricks_mirror_diff_deletes_composite_removed_key(tmp_path: Path) -> None:
    creds = require_env(HOST_ENV, HTTP_PATH_ENV, TOKEN_ENV, CATALOG_ENV, SCHEMA_ENV)
    catalog = creds[CATALOG_ENV]
    schema = creds[SCHEMA_ENV]
    source_table = unique_table("drt_mirror_diff_source")
    destination_table = unique_table("drt_mirror_diff_dest")
    source_fq = _quoted_fqn(creds, source_table)
    destination_fq = _quoted_fqn(creds, destination_table)
    sync_name = unique_table("databricks_mirror_diff")

    profile = DatabricksProfile(
        type="databricks",
        server_hostname=creds[HOST_ENV],
        http_path=creds[HTTP_PATH_ENV],
        access_token=creds[TOKEN_ENV],
        catalog=catalog,
        schema=schema,
        managed_schema=schema,
    )
    source = DatabricksSource()
    baseline, scratch = source._snapshot_table_names(sync_name)
    baseline_fq = _quoted_fqn(creds, baseline)
    scratch_fq = _quoted_fqn(creds, scratch)
    sync_digest = hashlib.sha1(sync_name.encode()).hexdigest()[:8]
    mirror_keys_fq = _quoted_fqn(
        creds, f"__drt_mirror_keys_{destination_table}_diff_{sync_name}_{sync_digest}"
    )

    destination = DatabricksDestinationConfig(
        **{
            "type": "databricks",
            "host_env": HOST_ENV,
            "http_path_env": HTTP_PATH_ENV,
            "token_env": TOKEN_ENV,
            "catalog": catalog,
            "schema": schema,
            "table": destination_table,
            "mode": "merge",
            "upsert_key": ["tenant_id", "id"],
        }
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

    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {source_fq} (tenant_id INT, id INT, label STRING) USING DELTA"
            )
            cur.execute(
                f"INSERT INTO {source_fq} VALUES "
                "(1, 1, 'keep-a'), (1, 2, 'remove'), (2, 1, 'keep-b')"
            )
            cur.execute(
                f"CREATE TABLE {destination_fq} (tenant_id INT, id INT, label STRING) USING DELTA"
            )

        first = run_sync(sync, source, DatabricksDestination(), profile, tmp_path)
        assert first.success == 3
        assert first.failed == 0
        assert first.diff_removed_keys == []

        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {source_fq} WHERE tenant_id = 1 AND id = 2")

        # No rows were added or changed, so this run reaches finalize_sync()
        # with no destination load batches and only one removed composite key.
        second = run_sync(sync, source, DatabricksDestination(), profile, tmp_path)
        assert second.success == 0
        assert second.failed == 0
        assert second.diff_removed_keys == [{"tenant_id": 1, "id": 2}]

        with conn.cursor() as cur:
            cur.execute(f"SELECT tenant_id, id, label FROM {destination_fq}")
            rows = set(cur.fetchall())
        assert rows == {(1, 1, "keep-a"), (2, 1, "keep-b")}
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {mirror_keys_fq}")
            cur.execute(f"DROP TABLE IF EXISTS {destination_fq}")
            cur.execute(f"DROP TABLE IF EXISTS {source_fq}")
            cur.execute(f"DROP TABLE IF EXISTS {scratch_fq}")
            cur.execute(f"DROP TABLE IF EXISTS {baseline_fq}")
        conn.close()
