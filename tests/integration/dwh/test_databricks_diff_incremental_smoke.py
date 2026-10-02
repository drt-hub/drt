"""Live Databricks snapshot-diff incremental coverage (#1114/#755).

Mock cursors cannot validate Databricks CTAS/TBLPROPERTIES/xxhash64/RESTORE
syntax or prove that a NULL-to-empty-string transition is classified as a
change. This suite runs only with the existing DRT_SMOKE_DATABRICKS_* secrets.
"""

from __future__ import annotations

import uuid

import pytest

from drt.config.credentials import DatabricksProfile
from drt.sources.databricks import DatabricksSource

from .conftest import require_env

pytestmark = pytest.mark.dwh_smoke

dbsql = pytest.importorskip("databricks.sql")

HOST_ENV = "DRT_SMOKE_DATABRICKS_HOST"
HTTP_PATH_ENV = "DRT_SMOKE_DATABRICKS_HTTP_PATH"
TOKEN_ENV = "DRT_SMOKE_DATABRICKS_TOKEN"
CATALOG_ENV = "DRT_SMOKE_DATABRICKS_CATALOG"
SCHEMA_ENV = "DRT_SMOKE_DATABRICKS_SCHEMA"


def _require_creds() -> dict[str, str]:
    return require_env(HOST_ENV, HTTP_PATH_ENV, TOKEN_ENV, CATALOG_ENV, SCHEMA_ENV)


def _profile(creds: dict[str, str]) -> DatabricksProfile:
    return DatabricksProfile(
        type="databricks",
        server_hostname=creds[HOST_ENV],
        http_path=creds[HTTP_PATH_ENV],
        access_token=creds[TOKEN_ENV],
        catalog=creds[CATALOG_ENV],
        schema=creds[SCHEMA_ENV],
        managed_schema=creds[SCHEMA_ENV],
    )


def _connect(creds: dict[str, str]):
    return dbsql.connect(
        server_hostname=creds[HOST_ENV],
        http_path=creds[HTTP_PATH_ENV],
        access_token=creds[TOKEN_ENV],
    )


def _quoted_fqn(creds: dict[str, str], table: str) -> str:
    return f"`{creds[CATALOG_ENV]}`.`{creds[SCHEMA_ENV]}`.`{table}`"


def test_databricks_diff_incremental_round_trip_and_crash_recovery() -> None:
    """Classify two generations and prove an uncommitted extract repeats."""
    creds = _require_creds()
    profile = _profile(creds)
    source = DatabricksSource()
    suffix = uuid.uuid4().hex[:10]
    source_table = f"drt_diff_source_{suffix}"
    source_fq = _quoted_fqn(creds, source_table)
    sync_name = f"databricks_diff_{suffix}"
    query = f"SELECT id AS `id`, note AS `note`, plan AS `plan` FROM {source_fq}"
    baseline, scratch = source._snapshot_table_names(sync_name)
    baseline_fq = _quoted_fqn(creds, baseline)
    scratch_fq = _quoted_fqn(creds, scratch)

    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {source_fq} (id INT, note STRING, plan STRING) USING DELTA")
            cur.execute(
                f"INSERT INTO {source_fq} VALUES "
                "(1, NULL, 'free'), (2, 'b', 'free'), (3, 'c', 'pro')"
            )

        first = source.extract_snapshot_diff(
            query,
            profile,
            sync_name=sync_name,
            key_columns=["ID"],  # Delta resolution is case-insensitive.
            hash_columns=["NOTE", "plan"],
        )
        assert first.is_first_run is True
        assert {row["ID"] for row in first.added} == {1, 2, 3}
        assert list(first.changed) == []
        assert list(first.removed_keys) == []
        source.commit_snapshot_diff(profile, sync_name)

        with conn.cursor() as cur:
            cur.execute(f"UPDATE {source_fq} SET note = '' WHERE id = 1")
            cur.execute(f"UPDATE {source_fq} SET plan = 'pro' WHERE id = 2")
            cur.execute(f"DELETE FROM {source_fq} WHERE id = 3")
            cur.execute(f"INSERT INTO {source_fq} VALUES (4, 'd', 'free')")

        second = source.extract_snapshot_diff(
            query,
            profile,
            sync_name=sync_name,
            key_columns=["ID"],
            hash_columns=["NOTE", "plan"],
        )
        assert second.is_first_run is False
        assert {row["ID"] for row in second.added} == {4}
        changed = list(second.changed)
        assert {row["ID"] for row in changed} == {1, 2}
        assert next(row for row in changed if row["ID"] == 1)["note"] == ""
        assert list(second.removed_keys) == [{"ID": 3}]
        # Simulate failed delivery: skip commit so the baseline stays stale.

        retry = source.extract_snapshot_diff(
            query,
            profile,
            sync_name=sync_name,
            key_columns=["ID"],
            hash_columns=["NOTE", "plan"],
        )
        assert {row["ID"] for row in retry.added} == {4}
        assert {row["ID"] for row in retry.changed} == {1, 2}
        assert list(retry.removed_keys) == [{"ID": 3}]
        source.commit_snapshot_diff(profile, sync_name)

        unchanged = source.extract_snapshot_diff(
            query,
            profile,
            sync_name=sync_name,
            key_columns=["ID"],
            hash_columns=["NOTE", "plan"],
        )
        assert list(unchanged.added) == []
        assert list(unchanged.changed) == []
        assert list(unchanged.removed_keys) == []
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {source_fq}")
            cur.execute(f"DROP TABLE IF EXISTS {scratch_fq}")
            cur.execute(f"DROP TABLE IF EXISTS {baseline_fq}")
        conn.close()


def test_databricks_diff_detects_concurrent_scratch_replacement() -> None:
    """A second run cannot be streamed or promoted by the first run."""
    creds = _require_creds()
    profile = _profile(creds)
    first_source = DatabricksSource()
    second_source = DatabricksSource()
    suffix = uuid.uuid4().hex[:10]
    source_table = f"drt_diff_race_source_{suffix}"
    source_fq = _quoted_fqn(creds, source_table)
    sync_name = f"databricks_diff_race_{suffix}"
    query = f"SELECT id AS `id`, note AS `note` FROM {source_fq}"
    baseline, scratch = first_source._snapshot_table_names(sync_name)
    baseline_fq = _quoted_fqn(creds, baseline)
    scratch_fq = _quoted_fqn(creds, scratch)

    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {source_fq} (id INT, note STRING) USING DELTA")
            cur.execute(f"INSERT INTO {source_fq} VALUES (1, 'first')")

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
    finally:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {source_fq}")
            cur.execute(f"DROP TABLE IF EXISTS {scratch_fq}")
            cur.execute(f"DROP TABLE IF EXISTS {baseline_fq}")
        conn.close()
