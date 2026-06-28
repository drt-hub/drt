"""Databricks DWH smoke test (#674 / #672) — mirrors test_snowflake_smoke.py.

seeded DuckDB ``users`` -> engine -> live Databricks Delta table -> read back.
Runs only when ``DRT_SMOKE_DATABRICKS_*`` secrets are present; skips otherwise.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from drt.config.models import DatabricksDestinationConfig, SyncConfig, SyncOptions
from drt.destinations.databricks import DatabricksDestination
from drt.engine.sync import run_sync

from .conftest import require_env, seed_duckdb_users, unique_table

pytestmark = pytest.mark.dwh_smoke

dbsql = pytest.importorskip("databricks.sql")

HOST_ENV = "DRT_SMOKE_DATABRICKS_HOST"
HTTP_PATH_ENV = "DRT_SMOKE_DATABRICKS_HTTP_PATH"
TOKEN_ENV = "DRT_SMOKE_DATABRICKS_TOKEN"


def _connect(creds: dict[str, str]):
    return dbsql.connect(
        server_hostname=creds[HOST_ENV],
        http_path=creds[HTTP_PATH_ENV],
        access_token=creds[TOKEN_ENV],
    )


def test_databricks_insert_roundtrip(tmp_path: Path) -> None:
    creds = require_env(
        HOST_ENV,
        HTTP_PATH_ENV,
        TOKEN_ENV,
        "DRT_SMOKE_DATABRICKS_CATALOG",
        "DRT_SMOKE_DATABRICKS_SCHEMA",
    )
    source, profile = seed_duckdb_users(tmp_path)
    table = unique_table("drt_smoke")
    catalog = creds["DRT_SMOKE_DATABRICKS_CATALOG"]
    schema = creds["DRT_SMOKE_DATABRICKS_SCHEMA"]
    fqn = f"`{catalog}`.`{schema}`.`{table}`"

    dest = DatabricksDestinationConfig(
        **{
            "type": "databricks",
            "host_env": HOST_ENV,
            "http_path_env": HTTP_PATH_ENV,
            "token_env": TOKEN_ENV,
            "catalog": catalog,
            "schema": schema,
            "table": table,
            "mode": "insert",
        }
    )
    sync = SyncConfig(
        name="databricks_smoke",
        model="ref('users')",
        destination=dest,
        sync=SyncOptions(batch_size=10),
    )

    try:
        result = run_sync(sync, source, DatabricksDestination(), profile, tmp_path)
        assert result.success == 3, f"expected 3 loaded rows, got {result.success}"
        assert result.failed == 0

        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT name FROM {fqn}")
                names = {row[0] for row in cur.fetchall()}
        finally:
            conn.close()
        assert names == {"Alice", "Bob", "Carol"}
    finally:
        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {fqn}")
        finally:
            conn.close()
