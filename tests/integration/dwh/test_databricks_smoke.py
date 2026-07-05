"""Databricks DWH smoke test (#674 / #672) — mirrors test_snowflake_smoke.py.

seeded DuckDB ``users`` -> engine -> live Databricks Delta table -> read back.
Runs only when ``DRT_SMOKE_DATABRICKS_*`` secrets are present; skips otherwise.

Covers, per the #672 split (BigQuery #673 / PR #700 is the reference shape):

- ``test_databricks_insert_roundtrip`` — the streaming ``mode: insert`` leg.
- ``test_databricks_replace_swap_roundtrip`` — ``INSERT OVERWRITE`` atomicity for
  ``replace_strategy: swap`` (#644): a pre-seeded stale row is replaced by the
  atomic finalize-time overwrite and the ``__drt_swap`` shadow is cleaned up.
- ``test_databricks_connection`` — fast credential check via ``test_connection``.

Complex-type / VARIANT serialization (the other #672 bullet, #317 Databricks
leg) is intentionally left as an optional follow-up to keep this leg's scope
aligned with #700 (which likewise deferred composite-key MERGE).
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

    # drt's insert mode appends into an existing table — pre-create it. Must be
    # a Delta table (the destination relies on Delta for merge/replace paths).
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {fqn} (id INT, name STRING, email STRING) USING DELTA")
    finally:
        conn.close()

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


def test_databricks_replace_swap_roundtrip(tmp_path: Path) -> None:
    """``replace_strategy: swap`` — atomic ``INSERT OVERWRITE`` from a shadow (#644).

    The Databricks analogue of ``test_bigquery_merge_roundtrip`` (#700): drives one
    non-``insert`` write mode end-to-end. Pre-seeds a stale row the source never
    emits, runs ``sync.mode: replace`` with ``replace_strategy: swap``, then asserts
    (a) the stale row is gone — the finalize-time ``INSERT OVERWRITE`` atomically
    replaced the table's data — and (b) the ``<table>__drt_swap`` shadow was dropped
    in ``finalize_sync``.
    """
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
    shadow_fqn = f"`{catalog}`.`{schema}`.`{table}__drt_swap`"

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
        name="databricks_swap_smoke",
        model="ref('users')",
        destination=dest,
        sync=SyncOptions(mode="replace", replace_strategy="swap", batch_size=10),
    )

    # Pre-create the target (Delta) and seed a stale row. The target must exist
    # for the shadow path to engage — a first run with no target falls through to
    # a direct write and never builds a shadow.
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {fqn} (id INT, name STRING, email STRING) USING DELTA")
            cur.execute(f"INSERT INTO {fqn} VALUES (99, 'Stale', 'stale@example.com')")
    finally:
        conn.close()

    try:
        result = run_sync(sync, source, DatabricksDestination(), profile, tmp_path)
        assert result.success == 3, f"expected 3 loaded rows, got {result.success}"
        assert result.failed == 0

        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT name FROM {fqn}")
                names = {row[0] for row in cur.fetchall()}
                # Shadow must be gone — finalize_sync drops it after the overwrite.
                cur.execute(f"SHOW TABLES IN `{catalog}`.`{schema}` LIKE '{table}__drt_swap'")
                shadow_rows = cur.fetchall()
        finally:
            conn.close()
        # Stale row replaced atomically; only the 3 source rows remain.
        assert names == {"Alice", "Bob", "Carol"}
        assert shadow_rows == [], "swap shadow was not cleaned up in finalize_sync"
    finally:
        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {fqn}")
                cur.execute(f"DROP TABLE IF EXISTS {shadow_fqn}")
        finally:
            conn.close()


def test_databricks_connection() -> None:
    """`test_connection` succeeds against the real account (fast credential check).

    Mirrors ``test_snowflake_connection`` / ``test_bigquery_connection``.
    """
    creds = require_env(
        HOST_ENV,
        HTTP_PATH_ENV,
        TOKEN_ENV,
        "DRT_SMOKE_DATABRICKS_CATALOG",
        "DRT_SMOKE_DATABRICKS_SCHEMA",
    )
    dest = DatabricksDestinationConfig(
        **{
            "type": "databricks",
            "host_env": HOST_ENV,
            "http_path_env": HTTP_PATH_ENV,
            "token_env": TOKEN_ENV,
            "catalog": creds["DRT_SMOKE_DATABRICKS_CATALOG"],
            "schema": creds["DRT_SMOKE_DATABRICKS_SCHEMA"],
            "table": "drt_smoke_connection_check",
            "mode": "insert",
        }
    )
    DatabricksDestination().test_connection(dest)
