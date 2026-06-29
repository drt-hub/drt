"""Snowflake DWH smoke test — reference shape for the harness (#674 / #671).

Drives the full pipeline against a *real* Snowflake account:
seeded DuckDB ``users`` -> engine -> live Snowflake table -> read back -> verify.

This is the canonical per-warehouse leg. Databricks (#672) and BigQuery (#673)
mirror this file; only the destination config + read-back/cleanup driver change.

Runs only when the ``DRT_SMOKE_SNOWFLAKE_*`` secrets are present (injected by the
dwh-smoke workflow). Otherwise it skips — safe no-op for forks / local runs.
See tests/integration/dwh/README.md for the secret list.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from drt.config.models import SnowflakeDestinationConfig, SyncConfig, SyncOptions
from drt.destinations.snowflake import SnowflakeDestination
from drt.engine.sync import run_sync

from .conftest import require_env, seed_duckdb_users, unique_table

pytestmark = pytest.mark.dwh_smoke

# Driver gate: skip the whole module if drt-core[snowflake] isn't installed.
snowflake_connector = pytest.importorskip("snowflake.connector")

# Env var NAMES the destination resolves credentials from. The smoke secrets
# are passed straight through under these names.
ACCOUNT_ENV = "DRT_SMOKE_SNOWFLAKE_ACCOUNT"
USER_ENV = "DRT_SMOKE_SNOWFLAKE_USER"
PASSWORD_ENV = "DRT_SMOKE_SNOWFLAKE_PASSWORD"


def _readback_count_and_names(creds: dict[str, str], table: str) -> tuple[int, set[str]]:
    """Open a fresh Snowflake connection and read the rows the sync wrote."""
    conn = snowflake_connector.connect(
        account=creds[ACCOUNT_ENV],
        user=creds[USER_ENV],
        password=creds[PASSWORD_ENV],
        warehouse=creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
        database=creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
        schema=creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
    )
    try:
        with conn.cursor() as cur:
            # Unquoted to match the destination's unquoted INSERT, which
            # Snowflake folds to UPPERCASE (quoted lowercase wouldn't match).
            cur.execute(f"SELECT name FROM {table}")
            names = {row[0] for row in cur.fetchall()}
        return len(names), names
    finally:
        conn.close()


def _create_table(creds: dict[str, str], table: str) -> None:
    """Pre-create the target table — drt's insert mode INSERTs into an existing
    table, it doesn't create one. Unquoted identifiers so Snowflake folds them
    to UPPERCASE, matching the destination's unquoted INSERT column list."""
    conn = snowflake_connector.connect(
        account=creds[ACCOUNT_ENV],
        user=creds[USER_ENV],
        password=creds[PASSWORD_ENV],
        warehouse=creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
        database=creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
        schema=creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {table} (id INTEGER, name VARCHAR, email VARCHAR)"
            )
    finally:
        conn.close()


def _drop_table(creds: dict[str, str], table: str) -> None:
    conn = snowflake_connector.connect(
        account=creds[ACCOUNT_ENV],
        user=creds[USER_ENV],
        password=creds[PASSWORD_ENV],
        warehouse=creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
        database=creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
        schema=creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
    )
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
    finally:
        conn.close()


def test_snowflake_insert_roundtrip(tmp_path: Path) -> None:
    """3 seeded rows sync into a real Snowflake table and read back intact."""
    creds = require_env(
        ACCOUNT_ENV,
        USER_ENV,
        PASSWORD_ENV,
        "DRT_SMOKE_SNOWFLAKE_DATABASE",
        "DRT_SMOKE_SNOWFLAKE_SCHEMA",
        "DRT_SMOKE_SNOWFLAKE_WAREHOUSE",
    )

    source, profile = seed_duckdb_users(tmp_path)
    table = unique_table("DRT_SMOKE")

    dest = SnowflakeDestinationConfig(
        **{
            "type": "snowflake",
            "account_env": ACCOUNT_ENV,
            "user_env": USER_ENV,
            "password_env": PASSWORD_ENV,
            "database": creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
            "schema": creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
            "table": table,
            "warehouse": creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
            "mode": "insert",
        }
    )
    sync = SyncConfig(
        name="snowflake_smoke",
        model="ref('users')",
        destination=dest,
        sync=SyncOptions(batch_size=10),
    )

    try:
        _create_table(creds, table)
        result = run_sync(sync, source, SnowflakeDestination(), profile, tmp_path)

        assert result.success == 3, f"expected 3 loaded rows, got {result.success}"
        assert result.failed == 0

        count, names = _readback_count_and_names(creds, table)
        assert count == 3
        assert names == {"Alice", "Bob", "Carol"}
    finally:
        _drop_table(creds, table)


def test_snowflake_connection() -> None:
    """`test_connection` succeeds against the real account (fast credential check)."""
    creds = require_env(
        ACCOUNT_ENV,
        USER_ENV,
        PASSWORD_ENV,
        "DRT_SMOKE_SNOWFLAKE_DATABASE",
        "DRT_SMOKE_SNOWFLAKE_SCHEMA",
        "DRT_SMOKE_SNOWFLAKE_WAREHOUSE",
    )
    dest = SnowflakeDestinationConfig(
        **{
            "type": "snowflake",
            "account_env": ACCOUNT_ENV,
            "user_env": USER_ENV,
            "password_env": PASSWORD_ENV,
            "database": creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
            "schema": creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
            "table": "DRT_SMOKE_CONNECTION_CHECK",
            "warehouse": creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
        }
    )
    SnowflakeDestination().test_connection(dest)
