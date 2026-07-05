"""Databricks DWH smoke test (#674 / #672) — mirrors test_snowflake_smoke.py.

seeded DuckDB ``users`` -> engine -> live Databricks Delta table -> read back.
Runs only when ``DRT_SMOKE_DATABRICKS_*`` secrets are present; skips otherwise.

Covers the #672 verification set on a real account:

- ``test_databricks_insert_roundtrip`` — the streaming ``mode: insert`` leg.
- ``test_databricks_replace_swap_roundtrip`` — ``INSERT OVERWRITE`` atomicity for
  ``replace_strategy: swap`` (#644): a pre-seeded stale row is replaced by the
  atomic finalize-time overwrite and the ``__drt_swap`` shadow is cleaned up.
- ``test_databricks_complex_type_serialization`` — complex-type / VARIANT-
  equivalent serialization (#317 Databricks leg): STRUCT / ARRAY reconstructed
  via ``from_json`` and a VARIANT column via ``parse_json``.
- ``test_databricks_connection`` — fast credential check via ``test_connection``.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from drt.config.credentials import DuckDBProfile
from drt.config.models import DatabricksDestinationConfig, SyncConfig, SyncOptions
from drt.destinations.databricks import DatabricksDestination
from drt.engine.sync import run_sync
from drt.sources.duckdb import DuckDBSource

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


def _require_creds() -> dict[str, str]:
    """The five env vars every Databricks smoke test needs (or skip)."""
    return require_env(
        HOST_ENV,
        HTTP_PATH_ENV,
        TOKEN_ENV,
        "DRT_SMOKE_DATABRICKS_CATALOG",
        "DRT_SMOKE_DATABRICKS_SCHEMA",
    )


def _dest_config(
    creds: dict[str, str], table: str, **overrides: Any
) -> DatabricksDestinationConfig:
    """Build a Databricks destination config against the smoke catalog/schema."""
    return DatabricksDestinationConfig(
        **{
            "type": "databricks",
            "host_env": HOST_ENV,
            "http_path_env": HTTP_PATH_ENV,
            "token_env": TOKEN_ENV,
            "catalog": creds["DRT_SMOKE_DATABRICKS_CATALOG"],
            "schema": creds["DRT_SMOKE_DATABRICKS_SCHEMA"],
            "table": table,
            "mode": "insert",
            **overrides,
        }
    )


def _fqn(creds: dict[str, str], table: str) -> str:
    catalog = creds["DRT_SMOKE_DATABRICKS_CATALOG"]
    schema = creds["DRT_SMOKE_DATABRICKS_SCHEMA"]
    return f"`{catalog}`.`{schema}`.`{table}`"


def test_databricks_insert_roundtrip(tmp_path: Path) -> None:
    creds = _require_creds()
    source, profile = seed_duckdb_users(tmp_path)
    table = unique_table("drt_smoke")
    fqn = _fqn(creds, table)

    dest = _dest_config(creds, table, mode="insert")
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

    Pre-seeds a stale row the source never emits, runs ``sync.mode: replace`` with
    ``replace_strategy: swap``, and asserts (a) the stale row is gone — the
    finalize-time ``INSERT OVERWRITE`` atomically replaced the table's data — and
    (b) the ``<table>__drt_swap`` shadow was dropped in ``finalize_sync``.
    """
    creds = _require_creds()
    source, profile = seed_duckdb_users(tmp_path)
    table = unique_table("drt_smoke")
    fqn = _fqn(creds, table)
    catalog = creds["DRT_SMOKE_DATABRICKS_CATALOG"]
    schema = creds["DRT_SMOKE_DATABRICKS_SCHEMA"]
    shadow_fqn = f"`{catalog}`.`{schema}`.`{table}__drt_swap`"

    dest = _dest_config(creds, table, mode="insert")
    sync = SyncConfig(
        name="databricks_swap_smoke",
        model="ref('users')",
        destination=dest,
        sync=SyncOptions(mode="replace", replace_strategy="swap", batch_size=10),
    )

    # Pre-create the target (Delta) and seed a stale row. The swap must exist for
    # the shadow path to engage — a first run with no target falls through to a
    # direct write and never builds a shadow.
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


def test_databricks_complex_type_serialization(tmp_path: Path) -> None:
    """Complex-type / VARIANT-equivalent serialization on a real account (#317 leg).

    Seeds a DuckDB source whose rows carry a Python ``list`` and ``dict`` and syncs
    them into a Databricks table with ARRAY / STRUCT / VARIANT columns. The write
    path introspects ``information_schema`` and wraps the binds
    (``from_json(%s, '<ddl>')`` for STRUCT/ARRAY, ``parse_json(%s)`` for VARIANT).
    Reading typed sub-fields back proves the values reconstructed as real complex
    types rather than opaque JSON strings.
    """
    creds = _require_creds()
    table = unique_table("drt_smoke")
    fqn = _fqn(creds, table)

    # Source: DuckDB LIST -> Python list, STRUCT -> Python dict. The `meta` STRUCT
    # targets a Databricks VARIANT (parse_json); `tags`/`attrs` target ARRAY/STRUCT
    # (from_json). One row is enough to exercise all three wrap sites.
    duckdb = pytest.importorskip("duckdb")
    db_path = str(tmp_path / "complex_source.duckdb")
    dconn = duckdb.connect(db_path)
    try:
        dconn.execute(
            "CREATE TABLE events ("
            "  id INTEGER,"
            "  tags VARCHAR[],"
            "  attrs STRUCT(theme VARCHAR, level INTEGER),"
            "  meta STRUCT(source VARCHAR, verified BOOLEAN)"
            ")"
        )
        dconn.execute(
            "INSERT INTO events VALUES "
            "(1, ['a', 'b'], {'theme': 'dark', 'level': 3}, "
            "{'source': 'crm', 'verified': true})"
        )
    finally:
        dconn.close()
    source = DuckDBSource()
    profile = DuckDBProfile(type="duckdb", database=db_path)

    dest = _dest_config(creds, table, mode="insert")
    sync = SyncConfig(
        name="databricks_complex_smoke",
        model="ref('events')",
        destination=dest,
        sync=SyncOptions(batch_size=10),
    )

    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {fqn} ("
                "  id INT,"
                "  tags ARRAY<STRING>,"
                "  attrs STRUCT<theme: STRING, level: INT>,"
                "  meta VARIANT"
                ") USING DELTA"
            )
    finally:
        conn.close()

    try:
        result = run_sync(sync, source, DatabricksDestination(), profile, tmp_path)
        assert result.success == 1, f"expected 1 loaded row, got {result.success}"
        assert result.failed == 0

        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                # Access typed sub-fields: array element, struct field, and a
                # VARIANT path (`meta:source`). If serialization had stored a raw
                # JSON string, these typed accessors would fail or return null.
                cur.execute(
                    "SELECT element_at(tags, 1), attrs.theme, attrs.level, "
                    f"CAST(meta:source AS STRING) FROM {fqn} WHERE id = 1"
                )
                row = cur.fetchone()
        finally:
            conn.close()
        assert row is not None, "row did not land"
        first_tag, theme, level, meta_source = row
        assert first_tag == "a"
        assert theme == "dark"
        assert int(level) == 3
        assert meta_source == "crm"
    finally:
        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {fqn}")
        finally:
            conn.close()


def test_databricks_connection() -> None:
    """`test_connection` succeeds against the real account (fast credential check)."""
    creds = _require_creds()
    dest = _dest_config(creds, "drt_smoke_connection_check")
    DatabricksDestination().test_connection(dest)
