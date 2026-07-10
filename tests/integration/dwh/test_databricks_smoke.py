"""Databricks DWH smoke test (#674 / #672) — mirrors test_snowflake_smoke.py.

seeded DuckDB ``users`` -> engine -> live Databricks Delta table -> read back.
Runs only when ``DRT_SMOKE_DATABRICKS_*`` secrets are present; skips otherwise.

Covers the #672 verification set (BigQuery #673 / PR #700 is the reference shape):

- ``test_databricks_insert_roundtrip`` — the streaming ``mode: insert`` leg.
- ``test_databricks_replace_swap_roundtrip`` — ``INSERT OVERWRITE`` atomicity for
  ``replace_strategy: swap`` (#644): a pre-seeded stale row is replaced by the
  atomic finalize-time overwrite and the ``__drt_swap`` shadow is cleaned up.
- ``test_databricks_complex_type_serialization`` — complex-type / VARIANT
  serialization (#317 Databricks leg): ARRAY / STRUCT reconstructed via
  ``from_json`` and a VARIANT column via ``parse_json``, proven by reading
  typed sub-fields back.
- ``test_databricks_connection`` — fast credential check via ``test_connection``.
- ``test_databricks_mirror_deletes_unobserved_keys`` — ``sync.mode: mirror``
  end-of-sync DELETE via the #707 staging anti-join removes the unobserved rows
  on a live warehouse (its parameter-limit immunity is covered by the unit suite).
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


def test_databricks_complex_type_serialization(tmp_path: Path) -> None:
    """Complex-type / VARIANT serialization on a real account (#317 Databricks leg).

    Seeds a DuckDB source whose row carries a Python ``list`` and ``dict`` and
    syncs it into a Databricks table with ARRAY / STRUCT / VARIANT columns. The
    write path introspects ``information_schema`` and wraps the binds
    (``from_json(%s, '<ddl>')`` for STRUCT/ARRAY, ``parse_json(%s)`` for VARIANT).
    Reading typed sub-fields back proves the values reconstructed as real complex
    types rather than opaque JSON strings.
    """
    creds = require_env(
        HOST_ENV,
        HTTP_PATH_ENV,
        TOKEN_ENV,
        "DRT_SMOKE_DATABRICKS_CATALOG",
        "DRT_SMOKE_DATABRICKS_SCHEMA",
    )
    table = unique_table("drt_smoke")
    catalog = creds["DRT_SMOKE_DATABRICKS_CATALOG"]
    schema = creds["DRT_SMOKE_DATABRICKS_SCHEMA"]
    fqn = f"`{catalog}`.`{schema}`.`{table}`"

    # Source: DuckDB LIST -> Python list, STRUCT -> Python dict. `tags`/`attrs`
    # target ARRAY/STRUCT (from_json); `meta` targets VARIANT (parse_json). One
    # row is enough to exercise all three wrap sites.
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
                # Access typed sub-fields: array element, struct fields, and a
                # VARIANT path (`meta:source`). If serialization had stored raw
                # JSON strings, these typed accessors would fail or return null.
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


def test_databricks_mirror_deletes_unobserved_keys(tmp_path: Path) -> None:
    """``sync.mode: mirror`` end-of-sync DELETE via the #707 staging anti-join.

    Pre-seeds the Delta target with more keys than the source produces, then runs
    a mirror sync: the MERGE upserts the observed rows and ``finalize_sync``
    removes the rows whose key was *not* observed, proving the staging anti-join
    (``DELETE … WHERE key NOT IN (SELECT key FROM staging)``) deletes correctly on
    a live warehouse. The anti-join's *parameter-limit immunity* (it binds no key
    parameters in the DELETE) is asserted in the mock unit suite, so the key count
    here is kept small — row-by-row Delta staging makes a large mirror slow (a
    300-key run measured ~20 min), and the count doesn't change what's proven.
    """
    creds = require_env(
        HOST_ENV,
        HTTP_PATH_ENV,
        TOKEN_ENV,
        "DRT_SMOKE_DATABRICKS_CATALOG",
        "DRT_SMOKE_DATABRICKS_SCHEMA",
    )
    catalog = creds["DRT_SMOKE_DATABRICKS_CATALOG"]
    schema = creds["DRT_SMOKE_DATABRICKS_SCHEMA"]
    table = unique_table("drt_smoke_mirror")
    fqn = f"`{catalog}`.`{schema}`.`{table}`"
    keys_fqn = f"`{catalog}`.`{schema}`.`__drt_mirror_keys_{table}`"

    # Kept small on purpose: row-by-row Delta staging is slow (a 300-key run
    # took ~20 min), and the anti-join deletes the same way for 20 keys or 20k.
    # The "binds no key parameters / scales past the limit" property is covered
    # by the mock unit suite; here we just prove the live DELETE removes the
    # unobserved rows.
    observed, stale = 20, 5

    duckdb = pytest.importorskip("duckdb")
    db_path = str(tmp_path / "mirror_source.duckdb")
    dconn = duckdb.connect(db_path)
    try:
        dconn.execute("CREATE TABLE items (id INTEGER, val VARCHAR)")
        dconn.executemany(
            "INSERT INTO items VALUES (?, ?)",
            [(i, f"v{i}") for i in range(1, observed + 1)],
        )
    finally:
        dconn.close()
    source, profile = DuckDBSource(), DuckDBProfile(type="duckdb", database=db_path)

    dest_kwargs: dict[str, Any] = {
        "type": "databricks",
        "host_env": HOST_ENV,
        "http_path_env": HTTP_PATH_ENV,
        "token_env": TOKEN_ENV,
        "catalog": catalog,
        "schema": schema,
        "table": table,
        "mode": "merge",
        "upsert_key": ["id"],
    }
    dest = DatabricksDestinationConfig(**dest_kwargs)
    sync = SyncConfig(
        name="databricks_mirror_smoke",
        model="ref('items')",
        destination=dest,
        sync=SyncOptions(mode="mirror", batch_size=100),
    )

    # Pre-seed the target with observed + stale keys; the stale ones
    # (ids observed+1 .. observed+stale) are absent from the source, so the
    # mirror finalize must delete exactly those.
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {fqn} (id INT, val STRING) USING DELTA")
            values = ", ".join(f"({i}, 'stale{i}')" for i in range(1, observed + stale + 1))
            cur.execute(f"INSERT INTO {fqn} (id, val) VALUES {values}")
    finally:
        conn.close()

    try:
        result = run_sync(sync, source, DatabricksDestination(), profile, tmp_path)
        assert result.failed == 0, f"mirror sync had failures: {result.errors[:3]}"

        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT count(*), min(id), max(id) FROM {fqn}")
                count, lo, hi = cur.fetchone()
        finally:
            conn.close()
        # Only the observed keys (1..observed) survive; the stale rows were
        # removed by the anti-join DELETE.
        assert count == observed, f"expected {observed} rows after mirror, got {count}"
        assert (lo, hi) == (1, observed)
    finally:
        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(f"DROP TABLE IF EXISTS {fqn}")
                cur.execute(f"DROP TABLE IF EXISTS {keys_fqn}")
        finally:
            conn.close()
