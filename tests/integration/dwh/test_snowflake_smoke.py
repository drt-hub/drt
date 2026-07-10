"""Snowflake DWH smoke test — reference shape for the harness (#674 / #671).

Drives the full pipeline against a *real* Snowflake account:
seeded DuckDB ``users`` -> engine -> live Snowflake table -> read back -> verify.

This is the canonical per-warehouse leg. Databricks (#672) and BigQuery (#673)
mirror this file; only the destination config + read-back/cleanup driver change.

Covers the #671 verification set (Priority 1 under epic #654):

- ``test_snowflake_insert_roundtrip`` — the ``mode: insert`` append leg.
- ``test_snowflake_replace_swap_roundtrip`` — ``ALTER TABLE ... SWAP WITH``
  atomicity for ``replace_strategy: swap`` (#434): a pre-seeded stale row is
  replaced by the atomic finalize-time swap and the ``<table>__drt_swap`` shadow
  is cleaned up.
- ``test_snowflake_complex_type_serialization`` — VARIANT / OBJECT / ARRAY
  serialization via ``PARSE_JSON`` (#317 Layer 3 / #653): a Python ``list`` +
  ``dict`` are reconstructed as real semi-structured values, proven by reading
  typed sub-fields (``tags[0]``, ``attrs:theme``, ``meta:source``) back.
- ``test_snowflake_connection`` — fast credential check via ``test_connection``.
- ``test_snowflake_mirror_deletes_unobserved_keys`` — ``sync.mode: mirror``
  end-of-sync DELETE (#340 Snowflake leg): a pre-seeded row the source never
  emits is removed because its key wasn't observed.

Runs only when the ``DRT_SMOKE_SNOWFLAKE_*`` secrets are present (injected by the
dwh-smoke workflow). Otherwise it skips — safe no-op for forks / local runs.
See tests/integration/dwh/README.md for the secret list.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import pytest

from drt.config.credentials import DuckDBProfile
from drt.config.models import SnowflakeDestinationConfig, SyncConfig, SyncOptions
from drt.destinations.snowflake import SnowflakeDestination
from drt.engine.sync import run_sync
from drt.sources.duckdb import DuckDBSource

from .conftest import require_env, seed_duckdb_users, unique_table

pytestmark = pytest.mark.dwh_smoke

# Driver gate: skip the whole module if drt-core[snowflake] isn't installed.
snowflake_connector = pytest.importorskip("snowflake.connector")

# Env var NAMES the destination resolves credentials from. The smoke secrets
# are passed straight through under these names.
ACCOUNT_ENV = "DRT_SMOKE_SNOWFLAKE_ACCOUNT"
USER_ENV = "DRT_SMOKE_SNOWFLAKE_USER"
PASSWORD_ENV = "DRT_SMOKE_SNOWFLAKE_PASSWORD"
# Key-pair auth (#737) — preferred; new Snowflake accounts enforce MFA on
# password users, so the smoke user is a TYPE = SERVICE user with an RSA key.
KEY_ENV = "DRT_SMOKE_SNOWFLAKE_PRIVATE_KEY"


def _require_creds() -> dict[str, str]:
    """Gate on the non-auth vars + at least one auth secret (key preferred)."""
    if not os.environ.get(KEY_ENV) and not os.environ.get(PASSWORD_ENV):
        pytest.skip(
            "Snowflake smoke auth not set: need DRT_SMOKE_SNOWFLAKE_PRIVATE_KEY "
            "(preferred) or DRT_SMOKE_SNOWFLAKE_PASSWORD."
        )
    return require_env(
        ACCOUNT_ENV,
        USER_ENV,
        "DRT_SMOKE_SNOWFLAKE_DATABASE",
        "DRT_SMOKE_SNOWFLAKE_SCHEMA",
        "DRT_SMOKE_SNOWFLAKE_WAREHOUSE",
    )


def _auth_config_kwargs() -> dict[str, str]:
    """Destination-config auth kwargs for whichever secret is present."""
    if os.environ.get(KEY_ENV):
        return {"private_key_env": KEY_ENV}
    return {"password_env": PASSWORD_ENV}


def _connect(creds: dict[str, str]):
    """Open a fresh Snowflake connection from the smoke creds (key preferred)."""
    auth: dict[str, object] = {}
    pem = os.environ.get(KEY_ENV)
    if pem:
        from drt.config.credentials import load_snowflake_private_key

        auth["private_key"] = load_snowflake_private_key(pem)
    else:
        auth["password"] = os.environ[PASSWORD_ENV]
    return snowflake_connector.connect(
        account=creds[ACCOUNT_ENV],
        user=creds[USER_ENV],
        warehouse=creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
        database=creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
        schema=creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
        **auth,
    )


def _readback_count_and_names(creds: dict[str, str], table: str) -> tuple[int, set[str]]:
    """Open a fresh Snowflake connection and read the rows the sync wrote."""
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            # Unquoted to match the destination's unquoted INSERT, which
            # Snowflake folds to UPPERCASE (quoted lowercase wouldn't match).
            cur.execute(f"SELECT name FROM {table}")
            rows = cur.fetchall()
        # Count the fetched rows (not distinct names) so a duplicate-row
        # regression can't be masked by set dedup; names stays for value checks.
        names = {row[0] for row in rows}
        return len(rows), names
    finally:
        conn.close()


def _create_table(creds: dict[str, str], table: str) -> None:
    """Pre-create the target table — drt's insert mode INSERTs into an existing
    table, it doesn't create one. Unquoted identifiers so Snowflake folds them
    to UPPERCASE, matching the destination's unquoted INSERT column list."""
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {table} (id INTEGER, name VARCHAR, email VARCHAR)")
    finally:
        conn.close()


def _drop_table(creds: dict[str, str], table: str) -> None:
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(f"DROP TABLE IF EXISTS {table}")
    finally:
        conn.close()


def test_snowflake_insert_roundtrip(tmp_path: Path) -> None:
    """3 seeded rows sync into a real Snowflake table and read back intact."""
    creds = _require_creds()

    source, profile = seed_duckdb_users(tmp_path)
    table = unique_table("DRT_SMOKE")

    dest = SnowflakeDestinationConfig(
        **{
            "type": "snowflake",
            "account_env": ACCOUNT_ENV,
            "user_env": USER_ENV,
            **_auth_config_kwargs(),
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


def test_snowflake_replace_swap_roundtrip(tmp_path: Path) -> None:
    """``replace_strategy: swap`` — atomic ``ALTER TABLE ... SWAP WITH`` (#434).

    Drives one non-``insert`` write path end-to-end (the Snowflake analogue of the
    BigQuery MERGE leg #700 / the Databricks INSERT OVERWRITE leg #705). Pre-seeds
    a stale row the source never emits, runs ``sync.mode: replace`` with
    ``replace_strategy: swap``, then asserts (a) the stale row is gone — the
    finalize-time ``SWAP WITH`` atomically exchanged the freshly written shadow
    for the target — and (b) the ``<table>__drt_swap`` shadow was dropped in
    ``finalize_sync`` (orphan cleanup, #434).
    """
    creds = _require_creds()
    source, profile = seed_duckdb_users(tmp_path)
    table = unique_table("DRT_SMOKE")
    shadow = f"{table}__drt_swap"

    dest = SnowflakeDestinationConfig(
        **{
            "type": "snowflake",
            "account_env": ACCOUNT_ENV,
            "user_env": USER_ENV,
            **_auth_config_kwargs(),
            "database": creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
            "schema": creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
            "table": table,
            "warehouse": creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
            "mode": "insert",
        }
    )
    sync = SyncConfig(
        name="snowflake_swap_smoke",
        model="ref('users')",
        destination=dest,
        sync=SyncOptions(mode="replace", replace_strategy="swap", batch_size=10),
    )

    # Pre-create the target and seed a stale row. The target must exist for the
    # shadow path to engage — a first run with no target falls through to a
    # direct write and never builds a shadow.
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {table} (id INTEGER, name VARCHAR, email VARCHAR)")
            cur.execute(f"INSERT INTO {table} VALUES (99, 'Stale', 'stale@example.com')")
    finally:
        conn.close()

    try:
        result = run_sync(sync, source, SnowflakeDestination(), profile, tmp_path)
        assert result.success == 3, f"expected 3 loaded rows, got {result.success}"
        assert result.failed == 0

        count, names = _readback_count_and_names(creds, table)
        # Stale row replaced atomically; only the 3 source rows remain.
        assert count == 3
        assert names == {"Alice", "Bob", "Carol"}

        # Shadow must be gone — finalize_sync SWAPs then drops it (#434).
        # Exact-match lookup via INFORMATION_SCHEMA (not SHOW TABLES LIKE, whose
        # '_' is a single-char wildcard that could over-match in a shared schema).
        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT COUNT(*) FROM "
                    f"{creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}.INFORMATION_SCHEMA.TABLES "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                    (creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"], shadow.upper()),
                )
                shadow_count = cur.fetchone()[0]
        finally:
            conn.close()
        assert shadow_count == 0, "swap shadow was not cleaned up in finalize_sync"
    finally:
        _drop_table(creds, table)
        _drop_table(creds, shadow)


def test_snowflake_complex_type_serialization(tmp_path: Path) -> None:
    """VARIANT / OBJECT / ARRAY serialization on a real account (#317 Layer 3 / #653).

    Seeds a DuckDB source whose row carries a Python ``list`` and ``dict`` and
    syncs it into a Snowflake table with ARRAY / OBJECT / VARIANT columns. The
    write path introspects ``INFORMATION_SCHEMA`` (``introspect_schema`` on by
    default), maps those columns to the ``json`` category, and wraps their binds
    with ``PARSE_JSON`` — switching the INSERT to the ``SELECT`` form because
    Snowflake disallows functions in a ``VALUES`` clause. Reading typed sub-fields
    back proves the values reconstructed as real semi-structured types rather than
    opaque JSON strings.
    """
    creds = _require_creds()
    table = unique_table("DRT_SMOKE")

    # Source: DuckDB LIST -> Python list, STRUCT -> Python dict. `tags` targets
    # ARRAY, `attrs` targets OBJECT, `meta` targets VARIANT — one row exercises
    # all three PARSE_JSON wrap sites.
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

    dest = SnowflakeDestinationConfig(
        **{
            "type": "snowflake",
            "account_env": ACCOUNT_ENV,
            "user_env": USER_ENV,
            **_auth_config_kwargs(),
            "database": creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
            "schema": creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
            "table": table,
            "warehouse": creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
            "mode": "insert",
        }
    )
    sync = SyncConfig(
        name="snowflake_complex_smoke",
        model="ref('events')",
        destination=dest,
        sync=SyncOptions(batch_size=10),
    )

    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"CREATE TABLE {table} (id INTEGER, tags ARRAY, attrs OBJECT, meta VARIANT)"
            )
    finally:
        conn.close()

    try:
        result = run_sync(sync, source, SnowflakeDestination(), profile, tmp_path)
        assert result.success == 1, f"expected 1 loaded row, got {result.success}"
        assert result.failed == 0

        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                # Access typed sub-fields: an ARRAY element, OBJECT paths, and a
                # VARIANT path. If serialization had stored raw JSON strings,
                # these semi-structured accessors would fail or return null.
                cur.execute(
                    f"SELECT tags[0]::STRING, attrs:theme::STRING, "
                    f"attrs:level::INT, meta:source::STRING FROM {table} WHERE id = 1"
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
        _drop_table(creds, table)


def test_snowflake_connection() -> None:
    """`test_connection` succeeds against the real account (fast credential check)."""
    creds = _require_creds()
    dest = SnowflakeDestinationConfig(
        **{
            "type": "snowflake",
            "account_env": ACCOUNT_ENV,
            "user_env": USER_ENV,
            **_auth_config_kwargs(),
            "database": creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
            "schema": creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
            "table": "DRT_SMOKE_CONNECTION_CHECK",
            "warehouse": creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
        }
    )
    SnowflakeDestination().test_connection(dest)


def test_snowflake_mirror_deletes_unobserved_keys(tmp_path: Path) -> None:
    """``sync.mode: mirror`` end-of-sync DELETE on a real account (#340 Snowflake leg).

    Pre-seeds a stale row (``id=99``) the source never emits, runs a mirror sync,
    and asserts the source rows land while the stale row is deleted — its key was
    not in the observed set, so ``finalize_sync``'s ``DELETE … WHERE id NOT IN
    (observed)`` removes it. This is the mirror leg the mock suite covers but no
    live smoke did.
    """
    creds = _require_creds()
    source, profile = seed_duckdb_users(tmp_path)  # ids 1..3 (Alice/Bob/Carol)
    table = unique_table("DRT_SMOKE_MIRROR")

    dest_kwargs: dict[str, Any] = {
        "type": "snowflake",
        "account_env": ACCOUNT_ENV,
        "user_env": USER_ENV,
        "password_env": PASSWORD_ENV,
        "database": creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
        "schema": creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
        "table": table,
        "warehouse": creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
        "mode": "merge",
        "upsert_key": ["id"],
    }
    dest = SnowflakeDestinationConfig(**dest_kwargs)
    sync = SyncConfig(
        name="snowflake_mirror_smoke",
        model="ref('users')",
        destination=dest,
        sync=SyncOptions(mode="mirror", batch_size=10),
    )

    try:
        _create_table(creds, table)
        # Stale row the source never emits — mirror must delete it (id 99 ∉ {1,2,3}).
        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"INSERT INTO {table} (id, name, email) "
                    "VALUES (99, 'Stale', 'stale@example.com')"
                )
        finally:
            conn.close()

        result = run_sync(sync, source, SnowflakeDestination(), profile, tmp_path)
        assert result.failed == 0, f"mirror sync had failures: {result.errors[:3]}"

        count, names = _readback_count_and_names(creds, table)
        # Source rows upserted; the unobserved stale row removed by the mirror DELETE.
        assert names == {"Alice", "Bob", "Carol"}
        assert count == 3, f"stale row not deleted — expected 3 rows, got {count}"
    finally:
        _drop_table(creds, table)
