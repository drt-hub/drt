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

from drt.config.credentials import DuckDBProfile, SnowflakeProfile
from drt.config.models import SnowflakeDestinationConfig, SyncConfig, SyncOptions
from drt.destinations.snowflake import SnowflakeDestination
from drt.engine.sync import run_sync
from drt.sources.duckdb import DuckDBSource
from drt.sources.snowflake import SnowflakeSource

from .conftest import (
    require_env,
    seed_duckdb_children,
    seed_duckdb_users,
    unique_table,
)

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
        **_auth_config_kwargs(),
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


# ---------------------------------------------------------------------------
# Source-side streaming extraction (#765)
# ---------------------------------------------------------------------------
#
# Everything above drives Snowflake as a *destination*. These exercise it as a
# *source*, which nothing else in the harness does — and which #765 changed
# from fetchall() to iterating the cursor in fetch_size batches.
#
# This leg exists because the other streaming slices were each verified against
# a live server (Postgres 16, MySQL 8, ClickHouse 24) and every one of them
# turned up a behaviour no mock predicted: description=None on a psycopg2 named
# cursor, pymysql needing cursor.close() before conn.close(), clickhouse-connect
# buffering HTTP internally. Snowflake has no local server, so this is the only
# place the equivalent can be caught.


def _snowflake_source_profile(creds: dict[str, str], **overrides: object) -> SnowflakeProfile:
    auth: dict[str, object] = {}
    if os.environ.get(KEY_ENV):
        auth["private_key_env"] = KEY_ENV
    else:
        auth["password_env"] = PASSWORD_ENV
    return SnowflakeProfile(
        type="snowflake",
        account=creds[ACCOUNT_ENV],
        user=creds[USER_ENV],
        database=creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
        schema=creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
        warehouse=creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
        **auth,  # type: ignore[arg-type]
    )


def test_snowflake_source_streams_a_generated_result_set() -> None:
    """Extract 50k generated rows and confirm every one arrives, in order.

    ``GENERATOR`` avoids needing a seeded table, so this asserts the streaming
    path itself rather than any fixture. 50k is enough to span many
    ``fetch_size`` batches (default 10000) — a boundary bug would show up as a
    short read or a duplicated row, both of which the checks below catch.
    """
    creds = _require_creds()
    profile = _snowflake_source_profile(creds)

    rows = list(
        SnowflakeSource().extract(
            "SELECT SEQ4() AS ID, 'x' AS PAYLOAD "
            "FROM TABLE(GENERATOR(ROWCOUNT => 50000)) ORDER BY ID",
            profile,
        )
    )

    assert len(rows) == 50000, "short read — a fetch_size batch boundary was dropped"
    assert rows[0] == {"ID": 0, "PAYLOAD": "x"}
    assert rows[-1] == {"ID": 49999, "PAYLOAD": "x"}
    assert len({r["ID"] for r in rows}) == 50000, "duplicate rows across batches"


def test_snowflake_source_respects_a_small_fetch_size() -> None:
    """A tiny fetch_size must change nothing about the rows returned.

    The batch size is a memory/round-trip knob, never a correctness one. Pinned
    at 100 against 1000 rows, so the result set spans ten batches instead of
    one.
    """
    creds = _require_creds()
    profile = _snowflake_source_profile(creds)
    profile.fetch_size = 100

    rows = list(
        SnowflakeSource().extract(
            "SELECT SEQ4() AS ID FROM TABLE(GENERATOR(ROWCOUNT => 1000)) ORDER BY ID",
            profile,
        )
    )

    assert [r["ID"] for r in rows] == list(range(1000))


def test_snowflake_source_empty_result_yields_nothing() -> None:
    """An empty result must not trip on column metadata."""
    creds = _require_creds()
    profile = _snowflake_source_profile(creds)

    assert list(SnowflakeSource().extract("SELECT 1 AS ID WHERE 1 = 0", profile)) == []


def test_snowflake_last_change_commit_time_no_change_tracking_required(tmp_path: Path) -> None:
    """#975 research probe: does SYSTEM$LAST_CHANGE_COMMIT_TIME need
    CHANGE_TRACKING enabled, and does its value actually increase across a
    DML op, on a real account?

    The table below deliberately leaves CHANGE_TRACKING at Snowflake's
    default (off) -- #975 speculated this might be a prerequisite for a
    dagster-drt Tier-2 sensor signal candidate; this is the direct check.
    Not a regression test of shipped drt behaviour -- nothing in drt calls
    this function yet.
    """
    creds = _require_creds()
    table = unique_table("DRT_SMOKE_LCC")
    fq = f"{creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}.{creds['DRT_SMOKE_SNOWFLAKE_SCHEMA']}.{table}"

    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {table} (id INTEGER)")
            cur.execute(f"SELECT SYSTEM$LAST_CHANGE_COMMIT_TIME('{fq}')")
            before = cur.fetchone()[0]
            cur.execute(f"INSERT INTO {table} VALUES (1)")
            cur.execute(f"SELECT SYSTEM$LAST_CHANGE_COMMIT_TIME('{fq}')")
            after = cur.fetchone()[0]
    finally:
        _drop_table(creds, table)
        conn.close()

    assert before is not None, (
        "#975: SYSTEM$LAST_CHANGE_COMMIT_TIME returned NULL without CHANGE_TRACKING "
        "enabled -- it IS a prerequisite after all, contrary to the docs page's silence"
    )
    assert after is not None and after > before, (
        f"#975: value did not increase after an INSERT (before={before}, after={after}) "
        "-- not usable as a cursor-diff sensor signal"
    )


def test_snowflake_last_change_commit_time_does_not_require_active_warehouse(
    tmp_path: Path,
) -> None:
    """#975 CONFIRMED FINDING, verified live 2026-08-18 against the drt smoke
    account: ``SYSTEM$LAST_CHANGE_COMMIT_TIME`` is metadata-only and does NOT
    require an active virtual warehouse. This is a hard assertion, not an
    informational skip, because the premise it protects -- a Tier-2 sensor
    being "cheap to poll" -- depends on this staying true. If Snowflake ever
    changes this function's behaviour, this test must fail loudly, not skip
    silently (a skip-based version of this test previously reported the
    finding via ``pytest.skip`` and needed a manual ``-rs`` flag to even be
    seen -- see #985).

    Scope note: this is a claim about this one function on this one account,
    not a general "SYSTEM$ functions are metadata-only" rule (``SHOW
    STREAMS`` was checked separately, in the trigger matrix research, and
    happens to also not need one -- that is supporting context, not proof of
    a class-wide guarantee).

    A prior version of this test tried isolating a no-warehouse *session*
    (no ``warehouse=`` at connect time) and skipped once it found the smoke
    user has an account-level default warehouse, making that approach
    inconclusive. This version instead deliberately SUSPENDs the smoke
    warehouse itself, calls the function, and asserts the warehouse's own
    state did not change.

    OPERATE is verified up front via a net-zero round trip (toggle the
    warehouse's state and immediately toggle it back) regardless of which
    state it starts in -- not just when it starts RUNNING, which would miss
    the normal nightly case (a warehouse that's SUSPENDED between smoke
    runs) and could otherwise leave an auto-resumed warehouse stuck running,
    with no way to undo it, for a role that never had permission to fix it.
    Skips (does not fail) if that round trip fails -- OPERATE is not always
    granted (it was granted specifically to run this verification once; see
    #985), and its absence says nothing about the finding itself. Once past
    it, restoring the warehouse to its original state after the real probe
    is expected to succeed and is *not* swallowed on failure -- a failure
    there despite the upfront check passing is a genuine surprise worth a
    loud test failure over a shared resource silently left running and
    billing.
    """
    creds = _require_creds()
    wh = creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"]
    table = unique_table("DRT_SMOKE_LCC_WH")
    fq = f"{creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}.{creds['DRT_SMOKE_SNOWFLAKE_SCHEMA']}.{table}"

    def _is_suspended(cur: Any) -> bool:
        cur.execute(f"SHOW WAREHOUSES LIKE '{wh}'")  # noqa: S608 -- test-only, fixed LIKE pattern from env config
        columns = [d[0] for d in cur.description]
        row = dict(zip(columns, cur.fetchone(), strict=True))
        return str(row["state"]) == "SUSPENDED"

    def _ensure_suspended(cur: Any, should_be_suspended: bool) -> None:
        """Idempotent: query the *actual* current state and only issue an
        ALTER if it doesn't already match. Never trust in-memory bookkeeping
        about which ALTER calls the client thinks succeeded -- Snowflake can
        apply a statement server-side even if the client-side acknowledgment
        is lost to a timeout, so "my execute() raised" does not reliably
        mean "nothing changed" (Codex review round 4, #985). Re-checking the
        real state before every state-changing decision makes this correct
        regardless of that ambiguity. Boolean rather than a state-string
        comparison on purpose: "SUSPENDED" is the only literal this file
        verifies against a real account -- the non-suspended state's exact
        string (STARTED? RUNNING?) was never confirmed, so branching on
        equality against a guessed value would be exactly the kind of
        unverified assumption this investigation exists to avoid.
        """
        if _is_suspended(cur) == should_be_suspended:
            return
        cur.execute(f"ALTER WAREHOUSE {wh} {'SUSPEND' if should_be_suspended else 'RESUME'}")

    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {table} (id INTEGER)")

            was_suspended = _is_suspended(cur)

            # Verify OPERATE *before* doing anything state-changing, and do
            # it regardless of the starting state -- checking only when the
            # warehouse starts RUNNING (an earlier version of this test)
            # missed the normal nightly case: a warehouse that starts
            # SUSPENDED never gets this check at all, so if the probe below
            # triggers an auto-resume, a role without OPERATE would have no
            # way to undo it (Codex review round 2, #985). A net-zero round
            # trip (toggle and toggle back) proves the capability both ways
            # without any lasting side effect, before the real probe runs.
            # _ensure_suspended re-checks real state before and after every
            # call, so a lost acknowledgment can't leave this ambiguous.
            try:
                _ensure_suspended(cur, not was_suspended)
                _ensure_suspended(cur, was_suspended)
            except Exception as exc:
                restore_note = ""
                try:
                    _ensure_suspended(cur, was_suspended)
                except Exception as restore_exc:
                    restore_note = (
                        f" (restore attempt ALSO failed: {restore_exc} -- "
                        f"MANUAL CHECK NEEDED for warehouse {wh})"
                    )
                pytest.skip(
                    f"#975: smoke role lacks OPERATE on warehouse {wh} ({exc})"
                    f"{restore_note} -- verified via a net-zero round trip before "
                    "running the probe; inconclusive from this account."
                )

            _ensure_suspended(cur, True)

            try:
                # Deliberately not caught here: an exception at this specific
                # call *would* be a real finding ("requires an active
                # warehouse and AUTO_RESUME couldn't/didn't help"), but
                # without a verified Snowflake error signature to match on,
                # blanket-catching this risks misclassifying an unrelated
                # failure (a network blip, an auth hiccup, a genuinely
                # invalid object) as a confirmed research conclusion (Codex
                # review round 3, #985) -- better to fail loudly with the
                # real error than manufacture an unverified finding.
                cur.execute(f"SELECT SYSTEM$LAST_CHANGE_COMMIT_TIME('{fq}')")
                val = cur.fetchone()[0]
                assert val is not None, (
                    "#975: call while warehouse was suspended returned NULL"
                )

                assert _is_suspended(cur), (
                    "#975 REGRESSION: warehouse is no longer SUSPENDED after "
                    "SYSTEM$LAST_CHANGE_COMMIT_TIME (it was SUSPENDED before the "
                    "call) -- Snowflake auto-resumed it, contradicting the "
                    "2026-08-18 finding that this call is metadata-only. The "
                    "Tier-2 sensor's cost model needs re-verifying if this fails."
                )
            finally:
                # Not swallowed on failure -- the round trip above already
                # confirmed OPERATE works both ways, so a failure here now
                # is a genuine surprise worth a loud test failure demanding
                # manual intervention, not a silent pass leaving shared
                # compute running and billing (Codex review round 2, #985).
                _ensure_suspended(cur, was_suspended)
    finally:
        conn.close()
        _drop_table(creds, table)


def test_snowflake_source_abandoned_mid_stream_does_not_hang() -> None:
    """`--limit` / `--fail-fast` stop consuming mid-stream (#775/#774).

    With the result set live server-side, the generator's ``finally`` is what
    closes the cursor and connection. If it did not run — or if closing a
    partially-read Snowflake cursor blocked — this would hang rather than fail,
    which is why it is worth a real-warehouse check rather than only a mock.
    """
    creds = _require_creds()
    profile = _snowflake_source_profile(creds)

    gen = SnowflakeSource().extract(
        "SELECT SEQ4() AS ID FROM TABLE(GENERATOR(ROWCOUNT => 50000)) ORDER BY ID",
        profile,
    )
    first = [next(gen) for _ in range(3)]
    gen.close()

    assert [r["ID"] for r in first] == [0, 1, 2]


# ---------------------------------------------------------------------------
# Tracked + scoped mirror (#686 / #687 / #692 / #694) — and the #890 backfill
# ---------------------------------------------------------------------------
#
# Nothing in this harness covered `strategy: tracked` or `mirror.scope` before,
# on any warehouse — the mirror smoke above is plain `mode: mirror`. So the
# composition has shipped since v0.7.10 (and across five dialects in v0.8.4)
# with mock coverage only. This closes that, and is also the only place #890's
# scope-column migration can be exercised against a real Snowflake: the ALTER,
# the backfill of rows tracked before the columns existed, and the predicate.


def test_snowflake_tracked_scoped_mirror_and_scope_backfill(tmp_path: Path) -> None:
    """A pre-#890 state table is migrated, backfilled, and filtered correctly.

    Sets up the exact upgrade path an existing user is on: a ``_drt_synced_keys``
    table with the three original columns, already tracking keys under two
    parents. A scoped run touching only parent 1 must then

    * add the scope columns (``ALTER``),
    * delete the stale child of parent 1 and nothing else,
    * leave parent 2's rows in the target untouched,
    * and heal the pre-existing state rows so later runs filter in SQL.
    """
    from drt.destinations._mirror_state import STATE_TABLE, key_hash, key_json

    creds = _require_creds()
    source, profile = seed_duckdb_children(tmp_path)  # parent 1: a, b
    table = unique_table("DRT_SMOKE_TRACKED_SCOPED")
    db, schema = creds["DRT_SMOKE_SNOWFLAKE_DATABASE"], creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"]
    state_fq = f"{db}.{schema}.{STATE_TABLE}"
    sync_name = f"tracked_scoped_{table.lower()}"

    dest = SnowflakeDestinationConfig(
        type="snowflake",
        account_env=ACCOUNT_ENV,
        user_env=USER_ENV,
        **_auth_config_kwargs(),
        database=db,
        schema=schema,
        table=table,
        warehouse=creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
        mode="merge",
        upsert_key=["parent_id", "id"],
    )
    sync = SyncConfig(
        name=sync_name,
        model="ref('children')",
        destination=dest,
        sync=SyncOptions(
            mode="mirror",
            batch_size=10,
            mirror={"strategy": "tracked", "scope": ["parent_id"]},
        ),
    )

    tracked = [(1, "a"), (1, "stale"), (2, "other")]
    conn = _connect(creds)
    try:
        with conn.cursor() as cur:
            cur.execute(f"CREATE TABLE {table} (parent_id INTEGER, id VARCHAR, label VARCHAR)")
            for p, i in tracked:
                cur.execute(f"INSERT INTO {table} VALUES ({p}, '{i}', 'seeded')")
            # A state table in its pre-#890 shape — three columns, no scope.
            cur.execute(f"DROP TABLE IF EXISTS {state_fq}")
            cur.execute(
                f"CREATE TABLE {state_fq} (sync_name VARCHAR(255) NOT NULL, "
                "key_hash CHAR(64) NOT NULL, key_json VARCHAR NOT NULL, "
                "PRIMARY KEY (sync_name, key_hash))"
            )
            for k in tracked:
                cur.execute(
                    f"INSERT INTO {state_fq} VALUES (%s, %s, %s)",
                    (sync_name, key_hash(k), key_json(k)),
                )
    finally:
        conn.close()

    try:
        result = run_sync(sync, source, SnowflakeDestination(), profile, tmp_path)
        assert result.failed == 0, f"tracked+scoped sync had failures: {result.errors[:3]}"

        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(f"SELECT parent_id, id FROM {table} ORDER BY parent_id, id")
                rows = {(r[0], r[1]) for r in cur.fetchall()}
                # (1,'stale') was tracked, is under the observed scope, and this
                # run did not emit it -> deleted. (2,'other') is under a parent
                # this run never saw -> must survive.
                assert (1, "stale") not in rows, f"stale in-scope row not deleted: {rows}"
                assert (2, "other") in rows, f"out-of-scope row was deleted: {rows}"
                assert (1, "a") in rows and (1, "b") in rows, rows

                cur.execute(
                    f"SELECT scope_spec, scope_key FROM {state_fq} WHERE sync_name = %s "
                    "AND key_json = %s",
                    (sync_name, key_json((2, "other"))),
                )
                healed = cur.fetchone()
                # #890: the row tracked before the columns existed must have been
                # backfilled, or the SQL filter would never engage on an upgraded
                # table. This is what no mock could catch.
                assert healed == ('["parent_id"]', "[2]"), f"scope backfill did not run: {healed}"
        finally:
            conn.close()
    finally:
        _drop_table(creds, table)
        conn = _connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {state_fq} WHERE sync_name = %s", (sync_name,))
        finally:
            conn.close()
