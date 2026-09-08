"""Snowflake ManagedTableCapable smoke test (#960/#1106).

A mock cursor can prove call shape but not real Snowflake semantics — in
particular, whether ``CREATE SCHEMA IF NOT EXISTS`` on Snowflake actually
races the same ungraceful way it does on Postgres is unverified by any prior
art in this repo, and whether an admin's unquoted escape-hatch ``CREATE
SCHEMA`` (from ``provisioning/snowflake.sql``) is actually found by
``ensure_managed_schema()``'s ``UPPER()``-normalized probe needs a live
round trip, not an assumption.

Runs only when ``DRT_SMOKE_SNOWFLAKE_*`` secrets are present (dwh-smoke
workflow injects them from repo secrets). The escape-hatch test below only
needs privileges the smoke role already has (``provisioning/snowflake.sql``
section 4). The positive-create-path and concurrent-race tests additionally
need the ``GRANT CREATE SCHEMA ON DATABASE`` added for #1106 (section 4b) —
they skip cleanly (not fail) via ``DRT_SMOKE_SNOWFLAKE_HAS_CREATE_SCHEMA_GRANT``
until an operator has actually run that grant against the real account.
"""

from __future__ import annotations

import os
import threading
import uuid

import pytest

from drt.config.credentials import SnowflakeProfile
from drt.sources.snowflake import SnowflakeSource

from .conftest import require_env

pytestmark = pytest.mark.dwh_smoke

snowflake_connector = pytest.importorskip("snowflake.connector")

ACCOUNT_ENV = "DRT_SMOKE_SNOWFLAKE_ACCOUNT"
USER_ENV = "DRT_SMOKE_SNOWFLAKE_USER"
PASSWORD_ENV = "DRT_SMOKE_SNOWFLAKE_PASSWORD"
KEY_ENV = "DRT_SMOKE_SNOWFLAKE_PRIVATE_KEY"
# Manual opt-in: set once an operator has run the #1106 CREATE SCHEMA grant
# (provisioning/snowflake.sql section 4b) against the real smoke account —
# there's no privilege-safe way to probe for the grant itself ahead of time.
_HAS_CREATE_SCHEMA_GRANT_ENV = "DRT_SMOKE_SNOWFLAKE_HAS_CREATE_SCHEMA_GRANT"


def _require_creds() -> dict[str, str]:
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


def _require_create_schema_grant() -> None:
    if not os.environ.get(_HAS_CREATE_SCHEMA_GRANT_ENV):
        pytest.skip(
            f"{_HAS_CREATE_SCHEMA_GRANT_ENV} not set — run provisioning/snowflake.sql "
            "section 4b (GRANT CREATE SCHEMA ON DATABASE) against the real smoke "
            "account first, then set this env var to enable this test."
        )


def _profile(creds: dict[str, str], **overrides: object) -> SnowflakeProfile:
    """``overrides`` (e.g. ``managed_schema=...``) must win over the
    defaults below — merged via a plain dict update rather than passed
    alongside them as separate kwargs, which silently dropped every caller's
    override until caught in review (the nightly workflow never actually
    ran this suite until #1108 wired it in, so nothing had exercised this
    path before)."""
    defaults: dict[str, object] = {
        "type": "snowflake",
        "account": creds[ACCOUNT_ENV],
        "user": creds[USER_ENV],
        "database": creds["DRT_SMOKE_SNOWFLAKE_DATABASE"],
        "schema": creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"],
        "warehouse": creds["DRT_SMOKE_SNOWFLAKE_WAREHOUSE"],
    }
    if os.environ.get(KEY_ENV):
        defaults["private_key_env"] = KEY_ENV
    else:
        defaults["password_env"] = PASSWORD_ENV
    defaults.update(overrides)
    return SnowflakeProfile(**defaults)  # type: ignore[arg-type]


def _admin_connect(creds: dict[str, str]):
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


def test_ensure_managed_schema_escape_hatch_finds_a_preexisting_schema() -> None:
    """The escape hatch (#695 discipline, no new grant needed): point
    managed_schema at the role's already-granted DRT_SMOKE.PUBLIC schema and
    confirm ensure_managed_schema() recognizes it as existing without
    attempting its own CREATE SCHEMA — the role has no such privilege by
    default (only section 4b's grant adds it), so a wrongly-attempted CREATE
    here would fail this test with a permission error instead of passing."""
    creds = _require_creds()
    profile = _profile(creds, managed_schema=creds["DRT_SMOKE_SNOWFLAKE_SCHEMA"])

    SnowflakeSource().ensure_managed_schema(profile)  # must not raise

    table = f"drt_smoke_managed_{uuid.uuid4().hex[:10]}"
    try:
        assert SnowflakeSource().managed_table_exists(profile, table) is False
        conn = _admin_connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"CREATE TABLE {creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}."
                    f"{creds['DRT_SMOKE_SNOWFLAKE_SCHEMA']}.{table} (id INTEGER)"
                )
        finally:
            conn.close()
        assert SnowflakeSource().managed_table_exists(profile, table) is True
        SnowflakeSource().drop_managed_table(profile, table)
        assert SnowflakeSource().managed_table_exists(profile, table) is False
    finally:
        conn = _admin_connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"DROP TABLE IF EXISTS {creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}."
                    f"{creds['DRT_SMOKE_SNOWFLAKE_SCHEMA']}.{table}"
                )
        finally:
            conn.close()


def test_ensure_managed_schema_finds_the_exact_unquoted_admin_create() -> None:
    """Round-trip the exact unquoted CREATE SCHEMA an admin would run
    following the provisioning docs, proving create-path, probe-path, and
    docs stay in agreement (the design concern this test exists to close)."""
    _require_create_schema_grant()
    creds = _require_creds()
    schema_name = f"drt_smoke_{uuid.uuid4().hex[:10]}"
    conn = _admin_connect(creds)
    try:
        with conn.cursor() as cur:
            # Deliberately unquoted and mixed-case, exactly as
            # provisioning/snowflake.sql's own CREATE SCHEMA statements are —
            # Snowflake folds this to uppercase.
            cur.execute(f"CREATE SCHEMA {creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}.{schema_name}")
    finally:
        conn.close()

    try:
        profile = _profile(creds, managed_schema=schema_name)
        SnowflakeSource().ensure_managed_schema(profile)  # must find it, not re-CREATE
    finally:
        conn = _admin_connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"DROP SCHEMA IF EXISTS {creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}.{schema_name}"
                )
        finally:
            conn.close()


def test_ensure_managed_schema_creates_when_truly_absent() -> None:
    creds = _require_creds()
    _require_create_schema_grant()
    schema_name = f"drt_smoke_{uuid.uuid4().hex[:10]}"
    profile = _profile(creds, managed_schema=schema_name)
    try:
        SnowflakeSource().ensure_managed_schema(profile)
        conn = _admin_connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"SELECT 1 FROM {creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}.information_schema"
                    ".schemata WHERE UPPER(schema_name) = UPPER(%s)",
                    (schema_name,),
                )
                assert cur.fetchone() is not None
        finally:
            conn.close()
        # Idempotent — a second call against an already-existing schema
        # must not raise (the escape-hatch probe short-circuits it).
        SnowflakeSource().ensure_managed_schema(profile)
    finally:
        conn = _admin_connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"DROP SCHEMA IF EXISTS {creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}.{schema_name}"
                )
        finally:
            conn.close()


def test_concurrent_first_use_never_raises_the_create_race() -> None:
    """8 threads race ensure_managed_schema() for a schema name none of them
    has seen before — mirrors the Postgres concurrent-first-use test that
    found a real UniqueViolation race on that dialect. Confirms empirically
    whether Snowflake's CREATE SCHEMA IF NOT EXISTS races the same way."""
    creds = _require_creds()
    _require_create_schema_grant()
    schema_name = f"drt_smoke_{uuid.uuid4().hex[:10]}"
    profile = _profile(creds, managed_schema=schema_name)
    errors: list[BaseException] = []
    lock = threading.Lock()

    def _call() -> None:
        try:
            SnowflakeSource().ensure_managed_schema(profile)
        except BaseException as e:  # noqa: BLE001 - collected, not swallowed
            with lock:
                errors.append(e)

    try:
        threads = [threading.Thread(target=_call) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert errors == [], f"concurrent ensure_managed_schema() raised: {errors!r}"
    finally:
        conn = _admin_connect(creds)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"DROP SCHEMA IF EXISTS {creds['DRT_SMOKE_SNOWFLAKE_DATABASE']}.{schema_name}"
                )
        finally:
            conn.close()


def test_managed_table_capable_protocol_satisfied() -> None:
    from drt.sources.base import ManagedTableCapable

    assert isinstance(SnowflakeSource(), ManagedTableCapable)
