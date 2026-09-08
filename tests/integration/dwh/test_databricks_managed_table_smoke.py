"""Databricks ManagedTableCapable smoke test (#960/#1108).

A mock cursor can prove call shape but not real Databricks/Unity Catalog
semantics — in particular, whether ``CREATE SCHEMA IF NOT EXISTS`` on
Databricks actually races the same ungraceful way it does on Postgres is
unverified by any prior art in this repo, and whether an admin's unquoted
escape-hatch ``CREATE SCHEMA`` (from ``provisioning/databricks.sql``) is
actually found by ``ensure_managed_schema()``'s plain, un-normalized probe
needs a live round trip, not an assumption — Unity Catalog is case-preserving
rather than case-folding, unlike Snowflake, so there is no ``UPPER()`` to
verify here, only that the literal names line up.

Runs only when ``DRT_SMOKE_DATABRICKS_*`` secrets are present (dwh-smoke
workflow injects them from repo secrets). The escape-hatch test below only
needs privileges the smoke principal already has (``provisioning/databricks.sql``
section 2). The positive-create-path and concurrent-race tests additionally
need the ``GRANT CREATE SCHEMA ON CATALOG`` added for #1108 — they skip
cleanly (not fail) via ``DRT_SMOKE_DATABRICKS_HAS_CREATE_SCHEMA_GRANT`` until
an operator has actually run that grant against the real account.
"""

from __future__ import annotations

import threading
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
# Manual opt-in: set once an operator has run the #1108 CREATE SCHEMA grant
# (provisioning/databricks.sql) against the real smoke account — there's no
# privilege-safe way to probe for the grant itself ahead of time.
_HAS_CREATE_SCHEMA_GRANT_ENV = "DRT_SMOKE_DATABRICKS_HAS_CREATE_SCHEMA_GRANT"


def _require_creds() -> dict[str, str]:
    return require_env(HOST_ENV, HTTP_PATH_ENV, TOKEN_ENV, CATALOG_ENV, SCHEMA_ENV)


def _require_create_schema_grant() -> None:
    import os

    if not os.environ.get(_HAS_CREATE_SCHEMA_GRANT_ENV):
        pytest.skip(
            f"{_HAS_CREATE_SCHEMA_GRANT_ENV} not set — run provisioning/databricks.sql's "
            "GRANT CREATE SCHEMA ON CATALOG against the real smoke account first, then "
            "set this env var to enable this test."
        )


def _profile(creds: dict[str, str], **overrides: object) -> DatabricksProfile:
    return DatabricksProfile(
        type="databricks",
        server_hostname=creds[HOST_ENV],
        http_path=creds[HTTP_PATH_ENV],
        access_token=creds[TOKEN_ENV],
        catalog=creds[CATALOG_ENV],
        schema=creds[SCHEMA_ENV],
        **overrides,  # type: ignore[arg-type]
    )


def _admin_connect(creds: dict[str, str]):
    return dbsql.connect(
        server_hostname=creds[HOST_ENV],
        http_path=creds[HTTP_PATH_ENV],
        access_token=creds[TOKEN_ENV],
    )


def test_ensure_managed_schema_escape_hatch_finds_a_preexisting_schema() -> None:
    """The escape hatch (#695 discipline, no new grant needed): point
    managed_schema at the principal's already-granted smoke schema and
    confirm ensure_managed_schema() recognizes it as existing without
    attempting its own CREATE SCHEMA — the principal has no such privilege
    by default (only the #1108 grant adds it), so a wrongly-attempted CREATE
    here would fail this test with a permission error instead of passing."""
    creds = _require_creds()
    profile = _profile(creds, managed_schema=creds[SCHEMA_ENV])

    DatabricksSource().ensure_managed_schema(profile)  # must not raise

    table = f"drt_smoke_managed_{uuid.uuid4().hex[:10]}"
    conn = _admin_connect(creds)
    try:
        assert DatabricksSource().managed_table_exists(profile, table) is False
        cur = conn.cursor()
        cur.execute(f"CREATE TABLE {creds[CATALOG_ENV]}.{creds[SCHEMA_ENV]}.{table} (id INT)")
        assert DatabricksSource().managed_table_exists(profile, table) is True
        DatabricksSource().drop_managed_table(profile, table)
        assert DatabricksSource().managed_table_exists(profile, table) is False
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP TABLE IF EXISTS {creds[CATALOG_ENV]}.{creds[SCHEMA_ENV]}.{table}")
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
        cur = conn.cursor()
        cur.execute(f"CREATE SCHEMA {creds[CATALOG_ENV]}.{schema_name}")

        profile = _profile(creds, managed_schema=schema_name)
        DatabricksSource().ensure_managed_schema(profile)  # must find it, not re-CREATE
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP SCHEMA IF EXISTS {creds[CATALOG_ENV]}.{schema_name} CASCADE")
        conn.close()


def test_ensure_managed_schema_creates_when_truly_absent() -> None:
    creds = _require_creds()
    _require_create_schema_grant()
    schema_name = f"drt_smoke_{uuid.uuid4().hex[:10]}"
    profile = _profile(creds, managed_schema=schema_name)
    conn = _admin_connect(creds)
    try:
        DatabricksSource().ensure_managed_schema(profile)
        cur = conn.cursor()
        cur.execute(f"SHOW SCHEMAS IN {creds[CATALOG_ENV]} LIKE '{schema_name}'")
        assert cur.fetchall()
        # Idempotent — a second call against an already-existing schema
        # must not raise (the escape-hatch probe short-circuits it).
        DatabricksSource().ensure_managed_schema(profile)
    finally:
        cur = conn.cursor()
        cur.execute(f"DROP SCHEMA IF EXISTS {creds[CATALOG_ENV]}.{schema_name} CASCADE")
        conn.close()


def test_concurrent_first_use_never_raises_the_create_race() -> None:
    """8 threads race ensure_managed_schema() for a schema name none of them
    has seen before — mirrors the Postgres/Snowflake concurrent-first-use
    test that found a real race on those dialects. Confirms empirically
    whether Databricks' CREATE SCHEMA IF NOT EXISTS races the same way."""
    creds = _require_creds()
    _require_create_schema_grant()
    schema_name = f"drt_smoke_{uuid.uuid4().hex[:10]}"
    profile = _profile(creds, managed_schema=schema_name)
    errors: list[BaseException] = []
    lock = threading.Lock()

    def _call() -> None:
        try:
            DatabricksSource().ensure_managed_schema(profile)
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
            cur = conn.cursor()
            cur.execute(f"DROP SCHEMA IF EXISTS {creds[CATALOG_ENV]}.{schema_name} CASCADE")
        finally:
            conn.close()


def test_managed_table_capable_protocol_satisfied() -> None:
    from drt.sources.base import ManagedTableCapable

    assert isinstance(DatabricksSource(), ManagedTableCapable)
