"""Compliance audit trail against a real Postgres (#1100, ADR 0005 step 5).

A mock cursor can prove the right SQL text is *issued*, not that
``executemany`` genuinely inserts one row per entry with a JSONB column
round-tripping correctly, or that concurrent ``log_delivered`` calls
during first-use table creation survive the same race #920/#960/#1099
already found and fixed for their own tables.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from drt.config.credentials import PostgresProfile
from drt.state.audit_trail import AuditEntry
from drt.state.warehouse import PostgresComplianceAuditTrail

from .conftest import require_docker

pytestmark = pytest.mark.local_sql_smoke

psycopg2 = pytest.importorskip("psycopg2")
testcontainers_postgres = pytest.importorskip("testcontainers.postgres")


@pytest.fixture
def pg_profile():
    require_docker()
    postgres_container = testcontainers_postgres.PostgresContainer
    with postgres_container(
        "postgres:16-alpine",
        username="admin",
        password="adminpass",
        dbname="testdb",
        driver=None,
    ) as postgres:
        yield PostgresProfile(
            type="postgres",
            host=postgres.get_container_host_ip(),
            port=int(postgres.get_exposed_port(5432)),
            dbname="testdb",
            user="admin",
            password="adminpass",
        )


def _read_all(pg_profile: PostgresProfile, sync_name: str) -> list[tuple]:
    conn = psycopg2.connect(
        host=pg_profile.host,
        port=pg_profile.port,
        dbname=pg_profile.dbname,
        user=pg_profile.user,
        password=pg_profile.password,
    )
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT sync_name, run_id, sync_run_id, record_key, logged_fields_json, "
            'destination_type, delivered_at FROM _drt."_drt_audit_log" '
            "WHERE sync_name = %s ORDER BY delivered_at",
            (sync_name,),
        )
        return cur.fetchall()
    finally:
        conn.close()


def test_log_delivered_round_trips_including_jsonb_fields(pg_profile: PostgresProfile) -> None:
    audit = PostgresComplianceAuditTrail(pg_profile)

    audit.log_delivered(
        "orders",
        "run-1",
        "sync-run-1",
        "slack",
        [
            AuditEntry(record_key="a@example.com", fields={"email": "a@example.com", "n": 1}),
            AuditEntry(record_key="b@example.com", fields={"email": "b@example.com", "n": 2}),
        ],
        "2026-09-07T00:00:00+00:00",
    )

    rows = _read_all(pg_profile, "orders")
    assert len(rows) == 2
    assert rows[0][3] == "a@example.com"
    assert rows[0][4] == {"email": "a@example.com", "n": 1}
    assert rows[0][5] == "slack"


def test_log_delivered_accepts_null_run_id_and_sync_run_id(pg_profile: PostgresProfile) -> None:
    audit = PostgresComplianceAuditTrail(pg_profile)

    audit.log_delivered("orders", None, None, "slack", [AuditEntry(record_key="k", fields={})], "t")

    rows = _read_all(pg_profile, "orders")
    assert rows[0][1] is None
    assert rows[0][2] is None


def test_prune_removes_only_rows_older_than_retention(pg_profile: PostgresProfile) -> None:
    audit = PostgresComplianceAuditTrail(pg_profile)
    now = datetime.now(timezone.utc)
    old = (now - timedelta(days=45)).isoformat()
    recent = (now - timedelta(hours=1)).isoformat()

    audit.log_delivered(
        "orders", "r", "sr", "slack", [AuditEntry(record_key="old", fields={})], old
    )
    audit.log_delivered(
        "orders", "r", "sr", "slack", [AuditEntry(record_key="recent", fields={})], recent
    )

    removed = audit.prune("orders", retain_days=30)

    assert removed == 1
    rows = _read_all(pg_profile, "orders")
    assert [row[3] for row in rows] == ["recent"]


def test_concurrent_first_writes_survive_table_creation_race(pg_profile: PostgresProfile) -> None:
    """Same race #920/#960/#1099 found and fixed in the shared
    _ensure_table_exists helper, reproduced here since _drt_audit_log is a
    new, independent first-use path through it."""
    import concurrent.futures

    def write(i: int) -> None:
        PostgresComplianceAuditTrail(pg_profile).log_delivered(
            "orders",
            "r",
            "sr",
            "slack",
            [AuditEntry(record_key=f"key_{i}", fields={})],
            "2026-09-07T00:00:00+00:00",
        )

    errors: list[BaseException] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        futures = [executor.submit(write, i) for i in range(8)]
        for future in concurrent.futures.as_completed(futures):
            exc = future.exception()
            if exc is not None:
                errors.append(exc)

    assert not errors, f"concurrent first writes raised: {errors}"
    rows = _read_all(pg_profile, "orders")
    assert {row[3] for row in rows} == {f"key_{i}" for i in range(8)}
