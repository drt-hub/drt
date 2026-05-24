"""Schema-evolution boundary tests: locks in "no schema enforcement" behaviour.

drt's engine does not maintain its own schema model. The Source yields
dicts shaped by the live table at extract time, the Destination receives
those dicts verbatim. When the table schema changes between runs, the
payload shape sent to the destination changes accordingly — there is no
validation layer that would reject a new column or fill in a removed one.

This file pins that behaviour so a future schema-validation feature
becomes a deliberate, test-flipping decision. See v0.8 #317 (schema-aware
serialization) for the longer-term direction.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")

from drt.config.credentials import DuckDBProfile  # noqa: E402
from drt.config.models import RestApiDestinationConfig, SyncConfig, SyncOptions  # noqa: E402
from drt.destinations.rest_api import RestApiDestination  # noqa: E402
from drt.engine.sync import run_sync  # noqa: E402
from drt.sources.duckdb import DuckDBSource  # noqa: E402


def _dest(httpserver) -> RestApiDestinationConfig:
    return RestApiDestinationConfig(
        type="rest_api",
        url=httpserver.url_for("/sink"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )


def _sync_cfg(dest: RestApiDestinationConfig) -> SyncConfig:
    return SyncConfig(
        name="evo",
        model="ref('t')",
        destination=dest,
        sync=SyncOptions(batch_size=10),
    )


def _make_handler(received: list[dict]):
    def handler(request):
        received.append(json.loads(request.data))
        from werkzeug.wrappers import Response

        return Response("OK", status=200)

    return handler


def test_added_column_appears_in_second_run(httpserver, tmp_path: Path) -> None:
    """Add a column between runs → second run's payload includes the new field."""
    db_path = str(tmp_path / "evo.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
        conn.execute("INSERT INTO t VALUES (1, 'a')")
    finally:
        conn.close()

    received: list[dict] = []
    httpserver.expect_request("/sink", method="POST").respond_with_handler(_make_handler(received))

    source = DuckDBSource()
    profile = DuckDBProfile(type="duckdb", database=db_path)
    run_sync(_sync_cfg(_dest(httpserver)), source, RestApiDestination(), profile, tmp_path)

    # Add a column and a row, sync again
    conn = duckdb.connect(db_path)
    try:
        conn.execute("ALTER TABLE t ADD COLUMN email VARCHAR")
        conn.execute("UPDATE t SET email = 'a@x.com' WHERE id = 1")
        conn.execute("INSERT INTO t VALUES (2, 'b', 'b@x.com')")
    finally:
        conn.close()

    received.clear()
    run_sync(_sync_cfg(_dest(httpserver)), source, RestApiDestination(), profile, tmp_path)

    by_id = {r["id"]: r for r in received}
    assert "email" in by_id[1] and by_id[1]["email"] == "a@x.com"
    assert "email" in by_id[2] and by_id[2]["email"] == "b@x.com"


def test_removed_column_disappears_from_second_run(httpserver, tmp_path: Path) -> None:
    """Drop a column between runs → second run's payload omits the dropped field."""
    db_path = str(tmp_path / "evo.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE TABLE t (id INTEGER, name VARCHAR, email VARCHAR)")
        conn.execute("INSERT INTO t VALUES (1, 'a', 'a@x.com')")
    finally:
        conn.close()

    received: list[dict] = []
    httpserver.expect_request("/sink", method="POST").respond_with_handler(_make_handler(received))

    source = DuckDBSource()
    profile = DuckDBProfile(type="duckdb", database=db_path)
    run_sync(_sync_cfg(_dest(httpserver)), source, RestApiDestination(), profile, tmp_path)
    assert "email" in received[0]

    # Drop the column
    conn = duckdb.connect(db_path)
    try:
        conn.execute("ALTER TABLE t DROP COLUMN email")
    finally:
        conn.close()

    received.clear()
    run_sync(_sync_cfg(_dest(httpserver)), source, RestApiDestination(), profile, tmp_path)

    assert received[0].keys() == {"id", "name"}  # email gone, no None placeholder


def test_renamed_column_appears_under_new_name(httpserver, tmp_path: Path) -> None:
    """Rename a column → destination sees the new key; the old key is gone."""
    db_path = str(tmp_path / "evo.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE TABLE t (id INTEGER, full_name VARCHAR)")
        conn.execute("INSERT INTO t VALUES (1, 'Alice')")
    finally:
        conn.close()

    received: list[dict] = []
    httpserver.expect_request("/sink", method="POST").respond_with_handler(_make_handler(received))

    source = DuckDBSource()
    profile = DuckDBProfile(type="duckdb", database=db_path)
    run_sync(_sync_cfg(_dest(httpserver)), source, RestApiDestination(), profile, tmp_path)
    assert "full_name" in received[0]

    conn = duckdb.connect(db_path)
    try:
        conn.execute("ALTER TABLE t RENAME COLUMN full_name TO display_name")
    finally:
        conn.close()

    received.clear()
    run_sync(_sync_cfg(_dest(httpserver)), source, RestApiDestination(), profile, tmp_path)

    assert "display_name" in received[0]
    assert "full_name" not in received[0]
