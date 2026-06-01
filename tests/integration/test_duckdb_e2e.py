"""End-to-end integration test for the DuckDB Source.

Drives the full reverse-ETL pipeline: real DuckDB (seeded by the
``duckdb_with_users`` fixture) → engine → pytest-httpserver REST destination.
No mocks of `drt.sources.duckdb` internals — a real `duckdb.connect()` runs
inside `DuckDBSource.extract()` and a real HTTP server receives the writes.

This file is the canonical template for Source E2E tests. To add
``test_postgres_e2e.py`` or ``test_mysql_e2e.py``, copy this file and swap
the seed fixture; the destination side stays the same.
See tests/integration/README.md for the full harness pattern.
"""

from __future__ import annotations

import json

from drt.config.models import RestApiDestinationConfig, SyncConfig, SyncOptions
from drt.destinations.rest_api import RestApiDestination
from drt.engine.sync import run_sync


def _dest_config(httpserver, path: str = "/users") -> RestApiDestinationConfig:
    return RestApiDestinationConfig(
        type="rest_api",
        url=httpserver.url_for(path),
        method="POST",
        headers={"Content-Type": "application/json"},
    )


def _sync(dest: RestApiDestinationConfig, model: str = "ref('users')") -> SyncConfig:
    return SyncConfig(
        name="duckdb_to_rest",
        model=model,
        destination=dest,
        sync=SyncOptions(batch_size=10),
    )


def test_full_pipeline_three_seeded_rows_reach_destination(httpserver, duckdb_with_users, tmp_path):
    """Happy path: 3 seeded rows in DuckDB → 3 POSTs received."""
    source, profile = duckdb_with_users
    received: list[dict] = []

    def handler(request):
        received.append(json.loads(request.data))
        from werkzeug.wrappers import Response

        return Response("OK", status=200)

    httpserver.expect_request("/users", method="POST").respond_with_handler(handler)

    sync = _sync(_dest_config(httpserver))
    result = run_sync(sync, source, RestApiDestination(), profile, tmp_path)

    assert result.success == 3
    assert result.failed == 0
    assert len(received) == 3
    assert {r["id"] for r in received} == {1, 2, 3}
    assert {r["name"] for r in received} == {"Alice", "Bob", "Carol"}


def test_empty_query_yields_no_destination_requests(httpserver, duckdb_with_users, tmp_path):
    """Empty result set: 0 rows extracted → 0 POSTs sent."""
    source, profile = duckdb_with_users
    received: list[dict] = []

    def handler(request):  # pragma: no cover — assertion is len==0
        received.append(json.loads(request.data))
        from werkzeug.wrappers import Response

        return Response("OK", status=200)

    httpserver.expect_request("/users", method="POST").respond_with_handler(handler)

    # Inline SQL bypasses ref() resolution and runs the WHERE-false query directly.
    sync = _sync(_dest_config(httpserver), model="SELECT * FROM users WHERE id > 1000")
    result = run_sync(sync, source, RestApiDestination(), profile, tmp_path)

    assert result.success == 0
    assert result.failed == 0
    assert received == []


def test_extracted_column_values_flow_into_request_body(httpserver, duckdb_with_users, tmp_path):
    """Values read from DuckDB appear unchanged in the destination request body."""
    source, profile = duckdb_with_users
    received: list[dict] = []

    def handler(request):
        received.append(json.loads(request.data))
        from werkzeug.wrappers import Response

        return Response("OK", status=200)

    httpserver.expect_request("/users", method="POST").respond_with_handler(handler)

    sync = _sync(_dest_config(httpserver))
    run_sync(sync, source, RestApiDestination(), profile, tmp_path)

    alice = next(r for r in received if r["id"] == 1)
    assert alice["name"] == "Alice"
    assert alice["email"] == "alice@example.com"


def test_real_duckdb_connection_test_succeeds(duckdb_with_users):
    """`test_connection` against the seeded DuckDB file returns True."""
    source, profile = duckdb_with_users
    assert source.test_connection(profile) is True
