"""Type-conversion boundary tests: DuckDB Source → REST destination.

Locks in the *current* serialization behaviour at the destination edge:
which DuckDB column types flow through ``run_sync`` into a JSON-body
REST POST cleanly, and which currently break. The intent is to make
future changes intentional — a green→red flip on any of these tests
should force a deliberate decision (and likely a CHANGELOG entry).

Related: v0.8 schema-aware serialization epic (#317).
"""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")  # noqa: F841  — module-gate

from drt.config.models import RestApiDestinationConfig, SyncConfig, SyncOptions  # noqa: E402
from drt.destinations.rest_api import RestApiDestination  # noqa: E402
from drt.engine.sync import run_sync  # noqa: E402
from tests.integration.conftest import seed_duckdb_table  # noqa: E402


def _dest(httpserver) -> RestApiDestinationConfig:
    return RestApiDestinationConfig(
        type="rest_api",
        url=httpserver.url_for("/sink"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )


def _sync_cfg(dest: RestApiDestinationConfig) -> SyncConfig:
    return SyncConfig(
        name="types",
        model="ref('t')",
        destination=dest,
        sync=SyncOptions(batch_size=10, on_error="skip"),
    )


def test_null_values_flow_as_json_null(httpserver, tmp_path: Path) -> None:
    source, profile = seed_duckdb_table(
        str(tmp_path / "t.duckdb"),
        "CREATE TABLE t (id INTEGER, label VARCHAR)",
        [(1, None), (2, "present")],
        "INSERT INTO t VALUES (?, ?)",
    )
    received: list[dict] = []

    def handler(request):
        received.append(json.loads(request.data))
        from werkzeug.wrappers import Response

        return Response("OK", status=200)

    httpserver.expect_request("/sink", method="POST").respond_with_handler(handler)
    run_sync(_sync_cfg(_dest(httpserver)), source, RestApiDestination(), profile, tmp_path)

    by_id = {r["id"]: r for r in received}
    assert by_id[1]["label"] is None  # JSON null round-trips
    assert by_id[2]["label"] == "present"


def test_bigint_extremes_preserved(httpserver, tmp_path: Path) -> None:
    source, profile = seed_duckdb_table(
        str(tmp_path / "t.duckdb"),
        "CREATE TABLE t (id INTEGER, big BIGINT)",
        [(1, 9223372036854775807), (2, -9223372036854775808)],
        "INSERT INTO t VALUES (?, ?)",
    )
    received: list[dict] = []

    def handler(request):
        received.append(json.loads(request.data))
        from werkzeug.wrappers import Response

        return Response("OK", status=200)

    httpserver.expect_request("/sink", method="POST").respond_with_handler(handler)
    run_sync(_sync_cfg(_dest(httpserver)), source, RestApiDestination(), profile, tmp_path)

    by_id = {r["id"]: r for r in received}
    assert by_id[1]["big"] == 9223372036854775807
    assert by_id[2]["big"] == -9223372036854775808


def test_double_preserved(httpserver, tmp_path: Path) -> None:
    source, profile = seed_duckdb_table(
        str(tmp_path / "t.duckdb"),
        "CREATE TABLE t (id INTEGER, dbl DOUBLE)",
        [(1, 1.7976931348623157e308), (2, -1.7976931348623157e308)],
        "INSERT INTO t VALUES (?, ?)",
    )
    received: list[dict] = []

    def handler(request):
        received.append(json.loads(request.data))
        from werkzeug.wrappers import Response

        return Response("OK", status=200)

    httpserver.expect_request("/sink", method="POST").respond_with_handler(handler)
    run_sync(_sync_cfg(_dest(httpserver)), source, RestApiDestination(), profile, tmp_path)

    by_id = {r["id"]: r for r in received}
    assert by_id[1]["dbl"] == 1.7976931348623157e308
    assert by_id[2]["dbl"] == -1.7976931348623157e308


def test_date_and_timestamp_currently_fail_serialization(
    httpserver, tmp_path: Path
) -> None:
    """DATE / TIMESTAMP fail at httpx JSON serialization today.

    The REST destination calls ``client.request(json=record)`` without a
    custom encoder, so ``datetime.date`` / ``datetime.datetime`` values
    raise ``TypeError`` inside httpx. ``on_error="skip"`` lets the engine
    swallow the failure and report it as failed records.

    This is the current behaviour. When schema-aware serialization (#317)
    lands, these failures should turn into successes — flip the assertion
    then.
    """
    source, profile = seed_duckdb_table(
        str(tmp_path / "t.duckdb"),
        "CREATE TABLE t (id INTEGER, d DATE, ts TIMESTAMP)",
        [(1, dt.date(2026, 5, 24), dt.datetime(2026, 5, 24, 12, 0, 0))],
        "INSERT INTO t VALUES (?, ?, ?)",
    )

    # No request expectations — the request never gets made because httpx
    # serialization fails first.
    result = run_sync(_sync_cfg(_dest(httpserver)), source, RestApiDestination(), profile, tmp_path)

    assert result.success == 0
    assert result.failed == 1
