"""Large-batch boundary tests: DuckDB Source → REST destination.

Verifies the engine streams source rows through batching without losing
records and without an unbounded memory footprint. The row count is a
compromise between catching regressions and keeping CI runtime tight —
the issue (#542) aspirationally calls for 100K rows, but in practice a
few hundred rows over real HTTP exercises the same code paths in
seconds.

For REST destinations, ``batch_size`` is internal plumbing — each row
still gets its own HTTP request, so we cannot directly observe batch
boundaries from the destination side. The meaningful assertion is
"every source row reaches the destination intact".
"""

from __future__ import annotations

import json
import tracemalloc
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")  # noqa: F841  — module-gate

from drt.config.models import RestApiDestinationConfig, SyncConfig, SyncOptions  # noqa: E402
from drt.destinations.rest_api import RestApiDestination  # noqa: E402
from drt.engine.sync import run_sync  # noqa: E402
from tests.integration.conftest import seed_duckdb_table  # noqa: E402

N_ROWS = 50  # large enough to exercise batching boundaries (batch_size=50 → 1 batch;
#             batch_size=10 → 5 batches), small enough that pytest-httpserver per-request
#             overhead (~90ms) keeps total file runtime under ~10s. Aspirational 100K row
#             test (per issue #542) is out of scope here — would need a staged
#             destination to avoid HTTP-per-row latency.


def _seed(tmp_path: Path, n: int) -> tuple:
    return seed_duckdb_table(
        str(tmp_path / "big.duckdb"),
        "CREATE TABLE t (id INTEGER, payload VARCHAR)",
        [(i, f"row-{i}") for i in range(n)],
        "INSERT INTO t VALUES (?, ?)",
    )


def _dest(httpserver) -> RestApiDestinationConfig:
    return RestApiDestinationConfig(
        type="rest_api",
        url=httpserver.url_for("/bulk"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )


def _sync_cfg(dest: RestApiDestinationConfig, batch_size: int = 50) -> SyncConfig:
    return SyncConfig(
        name="bulk",
        model="ref('t')",
        destination=dest,
        sync=SyncOptions(batch_size=batch_size),
    )


def test_all_rows_reach_destination(httpserver, tmp_path: Path) -> None:
    """No drop, no duplication: every source row becomes one destination request."""
    source, profile = _seed(tmp_path, N_ROWS)
    received: list[dict] = []

    def handler(request):
        received.append(json.loads(request.data))
        from werkzeug.wrappers import Response

        return Response("OK", status=200)

    httpserver.expect_request("/bulk", method="POST").respond_with_handler(handler)
    result = run_sync(_sync_cfg(_dest(httpserver)), source, RestApiDestination(), profile, tmp_path)

    assert result.success == N_ROWS
    assert result.failed == 0
    assert len(received) == N_ROWS
    # IDs must be exactly 0..N_ROWS-1, no dupes
    assert sorted(r["id"] for r in received) == list(range(N_ROWS))


def test_small_batch_size_does_not_break_streaming(httpserver, tmp_path: Path) -> None:
    """batch_size=1 forces single-record batches; all rows still arrive."""
    source, profile = _seed(tmp_path, 20)
    received: list[dict] = []

    def handler(request):
        received.append(json.loads(request.data))
        from werkzeug.wrappers import Response

        return Response("OK", status=200)

    httpserver.expect_request("/bulk", method="POST").respond_with_handler(handler)
    result = run_sync(
        _sync_cfg(_dest(httpserver), batch_size=1), source, RestApiDestination(), profile, tmp_path
    )

    assert result.success == 20
    assert len(received) == 20


def test_memory_smoke_remains_bounded(httpserver, tmp_path: Path) -> None:
    """Smoke: peak allocation should be well under a generous ceiling.

    This is not an SLA — it's a guard against accidental O(N) buffering
    that would blow up at real data volumes. The 50 MB ceiling for 50
    rows is generous enough that pytest-httpserver bookkeeping noise does
    not trip it, but tight enough that a "materialise all rows" regression
    on real data volumes would.
    """
    source, profile = _seed(tmp_path, N_ROWS)

    httpserver.expect_request("/bulk", method="POST").respond_with_data("OK", status=200)

    tracemalloc.start()
    try:
        run_sync(_sync_cfg(_dest(httpserver)), source, RestApiDestination(), profile, tmp_path)
        _, peak_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    peak_mb = peak_bytes / (1024 * 1024)
    assert peak_mb < 50, f"Peak memory {peak_mb:.1f} MB exceeded 50 MB ceiling"
