"""Manual cProfile experiments with real local network I/O (#1008).

This module deliberately lives outside pytest discovery. The Postgres leg
requires Docker and testcontainers; the REST leg starts pytest-httpserver and
adds a fixed delay in its server thread. Neither belongs in the default test
suite.

The attribution follows :mod:`benchmarks.profile_scenarios`: buckets are
non-overlapping portions of cProfile wall time and therefore sum to 100%.
Postgres's C driver does not expose its socket calls separately to cProfile,
so its source boundary is explicitly an I/O-bearing aggregate of database
wait, TCP wait, driver conversion, and Python record construction. The
pure-Python HTTP stack does expose socket primitives; the REST leg separates
their self time from the remaining destination call tree.
"""

from __future__ import annotations

import cProfile
import json
import math
import pstats
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from benchmarks.harness import SCENARIOS, BenchmarkScenario, get_git_commit
from benchmarks.profile_scenarios import (
    ProfileBucket,
    _bucket,
    _cumulative_seconds,
    _find_function,
    _format_timestamp,
    _ProfileStats,
)
from drt.config.credentials import PostgresProfile
from drt.config.models import (
    RateLimitConfig,
    RestApiDestinationConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.rate_limiter import _reset_limiter_registry
from drt.destinations.rest_api import RestApiDestination
from drt.sources.postgres import PostgresSource

SCHEMA_VERSION = 1
DEFAULT_LATENCIES_MS: tuple[int, ...] = (10, 50, 200)

_SOCKET_IO_FUNCTIONS = {
    "<built-in method _socket.getaddrinfo>",
    "<built-in method select.select>",
    "<method 'connect' of '_socket.socket' objects>",
    "<method 'recv' of '_socket.socket' objects>",
    "<method 'recv_into' of '_socket.socket' objects>",
    "<method 'send' of '_socket.socket' objects>",
    "<method 'sendall' of '_socket.socket' objects>",
    "<method 'poll' of 'select.poll' objects>",
}


@dataclass(frozen=True)
class RealIOProfileMeasurements:
    """Non-overlapping timings for one real-I/O profile."""

    duration_seconds: float
    function_calls: int
    primitive_calls: int
    buckets: dict[str, ProfileBucket]
    components: dict[str, float]


@dataclass(frozen=True)
class RealIOProfileResult:
    """Serializable result for one source or destination profile."""

    schema_version: int
    profiler: str
    leg: str
    scenario: str
    row_count: int
    batch_size: int
    request_count: int | None
    controlled_latency_ms: int | None
    git_commit: str
    timestamp: str
    measurements: RealIOProfileMeasurements

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class RealIOProfileArtifact:
    result: RealIOProfileResult
    path: Path


def _percentages(duration_seconds: float, seconds: tuple[float, ...]) -> tuple[float, ...]:
    """Return rounded exclusive percentages whose displayed values sum to 100%."""
    if duration_seconds <= 0:
        raise ValueError("duration_seconds must be positive")
    if not seconds:
        raise ValueError("at least one bucket is required")
    if any(value < 0 for value in seconds):
        raise ValueError("bucket seconds cannot be negative")
    leading = tuple(round(value / duration_seconds * 100, 2) for value in seconds[:-1])
    return (*leading, round(100.0 - sum(leading), 2))


def _profile_stats(profiler: cProfile.Profile, description: str) -> _ProfileStats:
    stats = cast(_ProfileStats, pstats.Stats(profiler))
    if stats.total_tt <= 0:
        raise RuntimeError(f"{description} recorded no elapsed time")
    return stats


def _synthetic_records(row_count: int) -> list[dict[str, Any]]:
    """Materialize #280's four-field payload for the destination-only leg."""
    return [
        {
            "id": row_id,
            "name": f"user-{row_id:08d}",
            "email": f"user-{row_id:08d}@example.com",
            "score": row_id % 1000,
        }
        for row_id in range(1, row_count + 1)
    ]


def _consume_postgres_source(
    source: PostgresSource,
    profile: PostgresProfile,
    row_count: int,
) -> int:
    query = f"""SELECT id, name, email, score
FROM drt_profile_records
WHERE id <= {row_count}
ORDER BY id"""
    observed = 0
    for record in source.extract(query, profile):
        observed += 1
        if observed == 1 and set(record) != {"id", "name", "email", "score"}:
            raise RuntimeError(f"unexpected Postgres record shape: {sorted(record)}")
    return observed


def _seed_postgres(postgres: Any, row_count: int) -> PostgresProfile:
    import psycopg2

    connection = psycopg2.connect(postgres.get_connection_url(driver=None))
    try:
        with connection.cursor() as cursor:
            cursor.execute("DROP TABLE IF EXISTS drt_profile_records")
            cursor.execute(
                """CREATE TABLE drt_profile_records AS
SELECT
    id,
    'user-' || lpad(id::text, 8, '0') AS name,
    'user-' || lpad(id::text, 8, '0') || '@example.com' AS email,
    id %% 1000 AS score
FROM generate_series(1, %s) AS synthetic(id)""",
                (row_count,),
            )
            cursor.execute("ALTER TABLE drt_profile_records ADD PRIMARY KEY (id)")
        connection.commit()
    finally:
        connection.close()

    return PostgresProfile(
        type="postgres",
        host=postgres.get_container_host_ip(),
        port=int(postgres.get_exposed_port(postgres.port)),
        dbname=postgres.dbname,
        user=postgres.username,
        password=postgres.password,
    )


def profile_postgres_scenario(
    scenario: BenchmarkScenario,
    profile: PostgresProfile,
    *,
    git_commit: str,
    timestamp: datetime | None = None,
) -> RealIOProfileResult:
    """Profile ``PostgresSource.extract`` over the container's TCP socket."""
    source = PostgresSource()
    if not source.test_connection(profile):
        raise RuntimeError("Postgres profile preflight failed")

    profiler = cProfile.Profile()
    observed = profiler.runcall(_consume_postgres_source, source, profile, scenario.row_count)
    if observed != scenario.row_count:
        raise RuntimeError(
            f"Postgres scenario {scenario.name!r} extracted {observed} rows; "
            f"expected {scenario.row_count}"
        )
    stats = _profile_stats(profiler, f"Postgres scenario {scenario.name!r}")

    extraction_seconds = _cumulative_seconds(
        stats,
        filename_suffix="/drt/sources/postgres.py",
        function_name="extract",
    )
    setup_seconds = _cumulative_seconds(
        stats,
        filename_suffix="/drt/sources/postgres.py",
        function_name="_connect_and_execute",
    )
    streaming_seconds = max(0.0, extraction_seconds - setup_seconds)
    consumer_seconds = max(0.0, stats.total_tt - extraction_seconds)
    percentages = _percentages(
        stats.total_tt,
        (setup_seconds, streaming_seconds, consumer_seconds),
    )

    started_at = timestamp or datetime.now(timezone.utc)
    if started_at.tzinfo is None:
        raise ValueError("profile timestamp must be timezone-aware")
    return RealIOProfileResult(
        schema_version=SCHEMA_VERSION,
        profiler="cProfile",
        leg="postgres_source",
        scenario=scenario.name,
        row_count=scenario.row_count,
        batch_size=scenario.batch_size,
        request_count=None,
        controlled_latency_ms=None,
        git_commit=git_commit,
        timestamp=_format_timestamp(started_at),
        measurements=RealIOProfileMeasurements(
            duration_seconds=round(stats.total_tt, 6),
            function_calls=stats.total_calls,
            primitive_calls=stats.prim_calls,
            buckets={
                "connection_query_setup": _bucket(
                    setup_seconds,
                    stats.total_tt,
                    "mixed_io_cpu",
                    percentage=percentages[0],
                ),
                "row_streaming_and_conversion": _bucket(
                    streaming_seconds,
                    stats.total_tt,
                    "mixed_io_cpu",
                    percentage=percentages[1],
                ),
                "consumer_cpu": _bucket(
                    consumer_seconds,
                    stats.total_tt,
                    "cpu_bound",
                    percentage=percentages[2],
                ),
            },
            components={
                "postgres_extraction_boundary_seconds": round(extraction_seconds, 6),
            },
        ),
    )


def _socket_io_seconds(stats: _ProfileStats) -> float:
    """Sum self time in blocking socket/select primitives in the profiled thread."""
    return sum(
        self_time
        for (_filename, _line, function_name), (
            _cc,
            _nc,
            self_time,
            _cumulative_time,
            _callers,
        ) in stats.stats.items()
        if function_name in _SOCKET_IO_FUNCTIONS
    )


def _load_rest_batches(
    records: list[dict[str, Any]],
    scenario: BenchmarkScenario,
    config: RestApiDestinationConfig,
    sync_options: SyncOptions,
) -> int:
    destination = RestApiDestination()
    success = 0
    for offset in range(0, len(records), scenario.batch_size):
        result = destination.load(
            records[offset : offset + scenario.batch_size],
            config,
            sync_options,
        )
        if result.failed or result.errors or result.row_errors:
            raise RuntimeError(
                f"REST scenario {scenario.name!r} failed: "
                f"failed={result.failed}, errors={result.errors}, "
                f"row_errors={result.row_errors}"
            )
        success += result.success
    return success


def profile_rest_scenario(
    scenario: BenchmarkScenario,
    url: str,
    *,
    controlled_latency_ms: int,
    git_commit: str,
    timestamp: datetime | None = None,
) -> RealIOProfileResult:
    """Profile real HTTP requests whose server response delay is controlled."""
    if controlled_latency_ms < 0:
        raise ValueError("controlled_latency_ms cannot be negative")
    records = _synthetic_records(scenario.row_count)
    request_count = math.ceil(scenario.row_count / scenario.batch_size)
    config = RestApiDestinationConfig(
        type="rest_api",
        url=url,
        method="POST",
        headers={"Content-Type": "application/json"},
        body_mode="batch",
        batch_template="{{ rows | tojson_safe }}",
        max_records_per_request=scenario.batch_size,
        retry=RetryConfig(max_attempts=1),
        rate_limit=RateLimitConfig(requests_per_second=0),
    )
    sync_options = SyncOptions(
        batch_size=scenario.batch_size,
        on_error="fail",
        retry=RetryConfig(max_attempts=1),
        rate_limit=RateLimitConfig(requests_per_second=0),
    )

    _reset_limiter_registry()
    profiler = cProfile.Profile()
    observed = profiler.runcall(
        _load_rest_batches,
        records,
        scenario,
        config,
        sync_options,
    )
    if observed != scenario.row_count:
        raise RuntimeError(
            f"REST scenario {scenario.name!r} loaded {observed} rows; "
            f"expected {scenario.row_count}"
        )
    stats = _profile_stats(
        profiler,
        f"REST scenario {scenario.name!r} at {controlled_latency_ms} ms",
    )
    load_key = _find_function(
        stats,
        filename_suffix="/drt/destinations/rest_api.py",
        function_name="load",
    )
    load_seconds = stats.stats[load_key][3]
    socket_seconds = min(_socket_io_seconds(stats), load_seconds)
    destination_cpu_seconds = max(0.0, load_seconds - socket_seconds)
    harness_cpu_seconds = max(0.0, stats.total_tt - load_seconds)
    percentages = _percentages(
        stats.total_tt,
        (socket_seconds, destination_cpu_seconds, harness_cpu_seconds),
    )

    started_at = timestamp or datetime.now(timezone.utc)
    if started_at.tzinfo is None:
        raise ValueError("profile timestamp must be timezone-aware")
    return RealIOProfileResult(
        schema_version=SCHEMA_VERSION,
        profiler="cProfile",
        leg="rest_destination",
        scenario=scenario.name,
        row_count=scenario.row_count,
        batch_size=scenario.batch_size,
        request_count=request_count,
        controlled_latency_ms=controlled_latency_ms,
        git_commit=git_commit,
        timestamp=_format_timestamp(started_at),
        measurements=RealIOProfileMeasurements(
            duration_seconds=round(stats.total_tt, 6),
            function_calls=stats.total_calls,
            primitive_calls=stats.prim_calls,
            buckets={
                "socket_io": _bucket(
                    socket_seconds,
                    stats.total_tt,
                    "io_bound",
                    percentage=percentages[0],
                ),
                "destination_cpu": _bucket(
                    destination_cpu_seconds,
                    stats.total_tt,
                    "cpu_bound",
                    percentage=percentages[1],
                ),
                "harness_cpu": _bucket(
                    harness_cpu_seconds,
                    stats.total_tt,
                    "cpu_bound",
                    percentage=percentages[2],
                ),
            },
            components={
                "rest_destination_load_seconds": round(load_seconds, 6),
            },
        ),
    )


def _write_result(result: RealIOProfileResult, profiles_dir: Path) -> Path:
    profiles_dir.mkdir(parents=True, exist_ok=True)
    timestamp_slug = result.timestamp.replace("-", "").replace(":", "").replace(".", "")
    latency_slug = (
        f"-{result.controlled_latency_ms}ms"
        if result.controlled_latency_ms is not None
        else ""
    )
    path = profiles_dir / (
        f"{timestamp_slug}-{result.leg}-{result.scenario}{latency_slug}-"
        f"{result.git_commit[:8]}.json"
    )
    path.write_text(json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n")
    return path


def run_postgres_profiles(
    scenarios: tuple[BenchmarkScenario, ...] = SCENARIOS,
    *,
    profiles_dir: Path,
    repo_root: Path,
) -> list[RealIOProfileArtifact]:
    """Start, seed, profile, and tear down one ephemeral Postgres container."""
    from testcontainers.postgres import PostgresContainer

    commit = get_git_commit(repo_root)
    artifacts: list[RealIOProfileArtifact] = []
    with PostgresContainer("postgres:16-alpine", driver=None) as postgres:
        profile = _seed_postgres(postgres, max(scenario.row_count for scenario in scenarios))
        for scenario in scenarios:
            result = profile_postgres_scenario(
                scenario,
                profile,
                git_commit=commit,
            )
            artifacts.append(
                RealIOProfileArtifact(result, _write_result(result, profiles_dir))
            )
    return artifacts


def run_rest_profiles(
    scenarios: tuple[BenchmarkScenario, ...] = SCENARIOS,
    latencies_ms: tuple[int, ...] = DEFAULT_LATENCIES_MS,
    *,
    profiles_dir: Path,
    repo_root: Path,
) -> list[RealIOProfileArtifact]:
    """Profile REST scenarios against fixed-delay pytest-httpserver instances."""
    from pytest_httpserver import HTTPServer
    from werkzeug.wrappers import Response

    commit = get_git_commit(repo_root)
    artifacts: list[RealIOProfileArtifact] = []
    for latency_ms in latencies_ms:
        if latency_ms < 0:
            raise ValueError("latency values cannot be negative")
        received_requests = 0

        def delayed_response(_request: Any) -> Response:
            nonlocal received_requests
            time.sleep(latency_ms / 1000)
            received_requests += 1
            return Response("{}", status=200, content_type="application/json")

        server = HTTPServer(host="127.0.0.1", port=0, threaded=True)
        server.expect_request("/records", method="POST").respond_with_handler(
            delayed_response
        )
        server.start()
        try:
            for scenario in scenarios:
                before = received_requests
                result = profile_rest_scenario(
                    scenario,
                    server.url_for("/records"),
                    controlled_latency_ms=latency_ms,
                    git_commit=commit,
                )
                observed_requests = received_requests - before
                if observed_requests != result.request_count:
                    raise RuntimeError(
                        f"REST scenario {scenario.name!r} made {observed_requests} requests; "
                        f"expected {result.request_count}"
                    )
                artifacts.append(
                    RealIOProfileArtifact(result, _write_result(result, profiles_dir))
                )
            server.check_assertions()
        finally:
            server.stop()
    return artifacts


def run_real_io_profiles(
    scenarios: tuple[BenchmarkScenario, ...],
    latencies_ms: tuple[int, ...],
    *,
    legs: frozenset[str],
    profiles_dir: Path,
    repo_root: Path,
) -> list[RealIOProfileArtifact]:
    """Run the selected manual real-I/O experiments."""
    artifacts: list[RealIOProfileArtifact] = []
    if "postgres" in legs:
        artifacts.extend(
            run_postgres_profiles(
                scenarios,
                profiles_dir=profiles_dir,
                repo_root=repo_root,
            )
        )
    if "rest" in legs:
        artifacts.extend(
            run_rest_profiles(
                scenarios,
                latencies_ms,
                profiles_dir=profiles_dir,
                repo_root=repo_root,
            )
        )
    return artifacts
