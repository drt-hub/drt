"""Non-network invariants for the manual real-I/O profiler (#1008)."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest

import benchmarks.profile_real_io as real_io_profiling
from benchmarks.harness import BenchmarkScenario
from benchmarks.profile_real_io import (
    RealIOProfileMeasurements,
    RealIOProfileResult,
    _percentages,
    _ProfileStats,
    _rest_bucket_seconds,
    _synthetic_records,
    _write_result,
)
from benchmarks.profile_scenarios import ProfileBucket
from drt.config.credentials import PostgresProfile


def test_percentages_sum_to_one_hundred_after_rounding() -> None:
    percentages = _percentages(3.0, (1.0, 1.0, 1.0))

    assert percentages == (33.33, 33.33, 33.34)
    assert sum(percentages) == 100.0


def test_rest_buckets_use_known_wait_and_leave_mixed_load_overhead() -> None:
    load_key = ("/workspace/drt/destinations/rest_api.py", 74, "load")
    stats = cast(
        _ProfileStats,
        SimpleNamespace(
            stats={
                load_key: (10, 10, 0.1, 2.5, {}),
            },
            total_tt=3.0,
            total_calls=10,
            prim_calls=10,
        ),
    )

    timings = _rest_bucket_seconds(
        stats,
        controlled_latency_ms=200,
        request_count=10,
    )

    assert timings.load == pytest.approx(2.5)
    assert timings.known_network_wait == pytest.approx(2.0)
    assert timings.load_overhead == pytest.approx(0.5)
    assert timings.harness_cpu == pytest.approx(0.5)


@pytest.mark.parametrize(
    ("total_seconds", "load_seconds"),
    [
        (3.0, 1.9),
        (2.4, 2.5),
    ],
)
def test_rest_buckets_reject_impossible_cprofile_timings(
    total_seconds: float,
    load_seconds: float,
) -> None:
    load_key = ("/workspace/drt/destinations/rest_api.py", 74, "load")
    stats = cast(
        _ProfileStats,
        SimpleNamespace(
            stats={load_key: (10, 10, 0.1, load_seconds, {})},
            total_tt=total_seconds,
            total_calls=10,
            prim_calls=10,
        ),
    )

    with pytest.raises(RuntimeError, match="invalid REST cProfile timing"):
        _rest_bucket_seconds(
            stats,
            controlled_latency_ms=200,
            request_count=10,
        )


def test_rest_buckets_absorb_noise_without_negative_percentages() -> None:
    """A tiny cProfile overshoot (within tolerance) must not break the 100%-sum invariant.

    Regression test for two failed real-I/O runs: the seconds-level clamp on
    ``harness_cpu`` alone left the three bucket seconds summing to more than
    ``total_tt``, which pushed the last bucket's residual percentage
    negative in ``_percentages`` and tripped the schema-validation gate in
    ``_write_result``.
    """
    # Exact values observed from a real (pre-fix) failing run: a ~30-microsecond
    # cProfile overshoot on a ~55-millisecond total is small in absolute terms
    # but large enough, relative to total_tt, to flip the residual percentage
    # negative once rounded to 2 decimals.
    load_key = ("/workspace/drt/destinations/rest_api.py", 74, "load")
    total_seconds = 0.053663046
    load_seconds = 0.053693042
    stats = cast(
        _ProfileStats,
        SimpleNamespace(
            stats={load_key: (10, 10, 0.1, load_seconds, {})},
            total_tt=total_seconds,
            total_calls=10,
            prim_calls=10,
        ),
    )

    timings = _rest_bucket_seconds(stats, controlled_latency_ms=10, request_count=1)

    assert timings.harness_cpu == 0.0
    bucket_sum = timings.known_network_wait + timings.load_overhead + timings.harness_cpu
    assert bucket_sum == pytest.approx(total_seconds)

    percentages = _percentages(
        total_seconds,
        (timings.known_network_wait, timings.load_overhead, timings.harness_cpu),
    )
    assert all(percentage >= 0 for percentage in percentages)
    assert sum(percentages) == 100.0


def test_synthetic_records_match_existing_benchmark_shape() -> None:
    assert _synthetic_records(2) == [
        {
            "id": 1,
            "name": "user-00000001",
            "email": "user-00000001@example.com",
            "score": 1,
        },
        {
            "id": 2,
            "name": "user-00000002",
            "email": "user-00000002@example.com",
            "score": 2,
        },
    ]


def test_postgres_profile_applies_scenario_batch_size_as_fetch_size(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_fetch_sizes: list[int] = []

    class FakePostgresSource:
        def test_connection(self, profile: PostgresProfile) -> bool:
            observed_fetch_sizes.append(profile.fetch_size)
            return True

    def consume(
        _source: FakePostgresSource,
        profile: PostgresProfile,
        row_count: int,
    ) -> int:
        observed_fetch_sizes.append(profile.fetch_size)
        return row_count

    extract_key = ("/workspace/drt/sources/postgres.py", 80, "extract")
    setup_key = ("/workspace/drt/sources/postgres.py", 120, "_connect_and_execute")
    stats = cast(
        _ProfileStats,
        SimpleNamespace(
            stats={
                extract_key: (1, 1, 0.1, 0.8, {}),
                setup_key: (1, 1, 0.1, 0.3, {}),
            },
            total_tt=1.0,
            total_calls=2,
            prim_calls=2,
        ),
    )
    monkeypatch.setattr(real_io_profiling, "PostgresSource", FakePostgresSource)
    monkeypatch.setattr(real_io_profiling, "_consume_postgres_source", consume)
    monkeypatch.setattr(real_io_profiling, "_profile_stats", lambda *_args: stats)
    profile = PostgresProfile(type="postgres", fetch_size=10_000)
    scenario = BenchmarkScenario(name="custom", row_count=12, batch_size=7)

    result = real_io_profiling.profile_postgres_scenario(
        scenario,
        profile,
        git_commit="a" * 40,
    )

    assert observed_fetch_sizes == [7, 7]
    assert profile.fetch_size == 10_000
    assert result.batch_size == 7


def _valid_result() -> RealIOProfileResult:
    return RealIOProfileResult(
        schema_version=1,
        profiler="cProfile",
        leg="rest_destination",
        scenario="test",
        row_count=1,
        batch_size=1,
        request_count=1,
        controlled_latency_ms=10,
        git_commit="a" * 40,
        timestamp="2026-08-23T00:00:00Z",
        measurements=RealIOProfileMeasurements(
            duration_seconds=1.0,
            function_calls=1,
            primitive_calls=1,
            buckets={
                "known_network_wait": ProfileBucket(0.5, 50.0, "io_bound"),
                "load_overhead": ProfileBucket(0.5, 50.0, "mixed_io_cpu"),
            },
            components={"rest_destination_load_seconds": 1.0},
        ),
    )


@pytest.mark.parametrize(
    "bad_bucket",
    [
        ProfileBucket(-0.01, 50.0, "io_bound"),
        ProfileBucket(0.5, 100.01, "io_bound"),
    ],
)
def test_write_result_rejects_schema_invalid_measurements_before_creating_directory(
    tmp_path: Path,
    bad_bucket: ProfileBucket,
) -> None:
    valid = _valid_result()
    result = replace(
        valid,
        measurements=replace(
            valid.measurements,
            buckets={**valid.measurements.buckets, "known_network_wait": bad_bucket},
        ),
    )
    profiles_dir = tmp_path / "profiles"

    with pytest.raises(RuntimeError, match="schema-invalid real-I/O profile artifact"):
        _write_result(result, profiles_dir)

    assert not profiles_dir.exists()
