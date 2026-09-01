"""Functional tests for cProfile attribution of benchmark scenarios (#301)."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from jsonschema import validate

from benchmarks.harness import SCENARIOS, BenchmarkScenario
from benchmarks.profile_scenarios import (
    _destination_file_io_seconds,
    _ProfileStats,
    profile_scenario,
    run_profiles,
)

_COMMIT = "a" * 40
_SCHEMA_PATH = Path(__file__).parent.parent.parent / "benchmarks" / "profile-result-schema.json"


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda scenario: scenario.name)
def test_standard_scenario_produces_complete_profile(
    scenario: BenchmarkScenario,
    tmp_path: Path,
) -> None:
    result = profile_scenario(
        scenario,
        tmp_path / scenario.name,
        git_commit=_COMMIT,
    )

    schema = json.loads(_SCHEMA_PATH.read_text())
    validate(instance=result.to_dict(), schema=schema)
    assert result.scenario == scenario.name
    assert result.row_count == scenario.row_count
    assert result.profiler == "cProfile"
    assert result.measurements.function_calls >= result.measurements.primitive_calls
    buckets = result.measurements.buckets
    assert buckets.source_extraction.classification == "cpu_bound"
    assert buckets.transformation_serialization.classification == "cpu_bound"
    assert buckets.destination_io.classification == "io_bound"
    assert (
        round(
            buckets.source_extraction.percentage
            + buckets.transformation_serialization.percentage
            + buckets.destination_io.percentage,
            2,
        )
        == 100.0
    )


def test_destination_file_io_includes_complete_makedirs_subtree_once() -> None:
    load_key = ("/benchmarks/harness.py", 118, "load")
    makedirs_key = ("<frozen os>", 200, "makedirs")
    stat_key = ("~", 0, "<built-in method posix.stat>")
    open_key = ("~", 0, "<built-in method _io.open>")
    stats = cast(
        _ProfileStats,
        SimpleNamespace(
            stats={
                load_key: (1, 1, 0.0, 1.0, {}),
                makedirs_key: (1, 1, 0.01, 0.5, {load_key: (1, 1, 0.01, 0.5)}),
                stat_key: (1, 1, 0.4, 0.4, {makedirs_key: (1, 1, 0.4, 0.4)}),
                open_key: (1, 1, 0.1, 0.2, {load_key: (1, 1, 0.1, 0.2)}),
            },
            total_tt=1.0,
            total_calls=4,
            prim_calls=4,
        ),
    )

    assert _destination_file_io_seconds(stats, load_key) == pytest.approx(0.6)


def test_written_profile_matches_schema(tmp_path: Path) -> None:
    scenario = BenchmarkScenario(name="test", row_count=7, batch_size=3)

    artifacts = run_profiles(
        (scenario,),
        tmp_path / "profiles",
        repo_root=tmp_path,
        git_commit=_COMMIT,
    )

    assert len(artifacts) == 1
    artifact = artifacts[0]
    payload = json.loads(artifact.path.read_text())
    schema = json.loads(_SCHEMA_PATH.read_text())
    validate(instance=payload, schema=schema)
    assert payload == artifact.result.to_dict()
    assert artifact.path.parent == tmp_path / "profiles"


def test_profile_timestamp_must_be_timezone_aware(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="profile timestamp must be timezone-aware"):
        profile_scenario(
            BenchmarkScenario(name="test", row_count=1),
            tmp_path,
            git_commit=_COMMIT,
            timestamp=datetime(2026, 8, 22),
        )

    result = profile_scenario(
        BenchmarkScenario(name="timestamp", row_count=1),
        tmp_path / "timestamp",
        git_commit=_COMMIT,
        timestamp=datetime(2026, 8, 22, 12, 34, 56, tzinfo=timezone.utc),
    )
    assert result.timestamp == "2026-08-22T12:34:56Z"
