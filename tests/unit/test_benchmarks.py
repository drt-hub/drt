"""Functional tests for the local sync-performance benchmark harness (#280)."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pytest
from jsonschema import validate

from benchmarks.harness import (
    SCENARIOS,
    BenchmarkScenario,
    execute_scenario,
    measure_scenario,
    run_benchmarks,
)

_COMMIT = "a" * 40
_SCHEMA_PATH = Path(__file__).parent.parent.parent / "benchmarks" / "result-schema.json"


def test_standard_scenarios_have_required_sizes() -> None:
    assert [(scenario.name, scenario.row_count) for scenario in SCENARIOS] == [
        ("small", 100),
        ("medium", 10_000),
        ("large", 100_000),
    ]


@pytest.mark.parametrize("scenario", SCENARIOS, ids=lambda scenario: scenario.name)
def test_standard_scenario_completes(scenario: BenchmarkScenario, tmp_path: Path) -> None:
    outcome = execute_scenario(scenario, tmp_path / scenario.name)

    assert outcome.rows_extracted == scenario.row_count
    assert outcome.rows_synced == scenario.row_count
    assert outcome.destination_call_count == math.ceil(scenario.row_count / scenario.batch_size)


def test_measurement_and_written_json_match_schema(tmp_path: Path) -> None:
    scenario = BenchmarkScenario(name="test", row_count=7, batch_size=3)
    timestamp = datetime(2026, 8, 21, 12, 34, 56, tzinfo=timezone.utc)

    artifacts = run_benchmarks(
        (scenario,),
        tmp_path / "results",
        repo_root=tmp_path,
        git_commit=_COMMIT,
    )

    assert len(artifacts) == 1
    artifact = artifacts[0]
    payload = json.loads(artifact.path.read_text())
    schema = json.loads(_SCHEMA_PATH.read_text())
    validate(instance=payload, schema=schema)
    assert payload == artifact.result.to_dict()
    assert artifact.path.parent == tmp_path / "results"
    assert artifact.result.measurements.duration_seconds > 0
    assert artifact.result.measurements.rows_per_second > 0
    assert artifact.result.measurements.peak_memory_bytes >= 0

    measured = measure_scenario(
        scenario,
        tmp_path / "measured",
        git_commit=_COMMIT,
        timestamp=timestamp,
    )
    assert measured.timestamp == "2026-08-21T12:34:56Z"
    validate(instance=measured.to_dict(), schema=schema)
