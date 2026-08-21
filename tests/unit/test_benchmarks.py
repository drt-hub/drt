"""Functional tests for the local sync-performance benchmark harness (#280)."""

from __future__ import annotations

import importlib
import json
import math
from datetime import datetime, timezone
from pathlib import Path

import pytest
from jsonschema import validate

from benchmarks import harness
from benchmarks.harness import (
    SCENARIOS,
    BenchmarkScenario,
    execute_scenario,
    measure_scenario,
    run_benchmarks,
)
from drt.config.models import DestinationConfig, FileDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.observability import otel

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
    work_dir = tmp_path / scenario.name
    outcome = execute_scenario(scenario, work_dir)

    assert outcome.rows_extracted == scenario.row_count
    assert outcome.rows_synced == scenario.row_count
    assert outcome.destination_call_count == math.ceil(scenario.row_count / scenario.batch_size)
    with (work_dir / f"{scenario.name}.jsonl").open() as persisted:
        assert sum(1 for _line in persisted) == scenario.row_count


def test_measurement_rejects_incomplete_persisted_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    def truncating_load(
        self: harness.CountingFileDestination,
        records: list[dict[str, object]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, FileDestinationConfig)
        self.call_count += 1
        with open(config.path, "w", encoding="utf-8") as output:
            for record in records:
                output.write(json.dumps(record) + "\n")
        return SyncResult(success=len(records))

    monkeypatch.setattr(harness.CountingFileDestination, "load", truncating_load)
    scenario = BenchmarkScenario(name="incomplete", row_count=7, batch_size=3)

    with pytest.raises(
        RuntimeError,
        match="persisted 1 JSONL rows; expected 7",
    ):
        measure_scenario(scenario, tmp_path, git_commit=_COMMIT)


def test_benchmark_forces_noop_telemetry_despite_environment(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://does-not-exist.invalid:4317")
    monkeypatch.setattr(otel, "_STATE", otel._ProviderState())
    monkeypatch.setattr(
        otel,
        "_load_observability_block",
        lambda _config_dir=None: pytest.fail("benchmark loaded operator observability config"),
    )
    real_import = importlib.import_module
    exporter_imports: list[str] = []

    def reject_exporter_import(name: str) -> object:
        if name.startswith(("opentelemetry.sdk", "opentelemetry.exporter")):
            exporter_imports.append(name)
            raise AssertionError(f"benchmark imported telemetry exporter module {name}")
        return real_import(name)

    monkeypatch.setattr(importlib, "import_module", reject_exporter_import)

    artifacts = run_benchmarks(
        (BenchmarkScenario(name="telemetry", row_count=3),),
        tmp_path / "results",
        repo_root=tmp_path,
        git_commit=_COMMIT,
    )

    assert len(artifacts) == 1
    assert artifacts[0].path.exists()
    assert exporter_imports == []
    assert otel._STATE.initialized is True
    assert otel._STATE.trace_provider is None
    assert otel._STATE.meter_provider is None


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
