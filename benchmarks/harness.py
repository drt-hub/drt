"""Reusable local sync-performance scenarios and measurement helpers (#280).

``execute_scenario`` is deliberately separate from the measurement and JSON
layers. Issue #301 can wrap that function with cProfile, py-spy, or another
profiler without reproducing source, destination, and sync setup.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
import tempfile
import time
import tracemalloc
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, cast

from drt.config.credentials import SQLiteProfile
from drt.config.models import (
    DestinationConfig,
    FileDestinationConfig,
    SyncConfig,
    SyncOptions,
)
from drt.destinations.base import SyncResult
from drt.engine.sync import run_sync
from drt.observability import otel
from drt.sources.sqlite import SQLiteSource

SCHEMA_VERSION = 1
_SCENARIO_NAME = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_FULL_GIT_HASH = re.compile(r"^[0-9a-f]{40}$")
_BENCHMARK_NOOP_TRACER = otel._FallbackNoOpTracer()
_BENCHMARK_NOOP_METER = otel._FallbackNoOpMeter()


@dataclass(frozen=True)
class BenchmarkScenario:
    """One deterministic synthetic sync workload."""

    name: str
    row_count: int
    batch_size: int = 100

    def __post_init__(self) -> None:
        if not _SCENARIO_NAME.fullmatch(self.name):
            raise ValueError(
                "scenario name must contain only lowercase letters, digits, underscores, "
                "and hyphens"
            )
        if self.row_count <= 0:
            raise ValueError("scenario row_count must be positive")
        if self.batch_size <= 0:
            raise ValueError("scenario batch_size must be positive")


SCENARIOS: tuple[BenchmarkScenario, ...] = (
    BenchmarkScenario(name="small", row_count=100),
    BenchmarkScenario(name="medium", row_count=10_000),
    BenchmarkScenario(name="large", row_count=100_000),
)


@dataclass(frozen=True)
class ScenarioOutcome:
    """Functional outcome returned by the unmeasured scenario runner."""

    rows_extracted: int
    rows_synced: int
    destination_call_count: int


@dataclass(frozen=True)
class BenchmarkMeasurements:
    """The four measurements required by the result schema."""

    duration_seconds: float
    rows_per_second: float
    peak_memory_bytes: int
    destination_call_count: int


@dataclass(frozen=True)
class BenchmarkResult:
    """Serializable result for one scenario execution."""

    schema_version: int
    scenario: str
    row_count: int
    git_commit: str
    timestamp: str
    measurements: BenchmarkMeasurements

    def to_dict(self) -> dict[str, Any]:
        """Return the stable JSON-compatible representation."""
        return asdict(self)


@dataclass(frozen=True)
class BenchmarkArtifact:
    """A measured result and the JSON path where it was persisted."""

    result: BenchmarkResult
    path: Path


class CountingFileDestination:
    """Persist JSONL batches and count load calls as an API-call proxy."""

    def __init__(self) -> None:
        self.call_count = 0
        self._has_written = False

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, FileDestinationConfig)
        self.call_count += 1
        if not records:
            return SyncResult()
        if config.format != "jsonl":
            raise ValueError("benchmark destination only supports JSONL output")

        result = SyncResult()
        try:
            os.makedirs(os.path.dirname(config.path) or ".", exist_ok=True)
            mode = "a" if self._has_written else "w"
            with open(config.path, mode, encoding="utf-8") as output:
                for record in records:
                    output.write(json.dumps(record, default=str) + "\n")
            self._has_written = True
            result.success = len(records)
        except Exception as exc:
            result.failed = len(records)
            result.errors.append(str(exc))
        return result


def _force_noop_telemetry() -> None:
    """Pin this process to the fallback no-op providers for benchmark runs."""
    if (
        otel._STATE.initialized
        and cast(object, otel._STATE.tracer) is _BENCHMARK_NOOP_TRACER
        and cast(object, otel._STATE.meter) is _BENCHMARK_NOOP_METER
    ):
        return

    otel.shutdown_telemetry()
    otel._STATE.initialized = True
    otel._STATE.tracer = cast(Any, _BENCHMARK_NOOP_TRACER)
    otel._STATE.meter = cast(Any, _BENCHMARK_NOOP_METER)
    otel._STATE.warned = False
    otel._STATE.trace_provider = None
    otel._STATE.meter_provider = None
    # run_sync() builds a final span status even on the fallback path. Resolve
    # that optional API module before timing so the first scenario does not pay
    # a one-time import cost that later scenarios avoid.
    otel.build_status(ok=True)


def _verify_persisted_row_count(
    destination_path: Path,
    *,
    scenario_name: str,
    expected_rows: int,
) -> None:
    try:
        with destination_path.open(encoding="utf-8") as persisted:
            actual_rows = sum(1 for _line in persisted)
    except OSError as exc:
        raise RuntimeError(
            f"benchmark scenario {scenario_name!r} could not verify persisted JSONL: {exc}"
        ) from exc

    if actual_rows != expected_rows:
        raise RuntimeError(
            f"benchmark scenario {scenario_name!r} persisted {actual_rows} JSONL rows; "
            f"expected {expected_rows}"
        )


def synthetic_query(row_count: int) -> str:
    """Build a deterministic SQLite query that streams ``row_count`` rows."""
    if row_count <= 0:
        raise ValueError("row_count must be positive")
    return f"""WITH RECURSIVE synthetic(id) AS (
    SELECT 1
    UNION ALL
    SELECT id + 1 FROM synthetic WHERE id < {row_count}
)
SELECT
    id,
    printf('user-%08d', id) AS name,
    printf('user-%08d@example.com', id) AS email,
    id % 1000 AS score
FROM synthetic"""


def execute_scenario(scenario: BenchmarkScenario, work_dir: Path) -> ScenarioOutcome:
    """Run one workload through the real source → engine → destination path.

    This function intentionally performs no timing, memory measurement, result
    serialization, or git inspection. Profiling tools can call it directly.
    """
    _force_noop_telemetry()
    work_dir.mkdir(parents=True, exist_ok=True)
    destination_path = work_dir / f"{scenario.name}.jsonl"
    sync = SyncConfig.model_validate(
        {
            "name": f"benchmark_{scenario.name}",
            "model": synthetic_query(scenario.row_count),
            "destination": {
                "type": "file",
                "path": str(destination_path),
                "format": "jsonl",
            },
            "sync": {"batch_size": scenario.batch_size, "on_error": "fail"},
        }
    )
    profile = SQLiteProfile(type="sqlite", database=":memory:")
    destination = CountingFileDestination()

    result = run_sync(
        sync=sync,
        source=SQLiteSource(),
        destination=destination,
        profile=profile,
        project_dir=work_dir,
    )
    if (
        result.rows_extracted != scenario.row_count
        or result.success != scenario.row_count
        or result.failed != 0
    ):
        raise RuntimeError(
            f"benchmark scenario {scenario.name!r} did not complete: "
            f"extracted={result.rows_extracted}, success={result.success}, "
            f"failed={result.failed}, errors={result.errors}"
        )
    return ScenarioOutcome(
        rows_extracted=result.rows_extracted,
        rows_synced=result.success,
        destination_call_count=destination.call_count,
    )


def measure_scenario(
    scenario: BenchmarkScenario,
    work_dir: Path,
    *,
    git_commit: str,
    timestamp: datetime | None = None,
) -> BenchmarkResult:
    """Measure one scenario with a monotonic clock and ``tracemalloc``."""
    _force_noop_telemetry()
    if tracemalloc.is_tracing():
        raise RuntimeError("tracemalloc is already active; cannot isolate benchmark peak memory")
    if not (_FULL_GIT_HASH.fullmatch(git_commit) or git_commit == "unknown"):
        raise ValueError("git_commit must be a full 40-character lowercase hash or 'unknown'")

    started_at = timestamp or datetime.now(timezone.utc)
    if started_at.tzinfo is None:
        raise ValueError("benchmark timestamp must be timezone-aware")

    tracemalloc.start()
    try:
        started = time.perf_counter()
        outcome = execute_scenario(scenario, work_dir)
        duration_seconds = time.perf_counter() - started
        _, peak_memory_bytes = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    _verify_persisted_row_count(
        work_dir / f"{scenario.name}.jsonl",
        scenario_name=scenario.name,
        expected_rows=scenario.row_count,
    )

    return BenchmarkResult(
        schema_version=SCHEMA_VERSION,
        scenario=scenario.name,
        row_count=scenario.row_count,
        git_commit=git_commit,
        timestamp=_format_timestamp(started_at),
        measurements=BenchmarkMeasurements(
            duration_seconds=round(duration_seconds, 6),
            rows_per_second=round(outcome.rows_extracted / duration_seconds, 2),
            peak_memory_bytes=peak_memory_bytes,
            destination_call_count=outcome.destination_call_count,
        ),
    )


def get_git_commit(repo_root: Path) -> str:
    """Return the checked-out commit, or ``unknown`` outside a git checkout."""
    try:
        commit = subprocess.check_output(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            stderr=subprocess.DEVNULL,
            text=True,
        ).strip()
    except (FileNotFoundError, subprocess.CalledProcessError):
        return "unknown"
    return commit if _FULL_GIT_HASH.fullmatch(commit) else "unknown"


def write_result(result: BenchmarkResult, results_dir: Path) -> Path:
    """Write one result JSON under ``results_dir`` and return its path."""
    results_dir.mkdir(parents=True, exist_ok=True)
    timestamp_slug = result.timestamp.replace("-", "").replace(":", "").replace(".", "")
    path = results_dir / (
        f"{timestamp_slug}-{result.scenario}-{result.git_commit[:8]}.json"
    )
    path.write_text(json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n")
    return path


def run_benchmarks(
    scenarios: tuple[BenchmarkScenario, ...],
    results_dir: Path,
    *,
    repo_root: Path,
    git_commit: str | None = None,
) -> list[BenchmarkArtifact]:
    """Measure and persist each selected scenario as its own JSON artifact."""
    _force_noop_telemetry()
    commit = git_commit or get_git_commit(repo_root)
    artifacts: list[BenchmarkArtifact] = []
    with tempfile.TemporaryDirectory(prefix="drt-benchmark-") as temporary_dir:
        work_root = Path(temporary_dir)
        for scenario in scenarios:
            result = measure_scenario(
                scenario,
                work_root / scenario.name,
                git_commit=commit,
            )
            artifacts.append(
                BenchmarkArtifact(result=result, path=write_result(result, results_dir))
            )
    return artifacts


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
