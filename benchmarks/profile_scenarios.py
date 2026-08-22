"""cProfile attribution for the reproducible sync scenarios (#301).

The profiler deliberately wraps :func:`benchmarks.harness.execute_scenario`
instead of rebuilding its source, destination, or sync configuration.  Bucket
times are non-overlapping:

* source extraction is cumulative time below ``SQLiteSource.extract``;
* destination I/O is self time in the actual ``_io`` open/write/close calls
  made directly by ``CountingFileDestination.load``;
* transformation/serialization is the remaining profiled wall time, including
  engine record handling, JSON encoding, and fixed scenario setup.

That attribution makes the three percentages sum to 100% while retaining the
call-graph boundary needed to distinguish JSON CPU from file I/O inside the
same destination ``load`` call.
"""

from __future__ import annotations

import cProfile
import json
import pstats
import re
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol, cast

from benchmarks.harness import (
    BenchmarkScenario,
    _force_noop_telemetry,
    execute_scenario,
    get_git_commit,
)

SCHEMA_VERSION = 1
_FULL_GIT_HASH = re.compile(r"^[0-9a-f]{40}$")
_FunctionKey = tuple[str, int, str]
_CallerStats = tuple[int, int, float, float]
_FunctionStats = tuple[int, int, float, float, dict[_FunctionKey, _CallerStats]]


class _ProfileStats(Protocol):
    """Runtime attributes populated by ``pstats.Stats`` but absent from typeshed."""

    stats: dict[_FunctionKey, _FunctionStats]
    total_tt: float
    total_calls: int
    prim_calls: int


@dataclass(frozen=True)
class ProfileBucket:
    """One non-overlapping portion of profiled wall time."""

    seconds: float
    percentage: float
    classification: str


@dataclass(frozen=True)
class ProfileBuckets:
    """The three issue-defined attribution buckets."""

    source_extraction: ProfileBucket
    transformation_serialization: ProfileBucket
    destination_io: ProfileBucket


@dataclass(frozen=True)
class ProfileComponents:
    """Inclusive call-tree timings that explain important bucket internals."""

    sqlite_extraction_seconds: float
    json_serialization_seconds: float
    destination_load_seconds: float


@dataclass(frozen=True)
class ProfileMeasurements:
    """Structured measurements from one cProfile run."""

    duration_seconds: float
    function_calls: int
    primitive_calls: int
    buckets: ProfileBuckets
    components: ProfileComponents


@dataclass(frozen=True)
class ProfileResult:
    """Serializable profiling result for one benchmark scenario."""

    schema_version: int
    profiler: str
    scenario: str
    row_count: int
    git_commit: str
    timestamp: str
    measurements: ProfileMeasurements

    def to_dict(self) -> dict[str, Any]:
        """Return the stable JSON-compatible representation."""
        return asdict(self)


@dataclass(frozen=True)
class ProfileArtifact:
    """A profiling result and the JSON path where it was persisted."""

    result: ProfileResult
    path: Path


def _find_function(
    stats: _ProfileStats,
    *,
    filename_suffix: str,
    function_name: str,
) -> _FunctionKey:
    matches = [
        key
        for key in stats.stats
        if key[0].replace("\\", "/").endswith(filename_suffix)
        and key[2] == function_name
    ]
    if len(matches) != 1:
        raise RuntimeError(
            f"cProfile expected one {filename_suffix}:{function_name} entry; "
            f"found {len(matches)}"
        )
    return matches[0]


def _direct_file_io_seconds(stats: _ProfileStats, load_key: _FunctionKey) -> float:
    """Return file-I/O self time attributed directly to benchmark ``load``."""
    io_functions = {
        "<built-in method _io.open>",
        "<method 'write' of '_io.TextIOWrapper' objects>",
        "<method '__exit__' of '_io._IOBase' objects>",
    }
    seconds = 0.0
    for key, (_cc, _nc, _self_time, _cumulative_time, callers) in stats.stats.items():
        if key[2] not in io_functions or load_key not in callers:
            continue
        # Caller tuples are (primitive calls, total calls, self, cumulative).
        seconds += callers[load_key][2]
    return seconds


def _cumulative_seconds(
    stats: _ProfileStats,
    *,
    filename_suffix: str,
    function_name: str,
) -> float:
    key = _find_function(
        stats,
        filename_suffix=filename_suffix,
        function_name=function_name,
    )
    return stats.stats[key][3]


def _bucket(
    seconds: float,
    duration_seconds: float,
    classification: str,
    *,
    percentage: float | None = None,
) -> ProfileBucket:
    computed_percentage = (
        percentage if percentage is not None else (seconds / duration_seconds * 100)
    )
    return ProfileBucket(
        seconds=round(seconds, 6),
        percentage=round(computed_percentage, 2),
        classification=classification,
    )


def _format_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _verify_persisted_rows(path: Path, expected_rows: int, scenario_name: str) -> None:
    try:
        with path.open(encoding="utf-8") as persisted:
            actual_rows = sum(1 for _line in persisted)
    except OSError as exc:
        raise RuntimeError(
            f"profile scenario {scenario_name!r} could not verify persisted JSONL: {exc}"
        ) from exc
    if actual_rows != expected_rows:
        raise RuntimeError(
            f"profile scenario {scenario_name!r} persisted {actual_rows} JSONL rows; "
            f"expected {expected_rows}"
        )


def profile_scenario(
    scenario: BenchmarkScenario,
    work_dir: Path,
    *,
    git_commit: str,
    timestamp: datetime | None = None,
) -> ProfileResult:
    """Profile one existing benchmark scenario and attribute its call graph."""
    if not (_FULL_GIT_HASH.fullmatch(git_commit) or git_commit == "unknown"):
        raise ValueError("git_commit must be a full 40-character lowercase hash or 'unknown'")
    started_at = timestamp or datetime.now(timezone.utc)
    if started_at.tzinfo is None:
        raise ValueError("profile timestamp must be timezone-aware")

    # Match measure_scenario(): resolve optional telemetry modules before the
    # measured call, while execute_scenario() still enforces the no-op state.
    _force_noop_telemetry()
    profiler = cProfile.Profile()
    profiler.runcall(execute_scenario, scenario, work_dir)
    stats = cast(_ProfileStats, pstats.Stats(profiler))
    duration_seconds = stats.total_tt
    if duration_seconds <= 0:
        raise RuntimeError(f"profile scenario {scenario.name!r} recorded no elapsed time")

    _verify_persisted_rows(
        work_dir / f"{scenario.name}.jsonl",
        scenario.row_count,
        scenario.name,
    )

    extraction_seconds = _cumulative_seconds(
        stats,
        filename_suffix="/drt/sources/sqlite.py",
        function_name="extract",
    )
    load_key = _find_function(
        stats,
        filename_suffix="/benchmarks/harness.py",
        function_name="load",
    )
    destination_io_seconds = _direct_file_io_seconds(stats, load_key)
    transformation_seconds = max(
        0.0,
        duration_seconds - extraction_seconds - destination_io_seconds,
    )

    extraction_percentage = round(extraction_seconds / duration_seconds * 100, 2)
    destination_io_percentage = round(destination_io_seconds / duration_seconds * 100, 2)
    transformation_percentage = round(
        100.0 - extraction_percentage - destination_io_percentage,
        2,
    )
    json_seconds = _cumulative_seconds(
        stats,
        filename_suffix="/json/__init__.py",
        function_name="dumps",
    )

    return ProfileResult(
        schema_version=SCHEMA_VERSION,
        profiler="cProfile",
        scenario=scenario.name,
        row_count=scenario.row_count,
        git_commit=git_commit,
        timestamp=_format_timestamp(started_at),
        measurements=ProfileMeasurements(
            duration_seconds=round(duration_seconds, 6),
            function_calls=stats.total_calls,
            primitive_calls=stats.prim_calls,
            buckets=ProfileBuckets(
                source_extraction=_bucket(
                    extraction_seconds,
                    duration_seconds,
                    "io_bound",
                    percentage=extraction_percentage,
                ),
                transformation_serialization=_bucket(
                    transformation_seconds,
                    duration_seconds,
                    "cpu_bound",
                    percentage=transformation_percentage,
                ),
                destination_io=_bucket(
                    destination_io_seconds,
                    duration_seconds,
                    "io_bound",
                    percentage=destination_io_percentage,
                ),
            ),
            components=ProfileComponents(
                sqlite_extraction_seconds=round(extraction_seconds, 6),
                json_serialization_seconds=round(json_seconds, 6),
                destination_load_seconds=round(stats.stats[load_key][3], 6),
            ),
        ),
    )


def write_profile_result(result: ProfileResult, profiles_dir: Path) -> Path:
    """Write one profile JSON under ``profiles_dir`` and return its path."""
    profiles_dir.mkdir(parents=True, exist_ok=True)
    timestamp_slug = result.timestamp.replace("-", "").replace(":", "").replace(".", "")
    path = profiles_dir / (
        f"{timestamp_slug}-{result.scenario}-{result.git_commit[:8]}.json"
    )
    path.write_text(json.dumps(result.to_dict(), indent=2, sort_keys=True) + "\n")
    return path


def run_profiles(
    scenarios: tuple[BenchmarkScenario, ...],
    profiles_dir: Path,
    *,
    repo_root: Path,
    git_commit: str | None = None,
) -> list[ProfileArtifact]:
    """Profile and persist each selected existing benchmark scenario."""
    _force_noop_telemetry()
    commit = git_commit or get_git_commit(repo_root)
    artifacts: list[ProfileArtifact] = []
    with tempfile.TemporaryDirectory(prefix="drt-profile-") as temporary_dir:
        work_root = Path(temporary_dir)
        for scenario in scenarios:
            result = profile_scenario(
                scenario,
                work_root / scenario.name,
                git_commit=commit,
            )
            artifacts.append(
                ProfileArtifact(
                    result=result,
                    path=write_profile_result(result, profiles_dir),
                )
            )
    return artifacts
