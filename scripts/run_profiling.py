#!/usr/bin/env python3
"""Profile drt's reproducible local sync-performance scenarios (#301)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from benchmarks.harness import SCENARIOS, BenchmarkScenario  # noqa: E402
from benchmarks.profile_scenarios import run_profiles  # noqa: E402


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        action="append",
        choices=[scenario.name for scenario in SCENARIOS],
        help="Scenario to run; repeat to select several (default: all).",
    )
    parser.add_argument(
        "--profiles-dir",
        type=Path,
        default=_REPO_ROOT / "benchmarks" / "profiles",
        help="Profile artifact directory (default: benchmarks/profiles).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    selected_names = set(args.scenario or [])
    scenarios: tuple[BenchmarkScenario, ...] = tuple(
        scenario for scenario in SCENARIOS if not selected_names or scenario.name in selected_names
    )
    artifacts = run_profiles(
        scenarios,
        args.profiles_dir,
        repo_root=_REPO_ROOT,
    )
    for artifact in artifacts:
        result = artifact.result
        buckets = result.measurements.buckets
        print(f"{result.scenario}: {result.measurements.duration_seconds:.6f}s cProfile wall time")
        print(
            "  SQLite extraction (I/O-bound): "
            f"{buckets.source_extraction.seconds:.6f}s "
            f"({buckets.source_extraction.percentage:.2f}%)"
        )
        print(
            "  transformation/serialization (CPU-bound): "
            f"{buckets.transformation_serialization.seconds:.6f}s "
            f"({buckets.transformation_serialization.percentage:.2f}%)"
        )
        print(
            "  destination file I/O (I/O-bound): "
            f"{buckets.destination_io.seconds:.6f}s "
            f"({buckets.destination_io.percentage:.2f}%)"
        )
        print(f"  {artifact.path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
