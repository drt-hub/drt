#!/usr/bin/env python3
"""Profile real local Postgres and controlled-latency REST I/O (#1008)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from benchmarks.harness import SCENARIOS, BenchmarkScenario  # noqa: E402
from benchmarks.profile_real_io import (  # noqa: E402
    DEFAULT_LATENCIES_MS,
    RealIOProfileArtifact,
    run_real_io_profiles,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--leg",
        action="append",
        choices=("postgres", "rest"),
        help="Experiment leg to run; repeat to select both (default: both).",
    )
    parser.add_argument(
        "--scenario",
        action="append",
        choices=[scenario.name for scenario in SCENARIOS],
        help="Scenario to run; repeat to select several (default: all).",
    )
    parser.add_argument(
        "--latency-ms",
        action="append",
        type=int,
        help="Controlled REST response delay; repeat as needed (default: 10, 50, 200).",
    )
    parser.add_argument(
        "--profiles-dir",
        type=Path,
        default=_REPO_ROOT / "benchmarks" / "profiles",
        help="Profile artifact directory (default: benchmarks/profiles).",
    )
    return parser


def _print_artifact(artifact: RealIOProfileArtifact) -> None:
    result = artifact.result
    latency = (
        f", controlled latency {result.controlled_latency_ms} ms"
        if result.controlled_latency_ms is not None
        else ""
    )
    print(
        f"{result.leg}/{result.scenario}{latency}: "
        f"{result.measurements.duration_seconds:.6f}s cProfile wall time"
    )
    for name, bucket in result.measurements.buckets.items():
        print(
            f"  {name} ({bucket.classification}): "
            f"{bucket.seconds:.6f}s ({bucket.percentage:.2f}%)"
        )
    print(f"  {artifact.path}")


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    selected_names = set(args.scenario or [])
    scenarios: tuple[BenchmarkScenario, ...] = tuple(
        scenario for scenario in SCENARIOS if not selected_names or scenario.name in selected_names
    )
    latencies_ms = tuple(args.latency_ms or DEFAULT_LATENCIES_MS)
    if any(latency < 0 for latency in latencies_ms):
        parser.error("--latency-ms cannot be negative")
    legs = frozenset(args.leg or ("postgres", "rest"))

    artifacts = run_real_io_profiles(
        scenarios,
        latencies_ms,
        legs=legs,
        profiles_dir=args.profiles_dir,
        repo_root=_REPO_ROOT,
    )
    for artifact in artifacts:
        _print_artifact(artifact)
    return 0


if __name__ == "__main__":
    sys.exit(main())
