#!/usr/bin/env python3
"""Run drt's reproducible local sync-performance benchmarks (#280)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from benchmarks.harness import SCENARIOS, BenchmarkScenario, run_benchmarks  # noqa: E402


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario",
        action="append",
        choices=[scenario.name for scenario in SCENARIOS],
        help="Scenario to run; repeat to select several (default: all).",
    )
    parser.add_argument(
        "--results-dir",
        type=Path,
        default=_REPO_ROOT / "benchmarks" / "results",
        help="Result directory (default: benchmarks/results).",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    selected_names = set(args.scenario or [])
    scenarios: tuple[BenchmarkScenario, ...] = tuple(
        scenario for scenario in SCENARIOS if not selected_names or scenario.name in selected_names
    )
    artifacts = run_benchmarks(
        scenarios,
        args.results_dir,
        repo_root=_REPO_ROOT,
    )
    for artifact in artifacts:
        measurement = artifact.result.measurements
        print(
            f"{artifact.result.scenario}: {measurement.duration_seconds:.6f}s, "
            f"{measurement.rows_per_second:.2f} rows/s, "
            f"{measurement.peak_memory_bytes} peak bytes, "
            f"{measurement.destination_call_count} destination calls"
        )
        print(f"  {artifact.path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
