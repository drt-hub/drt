#!/usr/bin/env python3
"""Measure drt fresh-process startup and import costs reproducibly (#1017).

Every sample starts a new interpreter with ``-X importtime``.  The first
sample is reported separately from the median of later samples, but this
script does not flush operating-system filesystem caches; see ``--help``.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import re
import statistics
import subprocess
import sys
import time
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

_REPO_ROOT = Path(__file__).resolve().parent.parent
_IMPORT_LINE = re.compile(
    r"^import time:\s+(?P<self>\d+)\s+\|\s+(?P<cumulative>\d+)\s+\|\s+(?P<module>.+?)\s*$"
)
_CLI_CODE = "from drt.cli.main import app; app()"


@dataclass(frozen=True)
class Scenario:
    name: str
    args: tuple[str, ...]
    cwd: Path
    root_module: str


@dataclass(frozen=True)
class ImportSample:
    wall_ms: float
    root_import_ms: float
    imports: dict[str, tuple[float, float]]


def _scenarios() -> tuple[Scenario, ...]:
    return (
        Scenario("import_drt", ("-c", "import drt"), _REPO_ROOT, "drt"),
        Scenario("cli_help", ("-c", _CLI_CODE, "--help"), _REPO_ROOT, "drt.cli.main"),
        Scenario(
            "validate_example",
            ("-c", _CLI_CODE, "validate"),
            _REPO_ROOT / "examples" / "review-demo",
            "drt.cli.main",
        ),
    )


def _parse_import_trace(stderr: str) -> dict[str, tuple[float, float]]:
    imports: dict[str, tuple[float, float]] = {}
    for line in stderr.splitlines():
        match = _IMPORT_LINE.match(line)
        if match is None:
            continue
        module = match.group("module").strip()
        imports[module] = (
            int(match.group("self")) / 1_000,
            int(match.group("cumulative")) / 1_000,
        )
    return imports


def _environment() -> dict[str, str]:
    return {
        "python": platform.python_version(),
        "implementation": platform.python_implementation(),
        "executable": sys.executable,
        "platform": platform.platform(),
        "machine": platform.machine(),
    }


def _run_sample(scenario: Scenario) -> ImportSample:
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH")
    env["PYTHONPATH"] = (
        f"{_REPO_ROOT}{os.pathsep}{existing_pythonpath}" if existing_pythonpath else str(_REPO_ROOT)
    )
    command = [sys.executable, "-X", "importtime", *scenario.args]
    started = time.perf_counter_ns()
    completed = subprocess.run(
        command,
        cwd=scenario.cwd,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        check=False,
    )
    wall_ms = (time.perf_counter_ns() - started) / 1_000_000
    if completed.returncode != 0:
        raise RuntimeError(
            f"{scenario.name} exited with {completed.returncode}:\n{completed.stderr}"
        )
    imports = _parse_import_trace(completed.stderr)
    if scenario.root_module not in imports:
        raise RuntimeError(
            f"{scenario.name} trace did not contain root module {scenario.root_module!r}"
        )
    return ImportSample(
        wall_ms=wall_ms,
        root_import_ms=imports[scenario.root_module][1],
        imports=imports,
    )


def _percentile(values: Sequence[float], percentile: float) -> float:
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, round((len(ordered) - 1) * percentile)))
    return ordered[index]


def _median_contributors(
    samples: Sequence[ImportSample], index: int, limit: int
) -> list[tuple[str, float]]:
    modules = {module for sample in samples for module in sample.imports}
    medians = [
        (
            module,
            statistics.median(
                sample.imports.get(module, (0.0, 0.0))[index] for sample in samples
            ),
        )
        for module in modules
    ]
    return sorted(medians, key=lambda item: (-item[1], item[0]))[:limit]


def _summarize(samples: Sequence[ImportSample], top: int) -> dict[str, Any]:
    wall = [sample.wall_ms for sample in samples]
    root_import = [sample.root_import_ms for sample in samples]
    subsequent = wall[1:] or wall
    return {
        "first_wall_ms": wall[0],
        "subsequent_wall_median_ms": statistics.median(subsequent),
        "wall_median_ms": statistics.median(wall),
        "wall_p95_ms": _percentile(wall, 0.95),
        "wall_range_ms": [min(wall), max(wall)],
        "root_import_median_ms": statistics.median(root_import),
        "top_self_ms": _median_contributors(samples, 0, top),
        "top_cumulative_ms": _median_contributors(samples, 1, top),
        "samples": [asdict(sample) for sample in samples],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "'First' and 'subsequent' are fresh Python processes. The script does not flush OS "
            "filesystem caches, so the first sample is not a guaranteed cold-cache measurement."
        ),
    )
    parser.add_argument("--runs", type=int, default=10, help="Fresh processes per scenario.")
    parser.add_argument(
        "--scenario",
        action="append",
        choices=[scenario.name for scenario in _scenarios()],
        help="Scenario to run; repeat to select several (default: all).",
    )
    parser.add_argument("--top", type=int, default=10, help="Import contributors to print.")
    parser.add_argument("--json", type=Path, help="Optionally write all raw samples as JSON.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.runs < 2:
        raise SystemExit("--runs must be at least 2 to compare first and subsequent processes")
    if args.top < 1:
        raise SystemExit("--top must be at least 1")

    selected = set(args.scenario or ())
    scenarios = [scenario for scenario in _scenarios() if not selected or scenario.name in selected]
    payload: dict[str, Any] = {
        "environment": _environment(),
        "runs": args.runs,
        "cache_caveat": "fresh processes; OS filesystem caches are not flushed",
        "scenarios": {},
    }
    print(json.dumps(payload["environment"], indent=2))
    print(f"runs={args.runs}; {payload['cache_caveat']}")
    for scenario in scenarios:
        samples = [_run_sample(scenario) for _ in range(args.runs)]
        summary = _summarize(samples, args.top)
        payload["scenarios"][scenario.name] = summary
        low, high = summary["wall_range_ms"]
        print(f"\n{scenario.name}")
        print(
            f"  wall: first={summary['first_wall_ms']:.1f} ms, "
            f"subsequent median={summary['subsequent_wall_median_ms']:.1f} ms, "
            f"all median={summary['wall_median_ms']:.1f} ms, "
            f"p95={summary['wall_p95_ms']:.1f} ms, range={low:.1f}-{high:.1f} ms"
        )
        print(f"  root import median: {summary['root_import_median_ms']:.1f} ms")
        print("  top self imports (median ms):")
        for module, duration in summary["top_self_ms"]:
            print(f"    {duration:8.1f}  {module}")
        print("  top cumulative imports (median ms, inclusive and non-additive):")
        for module, duration in summary["top_cumulative_ms"]:
            print(f"    {duration:8.1f}  {module}")

    if args.json is not None:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        args.json.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        print(f"\nraw results: {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
