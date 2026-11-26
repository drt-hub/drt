"""Fail-closed caller matrix for ADR 0021 slice 3."""

from __future__ import annotations

import ast
import inspect
from pathlib import Path

from drt.engine.sync import run_sync


def _engine_calls() -> dict[str, str | None]:
    calls: dict[str, str | None] = {}
    for root in (Path("drt"), Path("integrations")):
        for path in root.rglob("*.py"):
            tree = ast.parse(path.read_text())
            for node in ast.walk(tree):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
                    continue
                if node.func.id not in {"run_sync", "engine_run_sync"}:
                    continue
                value = next(
                    (
                        keyword.value
                        for keyword in node.keywords
                        if keyword.arg == "allow_backfill_execution"
                    ),
                    None,
                )
                calls[path.as_posix()] = ast.unparse(value) if value is not None else None
    return calls


def test_every_production_run_sync_caller_is_enumerated_and_fail_closed() -> None:
    """Adding an entry point requires making its slice-3 policy explicit here."""
    assert inspect.signature(run_sync).parameters["allow_backfill_execution"].default is False
    assert _engine_calls() == {
        # plan/apply preview: never executes windows
        "drt/cli/_plan_runner.py": None,
        # Airflow/Prefect and drt serve share this runner: refused
        "drt/integrations/_runner.py": None,
        # drt test's offline unit-test runner: refused if it ever sees backfill
        "drt/engine/unit_test_runner.py": None,
        # MCP is the one non-CLI execution entry point allowed in slice 3
        "drt/mcp/tools/run_sync.py": "True",
        # `_RunContext` selects CLI run vs build/apply below
        "drt/cli/commands/run.py": "ctx.allow_backfill_execution",
        # Dagster resource/assets/legacy assets (and sensor-triggered runs): refused
        "integrations/dagster-drt/dagster_drt/resource.py": None,
        "integrations/dagster-drt/dagster_drt/assets.py": None,
    }


def test_shared_cli_runner_opts_in_only_for_drt_run_not_build_or_apply() -> None:
    contexts: dict[str, str | None] = {}
    for filename in (
        "drt/cli/commands/run.py",
        "drt/cli/commands/build.py",
        "drt/cli/_apply_flow.py",
    ):
        tree = ast.parse(Path(filename).read_text())
        constructor = next(
            node
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "_RunContext"
        )
        value = next(
            (
                keyword.value
                for keyword in constructor.keywords
                if keyword.arg == "allow_backfill_execution"
            ),
            None,
        )
        contexts[filename] = ast.unparse(value) if value is not None else None
    assert contexts == {
        "drt/cli/commands/run.py": "True",
        "drt/cli/commands/build.py": None,
        "drt/cli/_apply_flow.py": None,
    }


def test_concurrency_and_downgrade_limits_are_operator_visible() -> None:
    guide = Path("docs/guides/windowed-backfill.md").read_text()
    assert "Run only one process for a given backfill sync at a time" in guide
    assert "Do not downgrade" in guide
    assert "Hand-editing" in guide or "hand-editing" in guide
