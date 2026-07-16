"""``drt test`` — run post-sync validation tests against destination data.

Extracted from ``drt/cli/main.py`` in Phase 2b PR (b) of the #546 split
(tracked under #573). The private ``_SyncTestResult`` TypedDict and
``_test_display_name`` shim move alongside since nothing else uses them.

Back-compat: ``drt.cli.main`` re-exports ``_SyncTestResult`` +
``_test_display_name`` so existing ``from drt.cli.main import ...``
paths keep working.

The module name is ``test``; the registered command is also ``test``
via ``@app.command(name="test")`` (Python function called
``test_syncs`` to avoid shadowing pytest in unrelated tooling).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

import typer

if TYPE_CHECKING:
    from drt.config.models import SyncConfig

from drt.cli._app import app
from drt.cli._selection import SelectionError, complete_selector, select_syncs
from drt.cli.output import (
    console,
    print_error,
    print_test_header,
    print_test_result,
    print_test_skip,
)


class _SyncTestResult(TypedDict, total=False):
    """Type hint for test result dict in JSON output."""

    sync: str
    tests: list[dict[str, object]]
    skipped: bool
    reason: str


def execute_tests_for_sync(
    sync: SyncConfig, *, dry_run: bool, json_mode: bool, quiet: bool = False
) -> tuple[_SyncTestResult, bool]:
    """Run one sync's ``tests:`` and return ``(result_dict, had_failures)``.

    Shared by ``drt test`` and ``drt build`` (#777). Non-queryable
    destinations are reported as skipped (never a failure); ``dry_run``
    lists the test plan without connecting. ``quiet`` silences text-mode
    output the same way ``drt run --quiet`` does — the result dict is
    unaffected, so JSON output and exit codes still carry every failure.
    """
    from drt.destinations.query import (
        execute_test_query,
        get_table_name,
        is_queryable,
    )
    from drt.engine.test_runner import build_test_query

    show = not json_mode and not quiet

    if show:
        print_test_header(sync.name)
    sync_results: _SyncTestResult = {"sync": sync.name, "tests": []}
    had_failures = False

    if not is_queryable(sync.destination):
        if show:
            if dry_run:
                console.print(
                    f"  [dim]⏭ {sync.name}: would be skipped"
                    f" (tests not supported for"
                    f" {sync.destination.type} destinations)[/dim]"
                )
            else:
                print_test_skip(
                    sync.name,
                    f"tests not supported for {sync.destination.type} destinations",
                )
        sync_results["skipped"] = True
        sync_results["reason"] = f"tests not supported for {sync.destination.type}"
        return sync_results, False

    table = get_table_name(sync.destination)
    for test_def in sync.tests:
        test_name = _test_display_name(test_def)
        if dry_run:
            if show:
                console.print(f"  [dim](dry-run)[/dim] {test_name}")
            sync_results["tests"].append({"name": test_name, "dry_run": True})
        else:
            try:
                query, check = build_test_query(test_def, table)
                result_val = execute_test_query(sync.destination, query)
                passed = check(result_val)
                if show:
                    print_test_result(test_name, passed, str(result_val))
                sync_results["tests"].append(
                    {"name": test_name, "passed": passed, "value": str(result_val)}
                )
                if not passed:
                    had_failures = True
            except Exception as e:
                if show:
                    print_test_result(test_name, False, str(e))
                sync_results["tests"].append({"name": test_name, "passed": False, "error": str(e)})
                had_failures = True

    return sync_results, had_failures


@app.command(name="test")
def test_syncs(
    output: str = typer.Option("text", "--output", "-o", help="Output format: text or json."),
    select: list[str] = typer.Option(
        None,
        "--select",
        "-s",
        help=(
            "Select syncs: name or glob, tag:<pattern>, destination:<type>, "
            'or "*" / "all". Repeat to union.'
        ),
        autocompletion=complete_selector,
    ),
    exclude: list[str] = typer.Option(
        None,
        "--exclude",
        help="Subtract syncs from the selection (same grammar as --select). Repeatable.",
        autocompletion=complete_selector,
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without running tests."),
    fail_fast: bool = typer.Option(
        False,
        "--fail-fast",
        help="Stop after the first sync with a failing test; remaining syncs are skipped.",
    ),
) -> None:
    """Run post-sync validation tests.

    With --dry-run, shows what tests would be executed without actually
    connecting to the destination or running queries.
    """
    from drt.config.parser import load_syncs

    json_mode = output == "json"
    results: list[_SyncTestResult] = []

    syncs = load_syncs(Path("."))
    if not syncs:
        if not json_mode:
            console.print("[dim]No syncs found.[/dim]")
        else:
            print(json.dumps({"status": "no_syncs", "results": []}))
        return

    try:
        syncs = select_syncs(syncs, select, exclude)
    except SelectionError as e:
        print_error(str(e))
        raise typer.Exit(1)
    if not syncs:
        print_error("Selection matched no syncs (after --exclude).")
        raise typer.Exit(1)

    syncs_with_tests = [s for s in syncs if s.tests]
    if not syncs_with_tests:
        if not json_mode:
            console.print("[dim]No tests defined in any sync.[/dim]")
        else:
            print(json.dumps({"status": "no_tests", "results": []}))
        return

    had_failures = False

    for i, sync in enumerate(syncs_with_tests):
        sync_results, sync_failed = execute_tests_for_sync(
            sync, dry_run=dry_run, json_mode=json_mode
        )
        results.append(sync_results)
        if sync_failed:
            had_failures = True

        # --fail-fast (#775): stop after the first sync with a failing test.
        if fail_fast and had_failures:
            remaining = syncs_with_tests[i + 1 :]
            for skipped_sync in remaining:
                results.append(
                    {
                        "sync": skipped_sync.name,
                        "tests": [],
                        "skipped": True,
                        "reason": "fail_fast",
                    }
                )
            if remaining and not json_mode:
                console.print(
                    f"[yellow]--fail-fast: skipped {len(remaining)} sync(s) "
                    "after the first failure.[/yellow]"
                )
            break

    if json_mode:
        print(
            json.dumps(
                {
                    "status": "failed" if had_failures else "passed",
                    "results": results,
                    "dry_run": dry_run,
                }
            )
        )
    elif dry_run:
        console.print("\n[dry-run] Preview of tests that would be executed")
    if had_failures:
        raise typer.Exit(1)


def _test_display_name(test_def: object) -> str:
    """Backward-compatible private wrapper — delegates to the public helper."""
    from drt.config.models import SyncTest
    from drt.engine.test_runner import test_display_name

    assert isinstance(test_def, SyncTest)
    return test_display_name(test_def)
