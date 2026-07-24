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
import re
from pathlib import Path
from typing import TYPE_CHECKING, TypedDict

import typer

if TYPE_CHECKING:
    from drt.config.models import SyncConfig, SyncTest

from drt.cli._app import app
from drt.cli._selection import SelectionError, complete_selector, select_syncs
from drt.cli.output import (
    console,
    print_error,
    print_test_header,
    print_test_result,
    print_test_skip,
)

_TEST_ID_RE = re.compile(r"[^A-Za-z0-9_]+")


class _SyncTestResult(TypedDict, total=False):
    """Type hint for test result dict in JSON output."""

    sync: str
    tests: list[dict[str, object]]
    skipped: bool
    reason: str


def _test_id(test_def: SyncTest, index: int) -> str:
    """Filesystem-safe identifier for one test, for --store-failures paths (#779).

    An explicit ``name:`` is trusted as unique (the operator chose it — same
    convention as dbt's singular-test-by-filename). Without one, falls back to
    a slugified display name prefixed with the test's position in ``tests:``,
    so two same-shaped tests (e.g. two ``not_null`` tests) never collide.
    """
    from drt.engine.test_runner import test_display_name

    if test_def.name:
        return _TEST_ID_RE.sub("-", test_def.name).strip("-").lower() or f"{index}-test"
    base = test_display_name(test_def)
    slug = _TEST_ID_RE.sub("-", base).strip("-").lower() or "test"
    return f"{index}-{slug}"


def _store_or_clear_failure_sample(
    *,
    sync: SyncConfig,
    test_def: SyncTest,
    test_id: str,
    failing_rows_query: str | None,
    passed: bool,
    project_dir: Path,
    store_failures_limit: int,
) -> tuple[Path, int] | None:
    """``--store-failures`` (#779): on failure, fetch + mask + write up to N
    failing rows; on pass, clear any stale sample from a previous failing run.

    ``failing_rows_query`` must be the SAME string already used to build the
    test's count query (computed once by the caller) — not recomputed here.
    For time-relative predicates (``freshness``'s ``datetime.now()``),
    recomputing independently let the count check and the stored sample
    observe different instants and drift apart (caught in CI, #779).

    Returns ``(path, count)`` when a sample was written, else ``None`` (test
    passed, or the type has no per-row failure concept — ``row_count``).
    """
    from drt.destinations.query import fetch_failing_rows
    from drt.engine.masking import apply_mask
    from drt.state.test_failures import clear_test_failures, write_test_failures

    if passed:
        clear_test_failures(project_dir, sync.name, test_id)
        return None

    if failing_rows_query is None:
        return None  # row_count: aggregate check, nothing to sample

    raw_rows = fetch_failing_rows(sync.destination, failing_rows_query, store_failures_limit)
    # Mask BEFORE anything else touches these rows (#427 reuse) — `raw_rows`
    # is never referenced again after this line, only `masked_rows` is.
    masked_rows = apply_mask(raw_rows, sync.sync.mask)
    path = write_test_failures(project_dir, sync.name, test_id, masked_rows)
    return path, len(masked_rows)


def execute_tests_for_sync(
    sync: SyncConfig,
    *,
    dry_run: bool,
    json_mode: bool,
    quiet: bool = False,
    store_failures: bool = False,
    store_failures_limit: int = 10,
    project_dir: Path = Path("."),
) -> tuple[_SyncTestResult, bool]:
    """Run one sync's ``tests:`` and return ``(result_dict, had_failures)``.

    Shared by ``drt test`` and ``drt build`` (#777). Non-queryable
    destinations are reported as skipped (never a failure); ``dry_run``
    lists the test plan without connecting. ``quiet`` silences text-mode
    output the same way ``drt run --quiet`` does — the result dict is
    unaffected, so JSON output and exit codes still carry every failure.

    ``had_failures`` (#779) reflects only ``severity: error`` failures — a
    ``severity: warn`` failure is still reported (and counted in each test's
    entry / a top-level ``warnings`` section by the caller) but never flips
    this to True, so it never fails ``drt test``'s exit code or ``drt
    build``'s per-sync status either (both share this function).
    """
    from drt.destinations.query import (
        execute_test_query,
        get_table_name,
        is_queryable,
    )
    from drt.engine.test_runner import (
        build_failing_rows_query,
        build_test_query,
        test_display_name,
    )

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
    for index, test_def in enumerate(sync.tests):
        test_name = test_display_name(test_def)
        if dry_run:
            if show:
                console.print(f"  [dim](dry-run)[/dim] {test_name}")
            sync_results["tests"].append(
                {"name": test_name, "dry_run": True, "severity": test_def.severity}
            )
        else:
            try:
                # Computed once (#779) and reused for both the count check and
                # --store-failures's sample — never rebuilt independently, so
                # a time-relative predicate (freshness's `now()`) can't drift
                # between the two.
                failing_rows_query = (
                    build_failing_rows_query(test_def, table) if store_failures else None
                )
                query, check = build_test_query(
                    test_def, table, failing_rows_query=failing_rows_query
                )
                result_val = execute_test_query(sync.destination, query)
                passed = check(result_val)
                entry: dict[str, object] = {
                    "name": test_name,
                    "passed": passed,
                    "value": str(result_val),
                    "severity": test_def.severity,
                }
                if store_failures:
                    stored = _store_or_clear_failure_sample(
                        sync=sync,
                        test_def=test_def,
                        test_id=_test_id(test_def, index),
                        failing_rows_query=failing_rows_query,
                        passed=passed,
                        project_dir=project_dir,
                        store_failures_limit=store_failures_limit,
                    )
                    if stored is not None:
                        path, count = stored
                        entry["failures_stored"] = {"path": str(path), "count": count}
                if show:
                    print_test_result(
                        test_name, passed, str(result_val), severity=test_def.severity
                    )
                    stored_info = entry.get("failures_stored")
                    if isinstance(stored_info, dict):
                        console.print(
                            f"    [dim]→ {stored_info['count']} failing row(s)"
                            f" written to {stored_info['path']}[/dim]"
                        )
                sync_results["tests"].append(entry)
                if not passed and test_def.severity != "warn":
                    had_failures = True
            except Exception as e:
                if show:
                    print_test_result(test_name, False, str(e), severity=test_def.severity)
                sync_results["tests"].append(
                    {
                        "name": test_name,
                        "passed": False,
                        "error": str(e),
                        "severity": test_def.severity,
                    }
                )
                if test_def.severity != "warn":
                    had_failures = True

    return sync_results, had_failures


def _collect_warnings(results: list[_SyncTestResult]) -> list[dict[str, object]]:
    """Flatten every ``severity: warn`` failure across *results* into a
    top-level list (#779) — so CI tooling can react without walking the
    nested per-sync/per-test structure."""
    warnings: list[dict[str, object]] = []
    for r in results:
        for t in r.get("tests", []):
            if t.get("severity") == "warn" and t.get("passed") is False:
                warnings.append(
                    {
                        "sync": r["sync"],
                        "test": t.get("name"),
                        "value": t.get("value", t.get("error")),
                    }
                )
    return warnings


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
    store_failures: bool = typer.Option(
        False,
        "--store-failures",
        help=(
            "Write up to N failing rows per failed test to "
            ".drt/test_failures/<sync>/<test>.jsonl (sync.mask applied before write; "
            "N set by --store-failures-limit)."
        ),
    ),
    store_failures_limit: int = typer.Option(
        10,
        "--store-failures-limit",
        help="Max rows written per failed test when --store-failures is set.",
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
            sync,
            dry_run=dry_run,
            json_mode=json_mode,
            store_failures=store_failures,
            store_failures_limit=store_failures_limit,
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

    warnings = _collect_warnings(results)

    if json_mode:
        print(
            json.dumps(
                {
                    "status": "failed" if had_failures else "passed",
                    "results": results,
                    "warnings": warnings,
                    "dry_run": dry_run,
                }
            )
        )
    elif dry_run:
        console.print("\n[dry-run] Preview of tests that would be executed")
    elif warnings:
        console.print(
            f"\n[yellow]{len(warnings)} warning(s) — reported but did not fail the run.[/yellow]"
        )
    if had_failures:
        raise typer.Exit(1)


def _test_display_name(test_def: object) -> str:
    """Backward-compatible private wrapper — delegates to the public helper."""
    from drt.config.models import SyncTest
    from drt.engine.test_runner import test_display_name

    assert isinstance(test_def, SyncTest)
    return test_display_name(test_def)
