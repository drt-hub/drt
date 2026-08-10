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
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

import typer

if TYPE_CHECKING:
    from drt.config.models import SyncConfig, SyncTest

from drt.cli._app import app
from drt.cli._selection import (
    SelectionError,
    complete_selector,
    is_state_only_select,
    select_syncs,
)
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


def _test_id(test_def: SyncTest, index: int, seen: set[str] | None = None) -> str:
    """Filesystem-safe identifier for one test, for --store-failures paths (#779).

    An explicit ``name:`` is trusted as unique (the operator chose it — same
    convention as dbt's singular-test-by-filename) *unless* it collides with
    another test's slug in the same sync (#835): ``"email nn"`` and
    ``"email/nn"`` both slugify to ``email-nn``, and without disambiguation the
    second write silently overwrites the first test's sample — the one thing
    ``--store-failures`` exists to preserve. ``seen`` tracks slugs already
    claimed in this sync; a collision falls back to the same
    ``{index}-{slug}`` prefixing the no-``name:`` path already uses, so it
    only changes the filename for the tests that would otherwise clobber each
    other, not every explicitly-named test.

    Without a ``name:``, falls back to a slugified display name prefixed with
    the test's position in ``tests:``, so two same-shaped tests (e.g. two
    ``not_null`` tests) never collide in the first place.
    """
    from drt.engine.test_runner import test_display_name

    if test_def.name:
        slug = _TEST_ID_RE.sub("-", test_def.name).strip("-").lower() or f"{index}-test"
    else:
        base = test_display_name(test_def)
        base_slug = _TEST_ID_RE.sub("-", base).strip("-").lower() or "test"
        slug = f"{index}-{base_slug}"

    if seen is not None:
        if slug in seen:
            slug = f"{index}-{slug}"
        seen.add(slug)
    return slug


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


def execute_unit_tests_for_sync(
    sync: SyncConfig, *, json_mode: bool, quiet: bool = False
) -> tuple[_SyncTestResult, bool]:
    """Run one sync's ``unit_tests:`` and return ``(result_dict, had_failures)``.

    Deliberately separate from :func:`execute_tests_for_sync` rather than a
    branch inside it: unit tests (#780) never touch a destination — no
    connection, no ``dry_run`` distinction, no ``severity: warn`` tier, none
    of the machinery that function exists for. Sharing a function would mean
    threading a growing set of "well, not for unit tests" exceptions through
    it instead of two functions that each do one thing.
    """
    from drt.engine.unit_test_runner import UnitTestLookupsUnsupportedError, run_unit_test

    show = not json_mode and not quiet
    if show:
        print_test_header(sync.name)

    sync_results: _SyncTestResult = {"sync": sync.name, "tests": []}
    had_failures = False

    try:
        for test_def in sync.unit_tests:
            result = run_unit_test(sync, test_def)
            message = "ok" if result.passed else "; ".join(result.mismatches)
            if show:
                print_test_result(test_def.name, result.passed, message)
            sync_results["tests"].append(
                {
                    "name": test_def.name,
                    "passed": result.passed,
                    "mismatches": result.mismatches,
                }
            )
            if not result.passed:
                had_failures = True
    except UnitTestLookupsUnsupportedError as e:
        # One config-shape problem, the same for every fixture on this sync —
        # surfaced once rather than once per unit_tests entry.
        if show:
            print_test_result(sync.name, False, str(e))
        sync_results["tests"].append({"name": sync.name, "passed": False, "error": str(e)})
        had_failures = True

    return sync_results, had_failures


def _run_unit_tests(syncs: Sequence[SyncConfig], *, json_mode: bool, fail_fast: bool) -> None:
    """``drt test --unit``'s body — separate from the main ``sync.tests:`` loop
    in :func:`test_syncs` for the same reason :func:`execute_unit_tests_for_sync`
    is separate from :func:`execute_tests_for_sync`: no destination, no
    ``dry_run``, no ``severity`` tier, no ``--store-failures``.
    """
    syncs_with_unit_tests = [s for s in syncs if s.unit_tests]
    if not syncs_with_unit_tests:
        if not json_mode:
            console.print("[dim]No unit_tests defined in any sync.[/dim]")
        else:
            print(json.dumps({"status": "no_tests", "results": []}))
        return

    results: list[_SyncTestResult] = []
    had_failures = False

    for i, sync in enumerate(syncs_with_unit_tests):
        sync_results, sync_failed = execute_unit_tests_for_sync(sync, json_mode=json_mode)
        results.append(sync_results)
        if sync_failed:
            had_failures = True

        if fail_fast and had_failures:
            remaining = syncs_with_unit_tests[i + 1 :]
            for skipped_sync in remaining:
                results.append(
                    {"sync": skipped_sync.name, "tests": [], "skipped": True, "reason": "fail_fast"}
                )
            if remaining and not json_mode:
                console.print(
                    f"[yellow]--fail-fast: skipped {len(remaining)} sync(s) "
                    "after the first failure.[/yellow]"
                )
            break

    if json_mode:
        print(json.dumps({"status": "failed" if had_failures else "passed", "results": results}))
    if had_failures:
        raise typer.Exit(1)


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
        # Same wording as the two printed variants above — and as the string
        # `drt_run_test` has always returned (#851), so routing the MCP tool
        # through this function keeps its `reason` field byte-identical.
        sync_results["reason"] = f"tests not supported for {sync.destination.type} destinations"
        return sync_results, False

    table = get_table_name(sync.destination)
    seen_test_ids: set[str] = set()
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
                continue

            # --store-failures (#779): deliberately outside the verdict
            # try/except above. A storage failure (locked file, disk
            # permissions, a destination error while sampling) must never
            # turn a passing test into a failure, or overwrite a genuine
            # failure's real value with the storage exception's text — the
            # count-check verdict is authoritative regardless of what
            # happens here.
            if store_failures:
                try:
                    stored = _store_or_clear_failure_sample(
                        sync=sync,
                        test_def=test_def,
                        test_id=_test_id(test_def, index, seen_test_ids),
                        failing_rows_query=failing_rows_query,
                        passed=passed,
                        project_dir=project_dir,
                        store_failures_limit=store_failures_limit,
                    )
                except Exception as store_err:
                    entry["failures_stored"] = {"error": str(store_err)}
                    if show:
                        console.print(
                            f"    [yellow]⚠ could not store failure sample:"
                            f" {store_err}[/yellow]"
                        )
                else:
                    if stored is not None:
                        path, count = stored
                        entry["failures_stored"] = {"path": str(path), "count": count}

            if show:
                print_test_result(test_name, passed, str(result_val), severity=test_def.severity)
                stored_info = entry.get("failures_stored")
                if isinstance(stored_info, dict) and "path" in stored_info:
                    console.print(
                        f"    [dim]→ {stored_info['count']} failing row(s)"
                        f" written to {stored_info['path']}[/dim]"
                    )
            sync_results["tests"].append(entry)
            if not passed and test_def.severity != "warn":
                had_failures = True

    return sync_results, had_failures


def _collect_warnings(
    results: Sequence[_SyncTestResult] | Sequence[Mapping[str, Any]],
) -> list[dict[str, object]]:
    """Flatten every ``severity: warn`` failure across *results* into a
    top-level list (#779) — so CI tooling can react without walking the
    nested per-sync/per-test structure.

    Accepts either ``drt test``'s own ``_SyncTestResult`` or ``drt build``'s
    per-sync ``dict[str, object]`` entries (#838) — both carry the same
    ``sync`` + ``tests`` shape this function actually reads, so ``drt build``
    reuses this rather than re-implementing it against a narrower type.

    Carries ``value`` when the test ran and returned one (a threshold
    breach — the data quality is known and numeric) or ``error`` when it
    raised (an infrastructure failure — the data quality is simply unknown)
    (#837). Never both, mirroring the per-test entry shape in
    ``results[].tests[]`` that already keeps these separate. A consumer
    counting "warnings over time" needs to be able to tell them apart —
    collapsing them into one ``value`` key silently mixed outages into a
    data-quality signal, and broke a numeric parse of ``value`` on the
    exception case with no way to have predicted it.
    """
    warnings: list[dict[str, object]] = []
    for r in results:
        for t in r.get("tests", []):
            if t.get("severity") == "warn" and t.get("passed") is False:
                entry: dict[str, object] = {"sync": r["sync"], "test": t.get("name")}
                if "error" in t:
                    entry["error"] = t["error"]
                else:
                    entry["value"] = t.get("value")
                warnings.append(entry)
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
            'state:modified/state:new, or "*" / "all". Repeat to union.'
        ),
        autocompletion=complete_selector,
    ),
    exclude: list[str] = typer.Option(
        None,
        "--exclude",
        help="Subtract syncs from the selection (same grammar as --select). Repeatable.",
        autocompletion=complete_selector,
    ),
    state: Path | None = typer.Option(
        None,
        "--state",
        help=(
            "Baseline manifest path for state:modified/state:new selectors "
            "(for example, a prior `drt docs generate --format json` CI artifact)."
        ),
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
        min=1,
        help="Max rows written per failed test when --store-failures is set.",
    ),
    unit: bool = typer.Option(
        False,
        "--unit",
        help=(
            "Run sync.unit_tests instead of sync.tests: fixture rows through "
            "the transform pipeline, zero credentials, zero network. "
            "Mutually exclusive with --dry-run and --store-failures, which "
            "are destination-connected concepts unit tests don't have."
        ),
    ),
) -> None:
    """Run post-sync validation tests.

    With --dry-run, shows what tests would be executed without actually
    connecting to the destination or running queries. With --unit, runs
    sync.unit_tests instead — see that flag's help.

    Examples:
      drt test --select state:modified --state ci-baseline/manifest.json --dry-run
    """
    from drt.config.parser import load_syncs

    json_mode = output == "json"
    results: list[_SyncTestResult] = []

    if unit and (dry_run or store_failures):
        print_error("--unit cannot be combined with --dry-run or --store-failures.")
        raise typer.Exit(2)

    syncs = load_syncs(Path("."))
    if not syncs:
        if not json_mode:
            console.print("[dim]No syncs found.[/dim]")
        else:
            print(json.dumps({"status": "no_syncs", "results": []}))
        return

    try:
        if state is not None:
            from drt.cli._state_selection import load_state_diff

            state_diff = load_state_diff(state, syncs, Path("."))
            syncs = select_syncs(syncs, select, exclude, state_diff=state_diff)
        else:
            syncs = select_syncs(syncs, select, exclude)
    except SelectionError as e:
        print_error(str(e))
        raise typer.Exit(1)
    if not syncs:
        if is_state_only_select(select):
            if not json_mode:
                console.print(
                    "[dim]No syncs changed relative to the baseline — nothing to test.[/dim]"
                )
            else:
                print(json.dumps({"status": "no_changes", "results": []}))
            return
        print_error("Selection matched no syncs (after --exclude).")
        raise typer.Exit(1)

    if unit:
        _run_unit_tests(syncs, json_mode=json_mode, fail_fast=fail_fast)
        return

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
