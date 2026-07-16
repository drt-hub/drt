"""`drt build` — run each sync and its tests in one invocation (#777).

dbt users type `dbt build` for exactly this loop. Per sync: run → on
success, immediately execute its ``tests:`` (non-queryable destinations
skip tests, exactly like `drt test`). A failed test marks the sync failed
— already-loaded data is **not** rolled back (documented, same as dbt).

v1 is sequential by design: `--threads` interleaving of run+test stages
needs the run-loop extraction tracked under the #723 family — use
`drt run --threads` + `drt test` separately when parallelism matters.
"""

from __future__ import annotations

import json
import time
from pathlib import Path

import typer

from drt.cli._app import app
from drt.cli._selection import SelectionError, complete_selector, select_syncs
from drt.cli.output import console, print_error


@app.command(name="build")
def build(
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
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview runs and list the test plan without executing."
    ),
    fail_fast: bool = typer.Option(
        False,
        "--fail-fast",
        help="Stop scheduling after the first failed sync or failed test.",
    ),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress output except errors."),
    verbose: bool = typer.Option(False, "--verbose", help="Show row-level error details."),
    output: str = typer.Option("text", "--output", "-o", help="Output format: text or json."),
    profile_name: str = typer.Option(
        None, "--profile", "-p", help="Override profile (default: drt_project.yml or DRT_PROFILE)."
    ),
) -> None:
    """Run sync(s) and their tests in one invocation (sequential).

    Per sync: run, then — on success — its tests. Overall sync status is
    run AND tests; a test failure exits non-zero but never rolls back
    loaded data.

    Examples:
      drt build
      drt build --select tag:crm --fail-fast
      drt build --dry-run
    """
    from drt.cli._helpers import get_source, resolve_profile_name
    from drt.cli.commands.run import _run_one, _RunContext
    from drt.cli.commands.test import execute_tests_for_sync
    from drt.config.credentials import load_profile
    from drt.config.parser import load_project, load_syncs
    from drt.state.history import HistoryManager
    from drt.state.manager import StateManager

    json_mode = output == "json"

    try:
        project = load_project(Path("."))
    except FileNotFoundError as e:
        print_error(str(e))
        raise typer.Exit(1)

    resolved = resolve_profile_name(profile_name, project.profile)
    try:
        profile = load_profile(resolved)
    except (FileNotFoundError, KeyError, ValueError) as e:
        print_error(str(e))
        raise typer.Exit(1)

    syncs = load_syncs(Path("."))
    if not syncs:
        if not json_mode:
            console.print("[dim]No syncs found in syncs/. Add .yml files to get started.[/dim]")
        raise typer.Exit()

    try:
        syncs = select_syncs(syncs, select, exclude)
    except SelectionError as e:
        print_error(str(e))
        raise typer.Exit(1)
    if not syncs:
        print_error("Selection matched no syncs (after --exclude).")
        raise typer.Exit(1)

    source = get_source(profile)
    state_mgr = StateManager(Path("."))
    history_cfg = project.history
    history_mgr = HistoryManager(Path(".")) if history_cfg.enabled else None

    ctx = _RunContext(
        source=source,
        state_mgr=state_mgr,
        history_mgr=history_mgr,
        history_retention_days=history_cfg.retention_days,
        json_mode=json_mode,
        dry_run=dry_run,
        verbose=verbose,
        quiet=quiet,
        log_json=False,
        cursor_value=None,
    )

    entries: list[dict[str, object]] = []
    succeeded = 0
    failed = 0
    skipped = 0
    t_total = time.monotonic()
    stop_scheduling = False

    for sync in syncs:
        if stop_scheduling:
            entries.append(
                {
                    "name": sync.name,
                    "status": "skipped",
                    "reason": "fail_fast",
                    "rows_extracted": 0,
                    "rows_synced": 0,
                    "rows_failed": 0,
                    "duration_seconds": 0.0,
                    "dry_run": dry_run,
                    "tests": [],
                }
            )
            skipped += 1
            continue

        name, entry, had_err = _run_one(sync, ctx, profile)

        # Tests run only after a successful (or dry-run) run of that sync —
        # a failed load makes its post-sync assertions meaningless. Every
        # entry still carries `tests` so JSON consumers see one stable shape
        # (an empty list = no tests ran, whatever the reason).
        entry["tests"] = []
        if not had_err and sync.tests:
            test_result, tests_failed = execute_tests_for_sync(
                sync, dry_run=dry_run, json_mode=json_mode, quiet=quiet
            )
            entry["tests"] = test_result.get("tests", [])
            if test_result.get("skipped"):
                entry["tests_skipped_reason"] = test_result.get("reason")
            if tests_failed:
                had_err = True
                entry["status"] = "tests_failed"

        entries.append(entry)
        if had_err:
            failed += 1
            if fail_fast:
                stop_scheduling = True
        else:
            succeeded += 1

    total_duration = round(time.monotonic() - t_total, 2)

    if skipped and not json_mode and not quiet:
        console.print(
            f"[yellow]--fail-fast: skipped {skipped} sync(s) after the first failure.[/yellow]"
        )
    if not json_mode and not quiet and len(syncs) > 1:
        console.print(
            f"\n[bold]Build summary:[/bold] {succeeded} succeeded, {failed} failed, "
            f"{total_duration}s total"
        )

    if json_mode:
        print(
            json.dumps(
                {
                    "syncs": entries,
                    "succeeded": succeeded,
                    "failed": failed,
                    "skipped": skipped,
                    "total_duration_seconds": total_duration,
                },
                indent=2,
            )
        )

    if failed > 0:
        raise typer.Exit(1)
