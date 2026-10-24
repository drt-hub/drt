"""``drt status`` — show most-recent run state, or ``--history`` log tail.

Extracted from ``drt/cli/main.py`` in Phase 2b PR (b) of the #546 split
(tracked under #573). The private ``_print_history`` helper moves
alongside since nothing else uses it.

Back-compat: ``drt.cli.main`` re-exports ``_print_history`` so existing
``from drt.cli.main import _print_history`` paths keep working.
"""

from __future__ import annotations

import json
from pathlib import Path

import typer

from drt.cli._app import app
from drt.cli.output import (
    console,
    print_status_table,
    print_status_verbose,
)


@app.command()
def status(
    verbose: bool = typer.Option(False, "--verbose", help="Show row-level error details."),
    output: str = typer.Option("text", "--output", "-o", help="Output format: text or json."),
    history: bool = typer.Option(
        False,
        "--history",
        help="Show past execution history instead of just the most recent run.",
    ),
    sync_name: str | None = typer.Option(
        None,
        "--sync",
        help="Only show entries for this sync (--history mode only).",
    ),
    limit: int = typer.Option(20, "--limit", help="Max entries to show in --history mode."),
    check: bool = typer.Option(
        False,
        "--check",
        help=(
            "Judge sync freshness against alerts.on_stale and exit 1 if any sync is stale "
            "(for cron / CI). Read-only; evaluated from run history."
        ),
    ),
    select: list[str] | None = typer.Option(
        None, "--select", help="With --check: only these syncs (name, glob, tag:..., ...)."
    ),
    notify: bool = typer.Option(
        False,
        "--notify",
        help="With --check: send an alert to each stale sync's alerts.on_stale.channels.",
    ),
) -> None:
    """Show the status of the most recent sync runs."""
    from drt.security import PermissionAction, get_permission_checker

    # Enterprise extension point (#298, ADR 0008) — no-op under the OSS
    # default; a registered Enterprise checker may deny status reads.
    # sync_name=None here — this checks project-wide view access; a
    # per-sync check would additionally apply once --sync narrows the read.
    get_permission_checker().check(PermissionAction.VIEW, None)

    if check:
        _check_freshness(select=select, notify=notify, output=output)
        return
    if notify or select:
        typer.echo("drt status: --select and --notify only apply together with --check.", err=True)
        raise typer.Exit(1)

    if history:
        _print_history(sync_name=sync_name, limit=limit, output=output)
        return

    from drt.config.base import ProjectConfig
    from drt.config.parser import load_project
    from drt.state.factory import build_state_bundle

    project_dir = Path(".")
    project = (
        load_project(project_dir)
        if (project_dir / "drt_project.yml").exists()
        else ProjectConfig(name="drt")
    )
    state_bundle = build_state_bundle(project, project_dir)
    states = state_bundle.state.get_all()
    dlq_depths = state_bundle.dlq.all_depths()

    if output == "json":
        print(
            json.dumps(
                {
                    "syncs": [
                        {
                            "name": name,
                            "status": state.status,
                            "last_run_at": state.last_run_at,
                            "records_synced": state.records_synced,
                            "last_cursor_value": state.last_cursor_value,
                            "error": state.error,
                            "dlq_depth": dlq_depths.get(name, 0),
                        }
                        for name, state in sorted(states.items())
                    ],
                },
                indent=2,
            )
        )
        return

    if verbose:
        print_status_verbose(states, {})
    else:
        print_status_table(states)

    # Dead Letter Queue depth (#278): surface any queued failures so the
    # operator knows there is something to `drt retry`. Includes queues for
    # syncs that have no state row yet (e.g. first run failed outright).
    if dlq_depths:
        console.print()
        for name in sorted(dlq_depths):
            console.print(
                f"[yellow]⚠ {name}: {dlq_depths[name]} record(s) in dead letter "
                f"queue — run [bold]drt retry {name}[/bold] to replay.[/yellow]"
            )


def _check_freshness(*, select: list[str] | None, notify: bool, output: str) -> None:
    """``drt status --check`` (#1224): exit 1 when any sync is stale."""
    from drt.alerts.dispatcher import dispatch_targets
    from drt.cli._selection import SelectionError, select_syncs
    from drt.config.parser import load_project, load_syncs
    from drt.engine.freshness import evaluate_freshness, has_failure
    from drt.state.factory import build_state_bundle

    project_dir = Path(".")
    try:
        project = load_project(project_dir)
        syncs = select_syncs(load_syncs(project_dir), select)
    except (FileNotFoundError, ValueError, SelectionError) as e:
        typer.echo(f"drt status --check: {e}", err=True)
        raise typer.Exit(1) from e

    bundle = build_state_bundle(project, project_dir)
    verdicts = evaluate_freshness(syncs, bundle.history, bundle.state.get_all())
    by_name = {s.name: s for s in syncs}

    if notify:
        for v in verdicts:
            if v.severity == "ok":
                continue
            config = by_name[v.sync_name].alerts.on_stale  # type: ignore[union-attr]
            dispatch_targets(
                config.channels,  # type: ignore[union-attr]
                {
                    "sync_name": v.sync_name,
                    "status": v.state,
                    "error": f"{v.state.replace('_', ' ')} \u2014 {v.message()}",
                    "last_success_at": v.last_success_at,
                    "age_seconds": v.age_seconds,
                    "max_age_seconds": v.max_age_seconds,
                },
            )

    failed = has_failure(verdicts)
    if output == "json":
        print(
            json.dumps(
                {
                    "ok": not failed,
                    "checked": len(verdicts),
                    "syncs": [v.to_dict() for v in verdicts],
                },
                indent=2,
            )
        )
    elif not verdicts:
        console.print(
            "[yellow]No sync selected configures alerts.on_stale; nothing to check.[/yellow]"
        )
    else:
        from rich.markup import escape

        for v in verdicts:
            colour = {"ok": "green", "warn": "yellow", "error": "red"}[v.severity]
            console.print(
                f"[{colour}]{v.state:<9}[/{colour}] {escape(v.sync_name)} \u2014 {v.message()}"
            )
    if failed:
        raise typer.Exit(1)


def _print_history(*, sync_name: str | None, limit: int, output: str) -> None:
    """Render ``drt status --history`` output for one or all syncs."""
    from dataclasses import asdict

    from drt.config.base import ProjectConfig
    from drt.config.parser import load_project
    from drt.state.factory import build_state_bundle

    project_dir = Path(".")
    project = (
        load_project(project_dir)
        if (project_dir / "drt_project.yml").exists()
        else ProjectConfig(name="drt")
    )
    entries = build_state_bundle(project, project_dir).history.read(
        sync_name=sync_name, limit=limit
    )

    if output == "json":
        print(
            json.dumps(
                {"entries": [asdict(e) for e in entries]},
                indent=2,
                default=str,
            )
        )
        return

    if not entries:
        scope = f"sync='{sync_name}'" if sync_name else "any sync"
        console.print(f"[yellow]No history found for {scope}.[/yellow]")
        return

    from rich.table import Table

    table = Table(
        title=(
            f"Execution history — sync='{sync_name}'"
            if sync_name
            else "Execution history (all syncs)"
        ),
        show_lines=False,
    )
    table.add_column("Started", style="cyan", no_wrap=True)
    table.add_column("Sync", style="magenta")
    table.add_column("Status", justify="center")
    table.add_column("Synced", justify="right")
    table.add_column("Failed", justify="right")
    table.add_column("Duration", justify="right")
    table.add_column("Error", overflow="fold")

    for e in entries:
        status_style = {
            "success": "green",
            "partial": "yellow",
            "failed": "red",
        }.get(e.status, "white")
        table.add_row(
            e.started_at[:19].replace("T", " "),
            e.sync_name,
            f"[{status_style}]{e.status}[/{status_style}]",
            str(e.records_synced),
            str(e.records_failed),
            f"{e.duration_seconds:.1f}s",
            (e.errors[0] if e.errors else ""),
        )
    console.print(table)
