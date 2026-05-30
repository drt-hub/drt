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
) -> None:
    """Show the status of the most recent sync runs."""

    if history:
        _print_history(sync_name=sync_name, limit=limit, output=output)
        return

    from drt.state.manager import StateManager

    states = StateManager(Path(".")).get_all()

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


def _print_history(*, sync_name: str | None, limit: int, output: str) -> None:
    """Render ``drt status --history`` output for one or all syncs."""
    from dataclasses import asdict

    from drt.state.history import HistoryManager

    entries = HistoryManager(Path(".")).read(sync_name=sync_name, limit=limit)

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
