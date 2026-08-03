"""``drt state`` — inspect and reset drt's durable state (#776).

drt keeps three kinds of state that outlive a run, and until now offered no
sanctioned way to clear any of them:

* the **watermark** (``.drt/watermarks.json``, or a GCS / BigQuery backend)
* the **run state** (``.drt/state.json`` — last status, row count, and the
  fallback cursor)
* the **tracked-mirror key table** (``_drt_synced_keys``, in the destination)

Users hand-edited JSON, which does nothing for remote watermark backends and
nothing at all for the key table. ``--cursor-value`` overrides one run's
predicate but leaves stored state untouched — a probe, not a reset.

**The levels are separate on purpose.** dlt learned this with
``drop_data`` / ``drop_resources`` / ``drop_sources``: folding several
destruction levels into one switch means someone eventually clears more than
they meant to. Here the asymmetry is sharper still — ``--tracked-mirror``
re-baselines the destination, after which rows the *application* wrote count
as drt's own and become deletion candidates on the next mirror pass. That is
precisely the risk tracked mirror (#686) exists to prevent, so it is never
implied by another flag and never included in ``--full-refresh``.

There is deliberately no ``--all``.
"""

from __future__ import annotations

import typer

from drt.cli._app import app
from drt.cli.output import console

state_app = typer.Typer(
    name="state",
    help="Inspect and reset drt's durable state (watermarks, run state, mirror keys).",
    no_args_is_help=True,
)
app.add_typer(state_app)


def _reset_tracked_state(sync_name: str) -> int:
    """Clear ``sync_name``'s rows from the destination's ``_drt_synced_keys``.

    Split out so the CLI tests can assert it is *not* reached by the other
    levels — the property that keeps ``--runs`` from touching a warehouse.
    """
    from pathlib import Path

    from drt.cli._helpers import get_destination
    from drt.config.parser import load_syncs

    syncs = [s for s in load_syncs(Path("syncs")) if s.name == sync_name]
    if not syncs:
        console.print(f"[yellow]No sync named '{sync_name}' — skipping mirror state.[/yellow]")
        return 0

    dest = get_destination(syncs[0])
    reset = getattr(dest, "reset_tracked_state", None)
    if reset is None:
        console.print(
            f"[yellow]{syncs[0].destination.type} does not support tracked mirror — "
            "nothing to reset.[/yellow]"
        )
        return 0
    return int(reset(syncs[0].destination, sync_name))


@state_app.command("show")
def state_show(
    sync_name: str = typer.Argument(None, help="Sync to inspect. Omit for all."),
) -> None:
    """Show stored watermark and last-run state."""
    from pathlib import Path

    from rich.table import Table

    from drt.state.manager import StateManager

    states = StateManager(Path(".")).get_all()

    if sync_name:
        s = states.get(sync_name)
        if s is None:
            console.print(f"[dim]No state recorded for '{sync_name}'.[/dim]")
            return
        console.print(f"\n[bold]{sync_name}[/bold]")
        console.print(f"  last run:   {s.last_run_at}")
        console.print(f"  status:     {s.status}")
        console.print(f"  records:    {s.records_synced}")
        console.print(f"  watermark:  {s.last_cursor_value or '(none)'}")
        console.print()
        return

    if not states:
        console.print("[dim]No state recorded yet.[/dim]")
        return

    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    table.add_column("Sync")
    table.add_column("Last run")
    table.add_column("Status")
    table.add_column("Watermark")
    for name, s in sorted(states.items()):
        table.add_row(name, s.last_run_at, s.status, s.last_cursor_value or "-")
    console.print(table)


@state_app.command("reset")
def state_reset(
    sync_name: str = typer.Argument(..., help="Sync whose state to reset."),
    watermark: bool = typer.Option(False, "--watermark", help="Clear the stored watermark."),
    runs: bool = typer.Option(False, "--runs", help="Clear recorded run state."),
    tracked_mirror: bool = typer.Option(
        False,
        "--tracked-mirror",
        help="Re-baseline the destination's _drt_synced_keys (see the warning).",
    ),
    dry_run: bool = typer.Option(False, "--dry-run", help="Report without changing anything."),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip the confirmation prompt."),
) -> None:
    """Reset one sync's state. At least one level flag is required."""
    from pathlib import Path

    from drt.cli._helpers import confirm_destructive, get_watermark_storage
    from drt.state.manager import StateManager

    if not (watermark or runs or tracked_mirror):
        # Never treat "no level" as "all levels" — the whole point of splitting
        # them is that the dangerous one must be asked for by name.
        console.print(
            "[red]Choose what to reset:[/red] --watermark, --runs, and/or --tracked-mirror.\n"
            "[dim]They are separate because --tracked-mirror re-baselines the "
            "destination's key table; see `drt state reset --help`.[/dim]"
        )
        raise typer.Exit(2)

    requested = (
        ("watermark", watermark),
        ("runs", runs),
        ("tracked-mirror", tracked_mirror),
    )
    levels = [name for name, enabled in requested if enabled]

    if tracked_mirror and not dry_run:
        console.print(
            "[yellow]⚠ --tracked-mirror re-baselines the destination's key table.[/yellow]\n"
            "[dim]  The next mirror pass treats what is currently in the target as "
            "drt's own, so rows written by other systems become deletion "
            "candidates. Same semantics as a first run (#686).[/dim]"
        )

    if dry_run:
        console.print(f"[dim]Would reset {', '.join(levels)} for '{sync_name}'.[/dim]")
        return

    if not confirm_destructive(f"Reset {', '.join(levels)} for '{sync_name}'?", yes=yes):
        console.print("[dim]Aborted.[/dim]")
        raise typer.Exit(0)

    project = Path(".")

    if watermark:
        cleared = False
        # The configured backend, when the sync declares one.
        from drt.config.parser import load_syncs

        for s in load_syncs(project / "syncs"):
            if s.name != sync_name:
                continue
            storage = get_watermark_storage(s, project)
            if storage is not None:
                storage.delete(sync_name)
                cleared = True
        console.print(
            "[green]✓[/green] watermark cleared"
            if cleared
            else "[dim]  no watermark backend configured — "
            "the fallback cursor is cleared by --runs.[/dim]"
        )

    if runs:
        removed = StateManager(project).reset(sync_name)
        console.print(
            "[green]✓[/green] run state cleared"
            if removed
            else f"[dim]  no run state recorded for '{sync_name}'.[/dim]"
        )

    if tracked_mirror:
        n = _reset_tracked_state(sync_name)
        console.print(
            f"[green]✓[/green] tracked-mirror state cleared ({n} key(s))"
            if n
            else "[dim]  no tracked-mirror state to clear.[/dim]"
        )
