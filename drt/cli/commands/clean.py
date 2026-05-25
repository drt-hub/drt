"""`drt clean` — clean up orphan __drt_swap shadow tables."""

from __future__ import annotations

from pathlib import Path

import typer

from drt.cli._app import app
from drt.cli._helpers import get_destination


@app.command()
def clean(
    orphans: bool = typer.Option(
        False, "--orphans", help="List or drop orphan __drt_swap tables."
    ),
    execute: bool = typer.Option(False, "--execute", help="Execute drops (default: dry-run)."),
    config: str = typer.Option("drt.yml", "--config", "-c", help="Path to config file."),
) -> None:
    """Clean up orphan __drt_swap shadow tables left by interrupted swaps.

    Use --orphans to list candidate shadow tables and --execute to drop them.
    """
    from drt.config.parser import load_syncs_safe
    from drt.destinations.base import OrphanCleanup

    if not orphans:
        return

    config_path = Path(config) if config != "drt.yml" else Path(".")
    result = load_syncs_safe(config_path)

    for sync in result.syncs:
        dest = get_destination(sync)
        if not isinstance(dest, OrphanCleanup):
            continue

        if not hasattr(sync.destination, "table"):
            continue
        base_table = sync.destination.table  # type: ignore[union-attr]
        orphan_tables = dest.list_orphan_swap_tables(sync.destination, base_table)

        if not orphan_tables:
            typer.echo("No orphan swap tables found.")
            continue

        if execute:
            dropped, failed = dest.drop_orphan_swap_tables(sync.destination, orphan_tables)
            typer.echo(f"Dropped: {len(dropped)} orphan swap table(s).")
            if failed:
                typer.echo(f"Failed: {len(failed)} orphan swap table(s).")
        else:
            typer.echo(f"Found {len(orphan_tables)} orphan swap table(s).")
            for table in orphan_tables:
                typer.echo(f"[DRY RUN] Would drop: {table}")
            typer.echo("Run with --execute to apply.")
