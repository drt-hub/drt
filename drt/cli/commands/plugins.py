"""``drt plugins`` — list installed third-party plugin entry points (#297).

Discovery covers six ``drt.*`` entry-point groups: sources, destinations,
secret providers, permission checkers, audit loggers, and extra observers.
Connector entries (sources/destinations) are marked distinctly — a
registered connector isn't yet usable from a sync YAML, since
``SyncConfig.destination`` / ``load_profile()`` validate against a closed
set of built-in ``type`` values before the connector registry is ever
consulted. See ``docs/adr/0009-plugin-config-union-blocker.md``.
"""

from __future__ import annotations

import typer

from drt.cli._app import app
from drt.cli.output import console
from drt.plugins import CONNECTOR_GROUPS, load_plugins

plugins_app = typer.Typer(
    name="plugins",
    help="Inspect installed drt plugins (entry-point extensions).",
    no_args_is_help=True,
)
app.add_typer(plugins_app)


@plugins_app.command("list")
def list_plugins(
    output: str = typer.Option(
        "table", "--format", "-o", help="Output format: 'table' (default) or 'json'."
    ),
) -> None:
    """List plugins discovered via drt.* entry points, and whether they loaded."""
    entries = load_plugins(force=True)

    if output == "json":
        import json as _json

        payload = [
            {
                "group": e.group,
                "name": e.name,
                "value": e.value,
                "dist_name": e.dist_name,
                "dist_version": e.dist_version,
                "author": e.author,
                "loaded": e.loaded,
                "error": e.error,
                "usable_in_sync_yaml": e.group not in CONNECTOR_GROUPS,
            }
            for e in entries
        ]
        # Plain print(), not console.print() — Rich would wrap long lines
        # and corrupt the JSON for machine consumers.
        print(_json.dumps({"plugins": payload}, indent=2))
        return

    if not entries:
        console.print(
            "No plugins discovered. Install a package that exposes one of the "
            "drt.* entry-point groups (drt.sources, drt.destinations, "
            "drt.secret_providers, drt.permission_checkers, drt.audit_loggers, "
            "drt.observers)."
        )
        return

    from rich.table import Table

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Group", style="cyan")
    table.add_column("Name", style="green")
    table.add_column("Package")
    table.add_column("Version")
    table.add_column("Author")
    table.add_column("Status")

    for e in entries:
        if e.error:
            status = f"[red]error: {e.error}[/red]"
        elif e.group in CONNECTOR_GROUPS:
            status = "[yellow]registered (not yet usable in sync YAML — see ADR 0009)[/yellow]"
        else:
            status = "[green]loaded[/green]"
        table.add_row(
            e.group, e.name, e.dist_name or "?", e.dist_version or "?", e.author or "?", status
        )

    console.print(table)
