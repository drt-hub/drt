"""``drt plugins`` — inspect installed entry-point extensions.

Reports every ``drt.*`` entry point discovered, which distribution shipped it,
and whether its registration callable ran. Connector groups (``drt.sources`` /
``drt.destinations``) used to be reported distinctly because a registered
connector still could not be named in a sync YAML; #997 closed that, so a
loaded connector is now reported like any other loaded plugin.
"""

from __future__ import annotations

import typer

from drt.cli._app import app
from drt.cli.output import console
from drt.plugins import load_plugins

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
    # No force=True: the root callback already ran load_plugins() once for
    # this process. Re-invoking would call every registration callable a
    # second time — secret-provider and connector registrars reject
    # duplicate registration (a legitimately loaded plugin would report as
    # an error), and register_extra_observer() is cumulative (an observer
    # would fire twice per event). Reuse the cached result instead.
    entries = load_plugins()

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
                # Was `e.group not in CONNECTOR_GROUPS`: a connector used to
                # register successfully and still be unnameable in a sync YAML.
                # #997 closed that, so the only thing that makes a plugin
                # unusable now is failing to load at all.
                #
                # `e.loaded`, not `e.error is None`: DiscoveredPlugin defaults to
                # loaded=False/error=None, which is exactly what discover_plugins()
                # returns for an entry point it never invoked — that must not read
                # as usable. It also keeps this in step with the table below, which
                # branches on `e.error` and would otherwise disagree whenever an
                # exception stringifies empty (`str(MyError())` == "").
                "usable_in_sync_yaml": e.loaded,
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
        if not e.loaded:
            # `not e.loaded` rather than `if e.error:` so an exception that
            # stringifies empty still renders as a failure instead of green.
            status = f"[red]error: {e.error}[/red]" if e.error else "[red]not loaded[/red]"
        else:
            # No connector special case since #997 — a registered source or
            # destination type is nameable in a sync YAML like any built-in.
            status = "[green]loaded[/green]"
        table.add_row(
            e.group, e.name, e.dist_name or "?", e.dist_version or "?", e.author or "?", status
        )

    console.print(table)
