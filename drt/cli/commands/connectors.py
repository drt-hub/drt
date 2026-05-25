"""`drt sources` and `drt destinations` — list available connectors.

Both commands share the same Rich-table / Rich-panel / JSON output
helpers so they live together. Phase 2 of #546 split this pair out
of ``drt/cli/main.py``.
"""

from __future__ import annotations

import typer

from drt.cli._app import app
from drt.cli.output import console


def _print_connectors_table(title: str, connectors: list[tuple[str, str]]) -> None:
    """Print connectors in a rich table."""
    from rich.table import Table

    console.print(f"\n[bold]{title}[/bold]\n")
    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Type", style="cyan")
    table.add_column("Description", style="green")

    for connector_type, description in connectors:
        table.add_row(connector_type, description)

    console.print(table)
    console.print()


def _print_connector_details(title: str, connectors: list[tuple[str, str]], kind: str) -> None:
    """Print one Rich panel per connector with derived field detail."""
    from rich.panel import Panel

    from drt.cli._connector_detail import (
        build_destination_detail,
        build_source_detail,
    )

    builder = build_source_detail if kind == "source" else build_destination_detail
    console.print(f"\n[bold]{title}[/bold]")
    for connector_type, description in connectors:
        detail = builder(connector_type, description)
        body_lines: list[str] = []
        if detail.required_env_vars:
            joined = ", ".join(detail.required_env_vars)
            body_lines.append(f"[bold]Required env vars:[/bold] {joined}")
        if detail.required_fields:
            joined = ", ".join(detail.required_fields)
            body_lines.append(f"[bold]Required fields:[/bold] {joined}")
        if detail.optional_env_vars:
            joined = ", ".join(detail.optional_env_vars)
            body_lines.append(f"[bold]Optional env vars:[/bold] {joined}")
        body_lines.append("")
        body_lines.append("[bold]Sample YAML:[/bold]")
        body_lines.append(detail.sample_yaml)
        console.print(
            Panel(
                "\n".join(body_lines),
                title=f"[cyan]{detail.type}[/cyan] — {detail.display_name}",
                border_style="cyan",
                expand=False,
            )
        )
    console.print()


def _emit_connectors_json(connectors: list[tuple[str, str]], kind: str, *, detailed: bool) -> None:
    """Emit machine-readable JSON for ``--format json`` consumers."""
    import json as _json

    if detailed:
        from drt.cli._connector_detail import (
            build_destination_detail,
            build_source_detail,
        )

        builder = build_source_detail if kind == "source" else build_destination_detail
        payload: list[dict[str, object]] = [builder(t, d).to_dict() for t, d in connectors]
    else:
        payload = [{"type": t, "display_name": d, "kind": kind} for t, d in connectors]
    # Use plain print(), not console.print() — Rich wraps long lines at the
    # terminal width and would corrupt the JSON for machine consumers.
    print(_json.dumps({"connectors": payload}, indent=2))


@app.command()
def sources(
    detailed: bool = typer.Option(
        False, "--detailed", help="Print per-connector fields, env vars, and sample YAML."
    ),
    output: str = typer.Option(
        "table", "--format", "-o", help="Output format: 'table' (default) or 'json'."
    ),
) -> None:
    """List available source connectors."""
    from drt.config.connectors import SOURCES

    if output == "json":
        _emit_connectors_json(SOURCES, "source", detailed=detailed)
        return
    if detailed:
        _print_connector_details("Available sources:", SOURCES, "source")
        return
    _print_connectors_table("Available sources:", SOURCES)


@app.command()
def destinations(
    detailed: bool = typer.Option(
        False, "--detailed", help="Print per-connector fields, env vars, and sample YAML."
    ),
    output: str = typer.Option(
        "table", "--format", "-o", help="Output format: 'table' (default) or 'json'."
    ),
) -> None:
    """List available destination connectors."""
    from drt.config.connectors import DESTINATIONS

    if output == "json":
        _emit_connectors_json(DESTINATIONS, "destination", detailed=detailed)
        return
    if detailed:
        _print_connector_details("Available destinations:", DESTINATIONS, "destination")
        return
    _print_connectors_table("Available destinations:", DESTINATIONS)
