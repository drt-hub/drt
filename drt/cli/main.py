"""drt CLI entry point."""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

import typer

if TYPE_CHECKING:
    from drt.config.credentials import BigQueryProfile, DuckDBProfile, PostgresProfile
    from drt.config.models import SyncConfig
    from drt.destinations.github_actions import GitHubActionsDestination
    from drt.destinations.hubspot import HubSpotDestination
    from drt.destinations.rest_api import RestApiDestination
    from drt.destinations.slack import SlackDestination
    from drt.sources.bigquery import BigQuerySource
    from drt.sources.duckdb import DuckDBSource
    from drt.sources.postgres import PostgresSource

from drt import __version__
from drt.cli.output import (
    console,
    print_error,
    print_init_success,
    print_row_errors,
    print_status_table,
    print_status_verbose,
    print_sync_result,
    print_sync_start,
    print_sync_table,
    print_validation_ok,
)

app = typer.Typer(
    name="drt",
    help="Reverse ETL for the code-first data stack.",
    no_args_is_help=True,
)


def version_callback(value: bool) -> None:
    if value:
        console.print(f"drt version {__version__}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False, "--version", "-v", callback=version_callback, is_eager=True, help="Show version."
    ),
) -> None:
    pass


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------

@app.command()
def init() -> None:
    """Initialize a new drt project in the current directory."""
    from drt.cli.init_wizard import run_wizard, scaffold_project

    try:
        answers = run_wizard()
        created = scaffold_project(answers, Path("."))
        print_init_success(created)
    except (KeyboardInterrupt, typer.Abort):
        console.print("\n[dim]Aborted.[/dim]")
        raise typer.Exit(1)
    except Exception as e:
        print_error(str(e))
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------

@app.command()
def run(
    select: str = typer.Option(None, "--select", "-s", help="Run a specific sync by name."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without writing data."),
    verbose: bool = typer.Option(False, "--verbose", help="Show row-level error details."),
) -> None:
    """Run sync(s) defined in the project."""
    from drt.config.credentials import load_profile
    from drt.config.parser import load_project, load_syncs
    from drt.engine.sync import run_sync
    from drt.state.manager import StateManager

    try:
        project = load_project(Path("."))
    except FileNotFoundError as e:
        print_error(str(e))
        raise typer.Exit(1)

    try:
        profile = load_profile(project.profile)
    except (FileNotFoundError, KeyError, ValueError) as e:
        print_error(str(e))
        raise typer.Exit(1)

    syncs = load_syncs(Path("."))
    if not syncs:
        console.print("[dim]No syncs found in syncs/. Add .yml files to get started.[/dim]")
        raise typer.Exit()

    if select:
        syncs = [s for s in syncs if s.name == select]
        if not syncs:
            print_error(f"No sync named '{select}' found.")
            raise typer.Exit(1)

    source = _get_source(profile)
    state_mgr = StateManager(Path("."))
    had_errors = False

    for sync in syncs:
        dest = _get_destination(sync)
        print_sync_start(sync.name, dry_run)
        t0 = time.monotonic()
        try:
            result = run_sync(sync, source, dest, profile, Path("."), dry_run, state_mgr)  # type: ignore[arg-type]
        except Exception as e:
            print_error(f"[{sync.name}] Unexpected error: {e}")
            had_errors = True
            continue
        print_sync_result(sync.name, result, time.monotonic() - t0)
        if result.failed > 0:
            had_errors = True
            if verbose and result.row_errors:
                print_row_errors(result.row_errors)

    if had_errors:
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

@app.command(name="list")
def list_syncs() -> None:
    """List all sync definitions in the project."""
    from drt.config.parser import load_syncs

    syncs = load_syncs(Path("."))
    print_sync_table(syncs)


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

@app.command()
def validate(
    emit_schema: bool = typer.Option(  # noqa: E501
        False, "--emit-schema", help="Write JSON Schemas to .drt/schemas/."
    ),
) -> None:
    """Validate sync definitions against the JSON Schema."""
    from drt.config.parser import load_syncs
    from drt.config.schema import write_schemas

    syncs = load_syncs(Path("."))
    if not syncs:
        console.print("[dim]No syncs found.[/dim]")
        return

    for sync in syncs:
        # Pydantic already validated on load; reaching here means OK
        print_validation_ok(sync.name)

    if emit_schema:
        schema_dir = Path(".") / ".drt" / "schemas"
        written = write_schemas(schema_dir)
        console.print(f"\n[dim]Schemas written to {schema_dir}/[/dim]")
        for p in written:
            console.print(f"  {p}")


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

@app.command()
def status(
    verbose: bool = typer.Option(False, "--verbose", help="Show row-level error details."),
) -> None:
    """Show the status of the most recent sync runs."""
    from drt.state.manager import StateManager

    states = StateManager(Path(".")).get_all()
    if verbose:
        # row_errors are not persisted in state; show table with placeholder for future extension
        print_status_verbose(states, {})
    else:
        print_status_table(states)


# ---------------------------------------------------------------------------
# mcp
# ---------------------------------------------------------------------------

mcp_app = typer.Typer(name="mcp", help="MCP server commands.", no_args_is_help=True)
app.add_typer(mcp_app)


@mcp_app.command(name="run")
def mcp_run() -> None:
    """Start the drt MCP server (stdio transport).

    Requires: pip install drt-core[mcp]

    Add to Claude Desktop or Cursor:
        {
          "mcpServers": {
            "drt": {
              "command": "uvx",
              "args": ["drt-core[mcp]", "mcp", "run"]
            }
          }
        }
    """
    try:
        from drt.mcp.server import run as mcp_server_run
    except ImportError:
        print_error("MCP server requires: pip install drt-core[mcp]")
        raise typer.Exit(1)

    mcp_server_run()


# ---------------------------------------------------------------------------
# Source / Destination factories
# ---------------------------------------------------------------------------

def _get_source(
    profile: BigQueryProfile | DuckDBProfile | PostgresProfile,
) -> BigQuerySource | DuckDBSource | PostgresSource:
    from drt.config.credentials import BigQueryProfile, DuckDBProfile, PostgresProfile
    from drt.sources.bigquery import BigQuerySource
    from drt.sources.duckdb import DuckDBSource
    from drt.sources.postgres import PostgresSource

    if isinstance(profile, BigQueryProfile):
        return BigQuerySource()
    if isinstance(profile, DuckDBProfile):
        return DuckDBSource()
    if isinstance(profile, PostgresProfile):
        return PostgresSource()
    raise ValueError(f"Unsupported source type: {type(profile)}")


def _get_destination(
    sync: SyncConfig,
) -> RestApiDestination | SlackDestination | GitHubActionsDestination | HubSpotDestination:
    from drt.config.models import (
        GitHubActionsDestinationConfig,
        HubSpotDestinationConfig,
        RestApiDestinationConfig,
        SlackDestinationConfig,
    )
    from drt.destinations.github_actions import GitHubActionsDestination
    from drt.destinations.hubspot import HubSpotDestination
    from drt.destinations.rest_api import RestApiDestination
    from drt.destinations.slack import SlackDestination

    dest = sync.destination
    if isinstance(dest, RestApiDestinationConfig):
        return RestApiDestination()
    if isinstance(dest, SlackDestinationConfig):
        return SlackDestination()
    if isinstance(dest, GitHubActionsDestinationConfig):
        return GitHubActionsDestination()
    if isinstance(dest, HubSpotDestinationConfig):
        return HubSpotDestination()
    raise ValueError(f"Unsupported destination type: {dest.type}")
