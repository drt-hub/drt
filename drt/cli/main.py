"""drt CLI entry point."""

from __future__ import annotations

import time
from pathlib import Path
from typing import TYPE_CHECKING

import typer

if TYPE_CHECKING:
    from drt.config.credentials import (
        BigQueryProfile,
        ClickHouseProfile,
        DuckDBProfile,
        PostgresProfile,
        RedshiftProfile,
        SnowflakeProfile,
        SQLiteProfile,
    )
    from drt.config.models import SyncConfig
    from drt.destinations.clickhouse import ClickHouseDestination
    from drt.destinations.discord import DiscordDestination
    from drt.destinations.email_smtp import EmailSmtpDestination
    from drt.destinations.file import FileDestination
    from drt.destinations.github_actions import GitHubActionsDestination
    from drt.destinations.google_sheets import GoogleSheetsDestination
    from drt.destinations.hubspot import HubSpotDestination
    from drt.destinations.mysql import MySQLDestination
    from drt.destinations.parquet import ParquetDestination
    from drt.destinations.postgres import PostgresDestination
    from drt.destinations.rest_api import RestApiDestination
    from drt.destinations.slack import SlackDestination
    from drt.destinations.teams import TeamsDestination
    from drt.sources.bigquery import BigQuerySource
    from drt.sources.clickhouse import ClickHouseSource
    from drt.sources.duckdb import DuckDBSource
    from drt.sources.postgres import PostgresSource
    from drt.sources.redshift import RedshiftSource
    from drt.sources.snowflake import SnowflakeSource
    from drt.sources.sqlite import SQLiteSource

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
    print_test_header,
    print_test_result,
    print_test_skip,
    print_validation_error,
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
    output: str = typer.Option(
        "text", "--output", "-o", help="Output format: text or json."
    ),
) -> None:
    """Run sync(s) defined in the project."""
    import json as json_mod

    from drt.config.credentials import load_profile
    from drt.config.parser import load_project, load_syncs
    from drt.engine.sync import run_sync
    from drt.state.manager import StateManager

    json_mode = output == "json"

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
        if not json_mode:
            console.print(
                "[dim]No syncs found in syncs/."
                " Add .yml files to get started.[/dim]"
            )
        raise typer.Exit()

    if select:
        syncs = [s for s in syncs if s.name == select]
        if not syncs:
            print_error(f"No sync named '{select}' found.")
            raise typer.Exit(1)

    source = _get_source(profile)
    state_mgr = StateManager(Path("."))
    had_errors = False
    json_results: list[dict[str, object]] = []
    t_total = time.monotonic()

    for sync in syncs:
        dest = _get_destination(sync)
        if not json_mode:
            print_sync_start(sync.name, dry_run)
        t0 = time.monotonic()
        try:
            result = run_sync(
                sync, source, dest, profile, Path("."), dry_run, state_mgr
            )
        except Exception as e:
            elapsed = round(time.monotonic() - t0, 2)
            if json_mode:
                json_results.append({
                    "name": sync.name,
                    "status": "failed",
                    "rows_synced": 0,
                    "rows_failed": 0,
                    "duration_seconds": elapsed,
                    "dry_run": dry_run,
                    "error": str(e),
                })
            else:
                print_error(f"[{sync.name}] Unexpected error: {e}")
            had_errors = True
            continue
        elapsed = round(time.monotonic() - t0, 2)
        if json_mode:
            json_results.append({
                "name": sync.name,
                "status": (
                    "success" if result.failed == 0
                    else "partial" if result.success > 0
                    else "failed"
                ),
                "rows_synced": result.success,
                "rows_failed": result.failed,
                "duration_seconds": elapsed,
                "dry_run": dry_run,
            })
        else:
            print_sync_result(sync.name, result, elapsed)
        if result.failed > 0:
            had_errors = True
            if not json_mode and verbose and result.row_errors:
                print_row_errors(result.row_errors)

    if json_mode:
        print(json_mod.dumps({
            "syncs": json_results,
            "total_duration_seconds": round(
                time.monotonic() - t_total, 2
            ),
        }, indent=2))

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
    from drt.config.parser import load_syncs_safe
    from drt.config.schema import write_schemas

    result = load_syncs_safe(Path("."))
    if not result.syncs and not result.errors:
        console.print("[dim]No syncs found.[/dim]")
        return

    for sync in result.syncs:
        print_validation_ok(sync.name)

    for name, errors in result.errors.items():
        print_validation_error(name, errors)

    if result.errors:
        raise typer.Exit(code=1)

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
    output: str = typer.Option(
        "text", "--output", "-o", help="Output format: text or json."
    ),
) -> None:
    """Show the status of the most recent sync runs."""
    import json as json_mod

    from drt.state.manager import StateManager

    states = StateManager(Path(".")).get_all()

    if output == "json":
        print(json_mod.dumps({
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
        }, indent=2))
        return

    if verbose:
        print_status_verbose(states, {})
    else:
        print_status_table(states)


# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------


@app.command(name="test")
def test_syncs(
    select: str = typer.Option(
        None, "--select", "-s", help="Test a specific sync by name."
    ),
) -> None:
    """Run post-sync validation tests."""
    from drt.config.parser import load_syncs
    from drt.destinations.query import (
        execute_test_query,
        get_table_name,
        is_queryable,
    )
    from drt.engine.test_runner import build_test_query

    syncs = load_syncs(Path("."))
    if not syncs:
        console.print("[dim]No syncs found.[/dim]")
        return

    if select:
        syncs = [s for s in syncs if s.name == select]
        if not syncs:
            print_error(f"No sync named '{select}' found.")
            raise typer.Exit(1)

    syncs_with_tests = [s for s in syncs if s.tests]
    if not syncs_with_tests:
        console.print("[dim]No tests defined in any sync.[/dim]")
        return

    had_failures = False

    for sync in syncs_with_tests:
        print_test_header(sync.name)

        if not is_queryable(sync.destination):
            print_test_skip(
                sync.name,
                f"tests not supported for {sync.destination.type}"
                " destinations",
            )
            continue

        table = get_table_name(sync.destination)
        for test_def in sync.tests:
            test_name = _test_display_name(test_def)
            try:
                query, check = build_test_query(test_def, table)
                result_val = execute_test_query(
                    sync.destination, query
                )
                passed = check(result_val)
                print_test_result(
                    test_name, passed, str(result_val)
                )
                if not passed:
                    had_failures = True
            except Exception as e:
                print_test_result(test_name, False, str(e))
                had_failures = True

    if had_failures:
        raise typer.Exit(1)


def _test_display_name(test_def: object) -> str:
    """Human-readable name for a test definition."""
    from drt.config.models import SyncTest

    assert isinstance(test_def, SyncTest)
    if test_def.row_count is not None:
        parts = []
        if test_def.row_count.min is not None:
            parts.append(f"min={test_def.row_count.min}")
        if test_def.row_count.max is not None:
            parts.append(f"max={test_def.row_count.max}")
        return f"row_count({', '.join(parts)})"
    if test_def.not_null is not None:
        cols = ", ".join(test_def.not_null.columns)
        return f"not_null({cols})"
    return "unknown"


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
    profile: (
        BigQueryProfile
        | DuckDBProfile
        | SQLiteProfile
        | PostgresProfile
        | RedshiftProfile
        | ClickHouseProfile
        | SnowflakeProfile
    ),
) -> (
    BigQuerySource
    | DuckDBSource
    | SQLiteSource
    | PostgresSource
    | RedshiftSource
    | ClickHouseSource
    | SnowflakeSource
):
    from drt.config.credentials import (
        BigQueryProfile,
        ClickHouseProfile,
        DuckDBProfile,
        PostgresProfile,
        RedshiftProfile,
        SnowflakeProfile,
        SQLiteProfile,
    )
    from drt.sources.bigquery import BigQuerySource
    from drt.sources.duckdb import DuckDBSource
    from drt.sources.postgres import PostgresSource
    from drt.sources.sqlite import SQLiteSource

    if isinstance(profile, BigQueryProfile):
        return BigQuerySource()
    if isinstance(profile, DuckDBProfile):
        return DuckDBSource()
    if isinstance(profile, SQLiteProfile):
        return SQLiteSource()
    if isinstance(profile, PostgresProfile):
        return PostgresSource()
    if isinstance(profile, RedshiftProfile):
        from drt.sources.redshift import RedshiftSource

        return RedshiftSource()
    if isinstance(profile, ClickHouseProfile):
        from drt.sources.clickhouse import ClickHouseSource

        return ClickHouseSource()
    if isinstance(profile, SnowflakeProfile):
        from drt.sources.snowflake import SnowflakeSource

        return SnowflakeSource()
    raise ValueError(f"Unsupported source type: {type(profile)}")


def _get_destination(
    sync: SyncConfig,
) -> (
    RestApiDestination
    | SlackDestination
    | DiscordDestination
    | GitHubActionsDestination
    | HubSpotDestination
    | GoogleSheetsDestination
    | PostgresDestination
    | MySQLDestination
    | TeamsDestination
    | ClickHouseDestination
    | ParquetDestination
    | FileDestination
    | EmailSmtpDestination
):
    from drt.config.models import (
        ClickHouseDestinationConfig,
        DiscordDestinationConfig,
        EmailSmtpDestinationConfig,
        FileDestinationConfig,
        GitHubActionsDestinationConfig,
        GoogleSheetsDestinationConfig,
        HubSpotDestinationConfig,
        MySQLDestinationConfig,
        ParquetDestinationConfig,
        PostgresDestinationConfig,
        RestApiDestinationConfig,
        SlackDestinationConfig,
        TeamsDestinationConfig,
    )
    from drt.destinations.clickhouse import ClickHouseDestination
    from drt.destinations.discord import DiscordDestination
    from drt.destinations.github_actions import GitHubActionsDestination
    from drt.destinations.hubspot import HubSpotDestination
    from drt.destinations.mysql import MySQLDestination
    from drt.destinations.postgres import PostgresDestination
    from drt.destinations.rest_api import RestApiDestination
    from drt.destinations.slack import SlackDestination

    dest = sync.destination
    if isinstance(dest, RestApiDestinationConfig):
        return RestApiDestination()
    if isinstance(dest, SlackDestinationConfig):
        return SlackDestination()
    if isinstance(dest, DiscordDestinationConfig):
        return DiscordDestination()
    if isinstance(dest, GitHubActionsDestinationConfig):
        return GitHubActionsDestination()
    if isinstance(dest, HubSpotDestinationConfig):
        return HubSpotDestination()
    if isinstance(dest, GoogleSheetsDestinationConfig):
        from drt.destinations.google_sheets import GoogleSheetsDestination

        return GoogleSheetsDestination()
    if isinstance(dest, PostgresDestinationConfig):
        return PostgresDestination()
    if isinstance(dest, MySQLDestinationConfig):
        return MySQLDestination()
    if isinstance(dest, TeamsDestinationConfig):
        from drt.destinations.teams import TeamsDestination

        return TeamsDestination()
    if isinstance(dest, ClickHouseDestinationConfig):
        return ClickHouseDestination()
    if isinstance(dest, ParquetDestinationConfig):
        from drt.destinations.parquet import ParquetDestination

        return ParquetDestination()
    if isinstance(dest, FileDestinationConfig):
        from drt.destinations.file import FileDestination

        return FileDestination()
    
    if isinstance(dest, EmailSmtpDestinationConfig):
        from drt.destinations.email_smtp import EmailSmtpDestination
        return EmailSmtpDestination()
    
    raise ValueError(f"Unsupported destination type: {dest.type}")

    
