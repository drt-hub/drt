"""drt CLI entry point."""

from __future__ import annotations

import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import click
import typer

if TYPE_CHECKING:
    from drt.config.credentials import (
        BigQueryProfile,
        ClickHouseProfile,
        DatabricksProfile,
        DuckDBProfile,
        MySQLProfile,
        PostgresProfile,
        RedshiftProfile,
        SnowflakeProfile,
        SQLiteProfile,
        SQLServerProfile,
    )
    from drt.config.models import SyncConfig
    from drt.destinations.clickhouse import ClickHouseDestination
    from drt.destinations.discord import DiscordDestination
    from drt.destinations.email_smtp import EmailSmtpDestination
    from drt.destinations.file import FileDestination
    from drt.destinations.github_actions import GitHubActionsDestination
    from drt.destinations.google_ads import GoogleAdsDestination
    from drt.destinations.google_sheets import GoogleSheetsDestination
    from drt.destinations.hubspot import HubSpotDestination
    from drt.destinations.intercom import IntercomDestination
    from drt.destinations.jira import JiraDestination
    from drt.destinations.linear import LinearDestination
    from drt.destinations.mysql import MySQLDestination
    from drt.destinations.notion import NotionDestination
    from drt.destinations.parquet import ParquetDestination
    from drt.destinations.postgres import PostgresDestination
    from drt.destinations.rest_api import RestApiDestination
    from drt.destinations.salesforce_bulk import SalesforceBulkDestination
    from drt.destinations.sendgrid import SendGridDestination
    from drt.destinations.slack import SlackDestination
    from drt.destinations.staged_upload import StagedUploadDestination
    from drt.destinations.teams import TeamsDestination
    from drt.destinations.twilio import TwilioDestination
    from drt.sources.bigquery import BigQuerySource
    from drt.sources.clickhouse import ClickHouseSource
    from drt.sources.databricks import DatabricksSource
    from drt.sources.duckdb import DuckDBSource
    from drt.sources.mysql import MySQLSource
    from drt.sources.postgres import PostgresSource
    from drt.sources.redshift import RedshiftSource
    from drt.sources.snowflake import SnowflakeSource
    from drt.sources.sqlite import SQLiteSource
    from drt.sources.sqlserver import SQLServerSource

from drt import __version__
from drt.cli.output import (
    console,
    print_dry_run_summary,
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


# ---------------------------------------------------------------------------
# JSON logging
# ---------------------------------------------------------------------------

_STANDARD_LOG_FIELDS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None)))


class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON object (JSON Lines format)."""

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload: dict[str, object] = {
            "ts": ts,
            "level": record.levelname,
            "msg": record.getMessage(),
        }
        # Merge any extra fields passed via the `extra` kwarg
        for key, value in record.__dict__.items():
            if key not in _STANDARD_LOG_FIELDS and not key.startswith("_"):
                payload[key] = value
        return json.dumps(payload)


def _configure_json_logging() -> None:
    """Replace root logger handlers with a stderr JSON handler."""
    handler = logging.StreamHandler()
    handler.setFormatter(_JsonFormatter())
    logging.root.handlers = [handler]
    logging.root.setLevel(logging.INFO)


def _resolve_profile_name(cli_flag: str | None, project_profile: str) -> str:
    """Resolve which profile to use.

    Precedence: --profile flag > DRT_PROFILE env var > drt_project.yml
    """
    if cli_flag:
        return cli_flag
    env = os.environ.get("DRT_PROFILE")
    if env:
        return env
    return project_profile


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
def init(
    from_dbt: str = typer.Option(
        None,
        "--from-dbt",
        help="Path to dbt manifest.json — generate sync YAMLs from dbt models.",
    ),
) -> None:
    """Initialize a new drt project in the current directory."""
    if from_dbt:
        _init_from_dbt(Path(from_dbt))
        return

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


def _init_from_dbt(manifest_path: Path) -> None:
    """Generate sync YAML scaffolds from dbt manifest.json."""
    import yaml

    from drt.integrations.dbt import list_models_from_manifest

    try:
        models = list_models_from_manifest(manifest_path)
    except FileNotFoundError as e:
        print_error(str(e))
        raise typer.Exit(1)

    if not models:
        console.print("[dim]No models found in manifest.[/dim]")
        return

    console.print(f"\n[bold]Found {len(models)} dbt models:[/bold]\n")
    for i, m in enumerate(models):
        desc = f" — {m.description}" if m.description else ""
        console.print(f"  {i + 1}. {m.name}{desc}")

    console.print("")
    raw = typer.prompt(
        "Select models (comma-separated numbers, or 'all')",
        default="all",
    )

    if raw.strip().lower() == "all":
        selected = models
    else:
        indices = [int(x.strip()) - 1 for x in raw.split(",") if x.strip().isdigit()]
        selected = [models[i] for i in indices if 0 <= i < len(models)]

    if not selected:
        console.print("[dim]No models selected.[/dim]")
        return

    syncs_dir = Path(".") / "syncs"
    syncs_dir.mkdir(exist_ok=True)
    created: list[str] = []

    for model in selected:
        sync_data = {
            "name": f"sync_{model.name}",
            "description": model.description or f"Sync {model.name} to destination",
            "model": f"ref('{model.name}')",
            "destination": {
                "type": "rest_api",
                "url": "https://example.com/api",
                "method": "POST",
            },
        }
        path = syncs_dir / f"sync_{model.name}.yml"
        if path.exists():
            console.print(f"  [dim]skip[/dim] {path} (already exists)")
            continue
        with path.open("w") as f:
            yaml.dump(sync_data, f, default_flow_style=False, sort_keys=False)
        created.append(str(path))

    if created:
        console.print(f"\n[green]Created {len(created)} sync file(s):[/green]")
        for c in created:
            console.print(f"  {c}")
        console.print(
            "\n[dim]Edit the destination config in each file,"
            " then run: drt validate && drt run --dry-run[/dim]"
        )
    else:
        console.print("[dim]No new sync files created.[/dim]")


# ---------------------------------------------------------------------------
# sources
# ---------------------------------------------------------------------------


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


@app.command()
def sources() -> None:
    """List available source connectors."""
    from drt.config.connectors import SOURCES

    _print_connectors_table("Available sources:", SOURCES)


# ---------------------------------------------------------------------------
# destinations
# ---------------------------------------------------------------------------


@app.command()
def destinations() -> None:
    """List available destination connectors."""
    from drt.config.connectors import DESTINATIONS

    _print_connectors_table("Available destinations:", DESTINATIONS)


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


@app.command()
def run(
    select: str = typer.Option(
        None,
        "--select",
        "-s",
        help='Run sync by name, tag (tag:crm), or "*" / "all" for every sync.',
    ),
    threads: int = typer.Option(1, "--threads", "-t", help="Parallel execution threads."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without writing data."),
    verbose: bool = typer.Option(False, "--verbose", help="Show row-level error details."),
    output: str = typer.Option("text", "--output", "-o", help="Output format: text or json."),
    profile_name: str = typer.Option(
        None, "--profile", "-p", help="Override profile (default: drt_project.yml or DRT_PROFILE)."
    ),
    log_format: str = typer.Option(
        "text",
        "--log-format",
        help=(
            "Log format: 'text' (default) or 'json' (structured JSON Lines,"
            " separate from --output json)."
        ),
        click_type=click.Choice(["text", "json"]),
    ),
    cursor_value: str = typer.Option(
        None,
        "--cursor-value",
        help="Override cursor/watermark value for incremental syncs (backfill/recovery).",
    ),
) -> None:
    """Run sync(s) defined in the project.

    Without --select, runs all syncs sequentially (existing behaviour).
    Use --select to filter by name or tag (e.g. --select tag:crm).
    Use --select "*" or --select all to be explicit about running every sync.
    Use --threads N for parallel execution.
    """
    import json as json_mod
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from drt.config.credentials import load_profile
    from drt.config.parser import load_project, load_syncs
    from drt.engine.sync import run_sync
    from drt.state.manager import StateManager

    if log_format == "json":
        _configure_json_logging()

    json_mode = output == "json"

    try:
        project = load_project(Path("."))
    except FileNotFoundError as e:
        print_error(str(e))
        raise typer.Exit(1)

    resolved = _resolve_profile_name(profile_name, project.profile)
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

    if select:
        if select in ("*", "all"):
            # Explicit "run every sync" sentinel — no filtering.
            pass
        elif select.startswith("tag:"):
            tag = select[4:]
            syncs = [s for s in syncs if tag in getattr(s, "tags", [])]
            if not syncs:
                print_error(f"No syncs with tag '{tag}' found.")
                raise typer.Exit(1)
        else:
            syncs = [s for s in syncs if s.name == select]
            if not syncs:
                print_error(f"No sync named '{select}' found.")
                raise typer.Exit(1)

    if cursor_value is not None:
        incremental = [s for s in syncs if s.sync.mode == "incremental"]
        if not incremental:
            print_error(
                "--cursor-value is only valid for incremental syncs,"
                " but no selected syncs are incremental."
            )
            raise typer.Exit(1)
        non_incremental = [s for s in syncs if s.sync.mode != "incremental"]
        if non_incremental and not json_mode:
            console.print(
                f"[yellow]Warning: --cursor-value will be ignored for non-incremental "
                f"syncs: {', '.join(s.name for s in non_incremental)}[/yellow]"
            )

    source = _get_source(profile)
    state_mgr = StateManager(Path("."))
    json_results: list[dict[str, object]] = []
    t_total = time.monotonic()
    succeeded = 0
    failed = 0

    def _run_one(sync: SyncConfig) -> tuple[str, dict[str, object], bool]:
        """Execute a single sync and return (name, result_dict, had_error)."""
        dest = _get_destination(sync)
        wm_storage = _get_watermark_storage(sync, Path("."))
        if not json_mode and not dry_run:
            print_sync_start(sync.name, dry_run)
        t0 = time.monotonic()
        if log_format == "json":
            logging.info("sync_started", extra={"sync": sync.name})
        try:
            result = run_sync(
                sync,
                source,
                dest,
                profile,
                Path("."),
                dry_run,
                state_mgr,
                watermark_storage=wm_storage,
                cursor_value_override=cursor_value if sync.sync.mode == "incremental" else None,
            )
        except Exception as e:
            elapsed = round(time.monotonic() - t0, 2)
            entry = {
                "name": sync.name,
                "status": "failed",
                "rows_synced": 0,
                "rows_failed": 0,
                "duration_seconds": elapsed,
                "dry_run": dry_run,
                "error": str(e),
            }
            if log_format == "json":
                logging.error(
                    "sync_complete",
                    extra={
                        "sync": sync.name,
                        "rows": 0,
                        "duration_ms": round(elapsed * 1000),
                        "status": "failed",
                    },
                )
            if not json_mode:
                print_error(f"[{sync.name}] Unexpected error: {e}")
            return sync.name, entry, True

        elapsed = round(time.monotonic() - t0, 2)
        status_str = (
            "success"
            if result.failed == 0
            else "partial"
            if result.success > 0
            else "failed"
        )
        entry = {
            "name": sync.name,
            "status": status_str,
            "rows_extracted": result.rows_extracted,
            "rows_synced": result.success,
            "rows_failed": result.failed,
            "duration_seconds": elapsed,
            "dry_run": dry_run,
        }
        if result.watermark_source:
            entry["watermark_source"] = result.watermark_source
        if result.cursor_value_used is not None:
            entry["cursor_value_used"] = result.cursor_value_used
        if log_format == "json":
            logging.info(
                "sync_complete",
                extra={
                    "sync": sync.name,
                    "rows": result.success,
                    "duration_ms": round(elapsed * 1000),
                    "status": status_str,
                },
            )
        if not json_mode:
            if dry_run:
                print_dry_run_summary(sync, profile, result.success, dest)
            else:
                print_sync_result(sync.name, result, elapsed)
        if not json_mode and verbose and result.row_errors:
            print_row_errors(result.row_errors)
        return sync.name, entry, result.failed > 0

    # Execute syncs — parallel if threads > 1, sequential otherwise
    if threads > 1 and len(syncs) > 1:
        if not json_mode:
            console.print(f"[dim]Running {len(syncs)} syncs with {threads} threads[/dim]\n")
        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = {pool.submit(_run_one, s): s for s in syncs}
            for future in as_completed(futures):
                name, entry, had_err = future.result()
                json_results.append(entry)
                if had_err:
                    failed += 1
                else:
                    succeeded += 1
    else:
        for sync in syncs:
            name, entry, had_err = _run_one(sync)
            json_results.append(entry)
            if had_err:
                failed += 1
            else:
                succeeded += 1

    total_duration = round(time.monotonic() - t_total, 2)

    # Summary report
    if not json_mode and len(syncs) > 1:
        console.print(f"\n[bold]Summary:[/bold] {succeeded} succeeded, {failed} failed, "
                       f"{total_duration}s total")

    # Watermark source summary (#391)
    if not json_mode:
        default_syncs = [
            e for e in json_results if e.get("watermark_source") == "default_value"
        ]
        override_syncs = [
            e for e in json_results if e.get("watermark_source") == "cli_override"
        ]
        if default_syncs:
            names = ", ".join(str(e["name"]) for e in default_syncs)
            console.print(
                f"\n[yellow]Note: {len(default_syncs)} sync(s) used watermark.default_value "
                f"(first run): {names}[/yellow]"
            )
        if override_syncs:
            names = ", ".join(str(e["name"]) for e in override_syncs)
            console.print(
                f"\n[cyan]Note: {len(override_syncs)} sync(s) used --cursor-value "
                f"override: {names}[/cyan]"
            )

    if json_mode:
        print(
            json_mod.dumps(
                {
                    "syncs": json_results,
                    "succeeded": succeeded,
                    "failed": failed,
                    "total_duration_seconds": total_duration,
                },
                indent=2,
            )
        )

    if failed > 0:
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


@app.command(name="list")
def list_syncs(
    output: str = typer.Option("text", "--output", "-o", help="Output format: text or json."),
) -> None:
    """List all sync definitions in the project."""
    import json as json_mod

    from drt.config.parser import load_syncs

    syncs = load_syncs(Path("."))

    if output == "json":
        print(
            json_mod.dumps(
                {
                    "syncs": [
                        {
                            "name": s.name,
                            "destination_type": s.destination.type,
                            "mode": s.sync.mode,
                            "description": s.description,
                        }
                        for s in syncs
                    ],
                },
                indent=2,
            )
        )
        return

    print_sync_table(syncs)


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


@app.command()
def validate(
    select: str = typer.Option(None, "--select", "-s", help="Validate a specific sync by name."),
    emit_schema: bool = typer.Option(  # noqa: E501
        False, "--emit-schema", help="Write JSON Schemas to .drt/schemas/."
    ),
    output: str = typer.Option("text", "--output", "-o", help="Output format: text or json."),
) -> None:
    """Validate sync definitions against the JSON Schema."""
    import json as json_mod

    from drt.config.parser import load_syncs_safe
    from drt.config.schema import write_schemas

    result = load_syncs_safe(Path("."))

    if select:
        result.syncs = [s for s in result.syncs if s.name == select]
        result.errors = {k: v for k, v in result.errors.items() if k == select}
        if not result.syncs and not result.errors:
            print_error(f"No sync named '{select}' found.")
            raise typer.Exit(1)

    if output == "json":
        print(
            json_mod.dumps(
                {
                    "results": [{"name": s.name, "valid": True} for s in result.syncs]
                    + [
                        {"name": name, "valid": False, "errors": errs}
                        for name, errs in result.errors.items()
                    ],
                },
                indent=2,
            )
        )
        if result.errors:
            raise typer.Exit(code=1)
        return

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
    output: str = typer.Option("text", "--output", "-o", help="Output format: text or json."),
) -> None:
    """Show the status of the most recent sync runs."""
    import json as json_mod

    from drt.state.manager import StateManager

    states = StateManager(Path(".")).get_all()

    if output == "json":
        print(
            json_mod.dumps(
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


# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------


@app.command(name="test")
def test_syncs(
    output: str = typer.Option("text", "--output", "-o", help="Output format: text or json."),
    select: str = typer.Option(None, "--select", "-s", help="Test a specific sync by name."),
) -> None:
    """Run post-sync validation tests."""
    import json as json_mod

    from drt.config.parser import load_syncs
    from drt.destinations.query import (
        execute_test_query,
        get_table_name,
        is_queryable,
    )
    from drt.engine.test_runner import build_test_query

    json_mode = output == "json"
    results = []

    syncs = load_syncs(Path("."))
    if not syncs:
        if not json_mode:
            console.print("[dim]No syncs found.[/dim]")
        else:
            print(json_mod.dumps({"status": "no_syncs", "results": []}))
        return

    if select:
        syncs = [s for s in syncs if s.name == select]
        if not syncs:
            print_error(f"No sync named '{select}' found.")
            raise typer.Exit(1)

    syncs_with_tests = [s for s in syncs if s.tests]
    if not syncs_with_tests:
        if not json_mode:
            console.print("[dim]No tests defined in any sync.[/dim]")
        else:
            print(json_mod.dumps({"status": "no_tests", "results": []}))
        return

    had_failures = False

    for sync in syncs_with_tests:
        if not json_mode:
            print_test_header(sync.name)
        sync_results = {"sync": sync.name, "tests": []}

        if not is_queryable(sync.destination):
            if not json_mode:
                print_test_skip(
                    sync.name,
                    f"tests not supported for {sync.destination.type} destinations",
                )
            sync_results["skipped"] = True
            sync_results["reason"] = f"tests not supported for {sync.destination.type}"
            results.append(sync_results)
            continue

        table = get_table_name(sync.destination)
        for test_def in sync.tests:
            test_name = _test_display_name(test_def)
            try:
                query, check = build_test_query(test_def, table)
                result_val = execute_test_query(sync.destination, query)
                passed = check(result_val)
                if not json_mode:
                    print_test_result(test_name, passed, str(result_val))
                sync_results["tests"].append(
                    {"name": test_name, "passed": passed, "value": str(result_val)}
                )
                if not passed:
                    had_failures = True
            except Exception as e:
                if not json_mode:
                    print_test_result(test_name, False, str(e))
                sync_results["tests"].append(
                    {"name": test_name, "passed": False, "error": str(e)}
                )
                had_failures = True
        
        results.append(sync_results)

    if json_mode:
        print(
            json_mod.dumps(
                {"status": "failed" if had_failures else "passed", "results": results}
            )
        )
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
# serve (webhook trigger)
# ---------------------------------------------------------------------------


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", "--host", help="Host to bind."),
    port: int = typer.Option(8080, "--port", "-p", help="Port to bind."),
    token_env: str = typer.Option(
        "DRT_WEBHOOK_TOKEN",
        "--token-env",
        help="Env var holding bearer token for auth. Empty/unset = no auth.",
    ),
) -> None:
    """Start an HTTP endpoint that triggers drt syncs on demand.

    Example:
        drt serve --port 8080 --token-env DRT_WEBHOOK_TOKEN

        curl -X POST http://localhost:8080/sync/my_sync \\
          -H "Authorization: Bearer $DRT_WEBHOOK_TOKEN"
    """
    from drt.cli.server import serve as serve_impl

    token = os.environ.get(token_env) or None
    serve_impl(host=host, port=port, token=token, project_dir=".")


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
        | MySQLProfile
        | SnowflakeProfile
        | DatabricksProfile
        | SQLServerProfile
    ),
) -> (
    BigQuerySource
    | DuckDBSource
    | SQLiteSource
    | PostgresSource
    | RedshiftSource
    | ClickHouseSource
    | MySQLSource
    | SnowflakeSource
    | DatabricksSource
    | SQLServerSource
):
    from drt.config.credentials import (
        BigQueryProfile,
        ClickHouseProfile,
        DatabricksProfile,
        DuckDBProfile,
        MySQLProfile,
        PostgresProfile,
        RedshiftProfile,
        SnowflakeProfile,
        SQLiteProfile,
        SQLServerProfile,
    )
    from drt.sources.bigquery import BigQuerySource
    from drt.sources.duckdb import DuckDBSource
    from drt.sources.mysql import MySQLSource
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
    if isinstance(profile, MySQLProfile):
        return MySQLSource()
    if isinstance(profile, RedshiftProfile):
        from drt.sources.redshift import RedshiftSource

        return RedshiftSource()
    if isinstance(profile, ClickHouseProfile):
        from drt.sources.clickhouse import ClickHouseSource

        return ClickHouseSource()
    if isinstance(profile, SnowflakeProfile):
        from drt.sources.snowflake import SnowflakeSource

        return SnowflakeSource()
    if isinstance(profile, DatabricksProfile):
        from drt.sources.databricks import DatabricksSource

        return DatabricksSource()
    if isinstance(profile, SQLServerProfile):
        from drt.sources.sqlserver import SQLServerSource

        return SQLServerSource()
    raise ValueError(f"Unsupported source type: {type(profile)}")


def _get_watermark_storage(
    sync: SyncConfig,
    project_dir: Path,
) -> Any:
    """Build watermark storage from sync config, or None if not configured."""
    from drt.state.watermark import (
        BigQueryWatermarkStorage,
        GCSWatermarkStorage,
        LocalWatermarkStorage,
    )

    wm = sync.sync.watermark
    if wm is None:
        return None

    if wm.storage == "local":
        return LocalWatermarkStorage(project_dir)
    elif wm.storage == "gcs":
        assert wm.bucket is not None
        assert wm.key is not None
        return GCSWatermarkStorage(bucket=wm.bucket, key=wm.key)
    elif wm.storage == "bigquery":
        assert wm.project is not None
        assert wm.dataset is not None
        return BigQueryWatermarkStorage(
            project=wm.project,
            dataset=wm.dataset,
        )
    return None


def _get_destination(
    sync: SyncConfig,
) -> (
    RestApiDestination
    | SlackDestination
    | DiscordDestination
    | GitHubActionsDestination
    | HubSpotDestination
    | JiraDestination
    | SendGridDestination
    | GoogleSheetsDestination
    | PostgresDestination
    | MySQLDestination
    | TeamsDestination
    | ClickHouseDestination
    | ParquetDestination
    | FileDestination
    | EmailSmtpDestination
    | LinearDestination
    | GoogleAdsDestination
    | NotionDestination
    | StagedUploadDestination
    | IntercomDestination
    | TwilioDestination
    | SalesforceBulkDestination
):
    from drt.config.models import (
        ClickHouseDestinationConfig,
        DiscordDestinationConfig,
        EmailSmtpDestinationConfig,
        FileDestinationConfig,
        GitHubActionsDestinationConfig,
        GoogleAdsDestinationConfig,
        GoogleSheetsDestinationConfig,
        HubSpotDestinationConfig,
        JiraDestinationConfig,
        LinearDestinationConfig,
        MySQLDestinationConfig,
        NotionDestinationConfig,
        ParquetDestinationConfig,
        PostgresDestinationConfig,
        RestApiDestinationConfig,
        SalesforceBulkDestinationConfig,
        SendGridDestinationConfig,
        SlackDestinationConfig,
        StagedUploadDestinationConfig,
        TeamsDestinationConfig,
        TwilioDestinationConfig,
    )
    from drt.destinations.clickhouse import ClickHouseDestination
    from drt.destinations.discord import DiscordDestination
    from drt.destinations.github_actions import GitHubActionsDestination
    from drt.destinations.hubspot import HubSpotDestination
    from drt.destinations.jira import JiraDestination
    from drt.destinations.linear import LinearDestination
    from drt.destinations.mysql import MySQLDestination
    from drt.destinations.notion import NotionDestination
    from drt.destinations.postgres import PostgresDestination
    from drt.destinations.rest_api import RestApiDestination
    from drt.destinations.sendgrid import SendGridDestination
    from drt.destinations.slack import SlackDestination
    from drt.destinations.twilio import TwilioDestination

    dest = sync.destination
    if isinstance(dest, RestApiDestinationConfig):
        return RestApiDestination()
    if isinstance(dest, SlackDestinationConfig):
        return SlackDestination()
    if isinstance(dest, TwilioDestinationConfig):
        return TwilioDestination()
    if isinstance(dest, DiscordDestinationConfig):
        return DiscordDestination()
    if isinstance(dest, GitHubActionsDestinationConfig):
        return GitHubActionsDestination()
    if isinstance(dest, HubSpotDestinationConfig):
        return HubSpotDestination()
    if isinstance(dest, JiraDestinationConfig):
        return JiraDestination()
    if isinstance(dest, SendGridDestinationConfig):
        return SendGridDestination()
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

    if isinstance(dest, LinearDestinationConfig):
        return LinearDestination()
    if isinstance(dest, GoogleAdsDestinationConfig):
        from drt.destinations.google_ads import GoogleAdsDestination

        return GoogleAdsDestination()
    if isinstance(dest, NotionDestinationConfig):
        return NotionDestination()
    if isinstance(dest, StagedUploadDestinationConfig):
        from drt.destinations.staged_upload import StagedUploadDestination

        return StagedUploadDestination()
    if isinstance(dest, SalesforceBulkDestinationConfig):
        from drt.destinations.salesforce_bulk import SalesforceBulkDestination

        return SalesforceBulkDestination()
    raise ValueError(f"Unsupported destination type: {dest.type}")
