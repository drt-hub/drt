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
    from drt.destinations.file import FileDestination
    from drt.destinations.github_actions import GitHubActionsDestination
    from drt.destinations.google_ads import GoogleAdsDestination
    from drt.destinations.google_sheets import GoogleSheetsDestination
    from drt.destinations.hubspot import HubSpotDestination
    from drt.destinations.jira import JiraDestination
    from drt.destinations.linear import LinearDestination
    from drt.destinations.mysql import MySQLDestination
    from drt.destinations.notion import NotionDestination
    from drt.destinations.parquet import ParquetDestination
    from drt.destinations.postgres import PostgresDestination
    from drt.destinations.rest_api import RestApiDestination
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
# run
# ---------------------------------------------------------------------------


@app.command()
def run(
    select: str = typer.Option(None, "--select", "-s", help="Run a specific sync by name."),
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
) -> None:
    """Run sync(s) defined in the project."""
    import json as json_mod

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
            )
        except Exception as e:
            elapsed = round(time.monotonic() - t0, 2)
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
            if json_mode:
                json_results.append(
                    {
                        "name": sync.name,
                        "status": "failed",
                        "rows_synced": 0,
                        "rows_failed": 0,
                        "duration_seconds": elapsed,
                        "dry_run": dry_run,
                        "error": str(e),
                    }
                )
            else:
                print_error(f"[{sync.name}] Unexpected error: {e}")
            had_errors = True
            continue
        elapsed = round(time.monotonic() - t0, 2)
        if log_format == "json":
            status_str = (
                "success" if result.failed == 0 else "partial" if result.success > 0 else "failed"
            )
            logging.info(
                "sync_complete",
                extra={
                    "sync": sync.name,
                    "rows": result.success,
                    "duration_ms": round(elapsed * 1000),
                    "status": status_str,
                },
            )
        if json_mode:
            json_results.append(
                {
                    "name": sync.name,
                    "status": (
                        "success"
                        if result.failed == 0
                        else "partial"
                        if result.success > 0
                        else "failed"
                    ),
                    "rows_extracted": result.rows_extracted,
                    "rows_synced": result.success,
                    "rows_failed": result.failed,
                    "duration_seconds": elapsed,
                    "dry_run": dry_run,
                }
            )
        else:
            if dry_run:
                print_dry_run_summary(sync, profile, result.success)
            else:
                print_sync_result(sync.name, result, elapsed)
        if result.failed > 0:
            had_errors = True
            if not json_mode and verbose and result.row_errors:
                print_row_errors(result.row_errors)

    if json_mode:
        print(
            json_mod.dumps(
                {
                    "syncs": json_results,
                    "total_duration_seconds": round(time.monotonic() - t_total, 2),
                },
                indent=2,
            )
        )

    if had_errors:
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
    select: str = typer.Option(None, "--select", "-s", help="Test a specific sync by name."),
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
                f"tests not supported for {sync.destination.type} destinations",
            )
            continue

        table = get_table_name(sync.destination)
        for test_def in sync.tests:
            test_name = _test_display_name(test_def)
            try:
                query, check = build_test_query(test_def, table)
                result_val = execute_test_query(sync.destination, query)
                passed = check(result_val)
                print_test_result(test_name, passed, str(result_val))
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
    | LinearDestination
    | GoogleAdsDestination
    | NotionDestination
    | StagedUploadDestination
    | TwilioDestination
):
    from drt.config.models import (
        ClickHouseDestinationConfig,
        DiscordDestinationConfig,
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
    raise ValueError(f"Unsupported destination type: {dest.type}")
