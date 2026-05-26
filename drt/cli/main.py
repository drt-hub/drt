"""drt CLI entry point."""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

import typer

if TYPE_CHECKING:
    from drt.config.credentials import ProfileConfig
    from drt.config.models import SyncConfig
    from drt.config.secrets import SecretFinding
    from drt.destinations.base import Destination
    from drt.sources.base import Source
    from drt.state.history import HistoryManager
    from drt.state.manager import StateManager


from drt import __version__
from drt.cli import commands as _commands  # noqa: F401 — register commands

# The shared Typer instance lives in drt.cli._app so that per-command
# modules under drt/cli/commands/ can import it without circular imports
# (this main module then imports the commands package to trigger their
# @app.command decorator side effects).
from drt.cli._app import app
from drt.cli.output import (
    console,
    print_dry_run_summary,
    print_error,
    print_row_errors,
    print_status_table,
    print_status_verbose,
    print_sync_result,
    print_sync_start,
    print_test_header,
    print_test_result,
    print_test_skip,
    print_validation_error,
    print_validation_ok,
)


class LogFormat(str, Enum):
    """Output format for application logs (separate from --output)."""

    TEXT = "text"
    JSON = "json"


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


@dataclass
class _RunContext:
    """Shared context for executing a single sync within ``run()``."""

    source: Source
    state_mgr: StateManager
    history_mgr: HistoryManager | None
    history_retention_days: int
    json_mode: bool
    dry_run: bool
    verbose: bool
    quiet: bool
    log_json: bool
    cursor_value: str | None
    # Cooperative shutdown flag — set by SIGTERM/SIGINT handler in run().
    # Each engine call checks this between batches and exits gracefully.
    stop_event: threading.Event | None = None
    # Diff preview (#413) — when both dry_run and compute_diff are True,
    # the engine populates result.diff for the renderer to display.
    compute_diff: bool = False
    diff_limit: int = 20


def _exit_code_for_signal(signum: int) -> int:
    """POSIX convention: 128 + signal number (SIGINT=2 → 130, SIGTERM=15 → 143)."""
    return 128 + signum


def _run_one(
    sync: SyncConfig,
    ctx: _RunContext,
    profile: ProfileConfig,
) -> tuple[str, dict[str, object], bool]:
    """Execute a single sync and return (name, result_dict, had_error)."""
    from drt import telemetry
    from drt.engine.observer import (
        CompositeObserver,
        LoggingObserver,
        StatePersistingObserver,
    )
    from drt.engine.sync import run_sync

    dest = _get_destination(sync)
    wm_storage = _get_watermark_storage(sync, Path("."))
    # Compose the engine's default observer surface: logging events to the
    # ``drt`` logger (legacy parity) + state/watermark persistence on
    # sync_completed. The engine itself no longer reaches for state directly
    # (#548); CLI is responsible for wiring this up.
    observer = CompositeObserver(
        [LoggingObserver(), StatePersistingObserver(ctx.state_mgr, wm_storage)]
    )
    if not ctx.json_mode and not ctx.dry_run and not ctx.quiet:
        print_sync_start(sync.name, ctx.dry_run)
    t0 = time.monotonic()
    if ctx.log_json:
        logging.info("sync_started", extra={"sync": sync.name})

    status_str = "failed"
    rows_synced = 0
    elapsed = 0.0
    return_value: tuple[str, dict[str, object], bool]
    try:
        try:
            result = run_sync(
                sync,
                ctx.source,
                dest,
                profile,
                Path("."),
                ctx.dry_run,
                ctx.state_mgr,
                watermark_storage=wm_storage,
                cursor_value_override=(
                    ctx.cursor_value if sync.sync.mode == "incremental" else None
                ),
                history_manager=ctx.history_mgr,
                history_retention_days=ctx.history_retention_days,
                stop_event=ctx.stop_event,
                compute_diff=ctx.compute_diff,
                diff_limit=ctx.diff_limit,
                observer=observer,
            )
        except Exception as e:
            from drt.cli.errors import format_error, render_to_console

            elapsed = round(time.monotonic() - t0, 2)
            fe = format_error(sync.name, e)
            entry: dict[str, object] = {
                "name": sync.name,
                "status": "failed",
                "rows_synced": 0,
                "rows_failed": 0,
                "duration_seconds": elapsed,
                "dry_run": ctx.dry_run,
                # Preserve `error` for backwards compatibility with JSON
                # consumers that already parse it. Add structured siblings
                # for new consumers (stage, error_type, error_suggestion).
                "error": str(e),
                "error_type": fe.error_type,
                "error_stage": fe.stage.value,
                "error_suggestion": fe.suggestion,
            }
            if ctx.log_json:
                logging.error(
                    "sync_complete",
                    extra={
                        "sync": sync.name,
                        "rows": 0,
                        "duration_ms": round(elapsed * 1000),
                        "status": "failed",
                        "error_stage": fe.stage.value,
                        "error_type": fe.error_type,
                    },
                )
            if not ctx.json_mode:
                render_to_console(fe)
            return_value = (sync.name, entry, True)
            return return_value

        elapsed = round(time.monotonic() - t0, 2)
        status_str = (
            "success" if result.failed == 0 else "partial" if result.success > 0 else "failed"
        )
        rows_synced = result.success
        entry = {
            "name": sync.name,
            "status": status_str,
            "rows_extracted": result.rows_extracted,
            "rows_synced": result.success,
            "rows_failed": result.failed,
            "duration_seconds": elapsed,
            "dry_run": ctx.dry_run,
        }
        if result.watermark_source:
            entry["watermark_source"] = result.watermark_source
        if result.cursor_value_used is not None:
            entry["cursor_value_used"] = result.cursor_value_used
        if ctx.log_json:
            logging.info(
                "sync_complete",
                extra={
                    "sync": sync.name,
                    "rows": result.success,
                    "duration_ms": round(elapsed * 1000),
                    "status": status_str,
                },
            )
        if not ctx.json_mode and not ctx.quiet:
            if ctx.dry_run:
                print_dry_run_summary(sync, profile, result.success, dest)
            else:
                print_sync_result(sync.name, result, elapsed)
        if not ctx.json_mode and ctx.verbose and not ctx.quiet and result.row_errors:
            print_row_errors(result.row_errors)
        diff_value = getattr(result, "diff", None)
        if diff_value is not None:
            if ctx.json_mode:
                from drt.cli.output import diff_to_dict

                entry["diff"] = diff_to_dict(diff_value)
            elif not ctx.quiet:
                from drt.cli.output import print_diff_table

                print_diff_table(diff_value, sync.name)
        return_value = (sync.name, entry, result.failed > 0)
        return return_value
    finally:
        if not ctx.dry_run:
            telemetry.track_sync_completed(
                sync_mode=sync.sync.mode,
                source_type=profile.type,
                destination_type=sync.destination.type,
                rows_synced=rows_synced,
                duration_seconds=elapsed,
                status=status_str,
            )


def _print_watermark_summary(results: list[dict[str, object]]) -> None:
    """Print notes about watermark sources used during a run."""
    default_syncs = [e for e in results if e.get("watermark_source") == "default_value"]
    override_syncs = [e for e in results if e.get("watermark_source") == "cli_override"]
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
        import platform
        import sys

        import drt as drt_pkg

        # First line stays `drt version X.Y.Z` so scripts grepping for that
        # pattern keep working. The follow-up lines are diagnostic context
        # that saves a round-trip on bug reports.
        py = sys.version_info
        impl = platform.python_implementation()
        install_path = Path(drt_pkg.__file__).resolve().parent
        plat = f"{platform.system()} {platform.release()} ({platform.machine()})"

        console.print(f"drt version {__version__}")
        console.print(f"Python {py.major}.{py.minor}.{py.micro} ({impl})")
        console.print(f"Install: {install_path}")
        console.print(f"Platform: {plat}")
        raise typer.Exit()


@app.callback()
def main(
    version: bool = typer.Option(
        False, "--version", "-v", callback=version_callback, is_eager=True, help="Show version."
    ),
) -> None:
    pass


# `drt init` lives in drt/cli/commands/init.py (#546 Phase 2)
# `drt sources` / `drt destinations` live in drt/cli/commands/connectors.py
# `drt clean` lives in drt/cli/commands/clean.py


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
    quiet: bool = typer.Option(
        False,
        "--quiet",
        "-q",
        help="Suppress output except errors. Wins over --verbose.",
    ),
    output: str = typer.Option("text", "--output", "-o", help="Output format: text or json."),
    profile_name: str = typer.Option(
        None, "--profile", "-p", help="Override profile (default: drt_project.yml or DRT_PROFILE)."
    ),
    log_format: LogFormat = typer.Option(
        LogFormat.TEXT,
        "--log-format",
        help=(
            "Log format: 'text' (default) or 'json' (structured JSON Lines,"
            " separate from --output json)."
        ),
    ),
    cursor_value: str = typer.Option(
        None,
        "--cursor-value",
        help="Override cursor/watermark value for incremental syncs (backfill/recovery).",
    ),
    diff: bool = typer.Option(
        False,
        "--diff",
        help=(
            "When combined with --dry-run, show record-level diff (added/"
            "updated/deleted) for queryable destinations or a sample of "
            "records to send for non-queryable destinations."
        ),
    ),
    diff_limit: int = typer.Option(
        20,
        "--diff-limit",
        help="Maximum number of records to show per diff category (default 20).",
    ),
) -> None:
    """Run sync(s) defined in the project.

    Without --select, runs all syncs sequentially (existing behaviour).
    Use --select to filter by name or tag (e.g. --select tag:crm).
    Use --select "*" or --select all to be explicit about running every sync.
    Use --threads N for parallel execution.
    Use --dry-run --diff to preview record-level changes (#413).

    Examples:
      drt run
      drt run --select post_users
      drt run --select tag:crm --threads 4
      drt run --dry-run --diff
    """
    if diff and not dry_run:
        print_error("--diff requires --dry-run")
        raise typer.Exit(1)
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from drt.config.credentials import load_profile
    from drt.config.parser import load_project, load_syncs
    from drt.state.manager import StateManager

    if log_format is LogFormat.JSON:
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

    # Resolve history config from project file (optional, defaults to enabled).
    from drt.config.parser import load_project
    from drt.state.history import HistoryManager

    history_cfg = load_project(Path(".")).history
    history_mgr = HistoryManager(Path(".")) if history_cfg.enabled else None

    json_results: list[dict[str, object]] = []
    t_total = time.monotonic()
    succeeded = 0
    failed = 0

    # Cooperative graceful shutdown for SIGTERM/SIGINT (#279).
    # Signals are delivered to the main thread by Python; the engine checks
    # stop_event between batches so the current batch always finishes cleanly,
    # state is persisted, and then we exit. A 30s watchdog forces _exit if
    # the current batch hangs (e.g. an unresponsive destination).
    stop_event = threading.Event()
    received_signal: dict[str, int | None] = {"sig": None}
    force_timer: dict[str, threading.Timer | None] = {"t": None}

    def _on_signal(signum: int, _frame: Any) -> None:
        if received_signal["sig"] is not None:
            return  # idempotent — second signal is a no-op
        received_signal["sig"] = signum
        stop_event.set()
        if not json_mode and not quiet:
            console.print(
                f"\n[yellow]Graceful shutdown requested "
                f"({signal.Signals(signum).name}). "
                f"Finishing current batch — force-exit in 30s.[/yellow]"
            )
        # Watchdog: if shutdown takes > 30s, hard-exit.
        timer = threading.Timer(30.0, lambda: os._exit(_exit_code_for_signal(signum)))
        timer.daemon = True
        timer.start()
        force_timer["t"] = timer

    signal.signal(signal.SIGINT, _on_signal)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _on_signal)

    ctx = _RunContext(
        source=source,
        state_mgr=state_mgr,
        history_mgr=history_mgr,
        history_retention_days=history_cfg.retention_days,
        json_mode=json_mode,
        dry_run=dry_run,
        verbose=verbose,
        quiet=quiet,
        log_json=log_format is LogFormat.JSON,
        cursor_value=cursor_value,
        stop_event=stop_event,
        compute_diff=diff,
        diff_limit=diff_limit,
    )

    # Execute syncs — parallel if threads > 1, sequential otherwise
    if threads > 1 and len(syncs) > 1:
        if not json_mode and not quiet:
            console.print(f"[dim]Running {len(syncs)} syncs with {threads} threads[/dim]\n")
        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = {pool.submit(_run_one, s, ctx, profile): s for s in syncs}
            for future in as_completed(futures):
                name, entry, had_err = future.result()
                json_results.append(entry)
                if had_err:
                    failed += 1
                else:
                    succeeded += 1
    else:
        for sync in syncs:
            name, entry, had_err = _run_one(sync, ctx, profile)
            json_results.append(entry)
            if had_err:
                failed += 1
            else:
                succeeded += 1

    total_duration = round(time.monotonic() - t_total, 2)

    # Summary report
    if not json_mode and not quiet and len(syncs) > 1:
        console.print(
            f"\n[bold]Summary:[/bold] {succeeded} succeeded, {failed} failed, "
            f"{total_duration}s total"
        )

    if not json_mode and not quiet:
        _print_watermark_summary(json_results)

    if json_mode:
        print(
            json.dumps(
                {
                    "syncs": json_results,
                    "succeeded": succeeded,
                    "failed": failed,
                    "total_duration_seconds": total_duration,
                },
                indent=2,
            )
        )

    # Graceful shutdown path (#279) takes precedence over the failure exit
    # code: even if some syncs reported failures before the signal arrived,
    # the operator's intent was "stop now", and the SIGTERM/SIGINT exit code
    # carries that information.
    if received_signal["sig"] is not None:
        if force_timer["t"] is not None:
            force_timer["t"].cancel()
        if not json_mode and not quiet:
            console.print(
                f"[yellow]Stopped after {succeeded + failed} sync(s). State persisted.[/yellow]"
            )
        raise typer.Exit(_exit_code_for_signal(received_signal["sig"]))

    if failed > 0:
        raise typer.Exit(1)


# `drt list` lives in drt/cli/commands/list_syncs.py (#546)


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


@app.command()
def validate(
    select: str = typer.Option(None, "--select", "-s", help="Validate a specific sync by name."),
    emit_schema: bool = typer.Option(  # noqa: E501
        False, "--emit-schema", help="Write JSON Schemas to .drt/schemas/."
    ),
    check_connection: bool = typer.Option(
        False, "--check-connection", help="Test connectivity to SQL destinations."
    ),
    output: str = typer.Option("text", "--output", "-o", help="Output format: text or json."),
    strict: bool = typer.Option(False, "--strict", help="Treat warnings as validation errors."),
) -> None:
    """Validate sync definitions against the JSON Schema.

    Examples:
      drt validate
      drt validate --select post_users
      drt validate --emit-schema
      drt validate --strict
    """

    from drt.config.parser import load_syncs_safe
    from drt.config.schema import write_schemas
    from drt.config.secrets import find_hardcoded_secrets

    result = load_syncs_safe(Path("."))
    secret_findings = find_hardcoded_secrets(Path("."))

    if select:
        result.syncs = [s for s in result.syncs if s.name == select]
        result.errors = {k: v for k, v in result.errors.items() if k == select}
        result.deprecations = {k: v for k, v in result.deprecations.items() if k == select}
        secret_findings = [finding for finding in secret_findings if finding.sync_name == select]
        if not result.syncs and not result.errors:
            print_error(f"No sync named '{select}' found.")
            raise typer.Exit(1)

    secret_warnings_by_sync = _group_secret_findings(secret_findings)

    if output == "json":
        # Collect all deprecations into a flat list for JSON output
        all_deprecations = []
        for sync_name, sync_deprecations in result.deprecations.items():
            all_deprecations.extend(sync_deprecations)

        results_json = []
        for s in result.syncs:
            entry = {
                "name": s.name,
                "valid": True,
                "deprecations": result.deprecations.get(s.name, []),
                "warnings": [
                    finding.to_dict() for finding in secret_warnings_by_sync.get(s.name, [])
                ],
            }
            if strict and entry["warnings"]:
                entry["valid"] = False
                entry["errors"] = [
                    finding.message for finding in secret_warnings_by_sync.get(s.name, [])
                ]
            if check_connection:
                entry["connection_test"] = _run_connection_test(s)
            results_json.append(entry)

        for name, errs in result.errors.items():
            results_json.append(
                {
                    "name": name,
                    "valid": False,
                    "errors": errs,
                    "warnings": [
                        finding.to_dict() for finding in secret_warnings_by_sync.get(name, [])
                    ],
                }
            )

        print(
            json.dumps(
                {"results": results_json},
                indent=2,
            )
        )
        if result.errors or (strict and secret_findings):
            raise typer.Exit(code=1)
        return

    if not result.syncs and not result.errors:
        console.print("[dim]No syncs found.[/dim]")
        return

    for sync in result.syncs:
        if strict and sync.name in secret_warnings_by_sync:
            continue
        print_validation_ok(sync.name)
        # Print deprecation warnings for this sync
        if sync.name in result.deprecations:
            for deprecation in result.deprecations[sync.name]:
                console.print(
                    f"  [yellow]⚠️  {deprecation['key']} is deprecated "
                    f"(removed in {deprecation['removed_in']})[/yellow]"
                )
                console.print(f"       Use {deprecation['replacement']} instead.")
                if deprecation["docs_link"]:
                    console.print(f"       See {deprecation['docs_link']}")

        for finding in secret_warnings_by_sync.get(sync.name, []):
            console.print(f"  [yellow]WARNING[/yellow] {finding.message}")

        if check_connection:
            from drt.cli.output import print_connection_test_result

            conn_res = _run_connection_test(sync)
            print_connection_test_result(
                sync.name,
                success=conn_res["success"],
                error=conn_res["error"],
            )

    for name, errors in result.errors.items():
        print_validation_error(name, errors)

    if strict:
        for name, findings in secret_warnings_by_sync.items():
            print_validation_error(name, [finding.message for finding in findings])

    if result.errors or (strict and secret_findings):
        raise typer.Exit(code=1)

    if emit_schema:
        schema_dir = Path(".") / ".drt" / "schemas"
        written = write_schemas(schema_dir)
        console.print(f"\n[dim]Schemas written to {schema_dir}/[/dim]")
        for p in written:
            console.print(f"  {p}")


def _group_secret_findings(
    findings: list[SecretFinding],
) -> dict[str, list[SecretFinding]]:
    grouped: dict[str, list[SecretFinding]] = {}
    for finding in findings:
        grouped.setdefault(finding.sync_name, []).append(finding)
    return grouped


def _run_connection_test(sync: SyncConfig) -> dict[str, Any]:
    """Internal helper to test connectivity for a sync's destination."""
    from drt.config.models import (
        ClickHouseDestinationConfig,
        MySQLDestinationConfig,
        PostgresDestinationConfig,
        SnowflakeDestinationConfig,
    )
    from drt.connectors.registry import get_destination
    from drt.destinations.base import ConnectionTestable

    dest_config = sync.destination
    is_sql = isinstance(
        dest_config,
        (
            PostgresDestinationConfig,
            MySQLDestinationConfig,
            ClickHouseDestinationConfig,
            SnowflakeDestinationConfig,
        ),
    )

    if not is_sql:
        return {"success": None, "error": None, "skipped": True}

    try:
        dest = get_destination(dest_config)
        if isinstance(dest, ConnectionTestable):
            dest.test_connection(dest_config)
            return {"success": True, "error": None, "skipped": False}
        else:
            return {
                "success": False,
                "error": "test_connection method missing",
                "skipped": False,
            }
    except Exception as e:
        return {"success": False, "error": str(e), "skipped": False}


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


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


# `drt doctor` lives in drt/cli/commands/doctor.py (#546)


# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------


class _SyncTestResult(TypedDict, total=False):
    """Type hint for test result dict in JSON output."""

    sync: str
    tests: list[dict[str, object]]
    skipped: bool
    reason: str


@app.command(name="test")
def test_syncs(
    output: str = typer.Option("text", "--output", "-o", help="Output format: text or json."),
    select: str = typer.Option(None, "--select", "-s", help="Test a specific sync by name."),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview without running tests."),
) -> None:
    """Run post-sync validation tests.

    With --dry-run, shows what tests would be executed without actually
    connecting to the destination or running queries.
    """
    from drt.config.parser import load_syncs
    from drt.destinations.query import (
        execute_test_query,
        get_table_name,
        is_queryable,
    )
    from drt.engine.test_runner import build_test_query

    json_mode = output == "json"
    results: list[_SyncTestResult] = []

    syncs = load_syncs(Path("."))
    if not syncs:
        if not json_mode:
            console.print("[dim]No syncs found.[/dim]")
        else:
            print(json.dumps({"status": "no_syncs", "results": []}))
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
            print(json.dumps({"status": "no_tests", "results": []}))
        return

    had_failures = False

    for sync in syncs_with_tests:
        if not json_mode:
            print_test_header(sync.name)
        sync_results: _SyncTestResult = {"sync": sync.name, "tests": []}

        if not is_queryable(sync.destination):
            if not json_mode:
                if dry_run:
                    console.print(
                        f"  [dim]⏭ {sync.name}: would be skipped"
                        f" (tests not supported for"
                        f" {sync.destination.type} destinations)[/dim]"
                    )
                else:
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
            if dry_run:
                if not json_mode:
                    console.print(f"  [dim](dry-run)[/dim] {test_name}")
                sync_results["tests"].append({"name": test_name, "dry_run": True})
            else:
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
            json.dumps(
                {
                    "status": "failed" if had_failures else "passed",
                    "results": results,
                    "dry_run": dry_run,
                }
            )
        )
    elif dry_run:
        console.print("\n[dry-run] Preview of tests that would be executed")
    if had_failures:
        raise typer.Exit(1)


def _test_display_name(test_def: object) -> str:
    """Backward-compatible private wrapper — delegates to the public helper."""
    from drt.config.models import SyncTest
    from drt.engine.test_runner import test_display_name

    assert isinstance(test_def, SyncTest)
    return test_display_name(test_def)


# `drt serve` lives in drt/cli/commands/serve.py (#546 Phase 2)


# Sub-Typer namespaces — each one lives in its own module under
# drt/cli/commands/ (#546). Imported via drt.cli.commands package which
# fires the registration decorators.
#
#   `drt config ...`  → drt/cli/commands/config.py
#   `drt cloud ...`   → drt/cli/commands/cloud.py
#   `drt docs ...`    → drt/cli/commands/docs.py
#   `drt mcp ...`     → drt/cli/commands/mcp.py


# ---------------------------------------------------------------------------
# Source / Destination factories — backward-compat shims
# ---------------------------------------------------------------------------
#
# The real implementations now live in ``drt/cli/_helpers.py``. These thin
# wrappers preserve the legacy ``from drt.cli.main import _get_source`` /
# ``_get_destination`` import path that several tests rely on (see #565
# back-compat note). New callers should import directly from _helpers.


def _get_source(profile: ProfileConfig) -> Source:
    """Back-compat shim — see ``drt.cli._helpers.get_source``."""
    from drt.cli._helpers import get_source

    return get_source(profile)


def _get_watermark_storage(sync: SyncConfig, project_dir: Path) -> Any:
    """Back-compat shim — see ``drt.cli._helpers.get_watermark_storage``."""
    from drt.cli._helpers import get_watermark_storage

    return get_watermark_storage(sync, project_dir)


def _get_destination(sync: SyncConfig) -> Destination:
    """Back-compat shim — see ``drt.cli._helpers.get_destination``."""
    from drt.cli._helpers import get_destination

    return get_destination(sync)
