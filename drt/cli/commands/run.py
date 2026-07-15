"""``drt run`` — sync execution command + supporting infrastructure.

Extracted from ``drt/cli/main.py`` in Phase 2b PR (a) of the #546 split
(see #573 for the umbrella). Bundles:

- ``LogFormat`` Enum (the ``--log-format`` option)
- ``_RunContext`` dataclass (shared state for one sync invocation)
- ``_exit_code_for_signal`` (POSIX 128 + signum convention)
- ``_run_one`` (per-sync execution; observer composition; telemetry)
- ``_print_watermark_summary`` (post-run notes about default / override
  watermark usage)
- ``run`` (the @app.command itself; signal handling; parallel/sequential
  dispatch; JSON-mode output)

The JSON Lines logging itself (``_JsonFormatter`` / ``_configure_json_logging``)
was factored out to ``drt.cli._logging`` (#723) and is re-imported here; the
rest stays because ``run`` is their only caller.

Back-compat: ``drt.cli.main`` re-exports each of the underscore-prefixed
names + ``LogFormat`` so that ``from drt.cli.main import _RunContext``
(used by tests + library callers) keeps working.
"""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any

import typer

if TYPE_CHECKING:
    from drt.config.credentials import ProfileConfig
    from drt.config.models import SyncConfig
    from drt.destinations.base import Destination  # noqa: F401 — _RunContext field
    from drt.sources.base import Source
    from drt.state.history import HistoryManager
    from drt.state.manager import StateManager


from drt.cli._app import app
from drt.cli._helpers import (
    get_destination,
    get_source,
    get_watermark_storage,
    resolve_profile_name,
)
from drt.cli._selection import SelectionError, complete_selector, select_syncs
from drt.cli.output import (
    console,
    print_dry_run_summary,
    print_error,
    print_row_errors,
    print_sync_result,
    print_sync_start,
)


class LogFormat(str, Enum):
    """Output format for application logs (separate from --output)."""

    TEXT = "text"
    JSON = "json"


# ---------------------------------------------------------------------------
# JSON logging
# ---------------------------------------------------------------------------

# JSON-lines logging (``--log-format json``) lives in ``drt.cli._logging``.
from drt.cli._logging import _configure_json_logging  # noqa: E402

# ---------------------------------------------------------------------------
# Run context + helpers
# ---------------------------------------------------------------------------


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
    # Sampling (#774) — cap extraction at N rows per sync; watermarks frozen.
    extract_limit: int | None = None
    # Project vars (#783) — resolved `vars:` + DRT_VAR_* + --vars, for var()
    # in model SQL. The YAML side is applied at load_syncs time.
    vars: dict[str, Any] | None = None


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
        DlqObserver,
        LoggingObserver,
        StatePersistingObserver,
    )
    from drt.engine.sync import run_sync

    dest = get_destination(sync)
    wm_storage = get_watermark_storage(sync, Path("."))
    # Compose the engine's default observer surface: logging events to the
    # ``drt`` logger (legacy parity) + state/watermark persistence on
    # sync_completed. The engine itself no longer reaches for state directly
    # (#548); CLI is responsible for wiring this up.
    observers: list[Any] = [
        LoggingObserver(),
        StatePersistingObserver(ctx.state_mgr, wm_storage),
    ]
    # Dead Letter Queue (#278): opt-in per sync. Adds a DlqObserver that
    # persists failed records to .drt/dlq/<sync>.jsonl for `drt retry`.
    # Skipped on dry runs — nothing is actually sent, so nothing can fail.
    if not ctx.dry_run and sync.sync.dlq is not None and sync.sync.dlq.enabled:
        from drt.state.dlq import DlqStore

        observers.append(DlqObserver(DlqStore(Path(".")), max_records=sync.sync.dlq.max_records))
    observer = CompositeObserver(observers)
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
                extract_limit=ctx.extract_limit,
                vars=ctx.vars,
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
        if result.watermark_lag is not None:
            entry["watermark_lag"] = result.watermark_lag
        if result.limit_applied is not None:
            entry["limit"] = result.limit_applied
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
    lag_syncs = [e for e in results if e.get("watermark_lag")]
    if lag_syncs:
        names = ", ".join(f"{e['name']} ({e['watermark_lag']})" for e in lag_syncs)
        console.print(
            f"\n[cyan]Note: {len(lag_syncs)} sync(s) widened the read window via "
            f"watermark.lag (overlap rows re-sent): {names}[/cyan]"
        )


# ---------------------------------------------------------------------------
# @app.command run
# ---------------------------------------------------------------------------


@app.command()
def run(
    select: list[str] = typer.Option(
        None,
        "--select",
        "-s",
        help=(
            "Select syncs: name or glob (users_*), tag:<pattern>, "
            'destination:<type>, or "*" / "all". Repeat to union.'
        ),
        autocompletion=complete_selector,
    ),
    exclude: list[str] = typer.Option(
        None,
        "--exclude",
        help="Subtract syncs from the selection (same grammar as --select). Repeatable.",
        autocompletion=complete_selector,
    ),
    failed_only: bool = typer.Option(
        False,
        "--failed",
        help=(
            "Re-run only syncs whose last recorded status was not success "
            "(intersects with --select/--exclude). Never-run syncs are not included."
        ),
    ),
    limit: int = typer.Option(
        None,
        "--limit",
        help=(
            "Extract at most N rows per sync — a sampled run for safe first sends. "
            "Watermarks do not advance; refused for mirror/replace syncs."
        ),
    ),
    vars_raw: str = typer.Option(
        None,
        "--vars",
        help=(
            "Override project vars for this run, e.g. --vars 'lookback_days: 1, "
            "tag: crm'. Takes precedence over DRT_VAR_* and drt_project.yml vars:."
        ),
    ),
    fail_fast: bool = typer.Option(
        False,
        "--fail-fast",
        help=(
            "Stop scheduling syncs after the first failure; in-flight syncs finish, "
            "remaining ones are reported as skipped."
        ),
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
    --select accepts a sync name or glob, tag:<pattern>, destination:<type>,
    or "*" / "all"; repeat --select to union, --exclude to subtract (#771).
    Use --threads N for parallel execution.
    Use --dry-run --diff to preview record-level changes (#413).

    Examples:
      drt run
      drt run --select post_users
      drt run --select 'users_*' --exclude users_backfill
      drt run --select tag:crm --select tag:ads --threads 4
      drt run --select destination:hubspot
      drt run --failed
      drt run --dry-run --diff
    """
    if diff and not dry_run:
        print_error("--diff requires --dry-run")
        raise typer.Exit(1)
    from concurrent.futures import Future, ThreadPoolExecutor, as_completed

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

    resolved = resolve_profile_name(profile_name, project.profile)
    try:
        profile = load_profile(resolved)
    except (FileNotFoundError, KeyError, ValueError) as e:
        print_error(str(e))
        raise typer.Exit(1)

    # Project vars (#783): `vars:` block < DRT_VAR_* < --vars. Resolved once and
    # used for both the YAML side (load_syncs) and model SQL (via _RunContext).
    from drt.config.vars import VarError, parse_cli_vars, resolve_vars

    try:
        cli_vars = parse_cli_vars(vars_raw) if vars_raw else None
        project_vars = resolve_vars(project.vars, cli_vars)
    except VarError as e:
        print_error(str(e))
        raise typer.Exit(1)

    try:
        syncs = load_syncs(Path("."), vars=project_vars)
    except VarError as e:
        print_error(str(e))
        raise typer.Exit(1)
    if not syncs:
        if not json_mode:
            console.print("[dim]No syncs found in syncs/. Add .yml files to get started.[/dim]")
        raise typer.Exit()

    try:
        syncs = select_syncs(syncs, select, exclude)
    except SelectionError as e:
        print_error(str(e))
        raise typer.Exit(1)
    if not syncs:
        print_error("Selection matched no syncs (after --exclude).")
        raise typer.Exit(1)

    # --failed (#773): sync-level re-run of the previous invocation's
    # failures. Applied after --select/--exclude (intersection semantics).
    # A clean previous state exits 0 — recovery loops shouldn't page when
    # there is nothing to recover. (Record-level replay is `drt retry`.)
    if failed_only:
        state_probe = StateManager(Path("."))

        def _last_run_failed(sync_cfg: SyncConfig) -> bool:
            prev = state_probe.get_last_sync(sync_cfg.name)
            return prev is not None and prev.status != "success"

        syncs = [s for s in syncs if _last_run_failed(s)]
        if not syncs:
            if json_mode:
                print(
                    json.dumps(
                        {
                            "syncs": [],
                            "succeeded": 0,
                            "failed": 0,
                            "note": "nothing_failed",
                        },
                        indent=2,
                    )
                )
            else:
                console.print(
                    "[green]Nothing failed in the previous run — nothing to re-run.[/green]"
                )
            raise typer.Exit(0)
        if not json_mode and not quiet:
            console.print(
                f"[dim]--failed: re-running {len(syncs)} sync(s): "
                f"{', '.join(s.name for s in syncs)}[/dim]"
            )

    # --limit (#774): sampled run guards. A sampled mirror would DELETE the
    # destination rows the sample skipped; a sampled replace would truncate
    # a full table down to N rows. Refuse both outright.
    if limit is not None:
        if limit < 1:
            print_error("--limit must be a positive integer.")
            raise typer.Exit(1)
        guarded = [s.name for s in syncs if s.sync.mode in ("mirror", "replace")]
        if guarded:
            print_error(
                "--limit is not allowed for mode=mirror/replace syncs "
                f"(a sample would delete or replace real rows): {', '.join(guarded)}"
            )
            raise typer.Exit(1)
        if not json_mode and not quiet:
            console.print(
                f"[yellow]--limit {limit}: sampled run — watermarks will not advance.[/yellow]"
            )

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

    source = get_source(profile)
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
    skipped = 0

    def _skipped_entry(sync_cfg: SyncConfig) -> dict[str, object]:
        # --fail-fast (#775): "didn't run" is distinct from "ran and failed".
        return {
            "name": sync_cfg.name,
            "status": "skipped",
            "reason": "fail_fast",
            "rows_extracted": 0,
            "rows_synced": 0,
            "rows_failed": 0,
            "duration_seconds": 0.0,
            "dry_run": dry_run,
        }

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
        extract_limit=limit,
        vars=project_vars,
    )

    # Execute syncs — parallel if threads > 1, sequential otherwise
    if threads > 1 and len(syncs) > 1:
        if not json_mode and not quiet:
            console.print(f"[dim]Running {len(syncs)} syncs with {threads} threads[/dim]\n")
        with ThreadPoolExecutor(max_workers=threads) as pool:
            futures = {pool.submit(_run_one, s, ctx, profile): s for s in syncs}
            reaped: set[Future[Any]] = set()
            for future in as_completed(futures):
                reaped.add(future)
                name, entry, had_err = future.result()
                json_results.append(entry)
                if had_err:
                    failed += 1
                    if fail_fast:
                        # Stop scheduling, then STOP ITERATING as_completed().
                        #
                        # shutdown(cancel_futures=True) calls Future.cancel() on
                        # everything still queued, but a future cancelled that way
                        # never reaches as_completed()'s waiter — the waiter is only
                        # notified from set_running_or_notify_cancel(), which a worker
                        # calls when it *picks up* the item, and these items were pulled
                        # off the queue instead. Staying in the loop would block forever
                        # waiting for futures that can never be delivered.
                        pool.shutdown(wait=False, cancel_futures=True)
                        break
                else:
                    succeeded += 1

            # Reap what the loop above didn't: cancelled futures (never started —
            # report them as skipped) and in-flight ones (let them drain; the
            # with-block would wait for them regardless).
            for future, sync in futures.items():
                if future in reaped:
                    continue
                if future.cancelled():
                    json_results.append(_skipped_entry(sync))
                    skipped += 1
                    continue
                name, entry, had_err = future.result()
                json_results.append(entry)
                if had_err:
                    failed += 1
                else:
                    succeeded += 1
    else:
        stop_scheduling = False
        for sync in syncs:
            if stop_scheduling:
                json_results.append(_skipped_entry(sync))
                skipped += 1
                continue
            name, entry, had_err = _run_one(sync, ctx, profile)
            json_results.append(entry)
            if had_err:
                failed += 1
                if fail_fast:
                    stop_scheduling = True
            else:
                succeeded += 1

    if skipped and not json_mode and not quiet:
        console.print(
            f"[yellow]--fail-fast: skipped {skipped} sync(s) after the first failure.[/yellow]"
        )

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
                    "skipped": skipped,
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
