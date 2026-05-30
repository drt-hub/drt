"""drt CLI entry point."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypedDict

import typer

if TYPE_CHECKING:
    from drt.config.credentials import ProfileConfig
    from drt.config.models import SyncConfig
    from drt.config.secrets import SecretFinding
    from drt.destinations.base import Destination
    from drt.sources.base import Source


from drt import __version__
from drt.cli import commands as _commands  # noqa: F401 — register commands

# The shared Typer instance lives in drt.cli._app so that per-command
# modules under drt/cli/commands/ can import it without circular imports
# (this main module then imports the commands package to trigger their
# @app.command decorator side effects).
from drt.cli._app import app
from drt.cli.output import (
    console,
    print_error,
    print_status_table,
    print_status_verbose,
    print_test_header,
    print_test_result,
    print_test_skip,
    print_validation_error,
    print_validation_ok,
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
# `drt run` lives in drt/cli/commands/run.py (#573 Phase 2b PR (a) —
# `LogFormat`, `_JsonFormatter`, `_configure_json_logging`, `_RunContext`,
# `_exit_code_for_signal`, `_run_one`, `_print_watermark_summary` are
# re-exported below as back-compat shims for callers that still import
# them from `drt.cli.main`)


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


# ---------------------------------------------------------------------------
# Back-compat re-exports from drt.cli.commands.run (#573 Phase 2b PR (a))
# ---------------------------------------------------------------------------
# Tests and library callers import these names from drt.cli.main. The
# implementations now live in drt/cli/commands/run.py; re-export so the
# legacy import path keeps working. New callers should import from
# drt.cli.commands.run directly.
from drt.cli.commands.run import (  # noqa: E402, F401
    LogFormat,
    _configure_json_logging,
    _exit_code_for_signal,
    _JsonFormatter,
    _print_watermark_summary,
    _run_one,
    _RunContext,
)
