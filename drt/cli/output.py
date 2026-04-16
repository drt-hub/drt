"""Centralized Rich output for the drt CLI.

All console output goes through this module — never call console.print()
directly from engine, config, or source/destination code.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from rich.console import Console
from rich.table import Table
from rich.text import Text

from drt.config.credentials import ProfileConfig
from drt.config.models import SyncConfig
from drt.destinations.base import SyncResult
from drt.state.manager import SyncState

if TYPE_CHECKING:
    from drt.destinations.row_errors import RowError

console = Console()


# ---------------------------------------------------------------------------
# init
# ---------------------------------------------------------------------------


def print_init_success(paths: list[str]) -> None:
    console.print()
    console.print("[bold green]✓ drt project initialized[/bold green]")
    for p in paths:
        console.print(f"  [dim]Created[/dim] {p}")
    console.print()
    console.print("Next steps:")
    console.print("  1. Edit [bold]drt_project.yml[/bold] if needed")
    console.print("  2. Add sync definitions to [bold]syncs/[/bold]")
    console.print("  3. Run [bold]drt run --dry-run[/bold] to preview")
    console.print()


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def print_sync_start(sync_name: str, dry_run: bool) -> None:
    tag = " [dim](dry-run)[/dim]" if dry_run else ""
    console.print(f"\n[bold]→ {sync_name}[/bold]{tag}")


def print_dry_run_summary(sync: SyncConfig, profile: ProfileConfig, rows: int) -> None:
    """Print a summary of what would be synced during a dry run."""
    from drt.engine.resolver import parse_ref

    source_desc = profile.describe()
    model_name = parse_ref(sync.model)
    if model_name:
        # bigquery (project.dataset.table)
        source_desc = source_desc.replace(")", f".{model_name})")

    console.print("Dry run summary:")
    console.print(f"  Source: {source_desc}")
    console.print(f"  Destination: {sync.destination.describe()}")
    console.print(f"  Rows to sync: {rows}")
    console.print(f"  Sync mode: {sync.sync.mode}")


def print_sync_result(sync_name: str, result: SyncResult, elapsed: float) -> None:
    if result.failed == 0:
        status = "[green]✓[/green]"
    elif result.success > 0:
        status = "[yellow]⚠[/yellow]"
    else:
        status = "[red]✗[/red]"

    console.print(
        f"  {status} {result.success} synced"
        + (f", {result.failed} failed" if result.failed else "")
        + (f", {result.skipped} skipped" if result.skipped else "")
        + f"  [dim]({elapsed:.1f}s)[/dim]"
    )
    for err in result.errors[:5]:
        console.print(f"  [red]  • {err}[/red]")
    if len(result.errors) > 5:
        console.print(f"  [red]  … and {len(result.errors) - 5} more errors[/red]")


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------


def print_sync_table(syncs: list[SyncConfig]) -> None:
    if not syncs:
        console.print("[dim]No syncs found. Add .yml files to the syncs/ directory.[/dim]")
        return

    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    table.add_column("name")
    table.add_column("model")
    table.add_column("destination")
    table.add_column("mode")
    table.add_column("description", style="dim")

    for sync in syncs:
        table.add_row(
            sync.name,
            sync.model,
            f"{sync.destination.type}",
            sync.sync.mode,
            sync.description or "",
        )

    console.print(table)


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------


def print_validation_ok(sync_name: str) -> None:
    console.print(f"[green]✓[/green] {sync_name}")


def print_validation_error(sync_name: str, errors: list[str]) -> None:
    console.print(f"[red]✗[/red] {sync_name}")
    for err in errors:
        console.print(f"  [red]• {err}[/red]")


# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------


def print_test_header(sync_name: str) -> None:
    console.print(f"\n[bold]{sync_name}[/bold]")


def print_test_result(test_name: str, passed: bool, message: str) -> None:
    mark = "[green]✓[/green]" if passed else "[red]✗[/red]"
    console.print(f"  {mark} {test_name}: {message}")


def print_test_skip(sync_name: str, reason: str) -> None:
    console.print(f"  [dim]⏭ {sync_name}: {reason}[/dim]")


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def print_status_table(states: dict[str, SyncState]) -> None:
    if not states:
        console.print("[dim]No sync history found. Run `drt run` first.[/dim]")
        return

    table = Table(show_header=True, header_style="bold", box=None, padding=(0, 2))
    table.add_column("sync")
    table.add_column("status")
    table.add_column("records")
    table.add_column("last run")
    table.add_column("error", style="dim")

    for name, state in sorted(states.items()):
        status_text = {
            "success": "[green]success[/green]",
            "failed": "[red]failed[/red]",
            "partial": "[yellow]partial[/yellow]",
        }.get(state.status, state.status)

        table.add_row(
            name,
            Text.from_markup(status_text),
            str(state.records_synced),
            state.last_run_at[:19].replace("T", " "),  # trim microseconds
            state.error or "",
        )

    console.print(table)


# ---------------------------------------------------------------------------
# verbose row errors
# ---------------------------------------------------------------------------


def print_row_errors(row_errors: list[RowError]) -> None:
    """Print per-row error details (used with --verbose flag)."""
    for re in row_errors:
        http_part = f"HTTP {re.http_status} " if re.http_status is not None else ""
        console.print(
            f"  [dim]row {re.batch_index}:[/dim] [red]{http_part}{re.error_message[:120]}[/red]"
        )


def print_status_verbose(
    states: dict[str, SyncState],
    row_errors_by_sync: dict[str, list[RowError]],
) -> None:
    """Print status table followed by per-row error details for each sync."""
    if not states:
        console.print("[dim]No sync history found. Run `drt run` first.[/dim]")
        return

    for name, state in sorted(states.items()):
        status_icon = {
            "success": "[green]✓[/green]",
            "failed": "[red]✗[/red]",
            "partial": "[yellow]⚠[/yellow]",
        }.get(state.status, state.status)

        last_run = state.last_run_at[:19].replace("T", " ")
        console.print(
            f"{name}  last run: {last_run}  "
            f"{status_icon} {state.records_synced}"
            + (f"  [red]✗ {state.error}[/red]" if state.error else "")
        )

        row_errs = row_errors_by_sync.get(name, [])
        for re in row_errs:
            http_part = f"HTTP {re.http_status} " if re.http_status is not None else ""
            console.print(
                f"  [dim]row {re.batch_index}:[/dim] [red]{http_part}{re.error_message[:120]}[/red]"
            )


# ---------------------------------------------------------------------------
# errors
# ---------------------------------------------------------------------------


def print_error(message: str) -> None:
    console.print(f"[bold red]Error:[/bold red] {message}")
