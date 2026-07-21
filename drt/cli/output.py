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
    console.print("  1. Edit [bold]drt_project.yml[/bold] — set your profile credentials")
    console.print("  2. Edit [bold]syncs/example_sync.yml[/bold] — define your first sync")
    console.print("  3. Run: [bold]drt validate[/bold]\t\t[dim](check config)[/dim]")
    console.print("  4. Run: [bold]drt run --dry-run[/bold]\t[dim](preview without writing)[/dim]")
    console.print("  5. Run: [bold]drt run[/bold]\t\t[dim](execute)[/dim]")
    console.print()
    console.print("Docs: https://github.com/drt-hub/drt#quickstart")
    console.print()


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


def print_sync_start(sync_name: str, dry_run: bool) -> None:
    tag = " [dim](dry-run)[/dim]" if dry_run else ""
    console.print(f"\n[bold]→ {sync_name}[/bold]{tag}")


def print_dry_run_summary(
    sync: SyncConfig,
    profile: ProfileConfig,
    rows: int,
    destination: object | None = None,
) -> None:
    """Print a summary of what would be synced during a dry run.

    Args:
        sync: Sync configuration.
        profile: Source profile configuration.
        rows: Number of rows that would be synced from source.
        destination: Destination instance (optional, used for row count in replace mode).
    """
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
    if sync.sync.mode == "replace":
        console.print(
            "  [yellow]⚠ replace mode will TRUNCATE the destination table"
            " before inserting rows[/yellow]"
        )
        # Show row count diff if destination is provided
        if destination is not None:
            _print_row_count_diff(sync, destination, rows)


def _print_row_count_diff(sync: SyncConfig, destination: object, new_rows: int) -> None:
    """Print current vs new row count for replace mode.

    Args:
        sync: Sync configuration.
        destination: Destination instance.
        new_rows: Number of new rows from source.
    """
    from drt.destinations.sql_utils import get_row_count_for_destination

    try:
        current_rows = get_row_count_for_destination(destination, sync.destination)
        if current_rows is not None:
            diff = new_rows - current_rows
            diff_str = f"{diff:+d}" if diff != 0 else "0"
            if diff > 0:
                diff_color = "green"
            elif diff < 0:
                diff_color = "red"
            else:
                diff_color = "dim"
            console.print(
                f"  Current destination rows: {current_rows} "
                f"→ New: {new_rows} "
                f"([{diff_color}]{diff_str}[/{diff_color}])"
            )
    except Exception as e:
        # Silently skip row count if unable to connect (not a blocking error)
        console.print(f"  [dim](Could not retrieve current row count: {type(e).__name__})[/dim]")


def print_sync_result(sync_name: str, result: SyncResult, elapsed: float) -> None:
    if result.failed == 0:
        status = "[green]✓[/green]"
    elif result.success > 0:
        status = "[yellow]⚠[/yellow]"
    else:
        status = "[red]✗[/red]"

    if result.rows_extracted == 0 and result.failed == 0:
        console.print(f"  {status} 0 rows [dim](no rows)[/dim]  [dim]({elapsed:.1f}s)[/dim]")
        return

    console.print(
        f"  {status} {result.success} synced"
        + (f", {result.failed} failed" if result.failed else "")
        + (
            f", {result.skipped} skipped"
            + (
                f" ({result.skipped_no_match} no match)"
                if result.skipped_no_match
                else ""
            )
            if result.skipped
            else ""
        )
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
        dest_label = (
            sync.destination.describe()
            if hasattr(sync.destination, "describe")
            else sync.destination.type
        )
        table.add_row(
            sync.name,
            sync.model,
            dest_label,
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


def print_connection_test_result(
    sync_name: str,
    success: bool | None,
    error: str | None = None,
) -> None:
    if success is None:
        console.print("  [dim]⏭ connection test skipped (non-SQL)[/dim]")
    elif success:
        console.print("  [green]✓ connection ok[/green]")
    elif error:
        console.print(f"  [red]✗ connection failed: {error}[/red]")
    else:
        # SQL destination but no tester method
        console.print("  [red]✗ connection failed: test_connection method missing[/red]")


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
    # Escape Rich markup in the user-supplied message so square-bracket
    # fragments like ``drt-core[mcp]`` or ``[INFO]`` aren't consumed as
    # style tags and silently dropped from the rendered output.
    from rich.markup import escape

    console.print(f"[bold red]Error:[/bold red] {escape(message)}")


# ---------------------------------------------------------------------------
# diff (#413)
# ---------------------------------------------------------------------------


def _format_row_keys(row: dict[str, object], max_chars: int = 80) -> str:
    """Render a record as ``key=value, ...`` with truncation."""
    parts = [f"{k}={v}" for k, v in row.items()]
    rendered = ", ".join(parts)
    if len(rendered) > max_chars:
        rendered = rendered[: max_chars - 1] + "…"
    return rendered


def print_diff_table(diff: object, sync_name: str) -> None:
    """Print a record-level diff produced by :func:`drt.engine.diff.compute_diff`.

    For queryable destinations: renders added / updated (with field-level
    changes) / deleted in colored sections. For non-queryable destinations:
    renders the sample of records with a clear "no comparison available" note.
    """
    from drt.engine.diff import DiffResult

    assert isinstance(diff, DiffResult)

    console.print(f"\n[bold]Diff preview — {sync_name}[/bold]")

    if not diff.supported:
        console.print(f"  [dim]{diff.fallback_reason}[/dim]")
        n_shown = len(diff.sample)
        n_total = diff.total_source_rows
        more = f" ({n_total - n_shown} more not shown)" if n_total > n_shown else ""
        console.print(
            f"  [bold]→ {n_total} record(s) would be sent.[/bold] "
            f"Sample (first {n_shown}{more}):"
        )
        for row in diff.sample:
            console.print(f"    {_format_row_keys(row)}")
        return

    n_added = len(diff.added)
    n_updated = len(diff.updated)
    n_deleted = len(diff.deleted)
    console.print(
        f"  [dim]source rows: {diff.total_source_rows} · "
        f"destination rows: {diff.total_destination_rows}[/dim]"
    )

    # Added
    if n_added:
        console.print(f"\n  [green]+ Added ({n_added}):[/green]")
        for row in diff.added:
            console.print(f"    [green]+[/green] {_format_row_keys(row)}")
    else:
        console.print("\n  [dim]+ Added: none[/dim]")

    # Updated — show field-level changes
    if n_updated:
        console.print(f"\n  [yellow]~ Updated ({n_updated}):[/yellow]")
        for old, new in diff.updated:
            changed = DiffResult.changed_fields(old, new)
            # Use the first column of new as a stable key label
            key_repr = next((f"{k}={v}" for k, v in new.items() if k in old), "(?)")
            change_repr = ", ".join(
                f"{c}: {old_v} → {new_v}" for c, (old_v, new_v) in changed.items()
            )
            console.print(f"    [yellow]~[/yellow] {key_repr} — {change_repr}")
    else:
        console.print("\n  [dim]~ Updated: none[/dim]")

    # Deleted (only populated for replace mode by the engine)
    if n_deleted:
        console.print(f"\n  [red]- Deleted ({n_deleted}):[/red]")
        for row in diff.deleted:
            console.print(f"    [red]-[/red] {_format_row_keys(row)}")
    elif diff.deleted == [] and any([n_added, n_updated]):
        # Don't always print "Deleted: none" — only when other change types
        # are present, to avoid noise on full-upsert mode where deleted
        # never applies.
        pass

    if diff.truncated:
        console.print(
            "\n  [dim]…some records omitted (limit reached). "
            "Use --diff-limit N to see more.[/dim]"
        )


def diff_to_dict(diff: object) -> dict[str, object]:
    """Serialise a DiffResult for ``--output json`` mode."""
    from drt.engine.diff import DiffResult

    assert isinstance(diff, DiffResult)

    if not diff.supported:
        return {
            "supported": False,
            "fallback_reason": diff.fallback_reason,
            "total_source_rows": diff.total_source_rows,
            "sample": diff.sample,
            "truncated": diff.truncated,
        }

    return {
        "supported": True,
        "total_source_rows": diff.total_source_rows,
        "total_destination_rows": diff.total_destination_rows,
        "added": diff.added,
        "updated": [
            {
                "old": old,
                "new": new,
                "changed_fields": list(DiffResult.changed_fields(old, new).keys()),
            }
            for old, new in diff.updated
        ],
        "deleted": diff.deleted,
        "truncated": diff.truncated,
    }
