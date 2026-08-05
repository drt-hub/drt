"""``drt retry`` — replay records from a sync's Dead Letter Queue (#278).

When a sync runs with ``sync.dlq.enabled: true``, records that fail during
``destination.load()`` are persisted to ``.drt/dlq/<sync_name>.jsonl``. This
command re-sends just those records to the destination, drops the ones that
now succeed, and writes the rest back with a bumped ``attempts`` count.

Retry needs only the destination (records are stored post-mapping, so they
replay verbatim) — no source extraction or profile resolution involved.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

import typer

from drt.cli._app import app
from drt.cli.output import console, print_error

if TYPE_CHECKING:
    from drt.config.models import SyncConfig
    from drt.state.dlq import DeadLetter


def _chunks(items: list[DeadLetter], size: int) -> list[list[DeadLetter]]:
    size = max(1, size)
    return [items[i : i + size] for i in range(0, len(items), size)]


def replay_dead_letters(
    sync: SyncConfig,
    *,
    limit: int | None = None,
    dry_run: bool = False,
    clear: bool = False,
    project_dir: Path = Path("."),
) -> dict[str, Any]:
    """Inspect / replay / clear a sync's Dead Letter Queue.

    Pure core shared by the ``drt retry`` CLI command and the MCP ``drt_retry``
    tool — no console output. Records replay verbatim (they're stored
    post-mapping), so this needs only the destination, no source or profile.

    Returns a summary dict whose ``status`` is one of:
        - ``"empty"``    — nothing queued
        - ``"cleared"``  — queue discarded (``clear=True``)
        - ``"dry_run"``  — nothing sent (``dry_run=True``)
        - ``"ok"``       — records replayed; see ``succeeded`` / ``still_failing``
    """
    from drt.cli._helpers import get_destination
    from drt.state.dlq import DeadLetter, DlqStore

    store = DlqStore(project_dir)
    entries = store.read(sync.name)
    if not entries:
        return {"sync": sync.name, "queued": 0, "status": "empty"}

    if clear:
        store.clear(sync.name)
        return {
            "sync": sync.name,
            "queued": len(entries),
            "cleared": len(entries),
            "status": "cleared",
        }

    to_retry = entries if limit is None else entries[:limit]
    untouched = [] if limit is None else entries[limit:]

    if dry_run:
        return {
            "sync": sync.name,
            "queued": len(entries),
            "would_retry": len(to_retry),
            "untouched": len(untouched),
            "status": "dry_run",
        }

    dest = get_destination(sync)
    remaining: list[DeadLetter] = []
    succeeded = 0
    failed_again = 0

    for chunk in _chunks(to_retry, sync.sync.batch_size):
        records = [e.record for e in chunk]
        result = dest.load(records, sync.destination, sync.sync)

        if result.failed == 0:
            succeeded += len(chunk)
            continue

        # Correlate which records failed again. RowError.batch_index pinpoints
        # the failures within this chunk; trust that correlation only when the
        # row_errors fully account for result.failed. Otherwise the batch
        # failed in a way we can't attribute per-record, so conservatively
        # keep the whole chunk queued rather than silently dropping records.
        # Trade-off: on an un-attributable batch, rows that actually succeeded
        # get re-queued and may be re-sent on the next retry — we prefer a
        # re-send (idempotent for upsert destinations) over a silent drop.
        failed_idx = {e.batch_index for e in result.row_errors if 0 <= e.batch_index < len(chunk)}
        pinpointed = len(failed_idx) == result.failed
        err_by_idx = {e.batch_index: e for e in result.row_errors}

        for i, entry in enumerate(chunk):
            if pinpointed and i not in failed_idx:
                succeeded += 1
                continue
            err = err_by_idx.get(i)
            remaining.append(
                DeadLetter(
                    record=entry.record,
                    error_message=(
                        err.error_message
                        if err is not None
                        else (result.errors[0] if result.errors else "retry failed")
                    ),
                    http_status=err.http_status if err is not None else None,
                    timestamp=entry.timestamp,  # preserve first-seen time
                    attempts=entry.attempts + 1,
                )
            )
            failed_again += 1

    store.replace(sync.name, untouched + remaining)
    return {
        "sync": sync.name,
        "queued": len(entries),
        "retried": len(to_retry),
        "succeeded": succeeded,
        "still_failing": failed_again,
        "remaining_depth": store.depth(sync.name),
        "status": "ok",
    }


@app.command()
def retry(
    sync_name: str = typer.Argument(..., help="Name of the sync whose DLQ to replay."),
    limit: int = typer.Option(None, "--limit", help="Only retry the oldest N queued records."),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Show what would be retried without sending."
    ),
    clear: bool = typer.Option(
        False,
        "--clear",
        help="Discard the queue without replaying (records are unrecoverable).",
    ),
) -> None:
    """Replay failed records from a sync's Dead Letter Queue.

    Examples:
      drt retry post_users                 # replay every queued record
      drt retry post_users --limit 100     # replay the oldest 100
      drt retry post_users --dry-run        # preview depth, send nothing
      drt retry post_users --clear          # give up — empty the queue
    """
    from drt.config.parser import load_syncs

    syncs = load_syncs(Path("."))
    sync = next((s for s in syncs if s.name == sync_name), None)
    if sync is None:
        print_error(f"No sync named '{sync_name}' found.")
        raise typer.Exit(1)

    if limit is not None and limit < 0:
        print_error("--limit must be >= 0.")
        raise typer.Exit(1)

    summary = replay_dead_letters(
        sync, limit=limit, dry_run=dry_run, clear=clear, project_dir=Path(".")
    )
    status = summary["status"]

    if status == "empty":
        console.print(f"[green]Dead letter queue for '{sync_name}' is empty.[/green]")
        return

    if status == "cleared":
        console.print(
            f"[yellow]Cleared {summary['cleared']} record(s) from '{sync_name}' DLQ.[/yellow]"
        )
        return

    if status == "dry_run":
        console.print(
            f"[cyan]Would retry {summary['would_retry']} of {summary['queued']} queued "
            f"record(s) for '{sync_name}'.[/cyan]"
        )
        if summary["untouched"]:
            console.print(
                f"[dim]{summary['untouched']} record(s) left untouched (--limit).[/dim]"
            )
        return

    style = "green" if summary["still_failing"] == 0 else "yellow"
    console.print(
        f"[{style}]Retry complete for '{sync_name}': "
        f"{summary['succeeded']} succeeded, {summary['still_failing']} still failing.[/{style}]"
    )
    if summary["remaining_depth"]:
        console.print(f"[dim]{summary['remaining_depth']} record(s) remain in the queue.[/dim]")
