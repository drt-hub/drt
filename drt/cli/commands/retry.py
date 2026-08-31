"""``drt retry`` — replay records from a sync's Dead Letter Queue (#278).

When a sync runs with ``sync.dlq.enabled: true``, records that fail during
``destination.load()`` are persisted to ``.drt/dlq/<sync_name>.jsonl``. This
command re-sends just those records to the destination, drops the ones that
now succeed, and writes the rest back with a bumped ``attempts`` count.

Retry needs only the destination (records are stored post-mapping, so they
replay verbatim) — no source extraction or profile resolution involved.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import TYPE_CHECKING, Any

import typer

from drt.cli._app import app
from drt.cli.output import console, print_error

if TYPE_CHECKING:
    from drt.config.models import ProjectConfig, SyncConfig
    from drt.destinations.base import SyncResult
    from drt.state.dlq import DeadLetter


def _chunks(items: list[DeadLetter], size: int) -> list[list[DeadLetter]]:
    size = max(1, size)
    return [items[i : i + size] for i in range(0, len(items), size)]


def replay_dead_letters(
    sync: SyncConfig,
    *,
    project: ProjectConfig | None = None,
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
        - ``"failed"``   — a staged destination failed during ``finalize()``
    """
    from drt.cli._helpers import get_destination
    from drt.config.base import ProjectConfig
    from drt.config.parser import load_project
    from drt.destinations.base import StagedDestination
    from drt.state.dlq import DeadLetter
    from drt.state.factory import build_state_bundle

    if project is None:
        project = (
            load_project(project_dir)
            if (project_dir / "drt_project.yml").exists()
            else ProjectConfig(name="drt")
        )
    store = build_state_bundle(project, project_dir).dlq
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
    remove_ids: set[str] = set()
    updates: dict[str, DeadLetter] = {}
    succeeded = 0
    failed_again = 0
    retry_groups: Iterator[tuple[list[DeadLetter], SyncResult]]

    if isinstance(dest, StagedDestination) and not to_retry:
        retry_groups = iter([])
    elif isinstance(dest, StagedDestination):
        for chunk in _chunks(to_retry, sync.sync.batch_size):
            records = [e.record for e in chunk]
            dest.stage(records, sync.destination, sync.sync)

        try:
            result = dest.finalize(sync.destination, sync.sync)
        except Exception as exc:
            # stage() only buffers; a raised finalize() means persistence was
            # never confirmed for any record. Keep every retried entry queued
            # and record the job-level failure against each one.
            error = f"Staged destination finalize failed: {exc}"
            for entry in to_retry:
                updates[entry.id] = DeadLetter(
                    id=entry.id,
                    record=entry.record,
                    error_message=error,
                    timestamp=entry.timestamp,
                    attempts=entry.attempts + 1,
                    sync_run_id=entry.sync_run_id,
                )
            final = store.reconcile(sync.name, remove_ids=set(), updates=updates)
            return {
                "sync": sync.name,
                "queued": len(entries),
                "retried": len(to_retry),
                "succeeded": 0,
                "still_failing": len(to_retry),
                "remaining_depth": len(final),
                "status": "failed",
                "error": error,
            }

        # finalize() covers the full accumulated staging set. Whether
        # RowError.batch_index is global across to_retry or local to the
        # individual stage() chunk it came from is not defined by the
        # StagedDestination Protocol — the correlation loop below only trusts
        # it when this whole set was staged in a single chunk, where the two
        # conventions coincide (see the guard there for the multi-chunk case).
        retry_groups = iter([(to_retry, result)])
    else:
        # Keep load() result processing interleaved with each call, preserving
        # the existing per-chunk behavior for ordinary destinations.
        retry_groups = (
            (
                chunk,
                dest.load(
                    [e.record for e in chunk],
                    sync.destination,
                    sync.sync,
                ),
            )
            for chunk in _chunks(to_retry, sync.sync.batch_size)
        )

    for retry_group, result in retry_groups:
        if result.failed == 0:
            succeeded += len(retry_group)
            remove_ids.update(e.id for e in retry_group)
            continue

        # Correlate which records failed again. RowError.batch_index pinpoints
        # the failures within this retry group; trust that correlation only
        # when the row_errors fully account for result.failed. Otherwise the
        # group failed in a way we can't attribute per-record, so
        # conservatively keep the whole group queued rather than silently
        # dropping records. For load() a group is one chunk; for a staged
        # destination it is the full accumulated record set finalized once.
        # Trade-off: on an un-attributable group, rows that actually succeeded
        # get re-queued and may be re-sent on the next retry — we prefer a
        # re-send (idempotent for upsert destinations) over a silent drop.
        failed_idx = {
            e.batch_index
            for e in result.row_errors
            if 0 <= e.batch_index < len(retry_group)
        }
        pinpointed = len(failed_idx) == result.failed
        if isinstance(dest, StagedDestination):
            if len(retry_group) > sync.sync.batch_size:
                # StagedDestination's Protocol (drt/destinations/base.py) does
                # not define whether RowError.batch_index from finalize() is
                # local to the individual stage() call it came from or global
                # across the whole accumulated set — caught in review. More
                # than one chunk was staged before this single finalize()
                # call, so trusting batch_index here would silently
                # misattribute a later chunk's failure to an earlier chunk's
                # record for any implementation that reports chunk-local
                # indices (the more natural convention, matching how
                # Destination.load()'s own batch_index is always chunk-local).
                # A single-chunk retry has no such ambiguity — chunk-local and
                # global indexing coincide when there was only one stage()
                # call — so only the multi-chunk case falls back here.
                pinpointed = False
            elif sync.destination.type == "salesforce_bulk":
                # Salesforce's failed-results CSV does not expose the original
                # accumulated-list position; its destination currently emits 0
                # for every RowError.batch_index regardless of chunk count.
                # Even a single-chunk retry's one such error therefore cannot
                # safely identify record 0 as the failed source row.
                pinpointed = False
        err_by_idx = {e.batch_index: e for e in result.row_errors}

        for i, entry in enumerate(retry_group):
            if pinpointed and i not in failed_idx:
                succeeded += 1
                remove_ids.add(entry.id)
                continue
            err = err_by_idx.get(i)
            updates[entry.id] = DeadLetter(
                id=entry.id,  # same identity — a retried entry is not a new one (#955)
                record=entry.record,
                error_message=(
                    err.error_message
                    if err is not None
                    else (result.errors[0] if result.errors else "retry failed")
                ),
                http_status=err.http_status if err is not None else None,
                timestamp=entry.timestamp,  # preserve first-seen time
                attempts=entry.attempts + 1,
                # Preserve the *original* failure's run correlation, not this
                # retry's (there isn't one — retry has no sync_run_id of its
                # own) — matches metadata_columns' (#762) same call that a
                # retried row traces back to when it first failed.
                sync_run_id=entry.sync_run_id,
            )
            failed_again += 1

    # reconcile() (#955) re-reads the queue itself rather than trusting the
    # `entries` snapshot read at the top of this function — a concurrent
    # `drt run` append that landed since then survives; only the entries this
    # retry actually touched (succeeded → removed, failed again → updated)
    # are named. `untouched` (beyond --limit) was never touched either way,
    # so it needs no special handling here anymore.
    final = store.reconcile(sync.name, remove_ids=remove_ids, updates=updates)
    return {
        "sync": sync.name,
        "queued": len(entries),
        "retried": len(to_retry),
        "succeeded": succeeded,
        "still_failing": failed_again,
        "remaining_depth": len(final),
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

    # No project= here: replay_dead_letters() resolves it itself (present
    # drt_project.yml -> load it; absent -> local-default ProjectConfig), the
    # same fallback every other state/DLQ surface (MCP's
    # load_project_for_state()) already gives a directory that only has
    # syncs/ + .drt/dlq/ with no project file.
    summary = replay_dead_letters(
        sync,
        limit=limit,
        dry_run=dry_run,
        clear=clear,
        project_dir=Path("."),
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

    if status == "failed":
        print_error(f"Retry failed for '{sync_name}': {summary['error']}")
        if summary["remaining_depth"]:
            console.print(f"[dim]{summary['remaining_depth']} record(s) remain in the queue.[/dim]")
        raise typer.Exit(1)

    style = "green" if summary["still_failing"] == 0 else "yellow"
    console.print(
        f"[{style}]Retry complete for '{sync_name}': "
        f"{summary['succeeded']} succeeded, {summary['still_failing']} still failing.[/{style}]"
    )
    if summary["remaining_depth"]:
        console.print(f"[dim]{summary['remaining_depth']} record(s) remain in the queue.[/dim]")
