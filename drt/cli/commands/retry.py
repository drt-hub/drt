"""``drt retry`` — replay records from a sync's Dead Letter Queue (#278).

When a sync runs with ``sync.dlq.enabled: true``, records that fail during
``destination.load()`` are persisted to ``.drt/dlq/<sync_name>.jsonl``. This
command re-sends just those records to the destination, drops the ones that
now succeed, and writes the rest back with a bumped ``attempts`` count.

Retry needs only the destination (records are stored post-mapping, so they
replay verbatim) — no source extraction or profile resolution involved.
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

import typer

from drt.cli._app import app
from drt.cli.output import console, print_error

if TYPE_CHECKING:
    from drt.config.models import ProjectConfig, SyncConfig
    from drt.destinations.base import SyncResult
    from drt.state.audit_trail import ComplianceAuditTrail
    from drt.state.dlq import DeadLetter
    from drt.state.idempotency import IdempotencyLedger


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
    from drt.state.audit_trail import AuditEntry, json_safe_audit_value
    from drt.state.dlq import DeadLetter
    from drt.state.factory import build_state_bundle
    from drt.state.idempotency import (
        compute_idempotency_key,
        resolve_idempotency_key_template,
        successful_indices,
    )

    if project is None:
        project = (
            load_project(project_dir)
            if (project_dir / "drt_project.yml").exists()
            else ProjectConfig(name="drt")
        )
    bundle = build_state_bundle(project, project_dir)
    store = bundle.dlq
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

    # Idempotency ledger + compliance audit log (#1099/#1100 via #1118).
    # Mirrors run_sync()'s own exclusion (drt/engine/sync.py): a staged
    # destination's or swap-mode replace's per-call "success" isn't a
    # trustworthy delivery signal until finalize()/the shadow-table cutover,
    # so neither feature applies to those destination shapes here either —
    # applying them where the engine itself refuses would be an asymmetry
    # baked into audit rows that's much harder to unwind later.
    _swap_mode = sync.sync.mode == "replace" and sync.sync.replace_strategy == "swap"
    ledger_audit_applicable = not isinstance(dest, StagedDestination) and not _swap_mode
    idempotency_ledger: IdempotencyLedger | None = (
        bundle.ledger if ledger_audit_applicable else None
    )
    audit_trail: ComplianceAuditTrail | None = (
        bundle.audit_trail if ledger_audit_applicable else None
    )
    audit_fields = project.state.audit_trail.fields
    audit_enabled = audit_trail is not None and bool(audit_fields)

    # Check-then-act, same contract as a live run's pre-send filter — but a
    # hit here does NOT delete the queued entry. The ledger keys on record
    # identity (usually `upsert_key`), not this exact payload, so a hit only
    # means "some version of this record was delivered," not "this queued
    # payload was" — deleting on that basis risks silently discarding a
    # payload that has since drifted from whatever version actually went
    # out (the same blast radius #955 exists to prevent). Instead: skip
    # resending it this round, report it as `skipped_duplicate`, and leave
    # it queued for an operator to inspect or a future retry to reconsider.
    key_by_id: dict[str, str] = {}
    duplicate_ids: set[str] = set()
    if idempotency_ledger is not None:
        template = resolve_idempotency_key_template(sync)
        if template is not None:
            for entry in to_retry:
                key = compute_idempotency_key(template, entry.record)
                if key is not None:
                    key_by_id[entry.id] = key
            candidate_keys = set(key_by_id.values())
            already = (
                idempotency_ledger.already_delivered(sync.name, candidate_keys)
                if candidate_keys
                else set()
            )
            duplicate_ids = {eid for eid, key in key_by_id.items() if key in already}
            if duplicate_ids:
                to_retry = [e for e in to_retry if e.id not in duplicate_ids]

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
            finalize_updates: dict[str, DeadLetter] = {}
            for entry in to_retry:
                finalize_updates[entry.id] = DeadLetter(
                    id=entry.id,
                    record=entry.record,
                    error_message=error,
                    timestamp=entry.timestamp,
                    attempts=entry.attempts + 1,
                    sync_run_id=entry.sync_run_id,
                )
            final = store.reconcile(sync.name, remove_ids=set(), updates=finalize_updates)
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

    # Ledger mark + audit log are persisted per retry_group/chunk below (not
    # accumulated and written once at the end) — #1127. A non-staged,
    # multi-chunk retry calls dest.load() lazily per chunk (see retry_groups
    # above); Destination.load() may raise an unrecoverable, batch-level
    # exception (documented, uncaught contract — drt/destinations/base.py),
    # which propagates out of the loop below. Persisting each chunk's
    # ledger/audit confirmation immediately means a later chunk's exception
    # only loses that *unprocessed* remainder, not an earlier chunk's
    # already-confirmed deliveries — mirrors run_sync()'s own per-batch
    # persistence (drt/engine/sync.py), for the same crash-safety reason.
    #
    # DLQ removal (`remove_ids`/`updates` below) stays accumulated and
    # reconciled once per invocation rather than once per chunk:
    # `reconcile()` re-reads and rewrites the *entire* remaining queue on
    # every call (see its own docstring), so calling it once per chunk would
    # make a large, small-batch-size queue's retry cost quadratic (caught in
    # Codex review on #1128) — `DlqBackend` has no cheaper mutation-only
    # primitive to persist a single chunk's removal without that full-queue
    # cost. The loop below is wrapped in a try/except that also calls
    # `reconcile()` (with whatever's accumulated so far) before re-raising a
    # mid-loop exception, closing the other half of #1127: without it, a
    # later chunk's exception skipped the final `reconcile()` entirely,
    # losing DLQ-removal confirmation for every earlier chunk this
    # invocation had already delivered — a plain resend on the next retry
    # for a project without `state.idempotency: true` (no ledger backstop).
    # A hard process kill (SIGKILL/OOM) between an earlier chunk's success
    # and this handler running still loses that window, same as run_sync()'s
    # own per-batch persistence does between batches — that residual gap is
    # out of scope here, same as it is for run_sync().
    remove_ids: set[str] = set()
    updates: dict[str, DeadLetter] = {}
    # How many of `to_retry`'s entries have a *fully processed* chunk behind
    # them -- incremented only once a chunk's own result has actually been
    # handled below (not merely fetched), so a chunk whose own processing
    # raises partway through is conservatively NOT counted as processed. Used
    # only by the except block's legacy-duplicate-id guard (see there); the
    # staged path's single group covers the whole of `to_retry` and only
    # reaches here after `finalize()` already succeeded (its own failure is
    # handled earlier, before this loop), so this count is not meaningful
    # for it beyond that same conservative default.
    processed_count = 0

    try:
        for retry_group, result in retry_groups:
            # Fails closed for the whole group on any unattributed skip — same
            # contract as run_sync()'s per-batch ledger mark / audit write.
            confirmed_idx = successful_indices([e.record for e in retry_group], result)
            group_delivered_ids: set[str] = set()

            if result.failed == 0:
                succeeded += len(retry_group)
                remove_ids.update(e.id for e in retry_group)
                group_delivered_ids.update(
                    entry.id for i, entry in enumerate(retry_group) if i in confirmed_idx
                )
            else:
                # Correlate which records failed again. RowError.batch_index
                # pinpoints the failures within this retry group; trust that
                # correlation only when the row_errors fully account for
                # result.failed. Otherwise the group failed in a way we can't
                # attribute per-record, so conservatively keep the whole group
                # queued rather than silently dropping records. For load() a
                # group is one chunk; for a staged destination it is the full
                # accumulated record set finalized once. Trade-off: on an
                # un-attributable group, rows that actually succeeded get
                # re-queued and may be re-sent on the next retry — we prefer a
                # re-send (idempotent for upsert destinations) over a silent drop.
                failed_idx = {
                    e.batch_index
                    for e in result.row_errors
                    if 0 <= e.batch_index < len(retry_group)
                }
                pinpointed = len(failed_idx) == result.failed
                if isinstance(dest, StagedDestination):
                    if len(retry_group) > sync.sync.batch_size:
                        # StagedDestination's Protocol (drt/destinations/base.py)
                        # does not define whether RowError.batch_index from
                        # finalize() is local to the individual stage() call it
                        # came from or global across the whole accumulated set —
                        # caught in review. More than one chunk was staged before
                        # this single finalize() call, so trusting batch_index
                        # here would silently misattribute a later chunk's
                        # failure to an earlier chunk's record for any
                        # implementation that reports chunk-local indices (the
                        # more natural convention, matching how
                        # Destination.load()'s own batch_index is always
                        # chunk-local). A single-chunk retry has no such
                        # ambiguity — chunk-local and global indexing coincide
                        # when there was only one stage() call — so only the
                        # multi-chunk case falls back here.
                        pinpointed = False
                    elif sync.destination.type == "salesforce_bulk":
                        # Salesforce's failed-results CSV does not expose the
                        # original accumulated-list position; its destination
                        # currently emits 0 for every RowError.batch_index
                        # regardless of chunk count. Even a single-chunk retry's
                        # one such error therefore cannot safely identify record
                        # 0 as the failed source row.
                        pinpointed = False
                err_by_idx = {e.batch_index: e for e in result.row_errors}

                for i, entry in enumerate(retry_group):
                    if pinpointed and i not in failed_idx:
                        succeeded += 1
                        remove_ids.add(entry.id)
                        if i in confirmed_idx:
                            group_delivered_ids.add(entry.id)
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
                        # Preserve the *original* failure's run correlation, not
                        # this retry's (there isn't one — retry has no
                        # sync_run_id of its own) — matches metadata_columns'
                        # (#762) same call that a retried row traces back to
                        # when it first failed.
                        sync_run_id=entry.sync_run_id,
                    )
                    failed_again += 1

            # Idempotency mark + audit log for THIS chunk only — only for
            # entries `group_delivered_ids` names, i.e. this chunk's own
            # correlation just positively attributed to a delivery (not merely
            # "removed from the DLQ" — see `group_delivered_ids`' population
            # above for why the two sets diverge on an unattributed skip).
            # Pre-filtered duplicates (`duplicate_ids`) never entered this loop
            # at all, so they can never end up here either — no double-marking,
            # no audit row claiming a delivery this retry didn't actually make.
            if group_delivered_ids:
                # Timestamped per chunk, not once for the whole retry — more
                # accurate for slow multi-chunk retries, and a deliberate
                # behavior change from #1118's original single-timestamp
                # version (see CHANGELOG).
                delivered_at = datetime.now(timezone.utc).isoformat()
                if idempotency_ledger is not None:
                    delivered_keys = [
                        key_by_id[eid] for eid in group_delivered_ids if eid in key_by_id
                    ]
                    if delivered_keys:
                        idempotency_ledger.mark_delivered(sync.name, delivered_keys, delivered_at)

                if audit_enabled:
                    assert audit_trail is not None
                    audit_entries = [
                        AuditEntry(
                            record_key=":".join(
                                str(entry.record[f]) for f in audit_fields if f in entry.record
                            ),
                            fields={
                                f: json_safe_audit_value(entry.record[f])
                                for f in audit_fields
                                if f in entry.record
                            },
                        )
                        for entry in retry_group
                        if entry.id in group_delivered_ids
                    ]
                    if audit_entries:
                        # No run_id/sync_run_id of retry's own to attach (see the
                        # sync_run_id comment above) — passing the *original*
                        # failing run's id would misattribute this delivery to a
                        # run that never actually sent it successfully. The
                        # Protocol explicitly allows None for library callers with
                        # no invocation-level id (see ComplianceAuditTrail docs).
                        audit_trail.log_delivered(
                            sync.name,
                            None,
                            None,
                            sync.destination.type,
                            audit_entries,
                            delivered_at,
                        )

            # Only now -- after this chunk's result has been fully handled
            # above -- does it count as processed for the except block's
            # legacy-duplicate-id guard.
            processed_count += len(retry_group)
    except Exception:
        # A later chunk's dest.load() raised (documented, uncaught contract --
        # drt/destinations/base.py) before the final reconcile() below could
        # run. Persist whatever remove_ids/updates were already accumulated
        # from fully-processed EARLIER chunks -- the chunk that raised (and
        # everything after it) is never given an updates entry or attempts
        # bump here, unlike the staged path above where finalize() already
        # covers the whole accumulated set.
        #
        # Guard against a legacy-duplicate-id false removal (Codex review on
        # #1146, rounds 2 and 3): reconcile()'s remove_ids is an id-set
        # filter, not an occurrence-count filter (see LocalDlqStore.reconcile()
        # ) -- if a legacy (pre-#955) DLQ holds two byte-identical entries
        # that therefore share the same content-derived id, and one was
        # confirmed by an earlier (fully processed) chunk while its twin was
        # never actually retried this invocation, naming that id in
        # remove_ids would delete BOTH physical entries, silently discarding
        # the untouched one. An entry can escape being retried this
        # invocation two ways: it's in `to_retry` but a later exception cut
        # the loop off before its own chunk ran (`to_retry[processed_count:]`
        # -- see the comment above `processed_count`'s declaration), or it
        # was excluded from `to_retry` altogether by `--limit` (`untouched`,
        # computed near the top of this function). Both sets name entries
        # this call will not touch either way, so excluding both from
        # remove_ids here means an id with a still-queued twin is simply
        # left queued instead of removed -- costs one more retry cycle,
        # nothing is lost. Round 1 of this review under-weighted that this
        # except block makes the hazard reachable on a *default*,
        # no-`--limit` `drt retry` too (previously it needed an explicit
        # `--limit` truncation), which is why it's guarded here at all;
        # round 3 caught that the first version of this guard checked only
        # `to_retry`'s unprocessed suffix and missed `untouched`, so a
        # `--limit`-excluded twin could still be wrongly deleted by this
        # except block even though `--limit` was never meant to touch it.
        #
        # The always-run, end-of-function reconcile() call below (reached
        # only when the loop completes with no exception) has no equivalent
        # `untouched` guard and is unchanged by this PR -- fixing it touches
        # already-reviewed, already-shipped behavior outside this fix's
        # scope, tracked as #1147 alongside the narrower `updates`
        # cross-contamination case (a legacy id's untouched twin can pick up
        # the same bumped `attempts`/error content without being deleted;
        # not filtered here either, since overwriting is not the same
        # failure mode as silently losing the entry).
        #
        # This closes the *exception* path only: a hard process kill
        # (SIGKILL/OOM) between an earlier chunk's success and this handler
        # still loses that window, same as run_sync()'s own per-batch
        # persistence does between batches (#1127, remaining scope).
        unprocessed_ids = {e.id for e in to_retry[processed_count:]} | {e.id for e in untouched}
        safe_remove_ids = remove_ids - unprocessed_ids
        try:
            store.reconcile(sync.name, remove_ids=safe_remove_ids, updates=updates)
        except Exception as reconcile_exc:
            # Don't mask the original destination failure with a DLQ
            # bookkeeping failure (e.g. ObjectStoreDlqBackend exhausting its
            # write-precondition retries) -- log and let the original
            # exception propagate via the bare `raise` below.
            logging.getLogger(__name__).warning(
                "DLQ reconcile after a retry failure also failed for sync=%r: %s",
                sync.name,
                reconcile_exc,
            )
        raise


    # reconcile() (#955) re-reads the queue itself rather than trusting the
    # `entries` snapshot read at the top of this function — a concurrent
    # `drt run` append that landed since then survives; only the entries
    # this retry actually touched (succeeded → removed, failed again →
    # updated) are named. Called once, with everything accumulated across
    # every chunk — deliberately NOT per chunk; see the comment above the
    # loop for why (#1127/#1128). `untouched` (beyond --limit) was never
    # touched either way, so it needs no special handling here. `duplicate_ids`
    # entries are deliberately not named here — they stay queued (see above).
    final = store.reconcile(sync.name, remove_ids=remove_ids, updates=updates)
    return {
        "sync": sync.name,
        "queued": len(entries),
        "retried": len(to_retry),
        "succeeded": succeeded,
        "still_failing": failed_again,
        "skipped_duplicate": len(duplicate_ids),
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
            console.print(f"[dim]{summary['untouched']} record(s) left untouched (--limit).[/dim]")
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
    if summary.get("skipped_duplicate"):
        console.print(
            f"[dim]{summary['skipped_duplicate']} record(s) skipped (already delivered) "
            "and left queued.[/dim]"
        )
    if summary["remaining_depth"]:
        console.print(f"[dim]{summary['remaining_depth']} record(s) remain in the queue.[/dim]")
