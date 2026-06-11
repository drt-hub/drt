"""Sync Engine — orchestrates extract → transform → load.

This module is the primary candidate for future Rust rewrite (PyO3).
Keep it pure: no I/O side effects beyond source/destination calls.
CLI owns all console output; engine only returns SyncResult.
"""

from __future__ import annotations

import threading
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

from drt.config.credentials import ProfileConfig
from drt.config.models import LookupConfig, SyncConfig
from drt.destinations.base import Destination, StagedDestination, SyncResult
from drt.destinations.lookup import (
    apply_lookups,
    build_lookup_map,
    detect_ambiguous_lookup_ordering,
)
from drt.engine.field_mappings import apply_field_mappings
from drt.engine.observer import NullObserver, SyncObserver
from drt.engine.resolver import resolve_model_ref
from drt.sources.base import Source
from drt.state.history import HistoryEntry, HistoryManager
from drt.state.manager import StateManager
from drt.state.watermark import WatermarkStorage


def _cursor_gt(new: str, current: str) -> bool:
    """Return True if new > current, using numeric comparison when both are numeric."""
    try:
        return float(new) > float(current)
    except (ValueError, TypeError):
        return new > current


def _stringify_cursor_value(val: Any) -> str:
    """Convert a cursor value into a stable string representation.

    For tz-aware datetimes (e.g. BigQuery TIMESTAMP returns from the
    Python BQ client), normalize to **naive UTC** before stringifying.
    This avoids a re-emit-at-boundary bug (#475) where the persisted
    watermark gained a ``+00:00`` suffix while user SQL / default_value
    is typically written tz-naive — causing the same instant to be
    represented two different ways and ``WHERE col >= TIMESTAMP(...)``
    to re-match the boundary row on every subsequent run.

    BigQuery (and most warehouses) parse ``TIMESTAMP('YYYY-MM-DD HH:MM:SS')``
    as UTC, so dropping the ``+00:00`` suffix preserves the same instant.
    Other types pass through ``str()`` unchanged.
    """
    if isinstance(val, datetime) and val.tzinfo is not None:
        val = val.astimezone(timezone.utc).replace(tzinfo=None)
    return str(val)


def batch(iterable: Iterator[Any], size: int) -> Iterator[list[Any]]:
    """Yield successive batches of `size` from an iterator."""
    chunk: list[Any] = []
    for item in iterable:
        chunk.append(item)
        if len(chunk) >= size:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


class _stage_ctx:
    """Tag any exception raised within the block with ``_drt_stage = <stage>``.

    Retrofit for #544 (ErrorFormatter): supersedes the traceback-walk
    heuristic in ``drt.cli.errors.infer_stage`` with an engine-emitted
    string tag. ``infer_stage`` reads ``getattr(exc, '_drt_stage', None)``
    first, falls back to the walk if absent (preserving back-compat for
    exceptions raised outside any ``_stage_ctx`` block).

    Stages are strings (``"source"`` / ``"destination"`` / ``"state"`` /
    ``"engine"``) rather than the ``drt.cli.errors.Stage`` enum so the
    engine module stays free of CLI imports — ``Stage`` has ``str`` as
    its base so ``Stage(tag)`` round-trips cleanly on the reader side.

    First writer wins. Nested blocks don't overwrite the outer-most
    attribution — if a source-raised exception bubbles up through a
    destination-stage block, the SOURCE tag set by the inner block
    stays. (Engine sites that wrap source iteration are themselves
    inside the for-loop that destination calls live in; the inner
    ``_stage_ctx("source")`` runs first.)
    """

    __slots__ = ("_stage",)

    def __init__(self, stage: str) -> None:
        self._stage = stage

    def __enter__(self) -> _stage_ctx:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, tb: Any) -> Literal[False]:
        if exc_val is not None and getattr(exc_val, "_drt_stage", None) is None:
            try:
                exc_val._drt_stage = self._stage
            except (AttributeError, TypeError):
                # Some C-level exception types reject attribute setting.
                # Silent skip — the traceback-walk fallback still works.
                pass
        return False  # propagate


def _staged_source_iter(
    source: Source, query: str, profile: ProfileConfig
) -> Iterator[dict[str, Any]]:
    """Wrap ``source.extract`` so iteration errors get tagged with stage="source".

    ``source.extract`` returns an iterator. Errors that fire during the
    initial call OR during subsequent ``__next__`` invocations both bubble
    through this generator's frame, which means ``_stage_ctx`` catches
    them whether the source materialises eagerly or lazily.
    """
    with _stage_ctx("source"):
        yield from source.extract(query, profile)


def run_sync(
    sync: SyncConfig,
    source: Source,
    destination: Destination | StagedDestination,
    profile: ProfileConfig,
    project_dir: Path,
    dry_run: bool = False,
    state_manager: StateManager | None = None,
    watermark_storage: WatermarkStorage | None = None,
    cursor_value_override: str | None = None,
    history_manager: HistoryManager | None = None,
    history_retention_days: int = 30,
    stop_event: threading.Event | None = None,
    compute_diff: bool = False,
    diff_limit: int = 20,
    observer: SyncObserver | None = None,
) -> SyncResult:
    """Run a single sync: extract from source, load to destination.

    Args:
        sync: Parsed sync YAML configuration.
        source: Source implementation (BigQuery, etc.).
        destination: Destination implementation (REST API, etc.).
        profile: Resolved source credentials.
        project_dir: Root of drt project (for ref() resolution).
        dry_run: If True, extract but do not call destination.load().
        state_manager: Source of truth for cursor reads (incremental sync
            watermark fallback chain). State PERSISTENCE no longer happens
            inside the engine — pass a ``StatePersistingObserver`` via
            ``observer=`` to persist the post-run ``SyncState`` (#548).
        watermark_storage: Source of truth for cursor reads from external
            watermark storage. As with ``state_manager``, watermark PERSISTENCE
            now flows through ``observer``.
        stop_event: Cooperative cancellation signal. When set (e.g. by a
            SIGTERM/SIGINT handler in the CLI), the engine finishes the
            current batch, marks ``result.interrupted=True``, persists state
            via the observer, and returns. ``None`` (default) preserves
            backward behaviour.
        compute_diff: When True (and ``dry_run`` is also True), compute a
            record-level diff (#413) against the destination's current state
            and populate ``SyncResult.diff``. Ignored when ``dry_run=False``.
        diff_limit: Cap on how many records appear in each diff category
            (added / updated / deleted / sample). Default 20.
        observer: Event sink for logging, state persistence, OTel spans,
            etc. Defaults to ``NullObserver`` — the engine becomes silent
            and does not persist state unless a real observer is passed.
            Library callers compose with ``CompositeObserver``; the CLI
            sets up ``LoggingObserver`` + ``StatePersistingObserver`` by
            default (see ``drt.cli.main._run_one``).

    Returns:
        Aggregated SyncResult across all batches.
    """
    if observer is None:
        observer = NullObserver()
    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.perf_counter()
    total_result = SyncResult()
    raised: BaseException | None = None

    observer.on_sync_started(sync.name, started_at)

    try:
        return _run_sync_body(
            sync=sync,
            source=source,
            destination=destination,
            profile=profile,
            project_dir=project_dir,
            dry_run=dry_run,
            state_manager=state_manager,
            watermark_storage=watermark_storage,
            cursor_value_override=cursor_value_override,
            stop_event=stop_event,
            compute_diff=compute_diff,
            diff_limit=diff_limit,
            started_at=started_at,
            t0=t0,
            total_result=total_result,
            observer=observer,
        )
    except BaseException as exc:
        raised = exc
        raise
    finally:
        duration_s = round(time.perf_counter() - t0, 3)
        if not dry_run and (raised is not None or total_result.failed > 0):
            try:
                from drt.alerts import build_context, dispatch_alerts

                dispatch_alerts(
                    sync.alerts,
                    "on_failure",
                    build_context(
                        sync_name=sync.name,
                        result=total_result,
                        duration_s=duration_s,
                        started_at=started_at,
                        exception=raised,
                    ),
                )
            except Exception as exc:  # noqa: BLE001 — best-effort
                observer.on_warning(sync.name, f"Alert dispatch outer failure: {exc}")

        # Append execution history (best-effort; never affects sync result).
        # Skip dry-run so test/preview invocations don't pollute history.
        if not dry_run and history_manager is not None:
            try:
                if raised is not None:
                    status = "failed"
                elif total_result.failed == 0:
                    status = "success"
                elif total_result.success > 0:
                    status = "partial"
                else:
                    status = "failed"

                error_strs: list[str] = list(total_result.errors)
                if raised is not None and not error_strs:
                    error_strs = [f"{type(raised).__name__}: {raised}"]

                history_manager.append(
                    HistoryEntry(
                        sync_name=sync.name,
                        started_at=started_at,
                        completed_at=datetime.now(timezone.utc).isoformat(),
                        duration_seconds=duration_s,
                        status=status,
                        records_synced=total_result.success,
                        records_failed=total_result.failed,
                        errors=error_strs,
                        cursor_value_used=getattr(total_result, "cursor_value_used", None),
                    )
                )
                history_manager.prune(sync.name, history_retention_days)
            except Exception as exc:  # noqa: BLE001 — best-effort
                observer.on_warning(sync.name, f"History append outer failure: {exc}")


def _run_sync_body(
    *,
    sync: SyncConfig,
    source: Source,
    destination: Destination | StagedDestination,
    profile: ProfileConfig,
    project_dir: Path,
    dry_run: bool,
    state_manager: StateManager | None,
    watermark_storage: WatermarkStorage | None,
    cursor_value_override: str | None,
    stop_event: threading.Event | None,
    compute_diff: bool,
    diff_limit: int,
    started_at: str,
    t0: float,
    total_result: SyncResult,
    observer: SyncObserver,
) -> SyncResult:
    """Inner body of run_sync. Mutates `total_result` in place so the outer
    finally-block can read partial results when an exception propagates.
    """
    # Load last cursor value for incremental syncs (fallback chain)
    cursor_field = sync.sync.cursor_field if sync.sync.mode == "incremental" else None
    last_cursor_value: str | None = None
    watermark_source: str | None = None  # "cli_override" | "storage" | "default_value"

    if cursor_field:
        # 1. CLI override (highest priority — backfill / recovery)
        if cursor_value_override is not None:
            last_cursor_value = cursor_value_override
            watermark_source = "cli_override"
            observer.on_watermark_resolved(sync.name, "cli_override", last_cursor_value)
        # 2. Watermark storage (GCS / BigQuery / local)
        elif watermark_storage:
            stored = watermark_storage.get(sync.name)
            if stored is not None:
                last_cursor_value = stored
                watermark_source = "storage"
                observer.on_watermark_resolved(sync.name, "storage", last_cursor_value)
        # 3. State manager fallback (local .drt/state.json)
        elif state_manager:
            prev = state_manager.get_last_sync(sync.name)
            if prev and prev.last_cursor_value is not None:
                last_cursor_value = prev.last_cursor_value
                watermark_source = "storage"

        # 4. default_value from watermark config
        wm = sync.sync.watermark
        if last_cursor_value is None and wm and wm.default_value is not None:
            last_cursor_value = wm.default_value
            watermark_source = "default_value"
            observer.on_watermark_resolved(sync.name, "default_value", last_cursor_value)

    query = resolve_model_ref(sync.model, project_dir, profile, cursor_field, last_cursor_value)

    # Source extraction wrapped via generator helper so exceptions raised
    # during iteration (not just the initial call) carry stage="source" (#544).
    records_iter = _staged_source_iter(source, query, profile)
    new_cursor_value: str | None = last_cursor_value
    is_staged = isinstance(destination, StagedDestination)
    staged_count = 0
    batches_processed = 0
    # When dry_run + compute_diff, accumulate (post-lookup) records to feed
    # the diff engine after extraction completes (#413).
    dry_run_records: list[dict[str, Any]] = []

    # Build lookup maps (one query per lookup, before the batch loop).
    # The build_lookup_map() call hits the destination, so tag failures
    # accordingly (#544).
    lookup_maps: dict[str, tuple[LookupConfig, dict[tuple[Any, ...], Any]]] = {}
    lookups: dict[str, LookupConfig] | None = getattr(
        sync.destination,
        "lookups",
        None,
    )
    if lookups:
        for warning in detect_ambiguous_lookup_ordering(lookups):
            observer.on_warning(sync.name, warning)
        for col_name, lk_config in lookups.items():
            with _stage_ctx("destination"):
                mapping = build_lookup_map(sync.destination, lk_config)
            lookup_maps[col_name] = (lk_config, mapping)

    for record_batch in batch(records_iter, sync.sync.batch_size):
        # Cooperative shutdown — break before processing this batch.
        # Existing batches are already finalized; partial state save below
        # stays consistent because the loop body never starts on this batch.
        if stop_event is not None and stop_event.is_set():
            total_result.interrupted = True
            observer.on_interrupted(sync.name, batches_processed)
            break

        total_result.rows_extracted += len(record_batch)

        # Track max cursor value seen across all batches.
        # Stringify with tz-naive UTC normalization for tz-aware datetimes
        # to avoid #475 (re-emit-at-boundary when user SQL is tz-naive).
        if cursor_field:
            for row in record_batch:
                val = row.get(cursor_field)
                if val is not None:
                    str_val = _stringify_cursor_value(val)
                    if new_cursor_value is None or _cursor_gt(str_val, new_cursor_value):
                        new_cursor_value = str_val

        # Apply destination lookups (FK resolution)
        if lookup_maps:
            batch_len_before = len(record_batch)
            record_batch, lookup_errors = apply_lookups(
                record_batch,
                lookup_maps,
                sync.sync.on_error,
            )
            total_result.row_errors.extend(lookup_errors)
            total_result.skipped += batch_len_before - len(record_batch)
            if not record_batch:
                continue

        # Declarative column rename (#415). Applied last — after cursor
        # tracking and lookups (both source-side) — so the mapped names
        # are what the destination, upsert_key, and the diff engine see.
        # Pure transform; no observer side effects.
        record_batch = apply_field_mappings(record_batch, sync.sync.field_mappings)

        if dry_run:
            total_result.success += len(record_batch)
            if compute_diff:
                dry_run_records.extend(record_batch)
            continue

        if is_staged:
            assert isinstance(destination, StagedDestination)
            with _stage_ctx("destination"):
                destination.stage(record_batch, sync.destination, sync.sync)
            staged_count += len(record_batch)
        else:
            assert isinstance(destination, Destination)
            with _stage_ctx("destination"):
                result = destination.load(record_batch, sync.destination, sync.sync)
            total_result.success += result.success
            total_result.failed += result.failed
            total_result.skipped += result.skipped
            total_result.errors.extend(result.errors)
            total_result.row_errors.extend(getattr(result, "row_errors", []))

            if sync.sync.on_error == "fail" and result.failed > 0:
                break

        batches_processed += 1

    # Finalize staged destinations (upload file, trigger job, poll).
    # finalize() is authoritative for staged success/failed counts —
    # stage() only buffers, so records aren't "successful" until finalize.
    if is_staged and not dry_run and staged_count > 0:
        assert isinstance(destination, StagedDestination)
        with _stage_ctx("destination"):
            finalize_result = destination.finalize(sync.destination, sync.sync)
        total_result.success += finalize_result.success
        total_result.failed += finalize_result.failed
        total_result.errors.extend(finalize_result.errors)
        total_result.row_errors.extend(getattr(finalize_result, "row_errors", []))

    # Duck-typed end-of-sync hook for non-staged destinations
    # (e.g. swap-finalize for replace_strategy=swap on Postgres/MySQL/ClickHouse).
    # The hook is intentionally NOT a Protocol — destinations opt in by simply
    # defining a finalize_sync() method.
    if not is_staged and not dry_run:
        finalizer = getattr(destination, "finalize_sync", None)
        if callable(finalizer):
            with _stage_ctx("destination"):
                finalize_result = finalizer(sync.destination, sync.sync)
            if finalize_result is not None:
                total_result.success += finalize_result.success
                total_result.failed += finalize_result.failed
                total_result.errors.extend(finalize_result.errors)
                total_result.row_errors.extend(
                    getattr(finalize_result, "row_errors", [])
                )

    # Compute the record-level diff after extraction completes (#413).
    # Only meaningful when dry_run is set; the engine collected all
    # post-lookup source records into dry_run_records during the loop.
    if dry_run and compute_diff:
        from drt.engine.diff import compute_diff as _compute_diff

        total_result.diff = _compute_diff(
            dry_run_records, sync.destination, sync.sync, limit=diff_limit
        )

    total_result.duration_seconds = round(time.perf_counter() - t0, 3)
    total_result.watermark_source = watermark_source
    total_result.cursor_value_used = last_cursor_value

    # State + watermark persistence is the observer's responsibility (#548).
    # The CLI composes ``LoggingObserver`` + ``StatePersistingObserver`` so
    # default user-facing behaviour is unchanged.
    observer.on_sync_completed(
        sync.name, total_result, started_at, new_cursor_value, cursor_field
    )

    return total_result
