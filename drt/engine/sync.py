"""Sync Engine — orchestrates extract → transform → load.

This module is the primary candidate for future Rust rewrite (PyO3).
Keep it pure: no I/O side effects beyond source/destination calls.
CLI owns all console output; engine only returns SyncResult.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from drt.config.credentials import ProfileConfig
from drt.config.models import LookupConfig, SyncConfig
from drt.destinations.base import Destination, StagedDestination, SyncResult
from drt.destinations.lookup import apply_lookups, build_lookup_map
from drt.engine.resolver import resolve_model_ref
from drt.sources.base import Source
from drt.state.manager import StateManager, SyncState
from drt.state.watermark import WatermarkStorage

logger = logging.getLogger("drt")


def _cursor_gt(new: str, current: str) -> bool:
    """Return True if new > current, using numeric comparison when both are numeric."""
    try:
        return float(new) > float(current)
    except (ValueError, TypeError):
        return new > current


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
) -> SyncResult:
    """Run a single sync: extract from source, load to destination.

    Args:
        sync: Parsed sync YAML configuration.
        source: Source implementation (BigQuery, etc.).
        destination: Destination implementation (REST API, etc.).
        profile: Resolved source credentials.
        project_dir: Root of drt project (for ref() resolution).
        dry_run: If True, extract but do not call destination.load().
        state_manager: If provided, persist sync result after completion.

    Returns:
        Aggregated SyncResult across all batches.
    """
    started_at = datetime.now(timezone.utc).isoformat()
    t0 = time.perf_counter()

    # Load last cursor value for incremental syncs (fallback chain)
    cursor_field = sync.sync.cursor_field if sync.sync.mode == "incremental" else None
    last_cursor_value: str | None = None
    watermark_source: str | None = None  # "cli_override" | "storage" | "default_value"

    if cursor_field:
        # 1. CLI override (highest priority — backfill / recovery)
        if cursor_value_override is not None:
            last_cursor_value = cursor_value_override
            watermark_source = "cli_override"
            logger.info(
                "sync='%s' watermark_source=cli_override cursor_value='%s'",
                sync.name,
                last_cursor_value,
            )
        # 2. Watermark storage (GCS / BigQuery / local)
        elif watermark_storage:
            stored = watermark_storage.get(sync.name)
            if stored is not None:
                last_cursor_value = stored
                watermark_source = "storage"
                logger.info(
                    "sync='%s' watermark_source=storage cursor_value='%s'",
                    sync.name,
                    last_cursor_value,
                )
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
            logger.info(
                "sync='%s' watermark_source=default_value cursor_value='%s' "
                "reason='no existing watermark'",
                sync.name,
                last_cursor_value,
            )

    query = resolve_model_ref(sync.model, project_dir, profile, cursor_field, last_cursor_value)

    records_iter = source.extract(query, profile)
    total_result = SyncResult()
    new_cursor_value: str | None = last_cursor_value
    is_staged = isinstance(destination, StagedDestination)
    staged_count = 0

    # Build lookup maps (one query per lookup, before the batch loop)
    lookup_maps: dict[str, tuple[LookupConfig, dict[tuple[Any, ...], Any]]] = {}
    lookups: dict[str, LookupConfig] | None = getattr(
        sync.destination,
        "lookups",
        None,
    )
    if lookups:
        for col_name, lk_config in lookups.items():
            mapping = build_lookup_map(sync.destination, lk_config)
            lookup_maps[col_name] = (lk_config, mapping)

    for record_batch in batch(records_iter, sync.sync.batch_size):
        total_result.rows_extracted += len(record_batch)

        # Track max cursor value seen across all batches
        if cursor_field:
            for row in record_batch:
                val = row.get(cursor_field)
                if val is not None:
                    str_val = str(val)
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

        if dry_run:
            total_result.success += len(record_batch)
            continue

        if is_staged:
            assert isinstance(destination, StagedDestination)
            destination.stage(record_batch, sync.destination, sync.sync)
            staged_count += len(record_batch)
        else:
            assert isinstance(destination, Destination)
            result = destination.load(record_batch, sync.destination, sync.sync)
            total_result.success += result.success
            total_result.failed += result.failed
            total_result.skipped += result.skipped
            total_result.errors.extend(result.errors)
            total_result.row_errors.extend(getattr(result, "row_errors", []))

            if sync.sync.on_error == "fail" and result.failed > 0:
                break

    # Finalize staged destinations (upload file, trigger job, poll).
    # finalize() is authoritative for staged success/failed counts —
    # stage() only buffers, so records aren't "successful" until finalize.
    if is_staged and not dry_run and staged_count > 0:
        assert isinstance(destination, StagedDestination)
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
            finalize_result = finalizer(sync.destination, sync.sync)
            if finalize_result is not None:
                total_result.success += finalize_result.success
                total_result.failed += finalize_result.failed
                total_result.errors.extend(finalize_result.errors)
                total_result.row_errors.extend(
                    getattr(finalize_result, "row_errors", [])
                )

    total_result.duration_seconds = round(time.perf_counter() - t0, 3)
    total_result.watermark_source = watermark_source
    total_result.cursor_value_used = last_cursor_value

    if state_manager is not None:
        status = (
            "success"
            if total_result.failed == 0
            else "partial"
            if total_result.success > 0
            else "failed"
        )
        state_manager.save_sync(
            SyncState(
                sync_name=sync.name,
                last_run_at=started_at,
                records_synced=total_result.success,
                status=status,
                error=total_result.errors[0] if total_result.errors else None,
                last_cursor_value=new_cursor_value if cursor_field else None,
            )
        )

    # Persist watermark to external storage if configured
    if watermark_storage is not None and cursor_field and new_cursor_value:
        watermark_storage.save(sync.name, new_cursor_value)

    return total_result
