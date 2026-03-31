"""Sync Engine — orchestrates extract → transform → load.

This module is the primary candidate for future Rust rewrite (PyO3).
Keep it pure: no I/O side effects beyond source/destination calls.
CLI owns all console output; engine only returns SyncResult.
"""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from drt.config.credentials import ProfileConfig
from drt.config.models import SyncConfig
from drt.destinations.base import Destination, SyncResult
from drt.engine.resolver import resolve_model_ref
from drt.sources.base import Source
from drt.state.manager import StateManager, SyncState


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
    destination: Destination,
    profile: ProfileConfig,
    project_dir: Path,
    dry_run: bool = False,
    state_manager: StateManager | None = None,
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

    # Load last cursor value for incremental syncs
    cursor_field = sync.sync.cursor_field if sync.sync.mode == "incremental" else None
    last_cursor_value: str | None = None
    if cursor_field and state_manager:
        prev = state_manager.get_last_sync(sync.name)
        if prev:
            last_cursor_value = prev.last_cursor_value

    query = resolve_model_ref(sync.model, project_dir, profile, cursor_field, last_cursor_value)

    records_iter = source.extract(query, profile)
    total_result = SyncResult()
    new_cursor_value: str | None = last_cursor_value

    for record_batch in batch(records_iter, sync.sync.batch_size):
        # Track max cursor value seen across all batches
        if cursor_field:
            for row in record_batch:
                val = row.get(cursor_field)
                if val is not None:
                    str_val = str(val)
                    if new_cursor_value is None or _cursor_gt(str_val, new_cursor_value):
                        new_cursor_value = str_val

        if dry_run:
            total_result.success += len(record_batch)
            continue

        result = destination.load(record_batch, sync.destination, sync.sync)
        total_result.success += result.success
        total_result.failed += result.failed
        total_result.skipped += result.skipped
        total_result.row_errors.extend(result.row_errors)

        if sync.sync.on_error == "fail" and result.failed > 0:
            break

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

    return total_result
