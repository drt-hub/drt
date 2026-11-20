"""Shared state-reset semantics for CLI and MCP entry points."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, replace

from drt.state.manager import StateStore, SyncState
from drt.state.watermark import WatermarkStorage


@dataclass(frozen=True)
class StateResetResult:
    watermark_cleared: bool = False
    runs_removed: bool = False
    exclusive_checkpoint_preserved: bool = False


def reset_watermark_and_runs(
    state: StateStore,
    sync_name: str,
    *,
    watermark_storages: Iterable[WatermarkStorage] = (),
    watermark: bool = False,
    runs: bool = False,
) -> StateResetResult:
    """Reset requested levels without separating an exclusive checkpoint pair."""
    current_state = state.get_last_sync(sync_name)
    watermark_cleared = False
    if watermark:
        for storage in watermark_storages:
            storage.delete(sync_name)
            watermark_cleared = True
        if current_state is not None and (
            current_state.last_cursor_value is not None
            or current_state.cursor_boundary is not None
        ):
            state.save_sync(
                replace(current_state, last_cursor_value=None, cursor_boundary=None)
            )
            watermark_cleared = True

    runs_removed = False
    exclusive_checkpoint_preserved = False
    if runs:
        exclusive_checkpoint_preserved = (
            not watermark
            and current_state is not None
            and current_state.cursor_boundary == "exclusive"
        )
        if exclusive_checkpoint_preserved:
            assert current_state is not None
            state.save_sync(
                SyncState(
                    sync_name=sync_name,
                    last_run_at="",
                    records_synced=0,
                    status="success",
                    last_cursor_value=current_state.last_cursor_value,
                    cursor_boundary=current_state.cursor_boundary,
                )
            )
            runs_removed = True
        else:
            runs_removed = state.reset(sync_name)

    return StateResetResult(
        watermark_cleared=watermark_cleared,
        runs_removed=runs_removed,
        exclusive_checkpoint_preserved=exclusive_checkpoint_preserved,
    )
