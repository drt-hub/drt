"""Idempotency boundary tests: state persistence + re-run semantics.

Locks in the current contract:

1. When ``state_manager`` is provided to ``run_sync``, a ``SyncState`` row is
   persisted with the correct ``records_synced`` and ``status``.
2. drt does NOT deduplicate at the engine layer. Re-running the same sync
   without a cursor sends every source row to the destination again. (This
   is by design — deduplication is the destination's responsibility, e.g.
   upsert-by-key for the REST destination.)
3. Re-running with the same state file overwrites the prior entry, not
   appends.

Out of scope here (tracked as follow-ups):

- Kill-mid-batch resume: requires hooking the engine to abort partway. The
  ``stop_event`` parameter exists for this but exercising it deterministically
  is its own test module.
- Cursor-based incremental idempotency: requires ``watermark_storage`` + a
  ``cursor_field`` on the sync. Worth its own dedicated file once
  watermark behaviour stabilises in v0.8.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

duckdb = pytest.importorskip("duckdb")  # noqa: F841  — module-gate

from drt.config.models import RestApiDestinationConfig, SyncConfig, SyncOptions  # noqa: E402
from drt.destinations.rest_api import RestApiDestination  # noqa: E402
from drt.engine.sync import run_sync  # noqa: E402
from drt.state.manager import StateManager  # noqa: E402


def _dest(httpserver) -> RestApiDestinationConfig:
    return RestApiDestinationConfig(
        type="rest_api",
        url=httpserver.url_for("/sink"),
        method="POST",
        headers={"Content-Type": "application/json"},
    )


def _sync_cfg(dest: RestApiDestinationConfig) -> SyncConfig:
    return SyncConfig(
        name="idem_sync",
        model="ref('users')",
        destination=dest,
        sync=SyncOptions(batch_size=10),
    )


def test_state_recorded_after_successful_sync(
    httpserver, duckdb_with_users, tmp_path: Path
) -> None:
    """After a successful run, StateManager holds a SyncState with success status."""
    source, profile = duckdb_with_users
    httpserver.expect_request("/sink", method="POST").respond_with_data("OK", status=200)

    state = StateManager(tmp_path)
    run_sync(_sync_cfg(_dest(httpserver)), source, RestApiDestination(), profile, tmp_path,
             state_manager=state)

    saved = state.get_last_sync("idem_sync")
    assert saved is not None
    assert saved.sync_name == "idem_sync"
    assert saved.status == "success"
    assert saved.records_synced == 3
    assert saved.error is None


def test_second_run_resends_all_rows_without_cursor(
    httpserver, duckdb_with_users, tmp_path: Path
) -> None:
    """No built-in dedup: two runs of the same sync produce 2× the requests.

    This is the documented behaviour. Deduplication is the destination's
    responsibility — REST upsert keys, SQL MERGE, etc.
    """
    source, profile = duckdb_with_users
    received: list[dict] = []

    def handler(request):
        received.append(json.loads(request.data))
        from werkzeug.wrappers import Response

        return Response("OK", status=200)

    httpserver.expect_request("/sink", method="POST").respond_with_handler(handler)

    state = StateManager(tmp_path)
    cfg = _sync_cfg(_dest(httpserver))
    run_sync(cfg, source, RestApiDestination(), profile, tmp_path, state_manager=state)
    run_sync(cfg, source, RestApiDestination(), profile, tmp_path, state_manager=state)

    assert len(received) == 6  # 3 rows × 2 runs
    # IDs duplicated, not deduplicated
    assert sorted(r["id"] for r in received) == [1, 1, 2, 2, 3, 3]


def test_state_entry_is_overwritten_not_appended(
    httpserver, duckdb_with_users, tmp_path: Path
) -> None:
    """A second run updates the existing state row in place (single entry per sync_name)."""
    source, profile = duckdb_with_users
    httpserver.expect_request("/sink", method="POST").respond_with_data("OK", status=200)

    state = StateManager(tmp_path)
    cfg = _sync_cfg(_dest(httpserver))
    run_sync(cfg, source, RestApiDestination(), profile, tmp_path, state_manager=state)
    first_run_at = state.get_last_sync("idem_sync").last_run_at  # type: ignore[union-attr]

    run_sync(cfg, source, RestApiDestination(), profile, tmp_path, state_manager=state)
    all_states = state.get_all()

    assert list(all_states.keys()) == ["idem_sync"]  # single key, not appended
    assert all_states["idem_sync"].last_run_at >= first_run_at  # timestamp moved forward
