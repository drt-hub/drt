"""Tests for the sync engine."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock, patch

from drt.config.credentials import BigQueryProfile, ProfileConfig
from drt.config.models import DestinationConfig, SyncConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.engine.sync import batch, run_sync

# ---------------------------------------------------------------------------
# Fakes (prefer over MagicMock — they document the Protocol)
# ---------------------------------------------------------------------------


class FakeSource:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict]:
        yield from self._rows

    def test_connection(self, config: ProfileConfig) -> bool:
        return True


class FakeDestination:
    def __init__(self, fail_indices: set[int] | None = None) -> None:
        self.calls: list[list[dict]] = []
        self._fail_indices = fail_indices or set()

    def load(
        self,
        records: list[dict],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        self.calls.append(records)
        result = SyncResult()
        for i, _ in enumerate(records):
            global_idx = sum(len(c) for c in self.calls[:-1]) + i
            if global_idx in self._fail_indices:
                result.failed += 1
                result.errors.append(f"Forced failure at index {global_idx}")
            else:
                result.success += 1
        return result


def _make_profile() -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="p", dataset="d")


def _make_sync(batch_size: int = 10, on_error: str = "fail") -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": "test_sync",
            "model": "ref('table')",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {"batch_size": batch_size, "on_error": on_error},
        }
    )


# ---------------------------------------------------------------------------
# batch() helper
# ---------------------------------------------------------------------------


def test_batch_exact_multiple() -> None:
    result = list(batch(iter([1, 2, 3, 4]), 2))
    assert result == [[1, 2], [3, 4]]


def test_batch_remainder() -> None:
    result = list(batch(iter([1, 2, 3]), 2))
    assert result == [[1, 2], [3]]


def test_batch_empty() -> None:
    assert list(batch(iter([]), 10)) == []


def test_batch_single_item() -> None:
    assert list(batch(iter([42]), 5)) == [[42]]


def test_batch_larger_than_size() -> None:
    result = list(batch(iter(range(10)), 3))
    assert len(result) == 4
    assert result[-1] == [9]


# ---------------------------------------------------------------------------
# run_sync()
# ---------------------------------------------------------------------------


def test_run_sync_all_success(tmp_path: Path) -> None:
    rows = [{"id": i} for i in range(5)]
    source = FakeSource(rows)
    dest = FakeDestination()
    sync = _make_sync(batch_size=3)

    result = run_sync(sync, source, dest, _make_profile(), tmp_path)

    assert result.success == 5
    assert result.failed == 0
    assert len(dest.calls) == 2  # batches: [0,1,2] + [3,4]
    assert result.duration_seconds is not None
    assert result.duration_seconds >= 0


def test_run_sync_dry_run(tmp_path: Path) -> None:
    rows = [{"id": i} for i in range(5)]
    source = FakeSource(rows)
    dest = FakeDestination()
    sync = _make_sync()

    result = run_sync(sync, source, dest, _make_profile(), tmp_path, dry_run=True)

    assert result.success == 5
    assert dest.calls == []  # destination never called


def test_run_sync_on_error_fail_stops(tmp_path: Path) -> None:
    rows = [{"id": i} for i in range(9)]
    source = FakeSource(rows)
    dest = FakeDestination(fail_indices={0})  # first record fails
    sync = _make_sync(batch_size=3, on_error="fail")

    result = run_sync(sync, source, dest, _make_profile(), tmp_path)

    assert result.failed > 0
    assert len(dest.calls) == 1  # stopped after first batch


def test_run_sync_on_error_skip_continues(tmp_path: Path) -> None:
    rows = [{"id": i} for i in range(6)]
    source = FakeSource(rows)
    dest = FakeDestination(fail_indices={0})
    sync = _make_sync(batch_size=3, on_error="skip")

    result = run_sync(sync, source, dest, _make_profile(), tmp_path)

    assert len(dest.calls) == 2  # both batches processed
    assert result.success == 5
    assert result.failed == 1


def test_run_sync_saves_state(tmp_path: Path) -> None:
    from drt.state.manager import StateManager

    rows = [{"id": 1}]
    source = FakeSource(rows)
    dest = FakeDestination()
    sync = _make_sync()
    state_mgr = StateManager(tmp_path)

    run_sync(sync, source, dest, _make_profile(), tmp_path, state_manager=state_mgr)

    state = state_mgr.get_last_sync("test_sync")
    assert state is not None
    assert state.status == "success"
    assert state.records_synced == 1


# ---------------------------------------------------------------------------
# incremental sync
# ---------------------------------------------------------------------------


def _make_incremental_sync(cursor_field: str = "updated_at") -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": "inc_sync",
            "model": "ref('events')",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {"mode": "incremental", "cursor_field": cursor_field, "batch_size": 10},
        }
    )


def test_incremental_saves_max_cursor(tmp_path: Path) -> None:
    from drt.state.manager import StateManager

    rows = [
        {"id": 1, "updated_at": "2024-01-01"},
        {"id": 2, "updated_at": "2024-01-03"},
        {"id": 3, "updated_at": "2024-01-02"},
    ]
    source = FakeSource(rows)
    dest = FakeDestination()
    sync = _make_incremental_sync()
    state_mgr = StateManager(tmp_path)

    run_sync(sync, source, dest, _make_profile(), tmp_path, state_manager=state_mgr)

    state = state_mgr.get_last_sync("inc_sync")
    assert state is not None
    assert state.last_cursor_value == "2024-01-03"


def test_incremental_uses_saved_cursor(tmp_path: Path) -> None:
    from drt.state.manager import StateManager, SyncState

    state_mgr = StateManager(tmp_path)
    state_mgr.save_sync(
        SyncState(
            sync_name="inc_sync",
            last_run_at="2024-01-01T00:00:00",
            records_synced=5,
            status="success",
            last_cursor_value="2024-01-01",
        )
    )

    captured_queries: list[str] = []

    class CapturingSource:
        def extract(self, query: str, config: object) -> list[dict]:
            captured_queries.append(query)
            return []

        def test_connection(self, config: object) -> bool:
            return True

    dest = FakeDestination()
    sync = _make_incremental_sync()

    run_sync(sync, CapturingSource(), dest, _make_profile(), tmp_path, state_manager=state_mgr)

    assert len(captured_queries) == 1
    assert "WHERE updated_at > '2024-01-01'" in captured_queries[0]


def test_watermark_storage_used_when_configured(tmp_path: Path) -> None:
    """When watermark config is set, engine uses WatermarkStorage."""
    from drt.state.watermark import LocalWatermarkStorage

    wm_storage = LocalWatermarkStorage(tmp_path)
    wm_storage.save("wm_sync", "2024-01-01")

    captured_queries: list[str] = []

    class CapturingSource:
        def extract(self, query: str, config: object) -> list[dict]:
            captured_queries.append(query)
            return [{"id": 1, "ts": "2024-01-05"}]

        def test_connection(self, config: object) -> bool:
            return True

    sync = SyncConfig.model_validate(
        {
            "name": "wm_sync",
            "model": "SELECT * FROM events WHERE ts >= '{{ cursor_value }}'",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {
                "mode": "incremental",
                "cursor_field": "ts",
                "watermark": {"storage": "local"},
            },
        }
    )
    dest = FakeDestination()
    result = run_sync(
        sync,
        CapturingSource(),
        dest,
        _make_profile(),
        tmp_path,
        watermark_storage=wm_storage,
    )

    assert result.success == 1
    assert "2024-01-01" in captured_queries[0]
    # Watermark should be updated
    assert wm_storage.get("wm_sync") == "2024-01-05"


# ---------------------------------------------------------------------------
# rows_extracted tracking (#342)
# ---------------------------------------------------------------------------


def test_rows_extracted_counts_source_rows(tmp_path: Path) -> None:
    rows = [{"id": i} for i in range(5)]
    source = FakeSource(rows)
    dest = FakeDestination()
    sync = _make_sync()

    result = run_sync(sync, source, dest, _make_profile(), tmp_path)
    assert result.rows_extracted == 5


def test_rows_extracted_with_failures(tmp_path: Path) -> None:
    rows = [{"id": i} for i in range(5)]
    source = FakeSource(rows)
    dest = FakeDestination(fail_indices={0, 2})
    sync = _make_sync(on_error="skip")

    result = run_sync(sync, source, dest, _make_profile(), tmp_path)
    assert result.rows_extracted == 5
    assert result.success == 3
    assert result.failed == 2


def test_rows_extracted_zero_rows(tmp_path: Path) -> None:
    source = FakeSource([])
    dest = FakeDestination()
    sync = _make_sync()

    result = run_sync(sync, source, dest, _make_profile(), tmp_path)
    assert result.rows_extracted == 0
    assert result.success == 0


def test_rows_extracted_dry_run(tmp_path: Path) -> None:
    rows = [{"id": i} for i in range(3)]
    source = FakeSource(rows)
    dest = FakeDestination()
    sync = _make_sync()

    result = run_sync(
        sync,
        source,
        dest,
        _make_profile(),
        tmp_path,
        dry_run=True,
    )
    assert result.rows_extracted == 3


# ---------------------------------------------------------------------------
# destination_lookup integration (#345)
# ---------------------------------------------------------------------------


def _make_lookup_sync() -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": "lookup_sync",
            "model": "ref('child_table')",
            "destination": {
                "type": "mysql",
                "host": "localhost",
                "dbname": "testdb",
                "table": "child_table",
                "upsert_key": ["parent_id", "code"],
                "lookups": {
                    "parent_id": {
                        "table": "parent_table",
                        "match": {"user_id": "user_id"},
                        "select": "id",
                        "on_miss": "skip",
                    },
                },
            },
            "sync": {"batch_size": 10},
        }
    )


@patch(
    "drt.engine.sync.build_lookup_map",
    return_value={("u1",): 10, ("u2",): 20},
)
def test_run_sync_with_lookup_all_match(
    mock_build: MagicMock,
    tmp_path: Path,
) -> None:
    rows = [
        {"user_id": "u1", "code": "a"},
        {"user_id": "u2", "code": "b"},
    ]
    source = FakeSource(rows)
    dest = FakeDestination()
    sync = _make_lookup_sync()

    result = run_sync(sync, source, dest, _make_profile(), tmp_path)

    assert result.rows_extracted == 2
    assert result.success == 2
    assert result.skipped == 0
    # Verify lookup values were injected
    loaded = dest.calls[0]
    assert loaded[0]["parent_id"] == 10
    assert loaded[1]["parent_id"] == 20


@patch(
    "drt.engine.sync.build_lookup_map",
    return_value={("u1",): 10},
)
def test_run_sync_with_lookup_skip_miss(
    mock_build: MagicMock,
    tmp_path: Path,
) -> None:
    rows = [
        {"user_id": "u1", "code": "a"},
        {"user_id": "unknown", "code": "b"},
    ]
    source = FakeSource(rows)
    dest = FakeDestination()
    sync = _make_lookup_sync()

    result = run_sync(sync, source, dest, _make_profile(), tmp_path)

    assert result.rows_extracted == 2
    assert result.success == 1
    assert result.skipped == 1
    assert len(result.row_errors) == 1


@patch(
    "drt.engine.sync.build_lookup_map",
    return_value={("u1",): 10, ("u2",): 20},
)
def test_run_sync_with_lookup_dry_run(
    mock_build: MagicMock,
    tmp_path: Path,
) -> None:
    rows = [{"user_id": "u1", "code": "a"}]
    source = FakeSource(rows)
    dest = FakeDestination()
    sync = _make_lookup_sync()

    result = run_sync(
        sync,
        source,
        dest,
        _make_profile(),
        tmp_path,
        dry_run=True,
    )

    assert result.rows_extracted == 1
    assert result.success == 1
    assert dest.calls == []  # destination never called


def test_full_sync_no_cursor_saved(tmp_path: Path) -> None:
    from drt.state.manager import StateManager

    rows = [{"id": 1, "updated_at": "2024-01-01"}]
    source = FakeSource(rows)
    dest = FakeDestination()
    sync = _make_sync()  # mode=full, no cursor_field
    state_mgr = StateManager(tmp_path)

    run_sync(sync, source, dest, _make_profile(), tmp_path, state_manager=state_mgr)

    state = state_mgr.get_last_sync("test_sync")
    assert state is not None
    assert state.last_cursor_value is None
