"""Sequential execution/checkpoint coverage for ADR 0021 slice 3."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from drt.cli.commands.retry import replay_dead_letters
from drt.cli.main import app
from drt.config.credentials import BigQueryProfile, RestApiProfile, SQLiteProfile
from drt.config.models import DestinationConfig, SyncConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.engine.observer import (
    CompositeObserver,
    DlqObserver,
    StatePersistingObserver,
    is_window_checkpoint_capable,
)
from drt.engine.plan import unsupported_reason
from drt.engine.sync import run_sync as engine_run_sync
from drt.sources.sqlite import SQLiteSource
from drt.state.history import HistoryEntry
from drt.state.manager import LocalStateManager, SyncState
from drt.state.watermark import LocalWatermarkStorage


def run_sync(*args: Any, **kwargs: Any) -> SyncResult:
    """Slice-3 engine tests opt in explicitly; entry-point tests use the raw API."""
    kwargs["allow_backfill_execution"] = True
    return engine_run_sync(*args, **kwargs)


class MemoryState:
    def __init__(self, state: SyncState | None = None) -> None:
        self.value = state
        self.saves: list[SyncState] = []

    def get_last_sync(self, sync_name: str) -> SyncState | None:
        return self.value if self.value is not None and self.value.sync_name == sync_name else None

    def get_all(self) -> dict[str, SyncState]:
        return {self.value.sync_name: self.value} if self.value is not None else {}

    def save_sync(self, state: SyncState) -> None:
        self.value = replace(state)
        self.saves.append(replace(state))

    def reset(self, sync_name: str) -> bool:
        existed = self.value is not None and self.value.sync_name == sync_name
        if existed:
            self.value = None
        return existed

    def now(self) -> str:
        return "2026-01-01T00:00:00+00:00"


class MemoryWatermark:
    def __init__(self, value: str | None = None, *, fail_save: bool = False) -> None:
        self.value = value
        self.fail_save = fail_save
        self.saves: list[str] = []

    def get(self, sync_name: str) -> str | None:
        return self.value

    def save(self, sync_name: str, value: str) -> None:
        self.saves.append(value)
        if self.fail_save:
            raise RuntimeError("watermark unavailable")
        self.value = value

    def delete(self, sync_name: str) -> None:
        self.value = None


class WindowSource:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.windows: list[tuple[str | int, str | int]] = []
        self.extract_calls = 0

    def extract(
        self,
        query: str,
        config: RestApiProfile,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        self.extract_calls += 1
        yield from ()

    def extract_window(
        self,
        query: str,
        profile: RestApiProfile,
        low: str | int,
        high: str | int,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        self.windows.append((low, high))
        for row in self.rows:
            cursor = row["cursor"]
            if low <= cursor < high:
                yield dict(row)

    def test_connection(self, config: RestApiProfile) -> bool:
        return True


class RecordingDestination:
    def __init__(self, *, fail_call: int | None = None, failed_rows: bool = False) -> None:
        self.calls: list[list[dict[str, Any]]] = []
        self.fail_call = fail_call
        self.failed_rows = failed_rows

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        self.calls.append([dict(row) for row in records])
        if self.fail_call == len(self.calls):
            self.fail_call = None
            raise RuntimeError("injected window failure")
        if self.failed_rows:
            return SyncResult(failed=len(records), errors=["injected row failure"])
        return SyncResult(success=len(records))


class FinalizingDestination(RecordingDestination):
    def __init__(self, *, fail_finalize: int | None = None) -> None:
        super().__init__()
        self.finalize_calls = 0
        self.fail_finalize = fail_finalize

    def finalize_sync(self, config: DestinationConfig, sync_options: SyncOptions) -> SyncResult:
        self.finalize_calls += 1
        if self.finalize_calls == self.fail_finalize:
            self.fail_finalize = None
            raise RuntimeError("injected finalizer failure")
        return SyncResult()


class RecordingHistory:
    def __init__(self) -> None:
        self.entries: list[HistoryEntry] = []

    def append(self, entry: HistoryEntry) -> None:
        self.entries.append(entry)

    def read(self, sync_name: str | None = None, limit: int = 20) -> list[HistoryEntry]:
        return self.entries[-limit:]

    def prune(self, sync_name: str, retention_days: int) -> int:
        return 0


def _sync(
    *,
    start: int = 0,
    end: int = 9,
    window: int = 3,
    on_error: str = "fail",
) -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": "events",
            "model": "events",
            "destination": {"type": "rest_api", "url": "https://example.test"},
            "sync": {
                "mode": "incremental",
                "cursor_field": "cursor",
                "batch_size": 100,
                "on_error": on_error,
                "backfill": {"window": window, "start": start, "end": end},
            },
        }
    )


def _profile() -> RestApiProfile:
    return RestApiProfile(type="rest_api", url="https://source.test")


def _observer(state: MemoryState, watermark: MemoryWatermark | None = None) -> CompositeObserver:
    return CompositeObserver([StatePersistingObserver(state, watermark)])


def test_happy_path_and_empty_windows_checkpoint_sequentially(tmp_path: Path) -> None:
    state = MemoryState()
    source = WindowSource([{"cursor": 0}, {"cursor": 7}])
    destination = RecordingDestination()

    result = run_sync(
        _sync(),
        source,
        destination,
        _profile(),
        tmp_path,
        state_manager=state,
        observer=_observer(state),
    )

    assert source.windows == [(0, 3), (3, 6), (6, 9)]
    assert [saved.last_cursor_value for saved in state.saves[:3]] == ["3", "6", "9"]
    assert all(saved.cursor_boundary == "exclusive" for saved in state.saves)
    assert len(destination.calls) == 2
    assert result.backfill is not None
    assert result.backfill["windows_completed"] == 3
    assert result.backfill["final_checkpoint"] == "9"
    assert len(result.windows) == 3


def test_failure_resumes_at_last_completed_window(tmp_path: Path) -> None:
    state = MemoryState()
    source = WindowSource([{"cursor": value} for value in range(9)])
    destination = RecordingDestination(fail_call=2)
    history = RecordingHistory()

    with pytest.raises(RuntimeError, match="injected window failure"):
        run_sync(
            _sync(),
            source,
            destination,
            _profile(),
            tmp_path,
            state_manager=state,
            observer=_observer(state),
            history_manager=history,
        )
    assert state.value is not None
    assert state.value.last_cursor_value == "3"

    result = run_sync(
        _sync(),
        source,
        destination,
        _profile(),
        tmp_path,
        state_manager=state,
        observer=_observer(state),
        history_manager=history,
    )
    assert source.windows[-2:] == [(3, 6), (6, 9)]
    assert result.backfill is not None and result.backfill["windows_completed"] == 2
    assert len(history.entries) == 2


@pytest.mark.parametrize(("on_error", "checkpoint"), [("skip", "9"), ("fail", None)])
def test_on_error_checkpoint_semantics(
    tmp_path: Path, on_error: str, checkpoint: str | None
) -> None:
    state = MemoryState()
    result = run_sync(
        _sync(on_error=on_error),
        WindowSource([{"cursor": 0}]),
        RecordingDestination(failed_rows=True),
        _profile(),
        tmp_path,
        state_manager=state,
        observer=_observer(state),
    )
    if checkpoint is None:
        assert state.value is not None and state.value.last_cursor_value is None
        assert result.backfill is not None and result.backfill["failed_window"] == 0
    else:
        assert state.value is not None and state.value.last_cursor_value == checkpoint


def test_checkpoint_split_failure_writes_state_first_and_aborts(tmp_path: Path) -> None:
    state = MemoryState()
    watermark = MemoryWatermark(fail_save=True)
    source = WindowSource([])

    with pytest.raises(RuntimeError, match="watermark unavailable"):
        run_sync(
            _sync(),
            source,
            RecordingDestination(),
            _profile(),
            tmp_path,
            state_manager=state,
            watermark_storage=watermark,
            observer=_observer(state, watermark),
        )
    assert state.value is not None
    assert state.value.last_cursor_value == "3"
    assert state.value.cursor_boundary == "exclusive"
    assert source.windows == [(0, 3)]


def test_local_state_round_trips_exclusive_boundary_and_loads_legacy(tmp_path: Path) -> None:
    manager = LocalStateManager(tmp_path)
    manager.save_sync(
        SyncState(
            "events",
            "now",
            3,
            "partial",
            last_cursor_value="3",
            cursor_boundary="exclusive",
        )
    )
    assert manager.get_last_sync("events") == SyncState(
        "events",
        "now",
        3,
        "partial",
        last_cursor_value="3",
        cursor_boundary="exclusive",
    )

    state_file = tmp_path / ".drt" / "state.json"
    state_file.write_text(
        '{"legacy": {"sync_name": "legacy", "last_run_at": "old", '
        '"records_synced": 1, "status": "success", "last_cursor_value": "1"}}'
    )
    legacy = manager.get_last_sync("legacy")
    assert legacy is not None and legacy.cursor_boundary is None


def test_checkpoint_never_regresses(tmp_path: Path) -> None:
    state = MemoryState(
        SyncState("events", "old", 1, "success", last_cursor_value="9", cursor_boundary="exclusive")
    )
    watermark = MemoryWatermark("9")
    result = run_sync(
        _sync(end=6),
        WindowSource([]),
        RecordingDestination(),
        _profile(),
        tmp_path,
        state_manager=state,
        watermark_storage=watermark,
        observer=_observer(state, watermark),
    )
    assert watermark.value == "9"
    assert state.value is not None and state.value.last_cursor_value == "9"
    assert result.backfill is not None and result.backfill["windows_planned"] == 0


def test_exclusive_marker_refuses_legacy_execution_before_extract(tmp_path: Path) -> None:
    state = MemoryState(
        SyncState("events", "old", 1, "success", last_cursor_value="3", cursor_boundary="exclusive")
    )
    sync = _sync()
    sync.sync.backfill = None
    source = WindowSource([])
    with pytest.raises(ValueError, match="exclusive window checkpoint.*backfill was removed"):
        run_sync(sync, source, RecordingDestination(), _profile(), tmp_path, state_manager=state)
    assert source.extract_calls == 0


def test_runs_only_reset_then_backfill_removal_still_refuses(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "syncs").mkdir()
    (tmp_path / "drt_project.yml").write_text("name: p\nversion: '1'\nprofile: dev\n")
    state = LocalStateManager(tmp_path)
    state.save_sync(
        SyncState(
            "events",
            "old",
            1,
            "partial",
            last_cursor_value="3",
            cursor_boundary="exclusive",
        )
    )
    result = CliRunner().invoke(app, ["state", "reset", "events", "--runs", "--yes"])
    assert result.exit_code == 0
    sync = _sync()
    sync.sync.backfill = None
    source = WindowSource([])
    with pytest.raises(ValueError, match="exclusive window checkpoint.*backfill was removed"):
        engine_run_sync(
            sync,
            source,
            RecordingDestination(),
            _profile(),
            tmp_path,
            state_manager=state,
        )
    assert source.extract_calls == 0


def test_null_observer_and_dry_run_are_refused_before_extraction(tmp_path: Path) -> None:
    source = WindowSource([])
    with pytest.raises(RuntimeError, match="checkpoint-capable observer"):
        run_sync(_sync(), source, RecordingDestination(), _profile(), tmp_path)
    assert source.windows == []

    with pytest.raises(ValueError, match="Dry-run execution does not support sync.backfill"):
        run_sync(
            _sync(),
            source,
            RecordingDestination(),
            _profile(),
            tmp_path,
            dry_run=True,
        )
    assert source.windows == []


def test_rows_outside_window_fail_closed(tmp_path: Path) -> None:
    class BrokenSource(WindowSource):
        def extract_window(
            self,
            query: str,
            profile: RestApiProfile,
            low: str | int,
            high: str | int,
            *,
            query_tags: dict[str, str] | None = None,
        ) -> Iterator[dict[str, Any]]:
            yield {"cursor": 999}

    state = MemoryState()
    with pytest.raises(ValueError, match="outside requested half-open window"):
        run_sync(
            _sync(),
            BrokenSource([]),
            RecordingDestination(),
            _profile(),
            tmp_path,
            state_manager=state,
            observer=_observer(state),
        )
    assert state.value is None


def test_finalizer_failure_leaves_window_resumable(tmp_path: Path) -> None:
    state = MemoryState()
    destination = FinalizingDestination(fail_finalize=2)
    with pytest.raises(RuntimeError, match="injected finalizer failure"):
        run_sync(
            _sync(),
            WindowSource([]),
            destination,
            _profile(),
            tmp_path,
            state_manager=state,
            observer=_observer(state),
        )
    assert state.value is not None and state.value.last_cursor_value == "3"
    assert destination.finalize_calls == 2

    result = run_sync(
        _sync(),
        WindowSource([]),
        destination,
        _profile(),
        tmp_path,
        state_manager=state,
        observer=_observer(state),
    )
    assert result.backfill is not None and result.backfill["windows_completed"] == 2
    assert state.value is not None and state.value.last_cursor_value == "9"


def test_cooperative_interrupt_between_windows_keeps_completed_checkpoint(
    tmp_path: Path,
) -> None:
    stop = threading.Event()
    state = MemoryState()

    class StopAfterCheckpoint(StatePersistingObserver):
        def checkpoint_window(
            self,
            sync_name: str,
            started_at: str,
            result: SyncResult,
            cursor_field: str,
            high: str | int,
        ) -> str:
            durable = super().checkpoint_window(sync_name, started_at, result, cursor_field, high)
            stop.set()
            return durable

    source = WindowSource([])
    result = run_sync(
        _sync(),
        source,
        RecordingDestination(),
        _profile(),
        tmp_path,
        state_manager=state,
        observer=CompositeObserver([StopAfterCheckpoint(state, None)]),
        stop_event=stop,
    )
    assert result.interrupted is True
    assert source.windows == [(0, 3)]
    assert state.value is not None and state.value.last_cursor_value == "3"


def test_final_partial_window_uses_fixed_exclusive_end(tmp_path: Path) -> None:
    state = MemoryState()
    source = WindowSource([])
    result = run_sync(
        _sync(end=8),
        source,
        RecordingDestination(),
        _profile(),
        tmp_path,
        state_manager=state,
        observer=_observer(state),
    )
    assert source.windows == [(0, 3), (3, 6), (6, 8)]
    assert result.windows[-1]["high"] == 8
    assert state.value is not None and state.value.last_cursor_value == "8"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"extract_limit": 1},
        {"cursor_value_override": "1"},
        {"full_refresh": True},
        {"exact": True},
        {"compute_diff": True, "dry_run": True},
    ],
)
def test_engine_temporary_guards_precede_extraction(tmp_path: Path, kwargs: dict[str, Any]) -> None:
    source = WindowSource([])
    with pytest.raises(ValueError):
        run_sync(_sync(), source, RecordingDestination(), _profile(), tmp_path, **kwargs)
    assert source.windows == []


def test_plan_and_apply_gate_remains_closed() -> None:
    reason = unsupported_reason(_sync())
    assert reason is not None
    assert "plan/apply" in reason


def test_retry_execution_gate_remains_closed(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="retry does not support sync.backfill"):
        replay_dead_letters(_sync(), project_dir=tmp_path)


def test_numeric_ceiling_is_snapshotted_once_and_empty_windows_advance(tmp_path: Path) -> None:
    class NumericSqlSource:
        def __init__(self) -> None:
            self.max_queries = 0
            self.window_queries = 0

        def extract(
            self,
            query: str,
            config: BigQueryProfile,
            *,
            query_tags: dict[str, str] | None = None,
        ) -> Iterator[dict[str, Any]]:
            if "MAX(" in query:
                self.max_queries += 1
                yield {"_drt_cursor_max": 5}
            else:
                self.window_queries += 1

        def test_connection(self, config: BigQueryProfile) -> bool:
            return True

    sync = _sync(end=9)
    assert sync.sync.backfill is not None
    sync.sync.backfill.end = None
    source = NumericSqlSource()
    state = MemoryState()
    result = run_sync(
        sync,
        source,
        RecordingDestination(),
        BigQueryProfile(type="bigquery", project="p", dataset="d"),
        tmp_path,
        state_manager=state,
        observer=_observer(state),
    )
    assert source.max_queries == 1
    assert source.window_queries == 2
    assert result.backfill is not None and result.backfill["ceiling"] == 6
    assert state.value is not None and state.value.last_cursor_value == "6"


def test_sqlite_end_to_end_resumes_three_window_backfill(tmp_path: Path) -> None:
    database = tmp_path / "source.db"
    with sqlite3.connect(database) as connection:
        connection.execute("CREATE TABLE events (cursor INTEGER PRIMARY KEY, payload TEXT)")
        connection.executemany(
            "INSERT INTO events VALUES (?, ?)", [(value, f"v{value}") for value in range(9)]
        )

    sync = _sync()
    sync.model = "SELECT cursor, payload FROM events"
    profile = SQLiteProfile(type="sqlite", database=str(database))
    state = MemoryState()
    destination = RecordingDestination(fail_call=2)

    with pytest.raises(RuntimeError, match="injected window failure"):
        run_sync(
            sync,
            SQLiteSource(),
            destination,
            profile,
            tmp_path,
            state_manager=state,
            observer=_observer(state),
        )
    assert state.value is not None and state.value.last_cursor_value == "3"

    result = run_sync(
        sync,
        SQLiteSource(),
        destination,
        profile,
        tmp_path,
        state_manager=state,
        observer=_observer(state),
    )
    assert result.success == 6
    assert state.value is not None and state.value.last_cursor_value == "9"
    assert [row["cursor"] for call in destination.calls[-2:] for row in call] == list(range(3, 9))


def test_library_default_refuses_backfill_before_extraction(tmp_path: Path) -> None:
    source = WindowSource([])
    with pytest.raises(ValueError, match="disabled for this entry point"):
        engine_run_sync(_sync(), source, RecordingDestination(), _profile(), tmp_path)
    assert source.windows == []


def test_null_cursor_fails_window_even_with_on_error_skip(tmp_path: Path) -> None:
    class MissingCursorSource(WindowSource):
        def extract_window(
            self,
            query: str,
            profile: RestApiProfile,
            low: str | int,
            high: str | int,
            *,
            query_tags: dict[str, str] | None = None,
        ) -> Iterator[dict[str, Any]]:
            yield {"payload": "missing cursor"}

    state = MemoryState()
    result = run_sync(
        _sync(on_error="skip"),
        MissingCursorSource([]),
        RecordingDestination(),
        _profile(),
        tmp_path,
        observer=_observer(state),
    )
    assert state.value is not None
    assert state.value.last_cursor_value is None
    assert state.value.cursor_boundary is None
    assert result.failed == 1
    assert result.skipped == 0
    assert result.backfill is not None and result.backfill["failed_window"] == 0


def test_only_designated_observer_checkpoints_but_all_receive_events(tmp_path: Path) -> None:
    class SpyCheckpoint(StatePersistingObserver):
        def __init__(self, state: MemoryState) -> None:
            super().__init__(state, None)
            self.checkpoints = 0
            self.started = 0

        def checkpoint_window(
            self,
            sync_name: str,
            started_at: str,
            result: SyncResult,
            cursor_field: str,
            high: str | int,
        ) -> str:
            self.checkpoints += 1
            return super().checkpoint_window(sync_name, started_at, result, cursor_field, high)

        def window_started(
            self, sync_name: str, index: int, low: str | int, high: str | int
        ) -> None:
            self.started += 1

    primary = SpyCheckpoint(MemoryState())
    extra = SpyCheckpoint(MemoryState())
    observer = CompositeObserver([primary, extra], checkpoint_observer=primary)
    run_sync(
        _sync(),
        WindowSource([]),
        RecordingDestination(),
        _profile(),
        tmp_path,
        observer=observer,
    )
    assert primary.checkpoints == 3
    assert extra.checkpoints == 0
    assert primary.started == extra.started == 3


def test_async_or_incompatible_checkpoint_capability_is_refused() -> None:
    class AsyncCheckpoint:
        async def checkpoint_window(self, *args: Any) -> str:
            return "0"

        def read_window_checkpoint(self, sync_name: str) -> tuple[str | None, str | None]:
            return None, None

    assert not is_window_checkpoint_capable(AsyncCheckpoint())


def test_resume_reads_from_checkpoint_observer_store_when_engine_store_is_none(
    tmp_path: Path,
) -> None:
    state = MemoryState(
        SyncState("events", "old", 1, "partial", last_cursor_value="3", cursor_boundary="exclusive")
    )
    source = WindowSource([])
    run_sync(
        _sync(),
        source,
        RecordingDestination(),
        _profile(),
        tmp_path,
        state_manager=None,
        observer=_observer(state),
    )
    assert source.windows == [(3, 6), (6, 9)]


def test_underreported_batch_fails_window_and_does_not_mark_ledger(tmp_path: Path) -> None:
    class UnderreportingDestination(RecordingDestination):
        def load(
            self,
            records: list[dict[str, Any]],
            config: DestinationConfig,
            sync_options: SyncOptions,
        ) -> SyncResult:
            return SyncResult(success=max(0, len(records) - 1))

    class Ledger:
        def __init__(self) -> None:
            self.marked: list[str] = []

        def already_delivered(self, sync_name: str, keys: set[str]) -> set[str]:
            return set()

        def mark_delivered(self, sync_name: str, keys: list[str], delivered_at: str) -> None:
            self.marked.extend(keys)

        def prune(self, sync_name: str, retention_days: int) -> int:
            return 0

    sync = _sync(end=3)
    sync.sync.idempotency_key = "{{ row.cursor }}"
    state = MemoryState()
    ledger = Ledger()
    with pytest.raises(RuntimeError, match="reported 1 outcomes for 2 records"):
        run_sync(
            sync,
            WindowSource([{"cursor": 0}, {"cursor": 1}]),
            UnderreportingDestination(),
            _profile(),
            tmp_path,
            observer=_observer(state),
            idempotency_ledger=ledger,
        )
    assert state.value is None
    assert ledger.marked == []


def test_dlq_append_failure_is_fatal_before_checkpoint(tmp_path: Path) -> None:
    class FailingDlq:
        def append(self, *args: Any, **kwargs: Any) -> int:
            raise OSError("disk full")

    class RowFailingDestination(RecordingDestination):
        def load(
            self,
            records: list[dict[str, Any]],
            config: DestinationConfig,
            sync_options: SyncOptions,
        ) -> SyncResult:
            return SyncResult(
                failed=len(records),
                row_errors=[RowError(0, "row", None, "rejected")],
            )

    state = MemoryState()
    checkpoint = StatePersistingObserver(state, None)
    observer = CompositeObserver(
        [checkpoint, DlqObserver(FailingDlq())], checkpoint_observer=checkpoint
    )
    with pytest.raises(RuntimeError, match="DLQ persist failure"):
        run_sync(
            _sync(end=3, on_error="skip"),
            WindowSource([{"cursor": 0}]),
            RowFailingDestination(),
            _profile(),
            tmp_path,
            observer=observer,
        )
    assert state.value is None


def test_staged_partial_success_checkpoints_on_skip_and_dlqs_failed_row(tmp_path: Path) -> None:
    class Staged:
        def __init__(self) -> None:
            self.records: list[dict[str, Any]] = []
            self.finalize_calls = 0

        def stage(
            self,
            records: list[dict[str, Any]],
            config: DestinationConfig,
            sync_options: SyncOptions,
        ) -> None:
            self.records.extend(records)

        def finalize(self, config: DestinationConfig, sync_options: SyncOptions) -> SyncResult:
            self.finalize_calls += 1
            if self.records:
                count = len(self.records)
                self.records.clear()
                return SyncResult(
                    success=count - 1,
                    failed=1,
                    row_errors=[RowError(0, "row", None, "rejected")],
                )
            return SyncResult()

    class RecordingDlq:
        def __init__(self) -> None:
            self.entries: list[Any] = []

        def append(self, sync_name: str, entries: list[Any], *, max_records: int = 10_000) -> int:
            self.entries.extend(entries)
            return len(self.entries)

    state = MemoryState()
    dlq = RecordingDlq()
    checkpoint = StatePersistingObserver(state, None)
    observer = CompositeObserver([checkpoint, DlqObserver(dlq)], checkpoint_observer=checkpoint)
    result = run_sync(
        _sync(on_error="skip"),
        WindowSource([{"cursor": 0}, {"cursor": 1}, {"cursor": 2}]),
        Staged(),
        _profile(),
        tmp_path,
        observer=observer,
    )
    assert result.failed == 1
    assert state.value is not None and state.value.last_cursor_value == "9"
    assert len(dlq.entries) == 1


def test_async_destination_flush_failure_prevents_checkpoint(tmp_path: Path) -> None:
    class AsyncDestination(RecordingDestination):
        def flush_pending(self) -> None:
            raise RuntimeError("async acknowledgement failed")

    state = MemoryState()
    with pytest.raises(RuntimeError, match="async acknowledgement failed"):
        run_sync(
            _sync(end=3),
            WindowSource([{"cursor": 0}]),
            AsyncDestination(),
            _profile(),
            tmp_path,
            observer=_observer(state),
        )
    assert state.value is None


def test_interrupted_backfill_history_and_state_are_partial(tmp_path: Path) -> None:
    stop = threading.Event()
    state = MemoryState()
    history = RecordingHistory()

    class StopAfterCheckpoint(StatePersistingObserver):
        def checkpoint_window(
            self,
            sync_name: str,
            started_at: str,
            result: SyncResult,
            cursor_field: str,
            high: str | int,
        ) -> str:
            durable = super().checkpoint_window(sync_name, started_at, result, cursor_field, high)
            stop.set()
            return durable

    result = run_sync(
        _sync(),
        WindowSource([]),
        RecordingDestination(),
        _profile(),
        tmp_path,
        observer=CompositeObserver([StopAfterCheckpoint(state, None)]),
        stop_event=stop,
        history_manager=history,
    )
    assert result.interrupted
    assert history.entries[-1].status == "partial"
    assert state.value is not None and state.value.status == "partial"


def test_corrupt_local_checkpoint_files_fail_closed_before_extraction(tmp_path: Path) -> None:
    state_dir = tmp_path / ".drt"
    state_dir.mkdir()
    (state_dir / "state.json").write_text("{broken")
    source = WindowSource([])
    state = LocalStateManager(tmp_path)
    with pytest.raises(ValueError, match="resume checkpoint cannot be trusted"):
        run_sync(
            _sync(),
            source,
            RecordingDestination(),
            _profile(),
            tmp_path,
            observer=CompositeObserver([StatePersistingObserver(state, None)]),
        )
    assert source.windows == []

    (state_dir / "state.json").write_text("{}")
    (state_dir / "watermarks.json").write_text("{broken")
    source = WindowSource([])
    watermark = LocalWatermarkStorage(tmp_path)
    checkpoint = StatePersistingObserver(state, watermark)
    with pytest.raises(ValueError, match="resume checkpoint cannot be trusted"):
        run_sync(
            _sync(),
            source,
            RecordingDestination(),
            _profile(),
            tmp_path,
            observer=CompositeObserver([checkpoint]),
        )
    assert source.windows == []


def test_lag_beyond_ledger_retention_warns_at_start(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    from drt.engine.observer import LoggingObserver

    sync = SyncConfig.model_validate(
        {
            "name": "events",
            "model": "events",
            "destination": {"type": "rest_api", "url": "https://example.test"},
            "sync": {
                "mode": "incremental",
                "cursor_field": "cursor",
                "backfill": {
                    "window": "1 day",
                    "start": "2026-01-01T00:00:00Z",
                    "end": "2026-01-02T00:00:00Z",
                },
                "watermark": {"lag": "8 days"},
            },
        }
    )

    class Ledger:
        def already_delivered(self, sync_name: str, keys: set[str]) -> set[str]:
            return set()

        def mark_delivered(self, sync_name: str, keys: list[str], delivered_at: str) -> None:
            return None

        def prune(self, sync_name: str, retention_days: int) -> int:
            return 0

    state = MemoryState()
    checkpoint = StatePersistingObserver(state, None)
    observer = CompositeObserver([LoggingObserver(), checkpoint], checkpoint_observer=checkpoint)
    with caplog.at_level("WARNING", logger="drt"):
        run_sync(
            sync,
            WindowSource([]),
            RecordingDestination(),
            _profile(),
            tmp_path,
            observer=observer,
            idempotency_ledger=Ledger(),
        )
    assert "exceeds the 7-day idempotency-ledger retention" in caplog.text
