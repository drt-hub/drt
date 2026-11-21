"""Sequential execution/checkpoint coverage for ADR 0021 slice 3."""

from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from drt.cli.commands.retry import replay_dead_letters
from drt.cli.main import app
from drt.config.credentials import BigQueryProfile, RestApiProfile, SQLiteProfile
from drt.config.models import (
    BigQueryDestinationConfig,
    DatabricksDestinationConfig,
    DestinationConfig,
    MySQLDestinationConfig,
    PostgresDestinationConfig,
    SnowflakeDestinationConfig,
    SyncConfig,
    SyncOptions,
)
from drt.connectors.registry import get_destination, windowed_backfill_destination_types
from drt.destinations.base import SyncResult
from drt.destinations.mysql import MySQLDestination
from drt.destinations.row_errors import RowError
from drt.engine.observer import (
    CompositeObserver,
    DlqObserver,
    NullObserver,
    StatePersistingObserver,
    is_window_checkpoint_capable,
)
from drt.engine.plan import unsupported_reason
from drt.engine.sync import run_sync as engine_run_sync
from drt.sources.sqlite import SQLiteSource
from drt.state.dlq import DeadLetter, DlqBackend, LocalDlqStore
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

    @classmethod
    def backfill_unsupported_reason(cls, config: DestinationConfig) -> str | None:
        return None

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


def _capable_destination_config(destination_type: str) -> Any:
    configs = {
        "bigquery": BigQueryDestinationConfig(
            type="bigquery",
            project="project",
            dataset="dataset",
            table="events",
            mode="merge",
            upsert_key=["tenant_id", "id"],
        ),
        "databricks": DatabricksDestinationConfig(
            type="databricks",
            host_env="DBX_HOST",
            http_path_env="DBX_PATH",
            token_env="DBX_TOKEN",
            catalog="main",
            schema="default",
            table="events",
            mode="merge",
            upsert_key=["tenant_id", "id"],
        ),
        "mysql": MySQLDestinationConfig(
            type="mysql",
            connection_string_env="MYSQL_URL",
            table="events",
            upsert_key=["tenant_id", "id"],
        ),
        "postgres": PostgresDestinationConfig(
            type="postgres",
            connection_string_env="PG_URL",
            table="events",
            upsert_key=["tenant_id", "id"],
        ),
        "snowflake": SnowflakeDestinationConfig(
            type="snowflake",
            account_env="SF_ACCOUNT",
            user_env="SF_USER",
            password_env="SF_PASSWORD",
            database="DB",
            schema="PUBLIC",
            table="EVENTS",
            warehouse="WH",
            mode="merge",
            upsert_key=["tenant_id", "id"],
        ),
    }
    if destination_type not in configs:
        raise AssertionError(
            f"Add a contract-test config for new WindowedBackfillCapable "
            f"destination {destination_type!r}."
        )
    return configs[destination_type]


def _sync_for_destination(config: Any) -> SyncConfig:
    payload = _sync(end=3).model_dump(mode="json", by_alias=True, exclude_none=True)
    payload["destination"] = config.model_dump(
        mode="json", by_alias=True, exclude_none=True, exclude_defaults=True
    )
    return SyncConfig.model_validate(payload)


class CountingCheckpoint(StatePersistingObserver):
    def __init__(self, state: MemoryState) -> None:
        super().__init__(state, None)
        self.checkpoints = 0

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


@pytest.mark.parametrize("destination_type", windowed_backfill_destination_types())
@pytest.mark.parametrize("bad_row", [{"id": None}, {}], ids=["null", "missing"])
def test_every_backfill_destination_rejects_invalid_key_before_write_and_checkpoint(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    destination_type: str,
    bad_row: dict[str, Any],
) -> None:
    config = _capable_destination_config(destination_type)
    sync = _sync_for_destination(config)
    destination = get_destination(config)
    load = MagicMock(return_value=SyncResult(success=1))
    monkeypatch.setattr(destination, "load", load)
    if destination_type == "mysql":
        monkeypatch.setattr(destination, "validate_backfill_target", lambda _config: None)
    state = MemoryState()
    checkpoint = CountingCheckpoint(state)
    observer = CompositeObserver([checkpoint], checkpoint_observer=checkpoint)
    row = {"cursor": 1, "tenant_id": "tenant", **bad_row}

    with pytest.raises(ValueError) as exc_info:
        run_sync(
            sync,
            WindowSource([row]),
            destination,
            _profile(),
            tmp_path,
            observer=observer,
        )

    message = str(exc_info.value)
    assert "events" in message
    assert "'id'" in message
    assert "[0, 3)" in message
    load.assert_not_called()
    assert checkpoint.checkpoints == 0
    assert state.value is None


@pytest.mark.parametrize("destination_type", windowed_backfill_destination_types())
def test_every_backfill_destination_replay_after_checkpoint_failure_is_idempotent(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    destination_type: str,
) -> None:
    config = _capable_destination_config(destination_type)
    sync = _sync_for_destination(config)
    destination = get_destination(config)
    if destination_type == "mysql":
        monkeypatch.setattr(destination, "validate_backfill_target", lambda _config: None)
    destination_rows: dict[tuple[Any, ...], dict[str, Any]] = {}
    write_calls = 0

    def keyed_load(
        records: list[dict[str, Any]],
        _config: DestinationConfig,
        _options: SyncOptions,
    ) -> SyncResult:
        nonlocal write_calls
        write_calls += 1
        for record in records:
            key = tuple(record[column] for column in config.upsert_key)
            destination_rows[key] = dict(record)
        return SyncResult(success=len(records))

    monkeypatch.setattr(destination, "load", keyed_load)
    state = MemoryState()

    class FailingOnceCheckpoint(CountingCheckpoint):
        def checkpoint_window(
            self,
            sync_name: str,
            started_at: str,
            result: SyncResult,
            cursor_field: str,
            high: str | int,
        ) -> str:
            if self.checkpoints == 0:
                self.checkpoints += 1
                raise RuntimeError("checkpoint unavailable")
            return super().checkpoint_window(sync_name, started_at, result, cursor_field, high)

    checkpoint = FailingOnceCheckpoint(state)
    observer = CompositeObserver([checkpoint], checkpoint_observer=checkpoint)
    source = WindowSource([{"cursor": 1, "tenant_id": "tenant", "id": 7}])

    with pytest.raises(RuntimeError, match="checkpoint unavailable"):
        run_sync(sync, source, destination, _profile(), tmp_path, observer=observer)
    assert state.value is None

    run_sync(sync, source, destination, _profile(), tmp_path, observer=observer)

    assert write_calls == 2
    assert list(destination_rows) == [("tenant", 7)]


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


def test_mysql_backfill_retry_without_unique_index_is_refused_before_extraction(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sync = SyncConfig.model_validate(
        {
            "name": "events",
            "model": "events",
            "destination": {
                "type": "mysql",
                "host": "localhost",
                "dbname": "test",
                "table": "events",
                "upsert_key": ["id"],
            },
            "sync": {
                "mode": "incremental",
                "cursor_field": "cursor",
                "backfill": {"window": 3, "start": 0, "end": 9},
            },
        }
    )
    state = MemoryState(
        SyncState("events", "old", 3, "partial", "", "3", "exclusive")
    )
    source = WindowSource([{"cursor": 3, "id": 1}])
    connection = MagicMock()
    connection.cursor.return_value.fetchall.return_value = []
    destination = MySQLDestination()
    monkeypatch.setattr(destination, "_connect", lambda _config: connection)

    with pytest.raises(ValueError, match="PRIMARY or UNIQUE"):
        run_sync(
            sync,
            source,
            destination,
            _profile(),
            tmp_path,
            state_manager=state,
            observer=_observer(state),
        )

    assert source.extract_calls == 0
    assert source.windows == []
    connection.close.assert_called_once_with()


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


def test_observer_only_exclusive_marker_refuses_legacy_execution_before_extract(
    tmp_path: Path,
) -> None:
    state = MemoryState(
        SyncState("events", "old", 1, "success", last_cursor_value="3", cursor_boundary="exclusive")
    )
    sync = _sync()
    sync.sync.backfill = None
    source = WindowSource([])

    with pytest.raises(ValueError, match="exclusive window checkpoint.*backfill was removed"):
        run_sync(
            sync,
            source,
            RecordingDestination(),
            _profile(),
            tmp_path,
            state_manager=None,
            observer=_observer(state),
        )

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


def test_unsupported_destination_is_refused_before_extraction(tmp_path: Path) -> None:
    source = WindowSource([])
    state = MemoryState()

    with pytest.raises(
        ValueError,
        match=r"destination type 'rest_api'.*Supported destination types",
    ):
        run_sync(
            _sync(),
            source,
            object(),
            _profile(),
            tmp_path,
            observer=_observer(state),
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


def test_same_null_cursor_violation_across_runs_has_one_dead_letter(tmp_path: Path) -> None:
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

    store = LocalDlqStore(tmp_path)
    state = MemoryState()
    for _ in range(2):
        checkpoint = StatePersistingObserver(state, None)
        observer = CompositeObserver(
            [checkpoint, DlqObserver(store)], checkpoint_observer=checkpoint
        )
        run_sync(
            _sync(on_error="skip"),
            MissingCursorSource([]),
            RecordingDestination(),
            _profile(),
            tmp_path,
            observer=observer,
        )

    entries = store.read("events")
    assert len(entries) == 1
    assert entries[0].record == {"payload": "missing cursor"}


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


def test_extra_observer_flush_window_cannot_block_checkpoint(tmp_path: Path) -> None:
    class ExtraObserver(NullObserver):
        def __init__(self) -> None:
            self.flush_calls = 0

        def flush_window(self, sync_name: str) -> None:
            self.flush_calls += 1
            raise RuntimeError("extra observer must not veto progress")

    state = MemoryState()
    checkpoint = StatePersistingObserver(state, None)
    extra = ExtraObserver()
    observer = CompositeObserver([checkpoint, extra], checkpoint_observer=checkpoint)

    result = run_sync(
        _sync(end=3),
        WindowSource([]),
        RecordingDestination(),
        _profile(),
        tmp_path,
        observer=observer,
    )

    assert result.backfill is not None and result.backfill["windows_completed"] == 1
    assert extra.flush_calls == 0


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


def _strict_dlq_backend(backend: str, tmp_path: Path) -> DlqBackend:
    if backend == "local":
        return LocalDlqStore(tmp_path)
    if backend in {"gcs", "s3"}:
        from drt.state._objectstore import ObjectStoreDlqBackend

        return ObjectStoreDlqBackend(MagicMock())
    if backend == "postgres":
        from drt.config.credentials import PostgresProfile
        from drt.state.warehouse import PostgresWarehouseDlqBackend

        return PostgresWarehouseDlqBackend(
            PostgresProfile(type="postgres", host="h", dbname="d", user="u")
        )
    if backend == "snowflake":
        from drt.config.credentials import SnowflakeProfile
        from drt.state.warehouse_snowflake import SnowflakeWarehouseDlqBackend

        return SnowflakeWarehouseDlqBackend(
            SnowflakeProfile(type="snowflake", account="a", user="u", database="d")
        )
    if backend == "databricks":
        from drt.config.credentials import DatabricksProfile
        from drt.state.warehouse_databricks import DatabricksWarehouseDlqBackend

        return DatabricksWarehouseDlqBackend(
            DatabricksProfile(
                type="databricks", server_hostname="h", http_path="/p", catalog="main"
            )
        )
    from drt.state.warehouse_bigquery import BigQueryWarehouseDlqBackend

    assert backend == "bigquery"
    return BigQueryWarehouseDlqBackend(
        BigQueryProfile(type="bigquery", project="p", dataset="analytics")
    )


@pytest.mark.parametrize(
    "backend",
    ["local", "gcs", "s3", "postgres", "snowflake", "databricks", "bigquery"],
)
@pytest.mark.parametrize(
    ("existing_values", "current_values"),
    [([], [0, 1]), ([99], [0])],
    ids=["current-window-overflow", "pre-populated-full-queue"],
)
def test_strict_dlq_capacity_overflow_does_not_evict_or_checkpoint(
    backend: str,
    existing_values: list[int],
    current_values: list[int],
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class RowFailingDestination(RecordingDestination):
        def load(
            self,
            records: list[dict[str, Any]],
            config: DestinationConfig,
            sync_options: SyncOptions,
        ) -> SyncResult:
            return SyncResult(
                failed=len(records),
                row_errors=[
                    RowError(index, "row", None, "rejected")
                    for index in range(len(records))
                ],
            )

    store = _strict_dlq_backend(backend, tmp_path)
    queue = [
        DeadLetter(record={"id": value}, error_message="existing")
        for value in existing_values
    ]
    append_calls: list[int] = []

    def depth(_sync_name: str) -> int:
        return len(queue)

    def append(
        _sync_name: str,
        entries: list[DeadLetter],
        *,
        max_records: int = 10_000,
    ) -> int:
        append_calls.append(max_records)
        queue.extend(entries)
        if max_records > 0 and len(queue) > max_records:
            del queue[: len(queue) - max_records]
        return len(queue)

    monkeypatch.setattr(store, "depth", depth)
    monkeypatch.setattr(store, "append", append)
    monkeypatch.setattr(store, "_append_strict", append)

    state = MemoryState()
    checkpoint = StatePersistingObserver(state, None)
    observer = CompositeObserver(
        [checkpoint, DlqObserver(store, max_records=1)], checkpoint_observer=checkpoint
    )

    with pytest.raises(
        RuntimeError,
        match=r"raise dlq\.max_records or drain the queue via `drt retry events`",
    ):
        run_sync(
            _sync(end=3, on_error="skip"),
            WindowSource([{"cursor": value} for value in current_values]),
            RowFailingDestination(),
            _profile(),
            tmp_path,
            observer=observer,
        )

    assert append_calls == []
    assert [entry.record["id"] for entry in queue] == existing_values
    assert state.value is None
    assert state.saves == []


def test_staged_destination_is_excluded_before_staging(tmp_path: Path) -> None:
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

    state = MemoryState()
    checkpoint = StatePersistingObserver(state, None)
    observer = CompositeObserver([checkpoint], checkpoint_observer=checkpoint)
    destination = Staged()

    with pytest.raises(ValueError, match="does not declare"):
        run_sync(
            _sync(on_error="skip"),
            WindowSource([{"cursor": 0}, {"cursor": 1}, {"cursor": 2}]),
            destination,
            _profile(),
            tmp_path,
            observer=observer,
        )

    assert destination.records == []
    assert destination.finalize_calls == 0
    assert state.value is None


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


def test_async_flush_failure_defers_ledger_and_audit_and_retry_resends(
    tmp_path: Path,
) -> None:
    class AsyncDestination(RecordingDestination):
        def __init__(self) -> None:
            super().__init__()
            self.flush_calls = 0

        def flush_pending(self) -> None:
            self.flush_calls += 1
            if self.flush_calls == 1:
                raise RuntimeError("async acknowledgement failed")

    class Ledger:
        def __init__(self) -> None:
            self.delivered: set[str] = set()

        def already_delivered(self, sync_name: str, keys: set[str]) -> set[str]:
            return keys & self.delivered

        def mark_delivered(self, sync_name: str, keys: list[str], delivered_at: str) -> None:
            self.delivered.update(keys)

        def prune(self, sync_name: str, retention_days: int) -> int:
            return 0

    class Audit:
        def __init__(self) -> None:
            self.logged = 0

        def log_delivered(
            self,
            sync_name: str,
            run_id: str | None,
            sync_run_id: str | None,
            destination_type: str,
            entries: list[Any],
            delivered_at: str,
        ) -> None:
            self.logged += len(entries)

        def prune(self, sync_name: str, retain_days: int) -> int:
            return 0

    sync = _sync(end=3)
    sync.sync.idempotency_key = "{{ row.cursor }}"
    destination = AsyncDestination()
    state = MemoryState()
    ledger = Ledger()
    audit = Audit()

    with pytest.raises(RuntimeError, match="async acknowledgement failed"):
        run_sync(
            sync,
            WindowSource([{"cursor": 1}]),
            destination,
            _profile(),
            tmp_path,
            observer=_observer(state),
            idempotency_ledger=ledger,
            audit_trail=audit,
            audit_fields=["cursor"],
        )

    assert destination.calls == [[{"cursor": 1}]]
    assert ledger.delivered == set()
    assert audit.logged == 0

    result = run_sync(
        sync,
        WindowSource([{"cursor": 1}]),
        destination,
        _profile(),
        tmp_path,
        observer=_observer(state),
        idempotency_ledger=ledger,
        audit_trail=audit,
        audit_fields=["cursor"],
    )

    assert result.success == 1
    assert destination.calls == [[{"cursor": 1}], [{"cursor": 1}]]
    assert ledger.delivered == {"1"}
    assert audit.logged == 1


def test_checkpoint_failure_then_retry_keeps_one_backfill_dead_letter(tmp_path: Path) -> None:
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

    class FailingCheckpoint(StatePersistingObserver):
        def checkpoint_window(
            self,
            sync_name: str,
            started_at: str,
            result: SyncResult,
            cursor_field: str,
            high: str | int,
        ) -> str:
            raise RuntimeError("checkpoint unavailable")

    store = LocalDlqStore(tmp_path)
    state = MemoryState()
    failing_checkpoint = FailingCheckpoint(state, None)
    first_observer = CompositeObserver(
        [failing_checkpoint, DlqObserver(store)], checkpoint_observer=failing_checkpoint
    )
    sync = _sync(end=3, on_error="skip")
    source_rows = [{"cursor": 1, "payload": "same"}]

    with pytest.raises(RuntimeError, match="checkpoint unavailable"):
        run_sync(
            sync,
            WindowSource(source_rows),
            RowFailingDestination(),
            _profile(),
            tmp_path,
            observer=first_observer,
        )

    first = store.read("events")
    assert len(first) == 1

    checkpoint = StatePersistingObserver(state, None)
    retry_observer = CompositeObserver(
        [checkpoint, DlqObserver(store)], checkpoint_observer=checkpoint
    )
    run_sync(
        sync,
        WindowSource(source_rows),
        RowFailingDestination(),
        _profile(),
        tmp_path,
        observer=retry_observer,
    )

    retried = store.read("events")
    assert len(retried) == 1
    assert retried[0].id == first[0].id


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
