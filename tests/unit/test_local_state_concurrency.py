"""Real cross-process concurrency coverage for local state stores (#963)."""

from __future__ import annotations

import multiprocessing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

import drt.state.history as history_module
from drt.state._filelock import advisory_lock
from drt.state.dlq import DeadLetter, LocalDlqStore
from drt.state.history import HistoryEntry, LocalHistoryManager
from drt.state.manager import LocalStateManager, SyncState
from drt.state.watermark import LocalWatermarkStorage

_PROCESS_TIMEOUT_SECONDS = 10
_BLOCKED_WRITER_PROBE_SECONDS = 1


def _wait(event: Any) -> None:
    if not event.wait(_PROCESS_TIMEOUT_SECONDS):
        raise TimeoutError("multiprocess test coordination timed out")


def _join(processes: list[Any]) -> None:
    for process in processes:
        process.join(_PROCESS_TIMEOUT_SECONDS)
        if process.is_alive():
            process.terminate()
            process.join(_PROCESS_TIMEOUT_SECONDS)
            raise AssertionError(f"worker {process.pid} did not exit")
        assert process.exitcode == 0


def _hold_advisory_lock(path: Path, acquired: Any, release: Any) -> None:
    with advisory_lock(path):
        acquired.set()
        _wait(release)


def _try_advisory_lock(path: Path, started: Any, acquired: Any) -> None:
    started.set()
    with advisory_lock(path):
        acquired.set()


@pytest.fixture(scope="module")
def file_lock_supported(tmp_path_factory: pytest.TempPathFactory) -> bool:
    """Capability canary: prove a separate process really blocks on the lock."""
    ctx = multiprocessing.get_context("spawn")
    path = tmp_path_factory.mktemp("file-lock-canary") / "state.json"
    holder_acquired = ctx.Event()
    contender_started = ctx.Event()
    contender_acquired = ctx.Event()
    release = ctx.Event()
    holder = ctx.Process(target=_hold_advisory_lock, args=(path, holder_acquired, release))
    contender = ctx.Process(
        target=_try_advisory_lock,
        args=(path, contender_started, contender_acquired),
    )

    holder.start()
    try:
        _wait(holder_acquired)
        contender.start()
        _wait(contender_started)
        assert not contender_acquired.wait(_BLOCKED_WRITER_PROBE_SECONDS)
    finally:
        release.set()
        processes = [holder]
        if contender.pid is not None:
            processes.append(contender)
        _join(processes)

    assert contender_acquired.is_set()
    return True


class _PausedStateManager(LocalStateManager):
    def __init__(self, project_dir: Path, read_complete: Any, release: Any) -> None:
        super().__init__(project_dir)
        self._read_complete = read_complete
        self._release = release

    def _load_all(self) -> dict[str, Any]:
        data = super()._load_all()
        self._read_complete.set()
        _wait(self._release)
        return data


def _sync_state(name: str) -> SyncState:
    return SyncState(
        sync_name=name,
        last_run_at="2026-08-30T00:00:00+00:00",
        records_synced=1,
        status="success",
    )


def _paused_state_save(project_dir: Path, read_complete: Any, release: Any) -> None:
    _PausedStateManager(project_dir, read_complete, release).save_sync(_sync_state("slow"))


def _state_save(project_dir: Path, started: Any, done: Any) -> None:
    manager = LocalStateManager(project_dir)
    started.set()
    manager.save_sync(_sync_state("fast"))
    done.set()


class _PausedWatermarkStorage(LocalWatermarkStorage):
    def __init__(self, project_dir: Path, read_complete: Any, release: Any) -> None:
        super().__init__(project_dir)
        self._read_complete = read_complete
        self._release = release

    def _load(self) -> dict[str, str]:
        data = super()._load()
        self._read_complete.set()
        _wait(self._release)
        return data


def _paused_watermark_save(project_dir: Path, read_complete: Any, release: Any) -> None:
    _PausedWatermarkStorage(project_dir, read_complete, release).save("slow", "one")


def _paused_watermark_delete(project_dir: Path, read_complete: Any, release: Any) -> None:
    _PausedWatermarkStorage(project_dir, read_complete, release).delete("deleted")


def _watermark_save(project_dir: Path, started: Any, done: Any) -> None:
    storage = LocalWatermarkStorage(project_dir)
    started.set()
    storage.save("fast", "two")
    done.set()


def _dead_letter(value: int) -> DeadLetter:
    return DeadLetter(record={"id": value}, error_message=f"boom {value}")


class _PausedDlqStore(LocalDlqStore):
    def __init__(self, project_dir: Path, read_complete: Any, release: Any) -> None:
        super().__init__(project_dir)
        self._read_complete = read_complete
        self._release = release

    def _read_raw(self, path: Path) -> list[str]:
        lines = super()._read_raw(path)
        self._read_complete.set()
        _wait(self._release)
        return lines


def _paused_dlq_append(project_dir: Path, read_complete: Any, release: Any) -> None:
    _PausedDlqStore(project_dir, read_complete, release).append(
        "sync", [_dead_letter(1)], max_records=0
    )


def _dlq_append(project_dir: Path, value: int, started: Any, done: Any) -> None:
    store = LocalDlqStore(project_dir)
    started.set()
    store.append("sync", [_dead_letter(value)], max_records=0)
    done.set()


class _PausedReconcileDlqStore(LocalDlqStore):
    def __init__(self, project_dir: Path, read_complete: Any, release: Any) -> None:
        super().__init__(project_dir)
        self._read_complete = read_complete
        self._release = release

    def _read_entries(self, sync_name: str) -> list[DeadLetter]:
        entries = super()._read_entries(sync_name)
        self._read_complete.set()
        _wait(self._release)
        return entries


def _paused_dlq_reconcile(
    project_dir: Path,
    entry_id: str,
    read_complete: Any,
    release: Any,
) -> None:
    _PausedReconcileDlqStore(project_dir, read_complete, release).reconcile(
        "sync", remove_ids={entry_id}
    )


def _history_entry(started_at: str) -> HistoryEntry:
    return HistoryEntry(
        sync_name="sync",
        started_at=started_at,
        completed_at=started_at,
        duration_seconds=1,
        status="success",
        records_synced=1,
        records_failed=0,
    )


def _paused_history_prune(project_dir: Path, read_complete: Any, release: Any) -> None:
    original_read = history_module._read_jsonl

    def paused_read(path: Path) -> list[HistoryEntry]:
        entries = original_read(path)
        read_complete.set()
        _wait(release)
        return entries

    history_module._read_jsonl = paused_read
    LocalHistoryManager(project_dir).prune("sync", retention_days=10)


def _history_prune(project_dir: Path, started: Any, done: Any) -> None:
    manager = LocalHistoryManager(project_dir)
    started.set()
    manager.prune("sync", retention_days=1)
    done.set()


def _history_append(project_dir: Path, started_at: str, started: Any, done: Any) -> None:
    manager = LocalHistoryManager(project_dir)
    started.set()
    manager.append(_history_entry(started_at))
    done.set()


def _run_ordered_race(
    ctx: Any,
    slow_target: Any,
    slow_args: tuple[Any, ...],
    fast_target: Any,
    fast_args: tuple[Any, ...],
) -> None:
    """Pause one worker after its read, then let a second writer contend."""
    read_complete = ctx.Event()
    release = ctx.Event()
    fast_started = ctx.Event()
    fast_done = ctx.Event()
    slow = ctx.Process(target=slow_target, args=(*slow_args, read_complete, release))
    fast = ctx.Process(target=fast_target, args=(*fast_args, fast_started, fast_done))

    slow.start()
    try:
        _wait(read_complete)
        fast.start()
        _wait(fast_started)
        # Without the file lock, the fast writer finishes against the slow
        # worker's stale snapshot. With it, this probe times out while blocked.
        fast_done.wait(_BLOCKED_WRITER_PROBE_SECONDS)
    finally:
        release.set()
        processes = [slow]
        if fast.pid is not None:
            processes.append(fast)
        _join(processes)


def test_state_saves_preserve_both_processes(tmp_path: Path, file_lock_supported: bool) -> None:
    assert file_lock_supported
    ctx = multiprocessing.get_context("spawn")
    _run_ordered_race(ctx, _paused_state_save, (tmp_path,), _state_save, (tmp_path,))

    assert set(LocalStateManager(tmp_path).get_all()) == {"slow", "fast"}


def test_watermark_saves_preserve_both_processes(tmp_path: Path, file_lock_supported: bool) -> None:
    assert file_lock_supported
    ctx = multiprocessing.get_context("spawn")
    _run_ordered_race(
        ctx,
        _paused_watermark_save,
        (tmp_path,),
        _watermark_save,
        (tmp_path,),
    )

    storage = LocalWatermarkStorage(tmp_path)
    assert storage.get("slow") == "one"
    assert storage.get("fast") == "two"


def test_watermark_delete_preserves_a_concurrent_process_save(
    tmp_path: Path, file_lock_supported: bool
) -> None:
    assert file_lock_supported
    storage = LocalWatermarkStorage(tmp_path)
    storage.save("deleted", "old")
    ctx = multiprocessing.get_context("spawn")
    _run_ordered_race(
        ctx,
        _paused_watermark_delete,
        (tmp_path,),
        _watermark_save,
        (tmp_path,),
    )

    assert storage.get("deleted") is None
    assert storage.get("fast") == "two"


def test_dlq_appends_preserve_both_processes(tmp_path: Path, file_lock_supported: bool) -> None:
    assert file_lock_supported
    ctx = multiprocessing.get_context("spawn")
    _run_ordered_race(ctx, _paused_dlq_append, (tmp_path,), _dlq_append, (tmp_path, 2))

    assert {entry.record["id"] for entry in LocalDlqStore(tmp_path).read("sync")} == {1, 2}


def test_dlq_reconcile_preserves_a_concurrent_process_append(
    tmp_path: Path, file_lock_supported: bool
) -> None:
    assert file_lock_supported
    store = LocalDlqStore(tmp_path)
    store.append("sync", [_dead_letter(1)], max_records=0)
    [existing] = store.read("sync")
    ctx = multiprocessing.get_context("spawn")
    _run_ordered_race(
        ctx,
        _paused_dlq_reconcile,
        (tmp_path, existing.id),
        _dlq_append,
        (tmp_path, 99),
    )

    assert [entry.record["id"] for entry in store.read("sync")] == [99]


def test_history_prunes_compose_across_processes(tmp_path: Path, file_lock_supported: bool) -> None:
    assert file_lock_supported
    manager = LocalHistoryManager(tmp_path)
    now = datetime.now(timezone.utc)
    manager.append(_history_entry((now - timedelta(days=20)).isoformat()))
    manager.append(_history_entry((now - timedelta(days=5)).isoformat()))
    manager.append(_history_entry(now.isoformat()))
    ctx = multiprocessing.get_context("spawn")
    _run_ordered_race(
        ctx,
        _paused_history_prune,
        (tmp_path,),
        _history_prune,
        (tmp_path,),
    )

    remaining = manager.read("sync")
    assert [entry.started_at for entry in remaining] == [now.isoformat()]


def test_history_append_survives_concurrent_prune(
    tmp_path: Path, file_lock_supported: bool
) -> None:
    """A prune's read-then-replace must not silently drop a racing append.

    POSIX O_APPEND alone only protects an append against *other appends* —
    not against a concurrent prune, which reads a snapshot and later
    overwrites the whole file with it. Without the shared lock, an append
    landing between prune's read and its ``tmp.replace()`` is lost when
    prune's stale snapshot overwrites the file.
    """
    assert file_lock_supported
    manager = LocalHistoryManager(tmp_path)
    now = datetime.now(timezone.utc)
    manager.append(_history_entry((now - timedelta(days=20)).isoformat()))
    ctx = multiprocessing.get_context("spawn")
    _run_ordered_race(
        ctx,
        _paused_history_prune,
        (tmp_path,),
        _history_append,
        (tmp_path, now.isoformat()),
    )

    remaining = {entry.started_at for entry in manager.read("sync")}
    assert now.isoformat() in remaining
