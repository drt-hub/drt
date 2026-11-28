"""Checkpoint and composite-observer behaviour for windowed backfill (ADR 0021).

The checkpoint is the only thing standing between "a window finished" and "never
re-extract it", so the never-regress rules and the fail-closed wiring checks are
tested here directly rather than only through full engine runs.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, cast

import pytest

from drt.destinations.base import SyncResult
from drt.engine.observer import (
    CompositeObserver,
    DlqObserver,
    NullObserver,
    StatePersistingObserver,
    _has_sync_method,
    is_window_checkpoint_capable,
)
from drt.state.dlq import LocalDlqStore
from drt.state.manager import StateManager, SyncState


class _Watermarks:
    def __init__(self, initial: dict[str, str] | None = None) -> None:
        self.values = dict(initial or {})
        self.saved: list[tuple[str, str]] = []

    def get(self, sync_name: str) -> str | None:
        return self.values.get(sync_name)

    def save(self, sync_name: str, value: str) -> None:
        self.saved.append((sync_name, value))
        self.values[sync_name] = value


def _checkpoint(
    observer: StatePersistingObserver,
    high: int | str,
    *,
    result: SyncResult | None = None,
) -> str:
    return observer.checkpoint_window(
        "s", "2026-11-24T00:00:00+00:00", result or SyncResult(), "id", high
    )


class TestCheckpointWindow:
    def test_requires_a_state_store(self) -> None:
        observer = StatePersistingObserver(None, None)
        with pytest.raises(RuntimeError, match="always-present StateStore"):
            _checkpoint(observer, 10)
        with pytest.raises(RuntimeError, match="always-present StateStore"):
            observer.read_window_checkpoint("s")

    def test_first_checkpoint_writes_state_and_the_authoritative_watermark(
        self, tmp_path: Path
    ) -> None:
        manager = StateManager(tmp_path)
        watermarks = _Watermarks()
        observer = StatePersistingObserver(manager, watermarks)

        durable = _checkpoint(observer, 10, result=SyncResult(success=4, errors=["first"]))

        assert durable == "10"
        state = manager.get_last_sync("s")
        assert state is not None
        assert (state.status, state.last_cursor_value, state.cursor_boundary) == (
            "partial",
            "10",
            "exclusive",
        )
        assert state.records_synced == 4 and state.error == "first"
        assert watermarks.saved == [("s", "10")]
        assert observer.read_window_checkpoint("s") == ("10", "exclusive")

    def test_state_that_is_already_ahead_is_never_regressed(self, tmp_path: Path) -> None:
        manager = StateManager(tmp_path)
        manager.save_sync(SyncState("s", "t", 1, "success", last_cursor_value="50"))
        observer = StatePersistingObserver(manager, None)

        durable = _checkpoint(observer, 10)

        assert durable == "50"
        state = manager.get_last_sync("s")
        assert state is not None and state.last_cursor_value == "50"
        assert state.cursor_boundary == "exclusive"

    def test_large_integer_checkpoint_comparison_is_exact(self, tmp_path: Path) -> None:
        manager = StateManager(tmp_path)
        manager.save_sync(
            SyncState(
                "s",
                "old",
                1,
                "success",
                last_cursor_value="9007199254740993",
            )
        )

        assert _checkpoint(StatePersistingObserver(manager, None), 9007199254740992) == (
            "9007199254740993"
        )
        state = manager.get_last_sync("s")
        assert state is not None and state.last_cursor_value == "9007199254740993"

    def test_a_watermark_that_is_already_ahead_is_kept_and_not_overwritten(
        self, tmp_path: Path
    ) -> None:
        watermarks = _Watermarks({"s": "99"})
        observer = StatePersistingObserver(StateManager(tmp_path), watermarks)

        durable = _checkpoint(observer, 10)

        assert durable == "99"
        assert watermarks.saved == []

        state = observer._state_manager.get_last_sync("s")
        assert state is not None and state.last_cursor_value == "99"

    def test_state_max_is_copied_to_a_lagging_watermark_store(self, tmp_path: Path) -> None:
        manager = StateManager(tmp_path)
        manager.save_sync(SyncState("s", "old", 1, "success", last_cursor_value="50"))
        watermarks = _Watermarks({"s": "3"})

        durable = _checkpoint(StatePersistingObserver(manager, watermarks), 10)

        assert durable == "50"
        state = manager.get_last_sync("s")
        assert state is not None and state.last_cursor_value == "50"
        assert watermarks.saved == [("s", "50")]

    def test_offset_watermark_max_is_canonicalized_into_both_stores(self, tmp_path: Path) -> None:
        manager = StateManager(tmp_path)
        watermarks = _Watermarks({"s": "2026-01-02T23:00:00-05:00"})
        observer = StatePersistingObserver(manager, watermarks)

        durable = _checkpoint(observer, "2026-01-03 02:00:00")

        assert durable == "2026-01-03 04:00:00"
        state = manager.get_last_sync("s")
        assert state is not None and state.last_cursor_value == durable
        assert watermarks.saved == [("s", durable)]

    def test_naive_timestamp_is_utc_when_compared_with_an_offset_high(self, tmp_path: Path) -> None:
        manager = StateManager(tmp_path)
        manager.save_sync(
            SyncState(
                "s",
                "old",
                1,
                "success",
                last_cursor_value="2026-01-03T03:00:00",
            )
        )
        watermarks = _Watermarks({"s": "2026-01-03 03:00:00"})

        durable = _checkpoint(
            StatePersistingObserver(manager, watermarks),
            "2026-01-02T23:00:00-05:00",
        )

        assert durable == "2026-01-03 04:00:00"
        state = manager.get_last_sync("s")
        assert state is not None and state.last_cursor_value == durable
        assert watermarks.values["s"] == durable

    def test_equal_values_remain_equal_in_both_stores(self, tmp_path: Path) -> None:
        manager = StateManager(tmp_path)
        manager.save_sync(SyncState("s", "old", 1, "success", last_cursor_value="10"))
        watermarks = _Watermarks({"s": "10"})

        assert _checkpoint(StatePersistingObserver(manager, watermarks), 10) == "10"
        state = manager.get_last_sync("s")
        assert state is not None and state.last_cursor_value == "10"
        assert watermarks.saved == []

    @pytest.mark.parametrize(
        ("state_value", "watermark_value", "high", "message"),
        [
            ("not-an-int", "10", 11, "StateStore"),
            (
                "2026-01-02 00:00:00",
                "not-a-timestamp",
                "2026-01-03 00:00:00",
                "WatermarkStorage",
            ),
        ],
    )
    def test_unparseable_stored_checkpoint_fails_before_either_store_is_written(
        self,
        tmp_path: Path,
        state_value: str,
        watermark_value: str,
        high: int | str,
        message: str,
    ) -> None:
        manager = StateManager(tmp_path)
        manager.save_sync(SyncState("s", "old", 1, "success", last_cursor_value=state_value))
        watermarks = _Watermarks({"s": watermark_value})
        with pytest.raises(ValueError, match=message):
            _checkpoint(StatePersistingObserver(manager, watermarks), high)

        state = manager.get_last_sync("s")
        assert state is not None and state.last_run_at == "old"
        assert state.last_cursor_value == state_value
        assert watermarks.saved == []

    def test_a_lower_or_equal_watermark_is_advanced(self, tmp_path: Path) -> None:
        watermarks = _Watermarks({"s": "3"})
        observer = StatePersistingObserver(StateManager(tmp_path), watermarks)

        assert _checkpoint(observer, 10) == "10"
        assert watermarks.values["s"] == "10"

    def test_resume_reads_the_watermark_when_one_is_configured(self, tmp_path: Path) -> None:
        manager = StateManager(tmp_path)
        manager.save_sync(
            SyncState("s", "t", 1, "partial", last_cursor_value="5", cursor_boundary="exclusive")
        )
        observer = StatePersistingObserver(manager, _Watermarks({"s": "7"}))

        assert observer.read_window_checkpoint("s") == ("7", "exclusive")

    def test_resume_with_no_state_reports_nothing(self, tmp_path: Path) -> None:
        observer = StatePersistingObserver(StateManager(tmp_path), None)
        assert observer.read_window_checkpoint("s") == (None, None)

    def test_resume_prefers_the_strict_readers_when_the_stores_offer_them(self) -> None:
        calls: list[str] = []

        class Manager:
            def _get_last_sync_for_backfill(self, name: str) -> SyncState:
                calls.append("state")
                return SyncState(name, "t", 1, "partial", last_cursor_value="5")

            def get_last_sync(self, name: str) -> SyncState:
                raise AssertionError("must use the strict reader")

        class Marks:
            def _get_for_backfill(self, name: str) -> str:
                calls.append("watermark")
                return "8"

            def get(self, name: str) -> str:
                raise AssertionError("must use the strict reader")

        observer = StatePersistingObserver(cast(Any, Manager()), cast(Any, Marks()))
        assert observer.read_window_checkpoint("s") == ("8", None)
        assert calls == ["state", "watermark"]


class _Checkpointer:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int]] = []

    def checkpoint_window(
        self, sync_name: str, started_at: str, result: SyncResult, cursor_field: str, high: Any
    ) -> str:
        self.calls.append((sync_name, high))
        return str(high)

    def read_window_checkpoint(self, sync_name: str) -> tuple[str | None, str | None]:
        return ("4", "exclusive")


class TestCompositeObserverWiring:
    def test_a_single_capable_child_is_the_checkpoint_observer(self) -> None:
        child = _Checkpointer()
        composite = CompositeObserver([NullObserver(), child])

        assert is_window_checkpoint_capable(composite)
        assert composite.checkpoint_window("s", "t", SyncResult(), "id", 9) == "9"
        assert child.calls == [("s", 9)]
        assert composite.read_window_checkpoint("s") == ("4", "exclusive")

    def test_without_a_capable_child_checkpointing_fails_closed(self) -> None:
        composite = CompositeObserver([NullObserver()])

        assert not is_window_checkpoint_capable(composite)
        with pytest.raises(RuntimeError, match="checkpoint-capable observer"):
            composite.checkpoint_window("s", "t", SyncResult(), "id", 9)
        with pytest.raises(RuntimeError, match="checkpoint-capable observer"):
            composite.read_window_checkpoint("s")

    def test_two_capable_children_require_an_explicit_choice(self) -> None:
        with pytest.raises(ValueError, match="Multiple checkpoint-capable"):
            CompositeObserver([_Checkpointer(), _Checkpointer()])

    def test_the_explicit_checkpoint_observer_must_be_a_child(self) -> None:
        with pytest.raises(ValueError, match="must also be a CompositeObserver child"):
            CompositeObserver([NullObserver()], checkpoint_observer=_Checkpointer())

    def test_an_incompatible_explicit_checkpoint_observer_is_rejected(self) -> None:
        class NotCapable:
            pass

        child = NotCapable()
        with pytest.raises(TypeError, match="incompatible or async"):
            CompositeObserver([child], checkpoint_observer=cast(Any, child))

    def test_a_state_observer_without_a_store_is_not_capable(self) -> None:
        assert not is_window_checkpoint_capable(StatePersistingObserver(None, None))

    def test_window_events_survive_a_raising_child(self, caplog: pytest.LogCaptureFixture) -> None:
        seen: list[int] = []

        class Boom:
            def window_started(self, *args: object) -> None:
                raise RuntimeError("boom")

        class Good:
            def window_started(self, name: str, index: int, low: Any, high: Any) -> None:
                seen.append(index)

        composite = CompositeObserver([cast(Any, Boom()), cast(Any, Good())])
        with caplog.at_level(logging.WARNING, logger="drt"):
            composite.window_started("s", 3, 0, 10)

        assert seen == [3]
        assert "window_started raised: boom" in caplog.text

    def test_flush_and_begin_only_reach_the_designated_dlq_observer(self, tmp_path: Path) -> None:
        events: list[str] = []

        class Audit:
            def flush_window(self, name: str) -> None:
                events.append("audit-flush")

            def begin_backfill(self, name: str) -> None:
                events.append("audit-begin")

        dlq = DlqObserver(LocalDlqStore(tmp_path))
        composite = CompositeObserver([cast(Any, Audit()), dlq])

        composite.begin_backfill("s")
        composite.flush_window("s")

        assert events == []


class TestHasSyncMethod:
    def test_async_and_missing_methods_do_not_qualify(self) -> None:
        class Obj:
            async def checkpoint_window(
                self, a: object, b: object, c: object, d: object, e: object
            ) -> None: ...

        assert not _has_sync_method(Obj(), "checkpoint_window", 5)
        assert not _has_sync_method(Obj(), "nope", 1)

    def test_wrong_positional_arity_does_not_qualify(self) -> None:
        class Obj:
            def read_window_checkpoint(self) -> None: ...

        assert not _has_sync_method(Obj(), "read_window_checkpoint", 1)

    def test_uninspectable_callables_do_not_qualify(self) -> None:
        class Obj:
            read_window_checkpoint = print  # builtins may not expose a signature

        # Either a signature is unavailable (False) or it has the wrong arity (False).
        assert _has_sync_method(Obj(), "read_window_checkpoint", 99) is False
