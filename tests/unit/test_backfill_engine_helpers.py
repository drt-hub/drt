"""Engine helpers behind the windowed-backfill loop (ADR 0021).

These are the small pieces that decide what an *uncheckpointed* window leaves behind,
so each one is tested directly: a failure here silently changes recovery behaviour.
"""

from __future__ import annotations

from typing import Any

import pytest

from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.engine import sync as engine
from drt.engine.sync import (
    _accumulate_result,
    _coerce_checkpoint,
    _flush_uncheckpointed_window,
    _window_event,
)


def _flush(observer: Any, result: SyncResult, original: BaseException | None) -> None:
    _flush_uncheckpointed_window(observer, "s", result, original_error=original)


class _Observer:
    def __init__(self, *, flush_error: BaseException | None = None) -> None:
        self.flush_error = flush_error
        self.flushed: list[str] = []
        self.warnings: list[tuple[str, str]] = []

    def flush_window(self, sync_name: str) -> None:
        self.flushed.append(sync_name)
        if self.flush_error is not None:
            raise self.flush_error

    def on_warning(self, sync_name: str, message: str) -> None:
        self.warnings.append((sync_name, message))


class TestCoerceCheckpoint:
    def test_integer_family_parses_the_stored_text(self) -> None:
        assert _coerce_checkpoint("42", 0) == 42

    def test_integer_family_rejects_non_integer_text_clearly(self) -> None:
        with pytest.raises(ValueError, match="is not an integer"):
            _coerce_checkpoint("2026-01-01", 0)

    def test_timestamp_family_keeps_a_canonical_utc_instant(self) -> None:
        value = _coerce_checkpoint("2026-01-02T00:00:00+00:00", "2026-01-01T00:00:00+00:00")
        assert isinstance(value, str)
        assert value == "2026-01-02 00:00:00"


class TestAccumulateResult:
    def test_every_counter_and_list_is_carried_into_the_total(self) -> None:
        total = SyncResult(rows_extracted=1, success=1, errors=["a"])
        window = SyncResult(
            rows_extracted=10,
            success=5,
            failed=2,
            skipped=3,
            skipped_no_match=1,
            skipped_duplicate=2,
            kept_values=4,
            errors=["b"],
            row_errors=[RowError(0, "row", None, "boom")],
            ddl=["ALTER TABLE t"],
        )

        _accumulate_result(total, window)

        assert (
            total.rows_extracted,
            total.success,
            total.failed,
            total.skipped,
            total.skipped_no_match,
            total.skipped_duplicate,
            total.kept_values,
        ) == (11, 6, 2, 3, 1, 2, 4)
        assert total.errors == ["a", "b"]
        assert [e.error_message for e in total.row_errors] == ["boom"]
        assert total.ddl == ["ALTER TABLE t"]
        assert total.interrupted is False

    def test_interruption_is_sticky(self) -> None:
        total = SyncResult()
        _accumulate_result(total, SyncResult(interrupted=True))
        _accumulate_result(total, SyncResult())
        assert total.interrupted is True


class TestWindowEvent:
    def test_missing_or_non_callable_hook_is_ignored(self) -> None:
        observer: Any = type("O", (), {"on_window_started": 3})()
        _window_event(observer, "on_window_started", 1)
        _window_event(observer, "not_defined", 1)

    def test_a_raising_hook_becomes_a_warning_not_a_failure(self) -> None:
        warnings: list[tuple[str, str]] = []

        class Observer:
            def on_window_started(self, *args: object) -> None:
                raise RuntimeError("hook exploded")

            def on_warning(self, name: str, message: str) -> None:
                warnings.append((name, message))

        _window_event(Observer(), "on_window_started", 1)

        assert warnings == [
            ("windowed-backfill", "Observer on_window_started raised: hook exploded")
        ]


class TestFlushUncheckpointedWindow:
    def test_observer_without_a_flusher_is_a_noop(self) -> None:
        result = SyncResult()
        _flush(object(), result, None)
        assert result.errors == []

    def test_successful_flush_records_nothing(self) -> None:
        observer = _Observer()
        result = SyncResult()
        original = RuntimeError("extraction failed")

        _flush(observer, result, original)

        assert observer.flushed == ["s"]
        assert result.errors == []
        assert not getattr(original, "__notes__", [])

    def test_flush_failure_is_recorded_without_masking_the_original_error(self) -> None:
        observer = _Observer(flush_error=RuntimeError("dlq full"))
        result = SyncResult()
        original = RuntimeError("extraction failed")

        _flush(observer, result, original)

        assert len(result.errors) == 1
        assert "dlq full" in result.errors[0] and "checkpoint was not written" in result.errors[0]
        assert observer.warnings == [("s", result.errors[0])]
        assert str(original) == "extraction failed"
        assert getattr(original, "__notes__") == [result.errors[0]]

    def test_notes_accumulate_on_an_error_that_already_has_them(self) -> None:
        observer = _Observer(flush_error=RuntimeError("dlq full"))
        original = RuntimeError("extraction failed")
        setattr(original, "__notes__", ["earlier note"])

        _flush(observer, SyncResult(), original)

        assert getattr(original, "__notes__")[0] == "earlier note"
        assert len(getattr(original, "__notes__")) == 2

    def test_no_original_error_still_records_the_flush_failure(self) -> None:
        observer = _Observer(flush_error=RuntimeError("dlq full"))
        result = SyncResult()

        _flush(observer, result, None)

        assert len(result.errors) == 1

    def test_a_raising_warning_hook_and_an_unwritable_error_do_not_escape(self) -> None:
        class Observer(_Observer):
            def on_warning(self, sync_name: str, message: str) -> None:
                raise RuntimeError("warning sink down")

        class Slotted(Exception):
            __slots__ = ()

        result = SyncResult()
        _flush(Observer(flush_error=RuntimeError("dlq full")), result, Slotted("x"))

        assert len(result.errors) == 1

    def test_even_a_base_exception_from_the_flush_is_contained(self) -> None:
        observer = _Observer(flush_error=KeyboardInterrupt())
        result = SyncResult()

        _flush(observer, result, None)

        assert len(result.errors) == 1


def test_engine_module_exposes_the_helpers_under_test() -> None:
    assert callable(engine._flush_uncheckpointed_window)
