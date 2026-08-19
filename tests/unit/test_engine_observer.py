"""Tests for drt.engine.observer.

Covers each concrete observer (NullObserver, LoggingObserver,
StatePersistingObserver, CompositeObserver) plus the engine purity
guarantee: ``engine/sync.py`` no longer imports ``logging`` and the
``logger`` global is gone.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.engine.observer import (
    CompositeObserver,
    DlqObserver,
    LoggingObserver,
    NullObserver,
    StatePersistingObserver,
    SyncObserver,
    _reset_extra_observers,
    register_extra_observer,
    registered_extra_observers,
)
from drt.state.dlq import DeadLetter, DlqStore
from drt.state.manager import StateManager

# ---------------------------------------------------------------------------
# NullObserver
# ---------------------------------------------------------------------------


def test_null_observer_implements_protocol() -> None:
    obs = NullObserver()
    assert isinstance(obs, SyncObserver)


def test_null_observer_methods_do_nothing() -> None:
    obs = NullObserver()
    # Should not raise. No state to assert; the contract is "no-op".
    obs.on_sync_started("s", "2026-05-24T00:00:00Z")
    obs.on_watermark_resolved("s", "storage", "v")
    obs.on_warning("s", "warn")
    obs.on_records_failed("s", [])
    obs.on_interrupted("s", 3)
    obs.on_sync_completed("s", SyncResult(), "2026-05-24T00:00:00Z", None, None)
    obs.on_sync_ended("s")


# ---------------------------------------------------------------------------
# LoggingObserver
# ---------------------------------------------------------------------------


def test_logging_observer_emits_warning(caplog: pytest.LogCaptureFixture) -> None:
    obs = LoggingObserver()
    with caplog.at_level(logging.WARNING, logger="drt"):
        obs.on_warning("my_sync", "lookup ambiguity detected")
    assert any("lookup ambiguity detected" in r.message for r in caplog.records)


def test_logging_observer_records_failed_is_silent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Failed records carry full payloads (possible PII) — must NOT be logged."""
    obs = LoggingObserver()
    with caplog.at_level(logging.DEBUG, logger="drt"):
        obs.on_records_failed("s", [DeadLetter(record={"id": 1}, error_message="x")])
    assert caplog.records == []


def test_logging_observer_emits_interrupted_info(caplog: pytest.LogCaptureFixture) -> None:
    obs = LoggingObserver()
    with caplog.at_level(logging.INFO, logger="drt"):
        obs.on_interrupted("my_sync", 4)
    assert any("graceful shutdown" in r.message for r in caplog.records)


def test_logging_observer_skips_storage_source_to_match_pre_refactor(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Pre-refactor engine only logged cli_override / default_value resolutions.

    Storage-source resolutions were intentionally silent (would generate
    one INFO line per incremental run, low signal). The observer must
    preserve that asymmetry to keep daily-run log noise unchanged.
    """
    obs = LoggingObserver()
    with caplog.at_level(logging.INFO, logger="drt"):
        obs.on_watermark_resolved("s", "storage", "2026-05-01")
    assert not any("watermark_source=storage" in r.message for r in caplog.records)


def test_logging_observer_emits_default_value_with_reason(
    caplog: pytest.LogCaptureFixture,
) -> None:
    obs = LoggingObserver()
    with caplog.at_level(logging.INFO, logger="drt"):
        obs.on_watermark_resolved("s", "default_value", "2024-01-01")
    msgs = [r.message for r in caplog.records]
    assert any("watermark_source=default_value" in m and "no existing watermark" in m for m in msgs)


# ---------------------------------------------------------------------------
# StatePersistingObserver
# ---------------------------------------------------------------------------


def test_state_persisting_observer_writes_state_on_sync_completed(tmp_path: Path) -> None:
    state_mgr = StateManager(tmp_path)
    obs = StatePersistingObserver(state_mgr, None)
    result = SyncResult(success=10, failed=0)

    obs.on_sync_completed("test_sync", result, "2026-05-24T00:00:00Z", None, None)

    saved = state_mgr.get_last_sync("test_sync")
    assert saved is not None
    assert saved.status == "success"
    assert saved.records_synced == 10


def test_state_persisting_observer_skips_dry_run(tmp_path: Path) -> None:
    """Regression test (#978): result.dry_run=True must skip state AND
    watermark persistence entirely, not just the cursor value. A dry run
    extracts (so new_cursor_value reflects rows seen) but never calls
    destination.load(), so persisting last_run_at/records_synced/cursor here
    would record a run that never actually happened — the next real run
    would then see a misleadingly "already synced" state or skip real data
    via the cursor. Carried on SyncResult rather than a new on_sync_completed
    parameter so existing/custom SyncObserver implementations are unaffected
    (Codex review on the first version of this fix)."""
    state_mgr = StateManager(tmp_path)
    wm = MagicMock()
    obs = StatePersistingObserver(state_mgr, wm)
    result = SyncResult(success=10, failed=0, dry_run=True)

    obs.on_sync_completed("test_sync", result, "2026-05-24T00:00:00Z", "5", "id")

    assert state_mgr.get_last_sync("test_sync") is None
    wm.save.assert_not_called()


def test_state_persisting_observer_marks_partial(tmp_path: Path) -> None:
    state_mgr = StateManager(tmp_path)
    obs = StatePersistingObserver(state_mgr, None)
    result = SyncResult(success=3, failed=2)

    obs.on_sync_completed("test_sync", result, "2026-05-24T00:00:00Z", None, None)

    saved = state_mgr.get_last_sync("test_sync")
    assert saved is not None and saved.status == "partial"


def test_state_persisting_observer_marks_failed_when_no_success(tmp_path: Path) -> None:
    state_mgr = StateManager(tmp_path)
    obs = StatePersistingObserver(state_mgr, None)
    result = SyncResult(success=0, failed=2)

    obs.on_sync_completed("test_sync", result, "2026-05-24T00:00:00Z", None, None)

    saved = state_mgr.get_last_sync("test_sync")
    assert saved is not None and saved.status == "failed"


def test_state_persisting_observer_persists_cursor_when_field_set(tmp_path: Path) -> None:
    state_mgr = StateManager(tmp_path)
    obs = StatePersistingObserver(state_mgr, None)
    result = SyncResult(success=5, failed=0)

    obs.on_sync_completed("inc", result, "2026-05-24T00:00:00Z", "2026-05-10", "updated_at")

    saved = state_mgr.get_last_sync("inc")
    assert saved is not None and saved.last_cursor_value == "2026-05-10"


def test_state_persisting_observer_skips_cursor_when_field_unset(tmp_path: Path) -> None:
    """Full sync (no cursor_field): last_cursor_value MUST be None, not '<value>'."""
    state_mgr = StateManager(tmp_path)
    obs = StatePersistingObserver(state_mgr, None)
    result = SyncResult(success=5, failed=0)

    obs.on_sync_completed("full", result, "2026-05-24T00:00:00Z", "should-be-ignored", None)

    saved = state_mgr.get_last_sync("full")
    assert saved is not None and saved.last_cursor_value is None


def test_state_persisting_observer_writes_watermark(tmp_path: Path) -> None:
    from drt.state.watermark import LocalWatermarkStorage

    wm = LocalWatermarkStorage(tmp_path)
    obs = StatePersistingObserver(None, wm)
    result = SyncResult(success=1, failed=0)

    obs.on_sync_completed("wm_sync", result, "2026-05-24T00:00:00Z", "2026-05-10", "updated_at")

    assert wm.get("wm_sync") == "2026-05-10"


def test_state_persisting_observer_swallows_state_save_errors(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Per fire-and-forget contract: a broken state manager must NOT crash a sync."""
    state_mgr = MagicMock()
    state_mgr.save_sync.side_effect = OSError("disk full")
    obs = StatePersistingObserver(state_mgr, None)

    with caplog.at_level(logging.WARNING, logger="drt"):
        obs.on_sync_completed("s", SyncResult(success=1), "ts", None, None)

    assert any("State persist failure" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# CompositeObserver
# ---------------------------------------------------------------------------


def test_composite_observer_broadcasts_to_all(tmp_path: Path) -> None:
    state_mgr = StateManager(tmp_path)
    obs = CompositeObserver([LoggingObserver(), StatePersistingObserver(state_mgr, None)])
    obs.on_sync_completed("s", SyncResult(success=1), "ts", None, None)

    saved = state_mgr.get_last_sync("s")
    assert saved is not None  # state observer ran


def test_composite_observer_forwards_every_event_method() -> None:
    """All 7 broadcast methods reach every child — guards future event additions."""
    child = MagicMock(spec=SyncObserver)
    obs = CompositeObserver([child])

    obs.on_sync_started("s", "ts")
    obs.on_watermark_resolved("s", "cli_override", "v")
    obs.on_warning("s", "msg")
    obs.on_records_failed("s", [])
    obs.on_interrupted("s", 4)
    obs.on_sync_completed("s", SyncResult(), "ts", None, None)
    obs.on_sync_ended("s")

    child.on_sync_started.assert_called_once_with("s", "ts")
    child.on_watermark_resolved.assert_called_once_with("s", "cli_override", "v")
    child.on_warning.assert_called_once_with("s", "msg")
    child.on_records_failed.assert_called_once_with("s", [])
    child.on_interrupted.assert_called_once_with("s", 4)
    child.on_sync_completed.assert_called_once_with("s", SyncResult(), "ts", None, None)
    child.on_sync_ended.assert_called_once_with("s")


def test_logging_observer_on_sync_started_is_silent(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Pre-refactor engine did not log a sync_start line — preserve that parity."""
    obs = LoggingObserver()
    with caplog.at_level(logging.DEBUG, logger="drt"):
        obs.on_sync_started("my_sync", "2026-05-24T00:00:00Z")
    assert caplog.records == []


def test_state_persisting_observer_swallows_watermark_save_errors(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Per fire-and-forget contract: a broken watermark storage must NOT crash a sync."""
    wm = MagicMock()
    wm.save.side_effect = OSError("disk full")
    obs = StatePersistingObserver(None, wm)

    with caplog.at_level(logging.WARNING, logger="drt"):
        obs.on_sync_completed("s", SyncResult(success=1), "ts", "2026-05-10", "updated_at")

    assert any("Watermark save failure" in r.message for r in caplog.records)


def test_composite_observer_continues_after_child_raises(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A child observer that breaks the no-raise contract must not abort the others."""
    bad = MagicMock(spec=SyncObserver)
    bad.on_warning.side_effect = RuntimeError("bad observer")
    good = MagicMock(spec=SyncObserver)

    obs = CompositeObserver([bad, good])
    with caplog.at_level(logging.WARNING, logger="drt"):
        obs.on_warning("s", "msg")

    good.on_warning.assert_called_once_with("s", "msg")
    assert any("raised" in r.message for r in caplog.records)


# ---------------------------------------------------------------------------
# Engine-side wiring: defensive except branches route through observer.on_warning
# ---------------------------------------------------------------------------


def test_engine_routes_alert_dispatch_failure_through_observer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """When ``drt.alerts.dispatch_alerts`` raises, the engine swallows it via
    ``observer.on_warning`` (used to be ``logger.warning`` pre-#548).
    """
    import drt.alerts
    from tests.unit.test_engine import FakeDestination, FakeSource, _make_profile, _make_sync

    monkeypatch.setattr(
        drt.alerts, "dispatch_alerts", MagicMock(side_effect=RuntimeError("alert sink down"))
    )
    obs = MagicMock(spec=SyncObserver)
    # Force the engine into the "raised or failed" branch that triggers
    # alert dispatch — easiest is to make the destination fail every row.
    dest = FakeDestination(fail_indices={0})
    sync = _make_sync(batch_size=1, on_error="skip")

    from drt.engine.sync import run_sync

    run_sync(sync, FakeSource([{"id": 1}]), dest, _make_profile(), tmp_path, observer=obs)

    warning_calls = [
        c for c in obs.on_warning.call_args_list if "Alert dispatch outer failure" in c.args[1]
    ]
    assert warning_calls, (
        f"Expected on_warning('Alert dispatch outer failure'...), "
        f"got {obs.on_warning.call_args_list}"
    )


def test_engine_routes_history_append_failure_through_observer(tmp_path: Path) -> None:
    """When the history manager raises during append, the engine swallows via observer."""
    from tests.unit.test_engine import FakeDestination, FakeSource, _make_profile, _make_sync

    history_mgr = MagicMock()
    history_mgr.append.side_effect = RuntimeError("history store down")
    obs = MagicMock(spec=SyncObserver)
    sync = _make_sync()

    from drt.engine.sync import run_sync

    run_sync(
        sync,
        FakeSource([{"id": 1}]),
        FakeDestination(),
        _make_profile(),
        tmp_path,
        history_manager=history_mgr,
        observer=obs,
    )

    warning_calls = [
        c for c in obs.on_warning.call_args_list if "History append outer failure" in c.args[1]
    ]
    assert warning_calls, (
        f"Expected on_warning('History append outer failure'...), "
        f"got {obs.on_warning.call_args_list}"
    )


def test_engine_calls_on_sync_ended_on_success(tmp_path: Path) -> None:
    from tests.unit.test_engine import FakeDestination, FakeSource, _make_profile, _make_sync

    obs = MagicMock(spec=SyncObserver)
    sync = _make_sync()

    from drt.engine.sync import run_sync

    run_sync(
        sync, FakeSource([{"id": 1}]), FakeDestination(), _make_profile(), tmp_path, observer=obs
    )

    obs.on_sync_ended.assert_called_once_with(sync.name)


def test_engine_calls_on_sync_ended_on_unhandled_exception(tmp_path: Path) -> None:
    """on_sync_ended fires from the outer `finally`, so it must still run
    (and see the exception path) even when run_sync propagates an error —
    this is the guarantee a buffering observer like DlqObserver depends on
    to never lose already-buffered entries on a mid-sync crash."""
    from tests.unit.test_engine import _make_profile, _make_sync

    class _RaisingSource:
        def extract(self, query, config, *, query_tags=None):  # type: ignore[no-untyped-def]
            raise RuntimeError("source blew up")
            yield  # pragma: no cover — makes this a generator function

        def test_connection(self, config):  # type: ignore[no-untyped-def]
            return True

    obs = MagicMock(spec=SyncObserver)
    sync = _make_sync()

    from drt.engine.sync import run_sync

    with pytest.raises(RuntimeError, match="source blew up"):
        run_sync(sync, _RaisingSource(), MagicMock(), _make_profile(), tmp_path, observer=obs)

    obs.on_sync_ended.assert_called_once_with(sync.name)


def test_engine_skips_on_sync_ended_on_dry_run(tmp_path: Path) -> None:
    from tests.unit.test_engine import FakeDestination, FakeSource, _make_profile, _make_sync

    obs = MagicMock(spec=SyncObserver)
    sync = _make_sync()

    from drt.engine.sync import run_sync

    run_sync(
        sync,
        FakeSource([{"id": 1}]),
        FakeDestination(),
        _make_profile(),
        tmp_path,
        dry_run=True,
        observer=obs,
    )

    obs.on_sync_ended.assert_not_called()


# ---------------------------------------------------------------------------
# DlqObserver (#278) — persists per-record load failures
# ---------------------------------------------------------------------------


def test_dlq_observer_implements_protocol() -> None:
    assert isinstance(DlqObserver(DlqStore(Path("."))), SyncObserver)


def _dead(value: int) -> DeadLetter:
    return DeadLetter(record={"id": value}, error_message="boom", http_status=500)


def test_dlq_observer_persists_failed_records(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    obs = DlqObserver(store)

    obs.on_records_failed("s", [_dead(1), _dead(2)])
    obs.on_sync_ended("s")

    assert store.depth("s") == 2
    assert [e.record["id"] for e in store.read("s")] == [1, 2]


def test_dlq_observer_empty_is_noop(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    DlqObserver(store).on_records_failed("s", [])
    assert store.depth("s") == 0


def test_dlq_observer_honours_max_records(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    obs = DlqObserver(store, max_records=2)
    obs.on_records_failed("s", [_dead(1), _dead(2), _dead(3)])
    obs.on_sync_ended("s")
    assert [e.record["id"] for e in store.read("s")] == [2, 3]


def test_dlq_observer_trims_buffer_during_accumulation_not_just_at_flush() -> None:
    """A sync with far more failures than max_records must not hold all of
    them in memory for the whole run — only the newest max_records ever
    survive the eventual flush, so the buffer should stay capped as
    failures arrive, not just get truncated once at the end."""
    obs = DlqObserver(MagicMock(), max_records=2)
    obs.on_records_failed("s", [_dead(1)])
    obs.on_records_failed("s", [_dead(2)])
    obs.on_records_failed("s", [_dead(3)])
    assert [d.record["id"] for d in obs._buffer["s"]] == [2, 3]


def test_dlq_observer_max_records_zero_is_unbounded() -> None:
    """max_records=0 means unbounded, matching store.append's own contract
    (documented in dead-letter-queue.md) — the buffer trim must not
    misread 0 as "cap at zero" and silently discard everything."""
    obs = DlqObserver(MagicMock(), max_records=0)
    for i in range(5):
        obs.on_records_failed("s", [_dead(i)])
    assert [d.record["id"] for d in obs._buffer["s"]] == [0, 1, 2, 3, 4]


def test_dlq_observer_swallows_store_errors(caplog: pytest.LogCaptureFixture) -> None:
    """Fire-and-forget: a broken DLQ store must NOT crash a sync."""
    store = MagicMock()
    store.append.side_effect = OSError("disk full")
    obs = DlqObserver(store)

    with caplog.at_level(logging.WARNING, logger="drt"):
        obs.on_records_failed("s", [_dead(1)])
        obs.on_sync_ended("s")  # must not raise

    assert any("DLQ persist failure" in r.message for r in caplog.records)


def test_dlq_observer_flushes_only_buffered_failures_on_completion(tmp_path: Path) -> None:
    store = DlqStore(tmp_path)
    obs = DlqObserver(store)
    obs.on_sync_started("s", "ts")
    obs.on_watermark_resolved("s", "storage", "v")
    obs.on_warning("s", "w")
    obs.on_interrupted("s", 1)
    obs.on_sync_completed("s", SyncResult(), "ts", None, None)
    obs.on_sync_ended("s")
    assert store.depth("s") == 0


def test_dlq_observer_on_sync_completed_alone_does_not_flush(tmp_path: Path) -> None:
    """The flush moved to on_sync_ended (fires on every exit path); a bare
    on_sync_completed call — e.g. a caller that forgets on_sync_ended, or a
    library integration that only wires the old hook — must not leak a
    write, since that would silently duplicate the real flush."""
    store = DlqStore(tmp_path)
    obs = DlqObserver(store)
    obs.on_records_failed("s", [_dead(1)])
    obs.on_sync_completed("s", SyncResult(), "ts", None, None)
    assert store.depth("s") == 0


def test_dlq_observer_buffered_flush_matches_per_batch_local_content(
    tmp_path: Path,
) -> None:
    """Batch buffering changes I/O count, never JSONL content or order."""
    batches = [[_dead(1)], [_dead(2), _dead(3)], [_dead(4)]]
    old_store = DlqStore(tmp_path / "old")
    for batch in batches:
        old_store.append("s", batch)

    new_store = DlqStore(tmp_path / "new")
    observer = DlqObserver(new_store)
    for batch in batches:
        observer.on_records_failed("s", batch)
    assert new_store.depth("s") == 0, "failures stay buffered until completion"
    observer.on_sync_ended("s")

    old_file = tmp_path / "old" / ".drt" / "dlq" / "s.jsonl"
    new_file = tmp_path / "new" / ".drt" / "dlq" / "s.jsonl"
    assert new_file.read_bytes() == old_file.read_bytes()


# ---------------------------------------------------------------------------
# Engine emits on_records_failed with full records (#278)
# ---------------------------------------------------------------------------


class _RowErrorDestination:
    """Fake destination that fails specific in-batch indices with RowErrors."""

    def __init__(self, fail_indices: set[int]) -> None:
        self._fail = fail_indices

    def load(self, records, config, sync_options):  # type: ignore[no-untyped-def]
        result = SyncResult()
        for i, _ in enumerate(records):
            if i in self._fail:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=i,
                        record_preview=str(records[i])[:200],
                        http_status=503,
                        error_message=f"rejected {i}",
                    )
                )
            else:
                result.success += 1
        return result


def test_engine_emits_records_failed_with_full_records(tmp_path: Path) -> None:
    """The engine correlates RowError.batch_index back to the full record sent."""
    from drt.engine.sync import run_sync
    from tests.unit.test_engine import FakeSource, _make_profile, _make_sync

    obs = MagicMock(spec=SyncObserver)
    rows = [{"id": 0, "name": "a"}, {"id": 1, "name": "b"}, {"id": 2, "name": "c"}]
    dest = _RowErrorDestination(fail_indices={1})
    sync = _make_sync(batch_size=10, on_error="skip")

    run_sync(sync, FakeSource(rows), dest, _make_profile(), tmp_path, observer=obs)

    obs.on_records_failed.assert_called_once()
    name_arg, dead_letters = obs.on_records_failed.call_args.args
    assert name_arg == "test_sync"
    assert len(dead_letters) == 1
    # Full record recovered — not the 200-char preview.
    assert dead_letters[0].record == {"id": 1, "name": "b"}
    assert dead_letters[0].error_message == "rejected 1"
    assert dead_letters[0].http_status == 503


def test_engine_does_not_emit_when_no_row_errors(tmp_path: Path) -> None:
    """A clean batch (or a destination that reports no per-record errors) is silent."""
    from drt.engine.sync import run_sync
    from tests.unit.test_engine import FakeDestination, FakeSource, _make_profile, _make_sync

    obs = MagicMock(spec=SyncObserver)
    run_sync(
        _make_sync(),
        FakeSource([{"id": 1}]),
        FakeDestination(),  # all success, no row_errors
        _make_profile(),
        tmp_path,
        observer=obs,
    )
    obs.on_records_failed.assert_not_called()


# ---------------------------------------------------------------------------
# Engine purity guarantee — boundary regression check
# ---------------------------------------------------------------------------


def test_engine_sync_module_does_not_import_logging() -> None:
    """Regression net for the #548 contract: engine/sync.py uses observers, not logging.

    A direct `import logging` or `logger.info(...)` line reintroduced into
    engine/sync.py would re-couple the engine to a side-effect path and
    should fail this test, prompting the author to add an
    `on_<event>` method to SyncObserver instead.
    """
    import drt.engine.sync as sync_mod

    source = Path(sync_mod.__file__).read_text()
    # Detect import statements only — keep the test resistant to commentary
    # mentioning "import logging" in a docstring (e.g. this very docstring
    # if it lived in sync.py).
    assert re.search(r"^\s*import logging\b", source, re.MULTILINE) is None, (
        "engine/sync.py must not import the logging module — emit events through "
        "a SyncObserver. See drt.engine.observer.LoggingObserver."
    )
    assert "logger.info" not in source and "logger.warning" not in source, (
        "engine/sync.py must not call logger directly — emit events through a SyncObserver."
    )


def test_engine_sync_module_does_not_call_state_manager_save_sync() -> None:
    """State persistence flows through observers; the engine never reaches for storage."""
    import drt.engine.sync as sync_mod

    source = Path(sync_mod.__file__).read_text()
    assert ".save_sync(" not in source, (
        "engine/sync.py must not call state_manager.save_sync directly — "
        "wire StatePersistingObserver via the `observer=` parameter."
    )
    assert "watermark_storage.save(" not in source, (
        "engine/sync.py must not call watermark_storage.save directly — "
        "wire StatePersistingObserver via the `observer=` parameter."
    )


# ---------------------------------------------------------------------------
# register_extra_observer / registered_extra_observers (#299, ADR 0008)
# ---------------------------------------------------------------------------


def test_extra_observers_empty_by_default() -> None:
    _reset_extra_observers()
    assert registered_extra_observers() == []


def test_register_extra_observer_is_cumulative_not_replace() -> None:
    """Unlike register_permission_checker/register_audit_logger's single
    active policy, multiple extra observers may coexist — matching how
    CompositeObserver already fans out to several built-in observers."""
    _reset_extra_observers()
    first = NullObserver()
    second = NullObserver()
    register_extra_observer(first)
    register_extra_observer(second)

    result = registered_extra_observers()

    assert result == [first, second]
    _reset_extra_observers()


def test_registered_extra_observer_receives_sync_events_via_composite() -> None:
    """The architectural point of ADR 0008 Decision 1: an Enterprise audit
    observer is just a SyncObserver, fanned out to by CompositeObserver
    alongside the built-in observers — no new Protocol needed."""
    calls: list[tuple[str, str]] = []

    class _AuditObserver(NullObserver):
        def on_sync_started(self, sync_name: str, started_at: str) -> None:
            calls.append(("started", sync_name))

    _reset_extra_observers()
    audit_observer = _AuditObserver()
    register_extra_observer(audit_observer)

    composite = CompositeObserver([LoggingObserver(), *registered_extra_observers()])
    composite.on_sync_started("my_sync", "2026-08-19T00:00:00Z")

    assert calls == [("started", "my_sync")]
    _reset_extra_observers()
