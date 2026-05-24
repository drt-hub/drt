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
from drt.engine.observer import (
    CompositeObserver,
    LoggingObserver,
    NullObserver,
    StatePersistingObserver,
    SyncObserver,
)
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
    obs.on_interrupted("s", 3)
    obs.on_sync_completed("s", SyncResult(), "2026-05-24T00:00:00Z", None, None)


# ---------------------------------------------------------------------------
# LoggingObserver
# ---------------------------------------------------------------------------


def test_logging_observer_emits_warning(caplog: pytest.LogCaptureFixture) -> None:
    obs = LoggingObserver()
    with caplog.at_level(logging.WARNING, logger="drt"):
        obs.on_warning("my_sync", "lookup ambiguity detected")
    assert any("lookup ambiguity detected" in r.message for r in caplog.records)


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
    """All 5 broadcast methods reach every child — guards future event additions."""
    child = MagicMock(spec=SyncObserver)
    obs = CompositeObserver([child])

    obs.on_sync_started("s", "ts")
    obs.on_watermark_resolved("s", "cli_override", "v")
    obs.on_warning("s", "msg")
    obs.on_interrupted("s", 4)
    obs.on_sync_completed("s", SyncResult(), "ts", None, None)

    child.on_sync_started.assert_called_once_with("s", "ts")
    child.on_watermark_resolved.assert_called_once_with("s", "cli_override", "v")
    child.on_warning.assert_called_once_with("s", "msg")
    child.on_interrupted.assert_called_once_with("s", 4)
    child.on_sync_completed.assert_called_once_with("s", SyncResult(), "ts", None, None)


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
        c for c in obs.on_warning.call_args_list
        if "Alert dispatch outer failure" in c.args[1]
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
        c for c in obs.on_warning.call_args_list
        if "History append outer failure" in c.args[1]
    ]
    assert warning_calls, (
        f"Expected on_warning('History append outer failure'...), "
        f"got {obs.on_warning.call_args_list}"
    )


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
