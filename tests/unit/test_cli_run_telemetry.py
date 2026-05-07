"""Verify `_run_one` calls telemetry.track_sync_completed with correct status."""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

from drt import telemetry
from drt.cli.main import _run_one, _RunContext
from drt.destinations.base import SyncResult


@pytest.fixture(autouse=True)
def _isolate_user_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    user_dir = tmp_path / ".drt"
    monkeypatch.setattr(telemetry, "_user_dir", lambda: user_dir)
    telemetry._load_config_cached.cache_clear()
    for var in ("DO_NOT_TRACK", "DRT_TELEMETRY", "DRT_TELEMETRY_ENDPOINT", "DRT_TELEMETRY_API_KEY"):
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def captured_calls(monkeypatch: pytest.MonkeyPatch) -> list[dict[str, Any]]:
    """Replace telemetry.track_sync_completed with a recording stub."""
    calls: list[dict[str, Any]] = []

    def stub(**kwargs: Any) -> None:
        calls.append(kwargs)

    monkeypatch.setattr(telemetry, "track_sync_completed", stub)
    return calls


def _fake_sync(name: str = "fake") -> Any:
    sync = MagicMock()
    sync.name = name
    sync.sync.mode = "full"
    sync.sync.cursor_field = None
    sync.destination.type = "rest_api"
    sync.tags = []
    return sync


def _fake_profile() -> Any:
    profile = MagicMock()
    profile.type = "duckdb"
    return profile


def _fake_ctx(*, dry_run: bool = False) -> _RunContext:
    return _RunContext(
        source=MagicMock(),
        state_mgr=MagicMock(),
        history_mgr=None,
        history_retention_days=30,
        json_mode=True,  # suppress rich output
        dry_run=dry_run,
        verbose=False,
        quiet=True,
        log_json=False,
        cursor_value=None,
    )


def test_run_one_success_emits_status_success(
    monkeypatch: pytest.MonkeyPatch,
    captured_calls: list[dict[str, Any]],
) -> None:
    sync = _fake_sync()
    ctx = _fake_ctx()

    def fake_run_sync(*_args: Any, **_kwargs: Any) -> SyncResult:
        return SyncResult(rows_extracted=3, success=3, failed=0)

    monkeypatch.setattr("drt.engine.sync.run_sync", fake_run_sync)
    monkeypatch.setattr(
        "drt.cli.main._get_destination", lambda _s: MagicMock(spec_set=["__call__"])
    )
    monkeypatch.setattr("drt.cli.main._get_watermark_storage", lambda _s, _d: None)

    name, _entry, had_err = _run_one(sync, ctx, _fake_profile())
    assert had_err is False
    assert name == "fake"
    assert len(captured_calls) == 1
    call = captured_calls[0]
    assert call["status"] == "success"
    assert call["rows_synced"] == 3
    assert call["source_type"] == "duckdb"
    assert call["destination_type"] == "rest_api"
    assert call["sync_mode"] == "full"


def test_run_one_failed_emits_status_failed(
    monkeypatch: pytest.MonkeyPatch,
    captured_calls: list[dict[str, Any]],
) -> None:
    sync = _fake_sync()
    ctx = _fake_ctx()

    def boom(*_args: Any, **_kwargs: Any) -> SyncResult:
        raise RuntimeError("simulated failure")

    monkeypatch.setattr("drt.engine.sync.run_sync", boom)
    monkeypatch.setattr("drt.cli.main._get_destination", lambda _s: MagicMock())
    monkeypatch.setattr("drt.cli.main._get_watermark_storage", lambda _s, _d: None)

    _name, _entry, had_err = _run_one(sync, ctx, _fake_profile())
    assert had_err is True
    assert len(captured_calls) == 1
    assert captured_calls[0]["status"] == "failed"
    assert captured_calls[0]["rows_synced"] == 0


def test_run_one_partial_emits_status_partial(
    monkeypatch: pytest.MonkeyPatch,
    captured_calls: list[dict[str, Any]],
) -> None:
    sync = _fake_sync()
    ctx = _fake_ctx()

    def fake_run_sync(*_args: Any, **_kwargs: Any) -> SyncResult:
        return SyncResult(rows_extracted=5, success=3, failed=2)

    monkeypatch.setattr("drt.engine.sync.run_sync", fake_run_sync)
    monkeypatch.setattr("drt.cli.main._get_destination", lambda _s: MagicMock())
    monkeypatch.setattr("drt.cli.main._get_watermark_storage", lambda _s, _d: None)

    _name, _entry, had_err = _run_one(sync, ctx, _fake_profile())
    assert had_err is True
    assert len(captured_calls) == 1
    assert captured_calls[0]["status"] == "partial"
    assert captured_calls[0]["rows_synced"] == 3


def test_run_one_dry_run_does_not_emit(
    monkeypatch: pytest.MonkeyPatch,
    captured_calls: list[dict[str, Any]],
) -> None:
    sync = _fake_sync()
    ctx = _fake_ctx(dry_run=True)

    def fake_run_sync(*_args: Any, **_kwargs: Any) -> SyncResult:
        return SyncResult(rows_extracted=3, success=3, failed=0)

    monkeypatch.setattr("drt.engine.sync.run_sync", fake_run_sync)
    monkeypatch.setattr("drt.cli.main._get_destination", lambda _s: MagicMock())
    monkeypatch.setattr("drt.cli.main._get_watermark_storage", lambda _s, _d: None)

    _run_one(sync, ctx, _fake_profile())
    assert captured_calls == []


def _ctx(**overrides: Any) -> _RunContext:
    base = {
        "source": MagicMock(),
        "state_mgr": MagicMock(),
        "history_mgr": None,
        "history_retention_days": 30,
        "json_mode": True,
        "dry_run": False,
        "verbose": False,
        "quiet": True,
        "log_json": False,
        "cursor_value": None,
    }
    base.update(overrides)
    return _RunContext(**base)  # type: ignore[arg-type]


def _wire_run_sync(monkeypatch: pytest.MonkeyPatch, fn: Any) -> None:
    monkeypatch.setattr("drt.engine.sync.run_sync", fn)
    monkeypatch.setattr("drt.cli.main._get_destination", lambda _s: MagicMock())
    monkeypatch.setattr("drt.cli.main._get_watermark_storage", lambda _s, _d: None)


def test_run_one_log_json_success_emits_info_log(
    monkeypatch: pytest.MonkeyPatch,
    captured_calls: list[dict[str, Any]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """log_json=True success path must emit a structured INFO sync_complete log."""
    _wire_run_sync(
        monkeypatch,
        lambda *_a, **_k: SyncResult(rows_extracted=3, success=3, failed=0),
    )
    with caplog.at_level("INFO"):
        _run_one(_fake_sync(), _ctx(log_json=True), _fake_profile())
    assert any(r.message == "sync_complete" and r.levelname == "INFO" for r in caplog.records)


def test_run_one_log_json_failed_emits_error_log(
    monkeypatch: pytest.MonkeyPatch,
    captured_calls: list[dict[str, Any]],
    caplog: pytest.LogCaptureFixture,
) -> None:
    """log_json=True error path must emit a structured ERROR sync_complete log."""

    def boom(*_a: Any, **_k: Any) -> SyncResult:
        raise RuntimeError("simulated failure")

    _wire_run_sync(monkeypatch, boom)
    with caplog.at_level("ERROR"):
        _run_one(_fake_sync(), _ctx(log_json=True), _fake_profile())
    assert any(r.message == "sync_complete" and r.levelname == "ERROR" for r in caplog.records)


def test_run_one_failed_non_json_prints_error(
    monkeypatch: pytest.MonkeyPatch,
    captured_calls: list[dict[str, Any]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """json_mode=False error path must print to stderr via print_error."""

    def boom(*_a: Any, **_k: Any) -> SyncResult:
        raise RuntimeError("kaboom-message")

    _wire_run_sync(monkeypatch, boom)
    _run_one(_fake_sync(), _ctx(json_mode=False), _fake_profile())
    out = capsys.readouterr().out + capsys.readouterr().err
    assert "kaboom-message" in out


def test_run_one_entry_includes_watermark_metadata(
    monkeypatch: pytest.MonkeyPatch,
    captured_calls: list[dict[str, Any]],
) -> None:
    """When SyncResult carries watermark fields, the entry dict surfaces them."""
    _wire_run_sync(
        monkeypatch,
        lambda *_a, **_k: SyncResult(
            rows_extracted=3,
            success=3,
            failed=0,
            watermark_source="default_value",
            cursor_value_used="2026-01-01",
        ),
    )
    _name, entry, _err = _run_one(_fake_sync(), _ctx(), _fake_profile())
    assert entry["watermark_source"] == "default_value"
    assert entry["cursor_value_used"] == "2026-01-01"


def test_run_one_rich_output_success(
    monkeypatch: pytest.MonkeyPatch,
    captured_calls: list[dict[str, Any]],
    capsys: pytest.CaptureFixture[str],
) -> None:
    """json_mode=False, quiet=False success path triggers rich print_sync_result."""
    _wire_run_sync(
        monkeypatch,
        lambda *_a, **_k: SyncResult(rows_extracted=2, success=2, failed=0),
    )
    _run_one(_fake_sync(), _ctx(json_mode=False, quiet=False), _fake_profile())
    captured = capsys.readouterr()
    assert "fake" in (captured.out + captured.err)


def test_run_one_verbose_prints_row_errors(
    monkeypatch: pytest.MonkeyPatch,
    captured_calls: list[dict[str, Any]],
) -> None:
    """verbose=True with row_errors triggers print_row_errors branch."""
    from drt.destinations.row_errors import RowError

    rows = [RowError(batch_index=0, record_preview="x", http_status=500, error_message="boom")]
    _wire_run_sync(
        monkeypatch,
        lambda *_a, **_k: SyncResult(
            rows_extracted=2, success=1, failed=1, row_errors=rows
        ),
    )
    called: list[Any] = []
    monkeypatch.setattr("drt.cli.main.print_row_errors", lambda errs: called.append(errs))
    _run_one(_fake_sync(), _ctx(json_mode=False, quiet=False, verbose=True), _fake_profile())
    assert called == [rows]
