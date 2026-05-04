"""Tests for ``drt run`` graceful shutdown wiring (#279).

The cooperative-shutdown correctness is covered at the engine layer in
``test_engine.TestGracefulShutdown``. This module verifies the CLI-side
glue: exit-code mapping, signal handler registration, and stop_event
propagation into the engine.

Real signal delivery is intentionally avoided here — sending SIGTERM to
the pytest process can race with the test harness. Instead we patch
``signal.signal`` to capture handlers, then invoke them directly.
"""

from __future__ import annotations

import signal
import threading
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import _exit_code_for_signal, app

runner = CliRunner()

PROFILE_NAME = "default"
PROFILE_YML = {"profiles": {PROFILE_NAME: {"type": "duckdb"}}}

SYNC_A: dict[str, Any] = {
    "name": "sync_a",
    "model": "SELECT 1",
    "destination": {"type": "rest_api", "url": "https://example.com/a", "method": "POST"},
}


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "test_project", "version": "0.1", "profile": PROFILE_NAME})
    )
    creds_dir = tmp_path / ".drt"
    creds_dir.mkdir()
    (creds_dir / "credentials.yml").write_text(yaml.dump(PROFILE_YML))
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "sync_a.yml").write_text(yaml.dump(SYNC_A))
    return tmp_path


class _FakeResult:
    success = 1
    failed = 0
    skipped = 0
    rows_extracted = 1
    row_errors: list[Any] = []
    errors: list[str] = []
    watermark_source: str | None = None
    cursor_value_used: str | None = None
    duration_seconds: float | None = 0.01
    interrupted = False


def _patch_runtime(monkeypatch: pytest.MonkeyPatch, fake_run_sync: Any) -> None:
    """Stub out destination/source/profile resolution and the engine,
    so the CLI ``run`` command exercises only its own wiring."""
    from drt.cli import main as cli_main
    from drt.config import credentials as creds
    from drt.engine import sync as sync_module

    def fake_load_profile(_name: str, *_a: Any, **_kw: Any) -> Any:
        return creds.DuckDBProfile(type="duckdb")

    monkeypatch.setattr(sync_module, "run_sync", fake_run_sync, raising=False)
    monkeypatch.setattr(creds, "load_profile", fake_load_profile, raising=False)
    monkeypatch.setattr(
        cli_main, "_get_source", lambda *_a, **_kw: object(), raising=False
    )
    monkeypatch.setattr(
        cli_main, "_get_destination", lambda *_a, **_kw: object(), raising=False
    )


# ---------------------------------------------------------------------------
# _exit_code_for_signal
# ---------------------------------------------------------------------------


class TestExitCodeForSignal:
    def test_sigint_returns_130(self) -> None:
        assert _exit_code_for_signal(signal.SIGINT) == 130

    def test_sigterm_returns_143(self) -> None:
        if hasattr(signal, "SIGTERM"):
            assert _exit_code_for_signal(signal.SIGTERM) == 143

    def test_follows_posix_128_plus_signum_convention(self) -> None:
        for signum in (1, 2, 9, 15):
            assert _exit_code_for_signal(signum) == 128 + signum


# ---------------------------------------------------------------------------
# Wiring: run() registers handlers and passes stop_event to engine
# ---------------------------------------------------------------------------


class TestRunRegistersSignalHandlers:
    def test_sigint_handler_registered(
        self, project: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        registered: dict[int, Any] = {}

        def capture(signum: int, handler: Any) -> Any:
            registered[signum] = handler
            return signal.SIG_DFL

        monkeypatch.setattr(signal, "signal", capture)
        _patch_runtime(monkeypatch, lambda *a, **kw: _FakeResult())

        runner.invoke(app, ["run"])

        assert signal.SIGINT in registered
        if hasattr(signal, "SIGTERM"):
            assert signal.SIGTERM in registered

    def test_run_passes_stop_event_to_engine(
        self, project: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured: dict[str, Any] = {}

        def fake_run_sync(*_args: Any, **kwargs: Any) -> _FakeResult:
            captured["stop_event"] = kwargs.get("stop_event")
            return _FakeResult()

        monkeypatch.setattr(signal, "signal", lambda *_a, **_k: signal.SIG_DFL)
        _patch_runtime(monkeypatch, fake_run_sync)

        runner.invoke(app, ["run"])

        assert isinstance(captured["stop_event"], threading.Event)
        assert not captured["stop_event"].is_set()


# ---------------------------------------------------------------------------
# Exit code path when signal handler fires
# ---------------------------------------------------------------------------


class TestRunExitsWithSignalCode:
    def test_sigterm_during_sync_yields_exit_143(
        self, project: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Simulate the OS calling our SIGTERM handler mid-run by capturing
        the registered handler and invoking it from inside fake_run_sync."""
        captured_handlers: dict[int, Any] = {}

        def capture_signal(signum: int, handler: Any) -> Any:
            captured_handlers[signum] = handler
            return signal.SIG_DFL

        def fake_run_sync(*_args: Any, **_kwargs: Any) -> _FakeResult:
            sigterm = signal.SIGTERM if hasattr(signal, "SIGTERM") else signal.SIGINT
            captured_handlers[sigterm](sigterm, None)
            return _FakeResult()

        monkeypatch.setattr(signal, "signal", capture_signal)
        _patch_runtime(monkeypatch, fake_run_sync)

        result = runner.invoke(app, ["run"])

        expected = (
            _exit_code_for_signal(signal.SIGTERM)
            if hasattr(signal, "SIGTERM")
            else _exit_code_for_signal(signal.SIGINT)
        )
        assert result.exit_code == expected

    def test_sigint_during_sync_yields_exit_130(
        self, project: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        captured_handlers: dict[int, Any] = {}

        def capture_signal(signum: int, handler: Any) -> Any:
            captured_handlers[signum] = handler
            return signal.SIG_DFL

        def fake_run_sync(*_args: Any, **_kwargs: Any) -> _FakeResult:
            captured_handlers[signal.SIGINT](signal.SIGINT, None)
            return _FakeResult()

        monkeypatch.setattr(signal, "signal", capture_signal)
        _patch_runtime(monkeypatch, fake_run_sync)

        result = runner.invoke(app, ["run"])

        assert result.exit_code == 130

    def test_second_signal_is_idempotent(
        self, project: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """A second SIGINT (e.g. user double-tapping Ctrl+C) must not
        re-trigger the watchdog timer or otherwise misbehave."""
        captured_handlers: dict[int, Any] = {}
        timers_started: list[Any] = []

        def capture_signal(signum: int, handler: Any) -> Any:
            captured_handlers[signum] = handler
            return signal.SIG_DFL

        original_timer = threading.Timer

        def counting_timer_factory(*args: Any, **kwargs: Any) -> Any:
            t = original_timer(*args, **kwargs)
            timers_started.append(t)
            return t

        def fake_run_sync(*_args: Any, **_kwargs: Any) -> _FakeResult:
            handler = captured_handlers[signal.SIGINT]
            handler(signal.SIGINT, None)
            handler(signal.SIGINT, None)  # second invocation must be a no-op
            return _FakeResult()

        monkeypatch.setattr(signal, "signal", capture_signal)
        monkeypatch.setattr(threading, "Timer", counting_timer_factory)
        _patch_runtime(monkeypatch, fake_run_sync)

        runner.invoke(app, ["run"])

        # Watchdog timer should have started exactly once
        assert len(timers_started) == 1
