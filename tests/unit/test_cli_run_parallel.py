"""Tests for ``drt run`` sync selection and parallel execution.

Covers ``--select`` (by name, by tag, and the ``*``/``all`` sentinels),
``--threads`` dispatch into a real worker pool, and error paths.

The engine and destinations are patched so we don't need real network
calls — each test only asserts the CLI wiring behaves as documented.
"""

from __future__ import annotations

import threading
import time
from pathlib import Path
from typing import Any

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.state.manager import StateManager, SyncState

runner = CliRunner()

PROFILE_NAME = "default"

PROFILE_YML = {
    "profiles": {
        PROFILE_NAME: {"type": "duckdb"},
    }
}

SYNC_A: dict[str, Any] = {
    "name": "sync_a",
    "model": "SELECT 1",
    "tags": ["crm"],
    "destination": {
        "type": "rest_api",
        "url": "https://example.com/a",
        "method": "POST",
    },
}

SYNC_B: dict[str, Any] = {
    "name": "sync_b",
    "model": "SELECT 2",
    "tags": ["crm", "hourly"],
    "destination": {
        "type": "rest_api",
        "url": "https://example.com/b",
        "method": "POST",
    },
}

SYNC_C: dict[str, Any] = {
    "name": "sync_c",
    "model": "SELECT 3",
    "tags": ["daily"],
    "destination": {
        "type": "rest_api",
        "url": "https://example.com/c",
        "method": "POST",
    },
}


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Minimal drt project with three syncs under ``tmp_path``."""
    monkeypatch.chdir(tmp_path)
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "test_project", "version": "0.1", "profile": PROFILE_NAME})
    )
    creds_dir = tmp_path / ".drt"
    creds_dir.mkdir()
    (creds_dir / "credentials.yml").write_text(yaml.dump(PROFILE_YML))

    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    for sync in (SYNC_A, SYNC_B, SYNC_C):
        (syncs_dir / f"{sync['name']}.yml").write_text(yaml.dump(sync))
    return tmp_path


class _FakeResult:
    """Shape-compatible stand-in for engine.sync.run_sync's return value.

    Mirrors the real ``SyncResult`` surface that ``cli.main.run`` reads:
    ``success``, ``failed``, ``row_errors``, and — since PR #345/#347 —
    ``rows_extracted``. Keeping the fake in lockstep with the real shape
    is cheaper than monkey-patching each attribute per test.
    """

    def __init__(
        self,
        success: int = 1,
        failed: int = 0,
        rows_extracted: int | None = None,
    ) -> None:
        self.success = success
        self.failed = failed
        self.rows_extracted = success if rows_extracted is None else rows_extracted
        self.row_errors: list[Any] = []
        self.watermark_source: str | None = None
        self.cursor_value_used: str | None = None


@pytest.fixture
def patched_engine(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """Patch the runtime helpers called inside ``_run_one`` so tests never
    touch DuckDB, HTTP, the real destination classes, or the global
    ``~/.drt/profiles.yml`` credentials file.
    """

    from drt.cli import main as cli_main
    from drt.config import credentials as creds
    from drt.engine import sync as sync_module

    calls: list[str] = []
    lock = threading.Lock()
    threads_seen: set[int] = set()

    def fake_run_sync(sync, *_args: Any, **_kwargs: Any) -> _FakeResult:
        # Small delay guarantees ThreadPoolExecutor actually spreads work
        # across workers when --threads > 1 (otherwise an idle-then-busy
        # pattern can reuse a single worker for fast tasks).
        time.sleep(0.05)
        with lock:
            calls.append(sync.name)
            threads_seen.add(threading.get_ident())
        return _FakeResult(success=1, failed=0)

    def fake_load_profile(profile_name: str, *_a: Any, **_kw: Any) -> Any:
        # Minimal stand-in so ``cli.main.run`` can proceed past credential
        # resolution without a real ``~/.drt/profiles.yml``.
        return creds.DuckDBProfile(type="duckdb")

    # cli.main imports these symbols locally at call time, so we patch
    # the source modules rather than the cli.main namespace.
    monkeypatch.setattr(sync_module, "run_sync", fake_run_sync, raising=False)
    monkeypatch.setattr(creds, "load_profile", fake_load_profile, raising=False)
    monkeypatch.setattr(
        cli_main,
        "_get_source",
        lambda *_a, **_kw: object(),
        raising=False,
    )
    monkeypatch.setattr(
        cli_main,
        "_get_destination",
        lambda *_a, **_kw: object(),
        raising=False,
    )
    return {"calls": calls, "threads_seen": threads_seen}


# ---------------------------------------------------------------------------
# --select filtering
# ---------------------------------------------------------------------------


def test_select_by_name_runs_single_sync(project: Path, patched_engine: dict[str, Any]) -> None:
    result = runner.invoke(app, ["run", "--select", "sync_b", "--output", "json"])
    assert result.exit_code == 0
    assert patched_engine["calls"] == ["sync_b"]


def test_select_by_tag_filters(project: Path, patched_engine: dict[str, Any]) -> None:
    result = runner.invoke(app, ["run", "--select", "tag:crm", "--output", "json"])
    assert result.exit_code == 0
    assert set(patched_engine["calls"]) == {"sync_a", "sync_b"}


def test_select_star_runs_every_sync(project: Path, patched_engine: dict[str, Any]) -> None:
    result = runner.invoke(app, ["run", "--select", "*", "--output", "json"])
    assert result.exit_code == 0
    assert set(patched_engine["calls"]) == {"sync_a", "sync_b", "sync_c"}


def test_select_all_sentinel_runs_every_sync(
    project: Path, patched_engine: dict[str, Any]
) -> None:
    result = runner.invoke(app, ["run", "--select", "all", "--output", "json"])
    assert result.exit_code == 0
    assert set(patched_engine["calls"]) == {"sync_a", "sync_b", "sync_c"}


def test_no_select_defaults_to_every_sync(
    project: Path, patched_engine: dict[str, Any]
) -> None:
    result = runner.invoke(app, ["run", "--output", "json"])
    assert result.exit_code == 0
    assert set(patched_engine["calls"]) == {"sync_a", "sync_b", "sync_c"}


def test_unknown_tag_exits_nonzero(project: Path, patched_engine: dict[str, Any]) -> None:
    result = runner.invoke(app, ["run", "--select", "tag:does_not_exist", "--output", "json"])
    assert result.exit_code == 1
    assert patched_engine["calls"] == []


def test_unknown_name_exits_nonzero(project: Path, patched_engine: dict[str, Any]) -> None:
    result = runner.invoke(app, ["run", "--select", "no_such_sync", "--output", "json"])
    assert result.exit_code == 1
    assert patched_engine["calls"] == []


def test_all_flag_was_removed(project: Path, patched_engine: dict[str, Any]) -> None:
    """The legacy ``--all`` boolean was replaced by --select * / --select all."""
    result = runner.invoke(app, ["run", "--all", "--output", "json"])
    assert result.exit_code != 0
    assert patched_engine["calls"] == []


# ---------------------------------------------------------------------------
# --threads — real parallel dispatch
# ---------------------------------------------------------------------------


def test_threads_flag_actually_parallelises(
    project: Path, patched_engine: dict[str, Any]
) -> None:
    result = runner.invoke(
        app,
        ["run", "--select", "*", "--threads", "3", "--output", "json"],
    )
    assert result.exit_code == 0
    assert set(patched_engine["calls"]) == {"sync_a", "sync_b", "sync_c"}
    # More than one OS thread should have handled the syncs when
    # --threads > 1 and there are multiple syncs.
    assert len(patched_engine["threads_seen"]) > 1


def test_threads_one_runs_sequentially(
    project: Path, patched_engine: dict[str, Any]
) -> None:
    result = runner.invoke(
        app,
        ["run", "--select", "*", "--threads", "1", "--output", "json"],
    )
    assert result.exit_code == 0
    # Single-threaded path should use exactly one worker — the main one.
    assert len(patched_engine["threads_seen"]) == 1


# ---------------------------------------------------------------------------
# StateManager concurrency — load-modify-save must be atomic under threads
# ---------------------------------------------------------------------------


def test_state_manager_save_sync_is_thread_safe(tmp_path: Path) -> None:
    """Concurrent save_sync writes for distinct syncs must all survive."""
    mgr = StateManager(tmp_path)
    sync_names = [f"sync_{i:03d}" for i in range(40)]
    threads: list[threading.Thread] = []

    def _write(name: str) -> None:
        mgr.save_sync(
            SyncState(
                sync_name=name,
                last_run_at="2024-01-01T00:00:00+00:00",
                records_synced=1,
                status="success",
            )
        )

    for name in sync_names:
        thread = threading.Thread(target=_write, args=(name,))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()

    persisted = mgr.get_all()
    assert set(persisted.keys()) == set(sync_names)
