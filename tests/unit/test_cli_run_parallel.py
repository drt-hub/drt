"""Tests for ``drt run`` sync selection and parallel execution.

Covers ``--select`` (by name, by tag, and the ``*``/``all`` sentinels),
``--threads`` dispatch into a real worker pool, and error paths.

The engine and destinations are patched so we don't need real network
calls — each test only asserts the CLI wiring behaves as documented.
"""

from __future__ import annotations

import json
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
        self.skipped = 0
        self.skipped_no_match = 0
        self.rows_extracted = success if rows_extracted is None else rows_extracted
        self.row_errors: list[Any] = []
        self.watermark_source: str | None = None
        self.cursor_value_used: str | None = None
        self.watermark_lag: str | None = None
        self.limit_applied: int | None = None


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
    limits: list[int | None] = []
    lock = threading.Lock()
    threads_seen: set[int] = set()

    def fake_run_sync(sync, *_args: Any, **_kwargs: Any) -> _FakeResult:
        # Small delay guarantees ThreadPoolExecutor actually spreads work
        # across workers when --threads > 1 (otherwise an idle-then-busy
        # pattern can reuse a single worker for fast tasks).
        time.sleep(0.05)
        with lock:
            calls.append(sync.name)
            limits.append(_kwargs.get("extract_limit"))
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
    return {"calls": calls, "threads_seen": threads_seen, "limits": limits}


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


def test_select_all_sentinel_runs_every_sync(project: Path, patched_engine: dict[str, Any]) -> None:
    result = runner.invoke(app, ["run", "--select", "all", "--output", "json"])
    assert result.exit_code == 0
    assert set(patched_engine["calls"]) == {"sync_a", "sync_b", "sync_c"}


def test_no_select_defaults_to_every_sync(project: Path, patched_engine: dict[str, Any]) -> None:
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
# selection v2 (#771): glob / repeated --select / --exclude / destination:
# ---------------------------------------------------------------------------


def test_select_glob_pattern(project: Path, patched_engine: dict[str, Any]) -> None:
    result = runner.invoke(app, ["run", "--select", "sync_*", "--output", "json"])
    assert result.exit_code == 0
    assert set(patched_engine["calls"]) == {"sync_a", "sync_b", "sync_c"}


def test_repeated_select_unions(project: Path, patched_engine: dict[str, Any]) -> None:
    result = runner.invoke(
        app, ["run", "--select", "sync_a", "--select", "sync_c", "--output", "json"]
    )
    assert result.exit_code == 0
    assert set(patched_engine["calls"]) == {"sync_a", "sync_c"}


def test_exclude_subtracts_from_selection(project: Path, patched_engine: dict[str, Any]) -> None:
    result = runner.invoke(
        app, ["run", "--select", "tag:crm", "--exclude", "sync_b", "--output", "json"]
    )
    assert result.exit_code == 0
    assert patched_engine["calls"] == ["sync_a"]


def test_exclude_without_select(project: Path, patched_engine: dict[str, Any]) -> None:
    result = runner.invoke(app, ["run", "--exclude", "sync_b", "--output", "json"])
    assert result.exit_code == 0
    assert set(patched_engine["calls"]) == {"sync_a", "sync_c"}


def test_exclude_everything_exits_nonzero(project: Path, patched_engine: dict[str, Any]) -> None:
    result = runner.invoke(app, ["run", "--exclude", "*", "--output", "json"])
    assert result.exit_code == 1
    assert patched_engine["calls"] == []


def test_unknown_selector_method_exits_nonzero(
    project: Path, patched_engine: dict[str, Any]
) -> None:
    result = runner.invoke(app, ["run", "--select", "source:bigquery", "--output", "json"])
    assert result.exit_code == 1
    assert patched_engine["calls"] == []


# ---------------------------------------------------------------------------
# --failed (#773): sync-level re-run of previous failures
# ---------------------------------------------------------------------------


def _seed_state(statuses: dict[str, str]) -> None:
    """Write .drt/state.json entries for the given sync statuses."""
    from drt.state.manager import StateManager, SyncState

    mgr = StateManager(Path("."))
    for name, status in statuses.items():
        mgr.save_sync(
            SyncState(
                sync_name=name,
                last_run_at="2026-07-10T00:00:00",
                records_synced=0,
                status=status,
                error="boom" if status != "success" else None,
                last_cursor_value=None,
            )
        )


def test_failed_reruns_only_failed_syncs(project: Path, patched_engine: dict[str, Any]) -> None:
    _seed_state({"sync_a": "failed", "sync_b": "success", "sync_c": "partial"})

    result = runner.invoke(app, ["run", "--failed", "--output", "json"])

    assert result.exit_code == 0
    assert set(patched_engine["calls"]) == {"sync_a", "sync_c"}  # partial counts as not-success


def test_failed_excludes_never_run_syncs(project: Path, patched_engine: dict[str, Any]) -> None:
    _seed_state({"sync_a": "failed"})  # sync_b / sync_c never ran

    result = runner.invoke(app, ["run", "--failed", "--output", "json"])

    assert result.exit_code == 0
    assert patched_engine["calls"] == ["sync_a"]


def test_failed_intersects_with_select(project: Path, patched_engine: dict[str, Any]) -> None:
    _seed_state({"sync_a": "failed", "sync_c": "failed"})

    result = runner.invoke(app, ["run", "--failed", "--select", "tag:crm", "--output", "json"])

    assert result.exit_code == 0
    assert patched_engine["calls"] == ["sync_a"]  # sync_c failed but isn't tag:crm


def test_failed_with_clean_state_exits_zero_without_running(
    project: Path, patched_engine: dict[str, Any]
) -> None:
    _seed_state({"sync_a": "success", "sync_b": "success"})

    result = runner.invoke(app, ["run", "--failed", "--output", "json"])

    assert result.exit_code == 0
    assert patched_engine["calls"] == []
    assert "nothing_failed" in result.output


# ---------------------------------------------------------------------------
# --limit (#774): sampled runs
# ---------------------------------------------------------------------------


def test_limit_forwarded_to_engine(project: Path, patched_engine: dict[str, Any]) -> None:
    result = runner.invoke(app, ["run", "--select", "sync_a", "--limit", "10", "--output", "json"])
    assert result.exit_code == 0
    assert patched_engine["calls"] == ["sync_a"]
    assert patched_engine["limits"] == [10]


def test_limit_rejects_non_positive(project: Path, patched_engine: dict[str, Any]) -> None:
    result = runner.invoke(app, ["run", "--limit", "0", "--output", "json"])
    assert result.exit_code == 1
    assert patched_engine["calls"] == []


def _patch_failing_engine(
    monkeypatch: pytest.MonkeyPatch, patched_engine: dict[str, Any], fail_names: set[str]
) -> None:
    """Re-patch run_sync so the named syncs report failure."""
    from drt.engine import sync as sync_module

    calls = patched_engine["calls"]

    def fake_run_sync(sync, *_args: Any, **_kwargs: Any) -> _FakeResult:
        calls.append(sync.name)
        if sync.name in fail_names:
            return _FakeResult(success=0, failed=1)
        return _FakeResult(success=1, failed=0)

    monkeypatch.setattr(sync_module, "run_sync", fake_run_sync, raising=False)


# ---------------------------------------------------------------------------
# --fail-fast (#775)
# ---------------------------------------------------------------------------


def test_fail_fast_sequential_skips_remaining(
    project: Path, patched_engine: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_failing_engine(monkeypatch, patched_engine, {"sync_a"})

    result = runner.invoke(app, ["run", "--fail-fast", "--output", "json"])

    assert result.exit_code == 1
    assert patched_engine["calls"] == ["sync_a"]  # b and c never scheduled
    payload = json.loads(result.output)
    assert payload["failed"] == 1
    assert payload["skipped"] == 2
    skipped = [e for e in payload["syncs"] if e["status"] == "skipped"]
    assert {e["name"] for e in skipped} == {"sync_b", "sync_c"}
    assert all(e["reason"] == "fail_fast" for e in skipped)


def test_without_fail_fast_all_syncs_still_run(
    project: Path, patched_engine: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    _patch_failing_engine(monkeypatch, patched_engine, {"sync_a"})

    result = runner.invoke(app, ["run", "--output", "json"])

    assert result.exit_code == 1
    assert set(patched_engine["calls"]) == {"sync_a", "sync_b", "sync_c"}
    payload = json.loads(result.output)
    assert payload["skipped"] == 0


def test_fail_fast_with_threads_accounts_for_every_sync(
    project: Path, patched_engine: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """Threaded fail-fast: no sync is lost — every sync is either run or
    reported skipped (exact split is timing-dependent by design)."""
    _patch_failing_engine(monkeypatch, patched_engine, {"sync_a", "sync_b", "sync_c"})

    result = runner.invoke(app, ["run", "--fail-fast", "--threads", "3", "--output", "json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    assert payload["succeeded"] + payload["failed"] + payload["skipped"] == 3
    assert len(payload["syncs"]) == 3


def test_fail_fast_with_threads_cancels_queued_syncs(
    project: Path, patched_engine: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """More syncs than workers, so some are still *queued* when the first one
    fails — those get cancelled and must surface as ``skipped``.

    The sibling test above runs 3 syncs on 3 threads, so every future starts
    immediately and the ``future.cancelled()`` branch never executes. Without
    this case, a regression that drops cancelled futures on the floor (losing
    syncs from the report entirely) would go unnoticed.
    """
    # Named to sort *after* sync_a, so the failing sync is submitted first and
    # the rest are still queued when it trips --fail-fast.
    syncs_dir = project / "syncs"
    for i in range(8):
        name = f"zz_queued_{i}"
        (syncs_dir / f"{name}.yml").write_text(
            yaml.dump({**SYNC_B, "name": name, "tags": ["queued"]})
        )

    calls: list[str] = []
    lock = threading.Lock()

    def fake_run_sync(sync, *_a: Any, **_kw: Any) -> _FakeResult:
        with lock:
            calls.append(sync.name)
        if sync.name == "sync_a":
            return _FakeResult(success=0, failed=1)  # fails immediately
        time.sleep(0.3)  # occupies its worker long enough for the queue to be cancelled
        return _FakeResult(success=1, failed=0)

    from drt.engine import sync as sync_module

    monkeypatch.setattr(sync_module, "run_sync", fake_run_sync, raising=False)

    result = runner.invoke(app, ["run", "--fail-fast", "--threads", "2", "--output", "json"])

    assert result.exit_code == 1
    payload = json.loads(result.output)
    total = len(payload["syncs"])
    assert total == 11  # sync_a/b/c + 8 queued
    assert payload["succeeded"] + payload["failed"] + payload["skipped"] == total
    # Cancellation actually happened: with 2 workers, most syncs never started.
    assert payload["skipped"] > 0
    assert len(calls) < total
    skipped = [e for e in payload["syncs"] if e["status"] == "skipped"]
    assert len(skipped) == payload["skipped"]
    assert all(e["reason"] == "fail_fast" for e in skipped)
    # A cancelled sync is reported, not silently dropped.
    assert {e["name"] for e in payload["syncs"]} >= {f"zz_queued_{i}" for i in range(8)}


def test_limit_applied_surfaces_in_json(
    project: Path, patched_engine: dict[str, Any], monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--limit`` must be echoed back per sync in JSON output, so a sampled
    run is distinguishable from a full one after the fact."""
    from drt.engine import sync as sync_module

    def fake_run_sync(sync, *_a: Any, **kw: Any) -> _FakeResult:
        res = _FakeResult(success=1, failed=0)
        res.limit_applied = kw.get("extract_limit")
        return res

    monkeypatch.setattr(sync_module, "run_sync", fake_run_sync, raising=False)

    result = runner.invoke(app, ["run", "--select", "sync_a", "--limit", "10", "--output", "json"])

    assert result.exit_code == 0
    entry = json.loads(result.output)["syncs"][0]
    assert entry["limit"] == 10


def test_limit_refused_for_mirror_and_replace(
    project: Path, patched_engine: dict[str, Any]
) -> None:
    (project / "syncs" / "sync_mirror.yml").write_text(
        yaml.dump(
            {
                "name": "sync_mirror",
                "model": "SELECT 1",
                "destination": {
                    "type": "postgres",
                    "host": "localhost",
                    "dbname": "d",
                    "user": "u",
                    "password_env": "PGPASSWORD",
                    "table": "t",
                    "upsert_key": ["id"],
                },
                "sync": {"mode": "mirror"},
            }
        )
    )

    result = runner.invoke(app, ["run", "--limit", "5", "--output", "json"])

    assert result.exit_code == 1
    assert "sync_mirror" in result.output
    assert patched_engine["calls"] == []


# ---------------------------------------------------------------------------
# --threads — real parallel dispatch
# ---------------------------------------------------------------------------


def test_threads_flag_actually_parallelises(project: Path, patched_engine: dict[str, Any]) -> None:
    result = runner.invoke(
        app,
        ["run", "--select", "*", "--threads", "3", "--output", "json"],
    )
    assert result.exit_code == 0
    assert set(patched_engine["calls"]) == {"sync_a", "sync_b", "sync_c"}
    # More than one OS thread should have handled the syncs when
    # --threads > 1 and there are multiple syncs.
    assert len(patched_engine["threads_seen"]) > 1


def test_threads_one_runs_sequentially(project: Path, patched_engine: dict[str, Any]) -> None:
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
