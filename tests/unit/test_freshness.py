"""Sync-level freshness: ``alerts.on_stale`` + ``drt status --check`` (#1224)."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import patch

import pytest
import yaml
from pydantic import ValidationError
from typer.testing import CliRunner

from drt.cli.main import app
from drt.config.duration import parse_max_age
from drt.config.models import AlertsConfig, OnStaleConfig
from drt.engine.freshness import evaluate_freshness, has_failure
from drt.state.history import HistoryEntry, HistoryManager

runner = CliRunner()
NOW = datetime(2026, 10, 24, 12, 0, tzinfo=timezone.utc)


def _entry(sync: str, hours_ago: float, status: str = "success") -> HistoryEntry:
    when = NOW - timedelta(hours=hours_ago)
    return HistoryEntry(
        sync_name=sync,
        started_at=when.isoformat(),
        completed_at=when.isoformat(),
        duration_seconds=1.0,
        status=status,
        records_synced=1,
        records_failed=0,
    )


def _sync(name: str, max_age: str | None = "6h", never_run: str = "warn") -> Any:
    alerts = (
        AlertsConfig(on_stale=OnStaleConfig(max_age=max_age, never_run=never_run))
        if max_age
        else None
    )
    return SimpleNamespace(name=name, alerts=alerts)


@pytest.mark.parametrize(
    ("text", "seconds"),
    [("6h", 21600), ("30m", 1800), ("2d", 172800), ("1w", 604800), ("6 hours", 21600)],
)
def test_max_age_accepts_compact_and_long_forms(text: str, seconds: int) -> None:
    assert parse_max_age(text).total_seconds() == seconds


@pytest.mark.parametrize("bad", ["", "soon", "0h", "-2h", "6", "1.5h", "6 fortnights"])
def test_a_malformed_max_age_is_a_config_error(bad: str) -> None:
    with pytest.raises(ValidationError):
        OnStaleConfig(max_age=bad)


def test_a_fresh_sync_is_ok_and_a_stale_one_is_an_error(tmp_path: Path) -> None:
    mgr = HistoryManager(tmp_path)
    mgr.append(_entry("fresh", 2))
    mgr.append(_entry("stale", 9))

    verdicts = {
        v.sync_name: v for v in evaluate_freshness([_sync("fresh"), _sync("stale")], mgr, {}, NOW)
    }

    assert (verdicts["fresh"].state, verdicts["fresh"].severity) == ("fresh", "ok")
    assert (verdicts["stale"].state, verdicts["stale"].severity) == ("stale", "error")
    assert verdicts["stale"].age_seconds == 9 * 3600
    assert has_failure(verdicts.values())


def test_only_a_success_resets_the_clock(tmp_path: Path) -> None:
    mgr = HistoryManager(tmp_path)
    mgr.append(_entry("orders", 20))  # last success, long ago
    mgr.append(_entry("orders", 1, status="failed"))
    mgr.append(_entry("orders", 0.5, status="partial"))

    [verdict] = evaluate_freshness([_sync("orders")], mgr, {}, NOW)

    assert verdict.state == "stale" and verdict.age_seconds == 20 * 3600


@pytest.mark.parametrize(
    ("never_run", "severity", "fails"), [("warn", "warn", False), ("error", "error", True)]
)
def test_a_sync_that_never_succeeded_follows_never_run(
    tmp_path: Path, never_run: str, severity: str, fails: bool
) -> None:
    [verdict] = evaluate_freshness(
        [_sync("new", never_run=never_run)], HistoryManager(tmp_path), {}, NOW
    )

    assert (verdict.state, verdict.severity) == ("never_run", severity)
    assert has_failure([verdict]) is fails
    assert "no successful run" in verdict.message()


def test_stored_state_is_the_fallback_when_history_has_no_success(tmp_path: Path) -> None:
    states = {
        "orders": SimpleNamespace(
            status="success", last_run_at=(NOW - timedelta(hours=1)).isoformat()
        )
    }

    [verdict] = evaluate_freshness([_sync("orders")], HistoryManager(tmp_path), states, NOW)

    assert verdict.state == "fresh"


def test_syncs_without_on_stale_are_skipped_and_a_naive_timestamp_is_utc(tmp_path: Path) -> None:
    mgr = HistoryManager(tmp_path)
    naive = _entry("a", 1)
    naive.completed_at = (NOW - timedelta(hours=1)).replace(tzinfo=None).isoformat()
    mgr.append(naive)

    verdicts = evaluate_freshness([_sync("a"), _sync("unconfigured", max_age=None)], mgr, {}, NOW)

    assert [v.sync_name for v in verdicts] == ["a"] and verdicts[0].state == "fresh"


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


@pytest.fixture
def project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    monkeypatch.chdir(tmp_path)
    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "t", "version": "0.1", "profile": "default"})
    )
    (tmp_path / "syncs").mkdir()
    return tmp_path


def _write_sync(
    project: Path, name: str, stale: dict[str, Any] | None, tags: list[str] | None = None
) -> None:
    cfg: dict[str, Any] = {
        "name": name,
        "model": "SELECT 1",
        "destination": {"type": "slack", "webhook_url": "https://example.invalid/hook"},
        "tags": tags or [],
    }
    if stale is not None:
        cfg["alerts"] = {"on_stale": stale}
    (project / "syncs" / f"{name}.yml").write_text(yaml.dump(cfg))


def _history(project: Path, sync: str, hours_ago: float) -> None:
    now = datetime.now(timezone.utc)
    when = now - timedelta(hours=hours_ago)
    HistoryManager(project).append(
        HistoryEntry(
            sync_name=sync,
            started_at=when.isoformat(),
            completed_at=when.isoformat(),
            duration_seconds=1.0,
            status="success",
            records_synced=1,
            records_failed=0,
        )
    )


def test_check_exits_1_when_a_sync_is_stale_and_0_when_all_are_fresh(project: Path) -> None:
    _write_sync(project, "daily", {"max_age": "6h"})
    _history(project, "daily", 9)

    stale = runner.invoke(app, ["status", "--check"])
    _history(project, "daily", 0.1)
    fresh = runner.invoke(app, ["status", "--check"])

    assert stale.exit_code == 1 and "stale" in stale.output
    assert fresh.exit_code == 0 and "fresh" in fresh.output


def test_check_json_and_select(project: Path) -> None:
    _write_sync(project, "a_daily", {"max_age": "6h"}, tags=["daily"])
    _write_sync(project, "b_weekly", {"max_age": "1w"}, tags=["weekly"])
    _history(project, "a_daily", 9)
    _history(project, "b_weekly", 9)

    everything = runner.invoke(app, ["status", "--check", "--output", "json"])
    weekly = runner.invoke(app, ["status", "--check", "--select", "tag:weekly", "--output", "json"])

    assert everything.exit_code == 1
    assert [s["sync"] for s in json.loads(everything.output)["syncs"]] == ["a_daily", "b_weekly"]
    assert weekly.exit_code == 0 and json.loads(weekly.output)["ok"] is True


def test_nothing_configured_is_reported_not_silently_green(project: Path) -> None:
    _write_sync(project, "plain", None)

    result = runner.invoke(app, ["status", "--check"])

    assert result.exit_code == 0 and "alerts.on_stale" in result.output


def test_notify_sends_only_for_stale_syncs(project: Path) -> None:
    hook = {"type": "webhook", "url": "https://example.invalid/stale"}
    _write_sync(project, "late", {"max_age": "6h", "channels": [hook]})
    _write_sync(project, "ok", {"max_age": "6h", "channels": [hook]})
    _history(project, "late", 9)
    _history(project, "ok", 1)

    with patch("drt.alerts.dispatcher.dispatch_targets") as sent:
        result = runner.invoke(app, ["status", "--check", "--notify"])

    assert result.exit_code == 1
    [(targets, context)] = [call.args for call in sent.call_args_list]
    assert context["sync_name"] == "late" and context["status"] == "stale"
    assert targets[0].url == hook["url"]


def test_check_never_notifies_without_the_flag(project: Path) -> None:
    hook = {"type": "webhook", "url": "https://example.invalid/stale"}
    _write_sync(project, "late", {"max_age": "6h", "channels": [hook]})
    _history(project, "late", 9)

    with patch("drt.alerts.dispatcher.dispatch_targets") as sent:
        runner.invoke(app, ["status", "--check"])

    sent.assert_not_called()


def test_select_or_notify_without_check_is_refused(project: Path) -> None:
    assert runner.invoke(app, ["status", "--notify"]).exit_code == 1
    assert runner.invoke(app, ["status", "--select", "x"]).exit_code == 1


def test_a_bad_selector_is_an_error_not_an_empty_pass(project: Path) -> None:
    _write_sync(project, "a", {"max_age": "6h"})

    assert runner.invoke(app, ["status", "--check", "--select", "tag:nope"]).exit_code == 1


def test_mcp_get_status_check_returns_the_same_verdict(project: Path) -> None:
    import asyncio

    from drt.mcp.server import create_server

    _write_sync(project, "daily", {"max_age": "6h"})
    _history(project, "daily", 9)
    server = create_server(project)

    async def call() -> dict[str, Any]:
        result = await server.call_tool("drt_get_status", {"check": True})
        return result.structured_content  # type: ignore[no-any-return]

    verdict = asyncio.run(call())

    assert verdict["ok"] is False and verdict["syncs"][0]["state"] == "stale"


def test_mcp_check_filters_to_one_sync_and_rejects_an_unknown_name(project: Path) -> None:
    import asyncio

    from drt.mcp.server import create_server

    _write_sync(project, "daily", {"max_age": "6h"})
    _write_sync(project, "other", {"max_age": "6h"})
    _history(project, "daily", 1)
    _history(project, "other", 9)
    server = create_server(project)

    async def call(**kwargs: Any) -> dict[str, Any]:
        result = await server.call_tool("drt_get_status", kwargs)
        return result.structured_content  # type: ignore[no-any-return]

    one = asyncio.run(call(check=True, sync_name="daily"))
    missing = asyncio.run(call(check=True, sync_name="nope"))

    assert one["ok"] is True and [s["sync"] for s in one["syncs"]] == ["daily"]
    assert "nope" in missing["error"]
