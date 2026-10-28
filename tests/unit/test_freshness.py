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
    assert "no run on record" in verdict.message()


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


def test_nothing_monitored_fails_closed_instead_of_reporting_green(project: Path) -> None:
    _write_sync(project, "plain", None)

    text = runner.invoke(app, ["status", "--check"])
    as_json = runner.invoke(app, ["status", "--check", "--output", "json"])

    assert text.exit_code == 1 and "Nothing was checked" in text.output
    payload = json.loads(as_json.output)
    assert payload["ok"] is False and payload["checked"] == 0
    assert payload["unmonitored"] == ["plain"]


def test_unmonitored_selected_syncs_are_listed(project: Path) -> None:
    _write_sync(project, "watched", {"max_age": "6h"})
    _write_sync(project, "forgotten", None)
    _history(project, "watched", 1)

    result = runner.invoke(app, ["status", "--check", "--output", "json"])

    payload = json.loads(result.output)
    assert result.exit_code == 0 and payload["unmonitored"] == ["forgotten"]


def test_check_cannot_be_combined_with_modes_it_would_silently_override(project: Path) -> None:
    _write_sync(project, "a", {"max_age": "6h"})

    for extra in (["--history"], ["--sync", "a"], ["--verbose"]):
        assert runner.invoke(app, ["status", "--check", *extra]).exit_code == 1


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


# ---------------------------------------------------------------------------
# Hardening from the adversarial review
# ---------------------------------------------------------------------------


def test_dry_run_entries_never_reset_the_clock(tmp_path: Path) -> None:
    mgr = HistoryManager(tmp_path)
    mgr.append(_entry("orders", 20))
    dry = _entry("orders", 1)
    dry.dry_run = True
    mgr.append(dry)

    [verdict] = evaluate_freshness([_sync("orders")], mgr, {}, NOW)

    assert verdict.state == "stale" and verdict.age_seconds == 20 * 3600


def test_a_sync_whose_recent_runs_all_failed_is_stale_not_never_run(tmp_path: Path) -> None:
    mgr = HistoryManager(tmp_path)
    for i in range(3):
        mgr.append(_entry("orders", 3 - i, status="failed"))

    [verdict] = evaluate_freshness([_sync("orders", never_run="warn")], mgr, {}, NOW)

    assert (verdict.state, verdict.severity) == ("stale", "error")
    assert verdict.last_success_at is None and "no successful run" in verdict.message()


def test_the_newest_success_is_chosen_by_completion_not_start(tmp_path: Path) -> None:
    """Run A starts first but finishes last; history is ordered by start time."""
    mgr = HistoryManager(tmp_path)
    a = _entry("orders", 12)  # started 12h ago
    a.completed_at = (NOW - timedelta(minutes=5)).isoformat()  # ...finished 5 minutes ago
    b = _entry("orders", 3)  # started later, finished earlier
    b.completed_at = (NOW - timedelta(hours=2, minutes=30)).isoformat()
    mgr.append(a)
    mgr.append(b)

    [verdict] = evaluate_freshness([_sync("orders", "20m")], mgr, {}, NOW)

    assert verdict.state == "fresh" and verdict.age_seconds == 300


def test_a_success_dated_in_the_future_is_an_error_not_fresh(tmp_path: Path) -> None:
    mgr = HistoryManager(tmp_path)
    mgr.append(_entry("orders", -24))  # a runner whose clock is a day fast

    [verdict] = evaluate_freshness([_sync("orders")], mgr, {}, NOW)

    assert (verdict.state, verdict.severity) == ("clock_skew", "error")
    assert "future" in verdict.message()


def test_small_clock_skew_is_tolerated(tmp_path: Path) -> None:
    mgr = HistoryManager(tmp_path)
    mgr.append(_entry("orders", -0.02))  # ~1 minute ahead

    [verdict] = evaluate_freshness([_sync("orders")], mgr, {}, NOW)

    assert verdict.state == "fresh"


def test_a_typo_in_the_policy_is_rejected_not_silently_defaulted() -> None:
    with pytest.raises(ValidationError, match="never_runs"):
        OnStaleConfig(max_age="6h", never_runs="error")  # type: ignore[call-arg]


def test_a_bad_max_age_error_does_not_echo_channel_secrets() -> None:
    secret = "https://hooks.example.invalid/SECRET-TOKEN-123"
    with pytest.raises(ValidationError) as raised:
        OnStaleConfig(max_age="soon", channels=[{"type": "webhook", "url": secret}])  # type: ignore[list-item]

    assert "SECRET-TOKEN-123" not in str(raised.value)


@pytest.mark.parametrize("huge", ["9" * 40 + "w", "4000d", "99999999999d"])
def test_enormous_durations_are_a_config_error_not_a_traceback(huge: str) -> None:
    with pytest.raises(ValueError):
        parse_max_age(huge)


def test_notify_context_supports_every_documented_template_variable(project: Path) -> None:
    hook = {
        "type": "slack",
        "webhook_url": "https://hooks.example.invalid/x",
        "message": "{sync_name} {error} {started_at} {rows_processed} {duration_s} {run_id}",
    }
    _write_sync(project, "late", {"max_age": "6h", "channels": [hook]})
    _history(project, "late", 9)
    sent_bodies: list[bytes] = []

    class _Response:
        def close(self) -> None:
            return None

    def fake_urlopen(request: Any, timeout: int = 10) -> _Response:
        sent_bodies.append(request.data)
        return _Response()

    with patch("drt.alerts.slack.urllib.request.urlopen", fake_urlopen):
        result = runner.invoke(app, ["status", "--check", "--notify"])

    assert result.exit_code == 1 and sent_bodies, "the alert must actually be sent"
    assert "late" in sent_bodies[0].decode()


def test_the_default_slack_message_says_stale_not_failed(project: Path) -> None:
    hook = {"type": "slack", "webhook_url": "https://hooks.example.invalid/x"}
    _write_sync(project, "late", {"max_age": "6h", "channels": [hook]})
    _history(project, "late", 9)

    with patch("drt.alerts.dispatcher.dispatch_targets") as sent:
        runner.invoke(app, ["status", "--check", "--notify"])

    [(targets, _context)] = [call.args for call in sent.call_args_list]
    assert "stale" in targets[0].message and "failed" not in targets[0].message


def test_a_sync_with_only_dry_run_history_is_never_run(tmp_path: Path) -> None:
    mgr = HistoryManager(tmp_path)
    dry = _entry("orders", 1)
    dry.dry_run = True
    mgr.append(dry)

    [verdict] = evaluate_freshness([_sync("orders", never_run="warn")], mgr, {}, NOW)

    assert (verdict.state, verdict.severity) == ("never_run", "warn")


def test_a_newer_successful_state_beats_an_older_history_success(tmp_path: Path) -> None:
    """History appends are best-effort and can lag (or history can be switched off)."""
    mgr = HistoryManager(tmp_path)
    mgr.append(_entry("orders", 25))
    states = {
        "orders": SimpleNamespace(
            status="success", last_run_at=(NOW - timedelta(hours=1)).isoformat()
        )
    }

    [verdict] = evaluate_freshness([_sync("orders")], mgr, states, NOW)

    assert verdict.state == "fresh" and verdict.age_seconds == 3600


def test_an_older_state_success_does_not_override_a_newer_history_success(tmp_path: Path) -> None:
    mgr = HistoryManager(tmp_path)
    mgr.append(_entry("orders", 1))
    states = {
        "orders": SimpleNamespace(
            status="success", last_run_at=(NOW - timedelta(hours=30)).isoformat()
        )
    }

    [verdict] = evaluate_freshness([_sync("orders")], mgr, states, NOW)

    assert verdict.state == "fresh" and verdict.age_seconds == 3600


def test_allow_empty_turns_nothing_monitored_into_a_warning(project: Path) -> None:
    _write_sync(project, "plain", None)

    text = runner.invoke(app, ["status", "--check", "--allow-empty"])
    as_json = runner.invoke(app, ["status", "--check", "--allow-empty", "--output", "json"])

    assert text.exit_code == 0 and "Nothing was checked" in text.output
    payload = json.loads(as_json.output)
    assert as_json.exit_code == 0 and payload["ok"] is True and payload["checked"] == 0


def test_allow_empty_never_hides_a_stale_sync(project: Path) -> None:
    _write_sync(project, "late", {"max_age": "6h"})
    _history(project, "late", 9)

    assert runner.invoke(app, ["status", "--check", "--allow-empty"]).exit_code == 1


def test_allow_empty_without_check_is_refused(project: Path) -> None:
    assert runner.invoke(app, ["status", "--allow-empty"]).exit_code == 1


def test_mcp_allow_empty_makes_an_unmonitored_project_ok(project: Path) -> None:
    import asyncio

    from drt.mcp.server import create_server

    _write_sync(project, "plain", None)
    server = create_server(project)

    async def call(**kwargs: Any) -> dict[str, Any]:
        result = await server.call_tool("drt_get_status", kwargs)
        return result.structured_content  # type: ignore[no-any-return]

    assert asyncio.run(call(check=True))["ok"] is False
    assert asyncio.run(call(check=True, allow_empty=True))["ok"] is True


def test_a_corrupt_state_file_fails_the_check_instead_of_reading_as_never_run(
    project: Path,
) -> None:
    """History off + a corrupted state document must not look like 'never ran, warn'."""
    _write_sync(project, "daily", {"max_age": "6h"})
    (project / ".drt").mkdir(exist_ok=True)
    (project / ".drt" / "state.json").write_text("{ this is not json")

    text = runner.invoke(app, ["status", "--check"])
    as_json = runner.invoke(app, ["status", "--check", "--output", "json"])

    assert text.exit_code == 1 and "could not be read" in text.output
    payload = json.loads(as_json.stdout)  # stdout only: the store also warns on stderr
    assert payload["ok"] is False and payload["unreadable_evidence"] >= 1


def test_a_malformed_history_line_fails_the_check(project: Path) -> None:
    _write_sync(project, "daily", {"max_age": "6h"})
    _history(project, "daily", 0.1)
    path = project / ".drt" / "history" / "daily.jsonl"
    path.write_text(path.read_text() + "not json at all\n")

    result = runner.invoke(app, ["status", "--check", "--output", "json"])

    assert result.exit_code == 1
    assert json.loads(result.stdout)["unreadable_evidence"] >= 1


def test_allow_empty_does_not_hide_unreadable_evidence(project: Path) -> None:
    _write_sync(project, "plain", None)
    (project / ".drt").mkdir(exist_ok=True)
    (project / ".drt" / "state.json").write_text("{ broken")

    assert runner.invoke(app, ["status", "--check", "--allow-empty"]).exit_code == 1


def test_mcp_flags_unreadable_evidence_and_rejects_a_dangling_allow_empty(
    project: Path,
) -> None:
    import asyncio

    from drt.mcp.server import create_server

    _write_sync(project, "daily", {"max_age": "6h"})
    (project / ".drt").mkdir(exist_ok=True)
    (project / ".drt" / "state.json").write_text("{ broken")
    server = create_server(project)

    async def call(**kwargs: Any) -> dict[str, Any]:
        result = await server.call_tool("drt_get_status", kwargs)
        return result.structured_content  # type: ignore[no-any-return]

    checked = asyncio.run(call(check=True))
    dangling = asyncio.run(call(allow_empty=True))

    assert checked["ok"] is False and checked["unreadable_evidence"] >= 1
    assert "check=True" in dangling["error"]


# ---------------------------------------------------------------------------
# Unreadable evidence: the side channel, its backends, and what cannot switch it off
# ---------------------------------------------------------------------------


def test_the_signal_survives_logging_being_disabled(project: Path) -> None:
    """The channel is not log records, so a host's logging config cannot silence it."""
    import logging

    _write_sync(project, "daily", {"max_age": "6h"})
    (project / ".drt").mkdir(exist_ok=True)
    (project / ".drt" / "state.json").write_text("{ broken")
    logging.disable(logging.CRITICAL)
    try:
        result = runner.invoke(app, ["status", "--check", "--output", "json"])
    finally:
        logging.disable(logging.NOTSET)

    assert result.exit_code == 1 and json.loads(result.stdout)["unreadable_evidence"] >= 1


@pytest.mark.parametrize("root", ["[]", "0", '""', "false"])
def test_a_falsey_non_object_state_root_is_corruption_not_empty_state(
    project: Path, root: str
) -> None:
    _write_sync(project, "daily", {"max_age": "6h"})
    (project / ".drt").mkdir(exist_ok=True)
    (project / ".drt" / "state.json").write_text(root)

    result = runner.invoke(app, ["status", "--check", "--output", "json"])

    assert result.exit_code == 1 and json.loads(result.stdout)["unreadable_evidence"] >= 1


def test_an_empty_json_null_state_is_just_empty(project: Path) -> None:
    from drt.state._integrity import collect_unreadable
    from drt.state.manager import StateManager

    (project / ".drt").mkdir(exist_ok=True)
    (project / ".drt" / "state.json").write_text("null")

    with collect_unreadable() as seen:
        assert StateManager(project).get_all() == {}

    assert seen == []


def test_corrupt_object_store_state_and_history_are_reported() -> None:
    from drt.state._integrity import collect_unreadable
    from drt.state._objectstore import ObjectStoreHistoryStore, ObjectStoreStateStore

    with collect_unreadable() as seen:
        assert ObjectStoreStateStore._decode(b"{broken") == {}
        assert ObjectStoreStateStore._decode(b"[1, 2]") == {}
        ObjectStoreHistoryStore._decode(b"not json\n", "history/x.jsonl")

    assert len(seen) == 3


def test_an_unreadable_success_timestamp_is_reported_not_ignored(tmp_path: Path) -> None:
    from drt.state._integrity import collect_unreadable

    mgr = HistoryManager(tmp_path)
    bad = _entry("orders", 1)
    bad.completed_at = "not a time"
    bad.started_at = "also not a time"
    mgr.append(bad)
    mgr.append(_entry("orders", 2))  # a valid fresh success alongside it

    with collect_unreadable() as seen:
        [verdict] = evaluate_freshness([_sync("orders")], mgr, {}, NOW)

    assert verdict.state == "fresh"  # the readable one still counts...
    assert seen, "...but the malformed record must be surfaced, not swallowed"


def test_concurrent_collectors_do_not_see_each_others_reports() -> None:
    import threading

    from drt.state._integrity import collect_unreadable, report_unreadable

    results: dict[str, list[str]] = {}
    ready, go = threading.Event(), threading.Event()

    def worker(name: str, emit: bool) -> None:
        with collect_unreadable() as seen:
            if emit:
                report_unreadable(f"{name} broke")
            ready.set()
            go.wait(2)
        results[name] = list(seen)

    a = threading.Thread(target=worker, args=("a", True))
    b = threading.Thread(target=worker, args=("b", False))
    a.start()
    ready.wait(2)
    b.start()
    go.set()
    a.join()
    b.join()

    assert results == {"a": ["a broke"], "b": []}


def test_reports_outside_a_collector_are_ignored() -> None:
    from drt.state._integrity import report_unreadable

    report_unreadable("nobody is listening")  # must not raise


def test_a_success_older_than_two_hundred_failures_is_still_found(tmp_path: Path) -> None:
    mgr = HistoryManager(tmp_path)
    mgr.append(_entry("orders", 5))  # the last success, 5h ago
    for i in range(300):  # 300 failures since
        mgr.append(_entry("orders", 4.9 - i / 1000, status="failed"))

    [verdict] = evaluate_freshness([_sync("orders")], mgr, {}, NOW)

    assert verdict.state == "fresh" and verdict.age_seconds == 5 * 3600


def test_a_garbled_completed_at_is_reported_even_when_started_at_is_usable(tmp_path: Path) -> None:
    from drt.state._integrity import collect_unreadable

    mgr = HistoryManager(tmp_path)
    entry = _entry("orders", 1)
    entry.completed_at = "garbled"  # started_at stays a valid recent time
    mgr.append(entry)

    with collect_unreadable() as seen:
        [verdict] = evaluate_freshness([_sync("orders")], mgr, {}, NOW)

    assert verdict.state == "fresh" and seen, "the damaged record must be surfaced"
