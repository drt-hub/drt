"""Unit tests for alert dispatch. Uses pytest-httpserver for webhook targets."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from drt.alerts.dispatcher import build_context, dispatch_alerts
from drt.config.models import AlertsConfig, SlackAlertConfig, WebhookAlertConfig
from drt.destinations.base import SyncResult


class TestBuildContext:
    def test_context_has_required_fields(self) -> None:
        result = SyncResult(success=10, failed=2)
        result.errors = ["BigQuery 403: Permission denied"]
        ctx = build_context(
            sync_name="my_sync",
            result=result,
            duration_s=12.34,
            started_at="2026-04-29T10:00:00",
        )
        assert ctx["sync_name"] == "my_sync"
        assert ctx["error"] == "BigQuery 403: Permission denied"
        assert ctx["rows_processed"] == 12  # success + failed
        assert ctx["duration_s"] == 12.34
        assert ctx["started_at"] == "2026-04-29T10:00:00"

    def test_context_with_no_errors_uses_placeholder(self) -> None:
        result = SyncResult(success=0, failed=5)  # failed but no error message
        ctx = build_context(sync_name="s", result=result, duration_s=1.0, started_at="t")
        assert ctx["error"] == "<no error message>"

    def test_context_from_exception(self) -> None:
        result = SyncResult(success=0, failed=0)
        ctx = build_context(
            sync_name="s",
            result=result,
            duration_s=1.0,
            started_at="t",
            exception=RuntimeError("connection refused"),
        )
        assert "connection refused" in ctx["error"]


class TestSlackSender:
    @patch("urllib.request.urlopen")
    def test_slack_posts_formatted_message(self, mock_urlopen: MagicMock) -> None:
        from drt.alerts.slack import send_slack_alert
        cfg = SlackAlertConfig(
            type="slack",
            webhook_url="https://hooks.slack.com/services/T/B/C",
            message="sync {sync_name} failed: {error}",
        )
        send_slack_alert(cfg, {"sync_name": "x", "error": "boom"})
        mock_urlopen.assert_called_once()
        req = mock_urlopen.call_args[0][0]
        assert req.full_url == "https://hooks.slack.com/services/T/B/C"
        body = req.data.decode()
        assert "sync x failed: boom" in body

    @patch("urllib.request.urlopen", side_effect=OSError("network down"))
    def test_slack_failure_does_not_raise(self, mock_urlopen: MagicMock) -> None:
        from drt.alerts.slack import send_slack_alert
        cfg = SlackAlertConfig(type="slack", webhook_url="https://x")
        # Must not raise — alert dispatch is best-effort
        send_slack_alert(cfg, {"sync_name": "x", "error": "y"})

    @patch("urllib.request.urlopen")
    def test_slack_resolves_url_from_env(
        self, mock_urlopen: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from drt.alerts.slack import send_slack_alert
        monkeypatch.setenv("SLACK_HOOK_URL", "https://env.example/hook")
        cfg = SlackAlertConfig(type="slack", webhook_url_env="SLACK_HOOK_URL")
        send_slack_alert(cfg, {"sync_name": "x", "error": "y"})
        req = mock_urlopen.call_args[0][0]
        assert req.full_url == "https://env.example/hook"

    @patch("urllib.request.urlopen")
    def test_slack_skips_when_env_unset(
        self, mock_urlopen: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from drt.alerts.slack import send_slack_alert
        monkeypatch.delenv("SLACK_HOOK_UNSET", raising=False)
        cfg = SlackAlertConfig(type="slack", webhook_url_env="SLACK_HOOK_UNSET")
        send_slack_alert(cfg, {"sync_name": "x", "error": "y"})
        mock_urlopen.assert_not_called()


class TestWebhookSender:
    @patch("urllib.request.urlopen")
    def test_webhook_posts_default_json_body(self, mock_urlopen: MagicMock) -> None:
        from drt.alerts.webhook import send_webhook_alert
        cfg = WebhookAlertConfig(type="webhook", url="https://example.com/hook")
        send_webhook_alert(cfg, {"sync_name": "x", "error": "y", "rows_processed": 5})
        req = mock_urlopen.call_args[0][0]
        import json
        payload = json.loads(req.data.decode())
        assert payload["sync_name"] == "x"
        assert payload["rows_processed"] == 5

    @patch("urllib.request.urlopen")
    def test_webhook_uses_custom_body_template(self, mock_urlopen: MagicMock) -> None:
        from drt.alerts.webhook import send_webhook_alert
        cfg = WebhookAlertConfig(
            type="webhook",
            url="https://example.com/hook",
            body_template='{{"text":"{sync_name} broke"}}',
        )
        send_webhook_alert(cfg, {"sync_name": "x", "error": "y"})
        req = mock_urlopen.call_args[0][0]
        assert b'"text":"x broke"' in req.data

    @patch("urllib.request.urlopen")
    def test_webhook_resolves_url_from_env(
        self, mock_urlopen: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from drt.alerts.webhook import send_webhook_alert
        monkeypatch.setenv("WEBHOOK_URL", "https://env.example/hook")
        cfg = WebhookAlertConfig(type="webhook", url_env="WEBHOOK_URL")
        send_webhook_alert(cfg, {"sync_name": "x", "error": "y"})
        req = mock_urlopen.call_args[0][0]
        assert req.full_url == "https://env.example/hook"

    @patch("urllib.request.urlopen")
    def test_webhook_skips_when_url_unresolved(
        self, mock_urlopen: MagicMock, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        from drt.alerts.webhook import send_webhook_alert
        monkeypatch.delenv("WEBHOOK_URL_UNSET", raising=False)
        cfg = WebhookAlertConfig(type="webhook", url_env="WEBHOOK_URL_UNSET")
        send_webhook_alert(cfg, {"sync_name": "x", "error": "y"})
        mock_urlopen.assert_not_called()

    @patch("urllib.request.urlopen", side_effect=OSError("network down"))
    def test_webhook_failure_does_not_raise(self, mock_urlopen: MagicMock) -> None:
        from drt.alerts.webhook import send_webhook_alert
        cfg = WebhookAlertConfig(type="webhook", url="https://x")
        # Must not raise — alert dispatch is best-effort
        send_webhook_alert(cfg, {"sync_name": "x", "error": "y"})


class TestDispatcher:
    @patch("drt.alerts.dispatcher.send_slack_alert")
    @patch("drt.alerts.dispatcher.send_webhook_alert")
    def test_dispatch_routes_by_type(
        self, mock_webhook: MagicMock, mock_slack: MagicMock
    ) -> None:
        cfg = AlertsConfig(on_failure=[
            {"type": "slack", "webhook_url": "https://x"},
            {"type": "webhook", "url": "https://y"},
        ])
        dispatch_alerts(cfg, "on_failure", context={"sync_name": "s", "error": "e"})
        mock_slack.assert_called_once()
        mock_webhook.assert_called_once()

    @patch("drt.alerts.dispatcher.send_slack_alert", side_effect=RuntimeError)
    @patch("drt.alerts.dispatcher.send_webhook_alert")
    def test_dispatch_continues_after_one_sender_fails(
        self, mock_webhook: MagicMock, mock_slack: MagicMock
    ) -> None:
        cfg = AlertsConfig(on_failure=[
            {"type": "slack", "webhook_url": "https://x"},
            {"type": "webhook", "url": "https://y"},
        ])
        dispatch_alerts(cfg, "on_failure", context={"sync_name": "s", "error": "e"})
        mock_webhook.assert_called_once()  # second sender still ran


class TestDispatchTargets:
    """Degraded conditions (#784) reuse the same per-channel dispatch."""

    @patch("drt.alerts.dispatcher.send_slack_alert")
    @patch("drt.alerts.dispatcher.send_webhook_alert")
    def test_dispatch_targets_routes_a_channel_list(
        self, mock_webhook: MagicMock, mock_slack: MagicMock
    ) -> None:
        from drt.alerts.dispatcher import dispatch_targets
        from drt.config.models import SlackAlertConfig, WebhookAlertConfig

        dispatch_targets(
            [
                SlackAlertConfig(type="slack", webhook_url="https://x"),
                WebhookAlertConfig(type="webhook", url="https://y"),
            ],
            context={"sync_name": "s", "error": "degraded — dlq_depth 5 (gt 0)"},
        )
        mock_slack.assert_called_once()
        mock_webhook.assert_called_once()

    def test_build_degraded_context_summarizes_trips(self) -> None:
        from drt.alerts.conditions import TrippedCondition
        from drt.alerts.dispatcher import build_degraded_context
        from drt.destinations.base import SyncResult

        tripped = [
            TrippedCondition("row_errors_pct", "gt", 1.0, 4.2),
            TrippedCondition("dlq_depth", "gt", 500.0, 530.0),
        ]
        ctx = build_degraded_context(
            sync_name="orders_to_pg",
            result=SyncResult(rows_extracted=1000, success=958, failed=42),
            duration_s=12.0,
            started_at="2026-07-19T00:00:00Z",
            tripped=tripped,
        )
        assert ctx["status"] == "degraded"
        # Both trips named in the one-line summary (coalesced into one message).
        assert "row_errors_pct 4.2 (gt 1.0)" in ctx["error"]
        assert "dlq_depth 530.0 (gt 500.0)" in ctx["error"]
        assert [c["metric"] for c in ctx["conditions_tripped"]] == [
            "row_errors_pct",
            "dlq_depth",
        ]


class TestOnDegradedCliSeam:
    """End-to-end at the CLI seam: _run_one evaluates conditions, coalesces one
    message per sync, and surfaces tripped conditions in --output json."""

    def _project(self, tmp_path, monkeypatch, conditions_yaml, *, dlq_lines=0):
        import yaml as _yaml

        from drt.config import credentials as creds

        monkeypatch.chdir(tmp_path)
        monkeypatch.setattr(
            creds, "load_profile", lambda *_a, **_k: creds.DuckDBProfile(type="duckdb"),
            raising=False,
        )
        (tmp_path / "drt_project.yml").write_text("name: demo\nprofile: default\n")
        (tmp_path / "syncs").mkdir()
        (tmp_path / "syncs" / "s.yml").write_text(
            "name: s\nmodel: 'SELECT 1'\n"
            "destination: {type: rest_api, url: 'https://x'}\n"
            "alerts:\n" + conditions_yaml
        )
        if dlq_lines:
            dlq = tmp_path / ".drt" / "dlq"
            dlq.mkdir(parents=True)
            (dlq / "s.jsonl").write_text(
                "\n".join(_yaml.dump({"record": i}) for i in range(dlq_lines)) + "\n"
            )
        return tmp_path

    def _patch_engine(self, monkeypatch, *, extracted, failed, duration):
        from drt.destinations.base import SyncResult
        from drt.engine import sync as sync_module

        fake = SyncResult(
            rows_extracted=extracted,
            success=extracted - failed,
            failed=failed,
            skipped=0,
            duration_seconds=duration,
        )
        monkeypatch.setattr(sync_module, "run_sync", lambda *a, **k: fake, raising=False)

    @patch("drt.alerts.dispatcher.send_slack_alert")
    def test_tripped_condition_dispatches_and_lands_in_json(
        self, mock_slack, tmp_path, monkeypatch
    ):
        import json as _json

        from typer.testing import CliRunner

        from drt.cli.main import app

        self._project(
            tmp_path,
            monkeypatch,
            "  on_degraded:\n"
            "    channels: [{type: slack, webhook_url: 'https://hooks/x'}]\n"
            "    conditions:\n"
            "      row_errors_pct: { gt: 1 }\n"
            "      dlq_depth: { gt: 0 }\n",
            dlq_lines=3,
        )
        self._patch_engine(monkeypatch, extracted=1000, failed=40, duration=5.0)
        result = CliRunner().invoke(app, ["run", "--output", "json"])
        payload = _json.loads(result.output)
        entry = payload["syncs"][0]
        metrics = {c["metric"] for c in entry["conditions_tripped"]}
        assert metrics == {"row_errors_pct", "dlq_depth"}  # dlq_depth from the 3 DLQ lines
        mock_slack.assert_called_once()  # coalesced: ONE message despite two trips

    @patch("drt.alerts.dispatcher.send_slack_alert")
    def test_healthy_sync_neither_dispatches_nor_annotates_json(
        self, mock_slack, tmp_path, monkeypatch
    ):
        import json as _json

        from typer.testing import CliRunner

        from drt.cli.main import app

        self._project(
            tmp_path,
            monkeypatch,
            "  on_degraded:\n"
            "    channels: [{type: slack, webhook_url: 'https://hooks/x'}]\n"
            "    conditions:\n      row_errors_pct: { gt: 1 }\n",
        )
        self._patch_engine(monkeypatch, extracted=1000, failed=0, duration=5.0)
        result = CliRunner().invoke(app, ["run", "--output", "json"])
        entry = _json.loads(result.output)["syncs"][0]
        assert "conditions_tripped" not in entry
        mock_slack.assert_not_called()
