"""Unit tests for alert dispatch. Uses pytest-httpserver for webhook targets."""
from __future__ import annotations

from unittest.mock import MagicMock, patch

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
