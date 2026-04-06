"""Unit tests for Microsoft Teams destination.

Uses pytest-httpserver to spin up a local HTTP server — no mocking.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pytest_httpserver import HTTPServer

from drt.config.models import SyncOptions, TeamsDestinationConfig
from drt.destinations.teams import TeamsDestination

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _options(**kwargs: Any) -> SyncOptions:
    return SyncOptions(**kwargs)


def _config(webhook_url: str, **overrides: Any) -> TeamsDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "teams",
        "webhook_url": webhook_url,
        "message_template": "{{ row.name }}: {{ row.score }}",
    }
    defaults.update(overrides)
    return TeamsDestinationConfig(**defaults)


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestTeamsDestinationConfig:
    def test_valid_config(self) -> None:
        config = _config("https://outlook.office.com/webhook/test")
        assert config.type == "teams"
        assert config.adaptive_card is False

    def test_adaptive_card_flag(self) -> None:
        config = _config("https://example.com/webhook", adaptive_card=True)
        assert config.adaptive_card is True

    def test_webhook_url_env(self) -> None:
        config = TeamsDestinationConfig(
            type="teams",
            webhook_url_env="TEAMS_WEBHOOK",
            message_template="test",
        )
        assert config.webhook_url_env == "TEAMS_WEBHOOK"


# ---------------------------------------------------------------------------
# Load behavior
# ---------------------------------------------------------------------------


class TestTeamsDestinationLoad:
    def test_success_plain_text(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/webhook").respond_with_data("1", status=200)

        config = _config(httpserver.url_for("/webhook"))
        records = [
            {"name": "alice", "score": 95},
            {"name": "bob", "score": 80},
        ]
        result = TeamsDestination().load(records, config, _options())

        assert result.success == 2
        assert result.failed == 0

    def test_plain_text_payload_format(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/webhook").respond_with_data("1", status=200)

        config = _config(httpserver.url_for("/webhook"))
        TeamsDestination().load([{"name": "alice", "score": 95}], config, _options())

        # Verify payload is plain text format
        req = httpserver.log[0][0]
        body = json.loads(req.data)
        assert "text" in body
        assert "alice" in body["text"]

    def test_adaptive_card_payload_format(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/webhook").respond_with_data("1", status=200)

        card_template = json.dumps(
            {
                "type": "AdaptiveCard",
                "version": "1.4",
                "body": [{"type": "TextBlock", "text": "{{ row.name }}"}],
            }
        )
        config = _config(
            httpserver.url_for("/webhook"),
            adaptive_card=True,
            message_template=card_template,
        )
        TeamsDestination().load([{"name": "alice", "score": 95}], config, _options())

        req = httpserver.log[0][0]
        body = json.loads(req.data)
        assert body["type"] == "message"
        assert body["attachments"][0]["contentType"] == "application/vnd.microsoft.card.adaptive"

    def test_http_error_tracked(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/webhook").respond_with_data("error", status=400)

        config = _config(httpserver.url_for("/webhook"))
        result = TeamsDestination().load([{"name": "fail", "score": 0}], config, _options())

        assert result.failed == 1
        assert result.success == 0
        assert len(result.row_errors) == 1
        assert result.row_errors[0].http_status == 400

    def test_missing_webhook_raises(self) -> None:
        config = TeamsDestinationConfig(type="teams", message_template="test")
        with pytest.raises(ValueError, match="webhook_url"):
            TeamsDestination().load([{"name": "test"}], config, _options())

    def test_empty_records(self, httpserver: HTTPServer) -> None:
        config = _config(httpserver.url_for("/webhook"))
        result = TeamsDestination().load([], config, _options())

        assert result.success == 0
        assert result.failed == 0

    def test_row_error_preview_truncated(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/webhook").respond_with_data("err", status=500)

        config = _config(httpserver.url_for("/webhook"))
        big_record = {"name": "x" * 500, "score": 0}
        result = TeamsDestination().load([big_record], config, _options())

        assert len(result.row_errors[0].record_preview) <= 200
