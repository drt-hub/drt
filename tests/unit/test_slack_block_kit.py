"""Tests for Slack Block Kit payloads."""

from __future__ import annotations

import json

from pytest_httpserver import HTTPServer

from drt.config.models import SlackDestinationConfig, SyncOptions
from drt.destinations.slack import SlackDestination


def _options() -> SyncOptions:
    return SyncOptions()


# ---------------------------------------------------------------------------
# Block Kit payload structure
# ---------------------------------------------------------------------------


class TestBlockKitPayload:
    def test_multiple_blocks(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/webhook").respond_with_data("ok", status=200)
        template = json.dumps(
            {
                "blocks": [
                    {"type": "section", "text": {"type": "mrkdwn", "text": "{{ row.title }}"}},
                    {"type": "divider"},
                    {
                        "type": "context",
                        "elements": [{"type": "mrkdwn", "text": "{{ row.footer }}"}],
                    },
                ]
            }
        )
        config = SlackDestinationConfig(
            type="slack",
            webhook_url=httpserver.url_for("/webhook"),
            block_kit=True,
            message_template=template,
        )
        result = SlackDestination().load(
            [{"title": "Hello", "footer": "by drt"}], config, _options()
        )
        assert result.success == 1
        assert result.failed == 0

    def test_plain_text_type(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/webhook").respond_with_data("ok", status=200)
        template = json.dumps(
            {
                "blocks": [
                    {"type": "section", "text": {"type": "plain_text", "text": "{{ row.msg }}"}},
                ]
            }
        )
        config = SlackDestinationConfig(
            type="slack",
            webhook_url=httpserver.url_for("/webhook"),
            block_kit=True,
            message_template=template,
        )
        result = SlackDestination().load([{"msg": "plain"}], config, _options())
        assert result.success == 1

    def test_payload_sent_as_json(self, httpserver: HTTPServer) -> None:
        """Verify the actual JSON sent to the webhook."""
        httpserver.expect_request("/webhook").respond_with_data("ok", status=200)
        template = json.dumps(
            {"blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "{{ row.v }}"}}]}
        )
        config = SlackDestinationConfig(
            type="slack",
            webhook_url=httpserver.url_for("/webhook"),
            block_kit=True,
            message_template=template,
        )
        SlackDestination().load([{"v": "check"}], config, _options())

        req = httpserver.log[0][0]
        body = json.loads(req.data)
        assert "blocks" in body
        assert body["blocks"][0]["text"]["text"] == "check"

    def test_multiple_records(self, httpserver: HTTPServer) -> None:
        for _ in range(3):
            httpserver.expect_request("/webhook").respond_with_data("ok", status=200)
        template = json.dumps(
            {"blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": "{{ row.n }}"}}]}
        )
        config = SlackDestinationConfig(
            type="slack",
            webhook_url=httpserver.url_for("/webhook"),
            block_kit=True,
            message_template=template,
        )
        records = [{"n": "one"}, {"n": "two"}, {"n": "three"}]
        result = SlackDestination().load(records, config, _options())
        assert result.success == 3
        assert result.failed == 0


# ---------------------------------------------------------------------------
# Block Kit error cases
# ---------------------------------------------------------------------------


class TestBlockKitErrors:
    def test_invalid_json_template(self, httpserver: HTTPServer) -> None:
        """block_kit=True with template that renders to invalid JSON."""
        config = SlackDestinationConfig(
            type="slack",
            webhook_url=httpserver.url_for("/webhook"),
            block_kit=True,
            message_template="{not valid json: {{ row.x }}",
        )
        opts = SyncOptions(on_error="skip")
        result = SlackDestination().load([{"x": "val"}], config, opts)
        assert result.failed == 1
        assert result.success == 0
        assert len(result.row_errors) == 1

    def test_missing_template_field(self, httpserver: HTTPServer) -> None:
        """Template references a field that doesn't exist in the record."""
        template = json.dumps(
            {
                "blocks": [
                    {"type": "section", "text": {"type": "mrkdwn", "text": "{{ row.missing }}"}}
                ]
            }
        )
        config = SlackDestinationConfig(
            type="slack",
            webhook_url=httpserver.url_for("/webhook"),
            block_kit=True,
            message_template=template,
        )
        opts = SyncOptions(on_error="skip")
        result = SlackDestination().load([{"other": "val"}], config, opts)
        assert result.failed == 1
        assert len(result.row_errors) == 1

    def test_block_kit_false_with_json_template(self, httpserver: HTTPServer) -> None:
        """block_kit=False wraps JSON string in {"text": ...} instead of parsing."""
        httpserver.expect_request("/webhook").respond_with_data("ok", status=200)
        config = SlackDestinationConfig(
            type="slack",
            webhook_url=httpserver.url_for("/webhook"),
            block_kit=False,
            message_template='{"blocks": []}',
        )
        result = SlackDestination().load([{"x": 1}], config, _options())
        assert result.success == 1

        req = httpserver.log[0][0]
        body = json.loads(req.data)
        # Should be wrapped as plain text, not parsed as Block Kit
        assert "text" in body
        assert body["text"] == '{"blocks": []}'

    def test_numeric_and_boolean_values(self, httpserver: HTTPServer) -> None:
        """Verify numeric and boolean values render correctly in Block Kit."""
        httpserver.expect_request("/webhook").respond_with_data("ok", status=200)
        template = json.dumps(
            {
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": "count={{ row.count }} active={{ row.active }}",
                        },
                    }
                ]
            }
        )
        config = SlackDestinationConfig(
            type="slack",
            webhook_url=httpserver.url_for("/webhook"),
            block_kit=True,
            message_template=template,
        )
        result = SlackDestination().load(
            [{"count": 42, "active": True}], config, _options()
        )
        assert result.success == 1

        req = httpserver.log[0][0]
        body = json.loads(req.data)
        assert "42" in body["blocks"][0]["text"]["text"]
        assert "True" in body["blocks"][0]["text"]["text"]
