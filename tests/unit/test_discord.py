"""Unit tests for Discord webhook destination."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from drt.config.models import DiscordDestinationConfig, RateLimitConfig, SyncOptions
from drt.destinations.discord import DiscordDestination


def _config(**overrides):
    defaults = {
        "type": "discord",
        "webhook_url": "https://discord.com/api/webhooks/test",
        "message_template": "Hello {{ row.name }}",
        "embeds": False,
    }
    defaults.update(overrides)
    return DiscordDestinationConfig(**defaults)


def _sync_options():
    return SyncOptions(
        mode="full",
        batch_size=100,
        on_error="skip",
        rate_limit=RateLimitConfig(requests_per_second=0),
    )


@pytest.fixture
def mock_discord_client():
    with patch("drt.destinations.discord.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_client.post.return_value = mock_response
        yield mock_client, mock_response


class TestDiscordDestination:
    def test_sends_plain_text_message(self, mock_discord_client):
        mock_client, _ = mock_discord_client
        dest = DiscordDestination()
        result = dest.load([{"name": "Alice"}], _config(), _sync_options())

        assert result.success == 1
        assert result.failed == 0
        mock_client.post.assert_called_once()
        assert mock_client.post.call_args[1]["json"] == {"content": "Hello Alice"}

    def test_sends_embed_message(self, mock_discord_client):
        mock_client, _ = mock_discord_client
        template = '{"embeds": [{"title": "{{ row.title }}"}]}'
        dest = DiscordDestination()
        result = dest.load(
            [{"title": "Test Embed"}],
            _config(embeds=True, message_template=template),
            _sync_options(),
        )

        assert result.success == 1
        assert mock_client.post.call_args[1]["json"]["embeds"][0]["title"] == "Test Embed"

    def test_handles_http_error(self, mock_discord_client):
        mock_client, mock_response = mock_discord_client
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_response.raise_for_status = MagicMock(
            side_effect=httpx.HTTPStatusError("401", request=MagicMock(), response=mock_response)
        )

        dest = DiscordDestination()
        result = dest.load([{"name": "Bob"}], _config(), _sync_options())

        assert result.failed == 1
        assert result.success == 0
        assert result.row_errors[0].http_status == 401

    def test_raises_on_missing_webhook_url(self):
        dest = DiscordDestination()
        config = _config(webhook_url=None, webhook_url_env=None)

        with pytest.raises(ValueError, match="provide 'webhook_url'"):
            dest.load([{"name": "test"}], config, _sync_options())

    def test_webhook_url_from_env(self, mock_discord_client):
        mock_client, _ = mock_discord_client
        dest = DiscordDestination()
        config = _config(webhook_url=None, webhook_url_env="DISCORD_URL")

        with patch.dict("os.environ", {"DISCORD_URL": "https://discord.com/api/webhooks/env"}):
            result = dest.load([{"name": "Eve"}], config, _sync_options())

        assert result.success == 1
        assert mock_client.post.call_args[0][0] == "https://discord.com/api/webhooks/env"

    def test_multiple_records(self, mock_discord_client):
        mock_client, _ = mock_discord_client
        dest = DiscordDestination()
        records = [{"name": f"User{i}"} for i in range(5)]

        result = dest.load(records, _config(), _sync_options())

        assert result.success == 5
        assert mock_client.post.call_count == 5
