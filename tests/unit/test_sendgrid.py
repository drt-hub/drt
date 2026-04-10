"""Unit tests for SendGrid destination."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from drt.config.models import (
    SendGridDestinationConfig,
    RateLimitConfig,
    SyncOptions,
    BearerAuth,
)
from drt.destinations.sendgrid import SendGridDestination


def _config(**overrides):
    defaults = {
        "type": "sendgrid",
        "from_email": "noreply@example.com",
        "from_name": "My App",
        "subject_template": "Hello {{ row.first_name }}",
        "body_template": "Welcome {{ row.first_name }}",
        "to_email_field": "email",
        "list_ids": None,
        "auth": BearerAuth(type="bearer", token="test-api-key"),
    }
    defaults.update(overrides)
    return SendGridDestinationConfig(**defaults)


def _sync_options():
    return SyncOptions(
        mode="full",
        batch_size=100,
        on_error="skip",
        rate_limit=RateLimitConfig(requests_per_second=0),
    )


@pytest.fixture
def mock_sendgrid_client():
    with patch("drt.destinations.sendgrid.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()

        mock_client.post.return_value = mock_response

        yield mock_client, mock_response


class TestSendGridDestination:
    def test_sends_email_success(self, mock_sendgrid_client):
        mock_client, _ = mock_sendgrid_client

        dest = SendGridDestination()
        result = dest.load(
            [{"first_name": "Alice", "email": "alice@example.com"}],
            _config(),
            _sync_options(),
        )

        assert result.success == 1
        assert result.failed == 0

        mock_client.post.assert_called_once()
        payload = mock_client.post.call_args[1]["json"]

        assert payload["personalizations"][0]["to"][0]["email"] == "alice@example.com"
        assert payload["personalizations"][0]["subject"] == "Hello Alice"
        assert payload["content"][0]["value"] == "Welcome Alice"

    def test_auth_error_missing_api_key(self):
        dest = SendGridDestination()

        config = _config(auth=BearerAuth(type="bearer", token=None, token_env=None))

        with pytest.raises(ValueError, match="missing API key"):
            dest.load(
                [{"first_name": "Alice", "email": "alice@example.com"}],
                config,
                _sync_options(),
            )

    def test_template_error(self, mock_sendgrid_client):
        with patch(
            "drt.destinations.sendgrid.render_template",
            side_effect=Exception("Template failed"),
        ):
            dest = SendGridDestination()

            result = dest.load(
                [{"first_name": "Alice", "email": "alice@example.com"}],
                _config(),
                _sync_options(),
            )

        assert result.success == 0
        assert result.failed == 1
        assert "Template failed" in result.row_errors[0].error_message

    def test_rate_limit_called_per_record(self, mock_sendgrid_client):
        with patch("drt.destinations.sendgrid.RateLimiter") as mock_rl_cls:
            mock_rl = MagicMock()
            mock_rl_cls.return_value = mock_rl

            dest = SendGridDestination()
            records = [
                {"first_name": f"user{i}", "email": f"user{i}@example.com"}
                for i in range(3)
            ]

            dest.load(records, _config(), _sync_options())

            assert mock_rl.acquire.call_count == 3

    def test_retry_on_failure(self, mock_sendgrid_client):
        mock_client, mock_response = mock_sendgrid_client

        error_response = MagicMock()
        error_response.status_code = 500
        error_response.text = "Server Error"

        def fail_once_then_succeed(*args, **kwargs):
            if not hasattr(fail_once_then_succeed, "called"):
                fail_once_then_succeed.called = True
                raise httpx.HTTPStatusError(
                    "500", request=MagicMock(), response=error_response
                )
            return mock_response

        mock_client.post.side_effect = fail_once_then_succeed

        dest = SendGridDestination()
        result = dest.load(
            [{"first_name": "Alice", "email": "alice@example.com"}],
            _config(),
            _sync_options(),
        )

        assert result.success == 1
        assert result.failed == 0
        assert mock_client.post.call_count >= 2

    def test_missing_email_field_fails(self, mock_sendgrid_client):
        dest = SendGridDestination()

        result = dest.load(
            [{"first_name": "Alice"}],  # no email
            _config(),
            _sync_options(),
        )

        assert result.success == 0
        assert result.failed == 1
        assert "missing 'email'" in result.row_errors[0].error_message
        