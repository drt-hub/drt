"""Unit tests for Email SMTP destination."""

from __future__ import annotations

import smtplib
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import EmailSmtpDestinationConfig, RateLimitConfig, SyncOptions
from drt.destinations.email_smtp import EmailSmtpDestination


def _config(**overrides):
    defaults = {
        "type": "email_smtp",
        "host": "smtp.example.com",
        "port": 587,
        "sender": "noreply@example.com",
        "recipients": ["admin@example.com"],
        "subject_template": "Hello {{ row.name }}",
        "body_template": "Record: {{ row.name }}",
        "use_tls": True,
        "username": "user@example.com",
        "password": "secret",
    }
    defaults.update(overrides)
    return EmailSmtpDestinationConfig(**defaults)


def _sync_options():
    return SyncOptions(
        mode="full",
        batch_size=100,
        on_error="skip",
        rate_limit=RateLimitConfig(requests_per_second=0),
    )


@pytest.fixture
def mock_smtp():
    with patch("drt.destinations.email_smtp.smtplib.SMTP") as mock_cls:
        mock_server = MagicMock()
        mock_cls.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_cls.return_value.__exit__ = MagicMock(return_value=False)
        yield mock_cls, mock_server


class TestEmailSmtpDestination:
    def test_sends_email(self, mock_smtp):
        _, server = mock_smtp
        dest = EmailSmtpDestination()
        result = dest.load([{"name": "Alice"}], _config(), _sync_options())

        assert result.success == 1
        assert result.failed == 0
        server.starttls.assert_called_once()
        server.login.assert_called_once_with("user@example.com", "secret")
        server.send_message.assert_called_once()

    def test_subject_and_body_rendered(self, mock_smtp):
        _, server = mock_smtp
        dest = EmailSmtpDestination()
        dest.load([{"name": "Bob"}], _config(), _sync_options())

        sent_msg = server.send_message.call_args[0][0]
        assert sent_msg["Subject"] == "Hello Bob"
        assert "Bob" in sent_msg.get_payload()[0].get_payload()

    def test_recipients_joined(self, mock_smtp):
        _, server = mock_smtp
        dest = EmailSmtpDestination()
        config = _config(recipients=["a@example.com", "b@example.com"])
        dest.load([{"name": "Eve"}], config, _sync_options())

        sent_msg = server.send_message.call_args[0][0]
        assert sent_msg["To"] == "a@example.com, b@example.com"

    def test_no_starttls_when_disabled(self, mock_smtp):
        _, server = mock_smtp
        dest = EmailSmtpDestination()
        dest.load([{"name": "Carol"}], _config(use_tls=False), _sync_options())

        server.starttls.assert_not_called()
        server.login.assert_called_once()

    def test_handles_smtp_auth_error(self, mock_smtp):
        _, server = mock_smtp
        server.login.side_effect = smtplib.SMTPAuthenticationError(535, b"Bad credentials")

        dest = EmailSmtpDestination()
        result = dest.load([{"name": "Dave"}], _config(), _sync_options())

        assert result.failed == 1
        assert result.success == 0
        assert "535" in result.row_errors[0].error_message

    def test_handles_smtp_exception(self, mock_smtp):
        _, server = mock_smtp
        server.send_message.side_effect = smtplib.SMTPException("Connection lost")

        dest = EmailSmtpDestination()
        result = dest.load([{"name": "Frank"}], _config(), _sync_options())

        assert result.failed == 1
        assert result.row_errors[0].http_status is None
        assert "Connection lost" in result.row_errors[0].error_message

    def test_raises_on_missing_credentials(self):
        dest = EmailSmtpDestination()
        config = _config(username=None, username_env=None)

        with pytest.raises(ValueError, match="username"):
            dest.load([{"name": "test"}], config, _sync_options())

    def test_credentials_from_env(self, mock_smtp):
        _, server = mock_smtp
        dest = EmailSmtpDestination()
        config = _config(username=None, username_env="SMTP_USER", password=None, password_env="SMTP_PASS")

        with patch.dict("os.environ", {"SMTP_USER": "env_user", "SMTP_PASS": "env_pass"}):
            result = dest.load([{"name": "Grace"}], config, _sync_options())

        assert result.success == 1
        server.login.assert_called_once_with("env_user", "env_pass")

    def test_multiple_records(self, mock_smtp):
        _, server = mock_smtp
        dest = EmailSmtpDestination()
        records = [{"name": f"User{i}"} for i in range(4)]

        result = dest.load(records, _config(), _sync_options())

        assert result.success == 4
        assert server.send_message.call_count == 4

    def test_partial_failures_tracked(self, mock_smtp):
        _, server = mock_smtp
        server.send_message.side_effect = [
            None,
            smtplib.SMTPException("Temp failure"),
            None,
        ]

        dest = EmailSmtpDestination()
        result = dest.load(
            [{"name": "A"}, {"name": "B"}, {"name": "C"}], _config(), _sync_options()
        )

        assert result.success == 2
        assert result.failed == 1
        assert result.row_errors[0].batch_index == 1
