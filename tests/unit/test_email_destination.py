import pytest
from unittest.mock import MagicMock, patch

from drt.destinations.email_smtp import EmailDestination
from drt.config.models import EmailDestinationConfig, SyncOptions, RetryConfig, RateLimitConfig


def test_email_retry_then_success():
    mock_smtp = MagicMock()

    mock_smtp.send_message.side_effect = [
        Exception("fail"),
        None,
    ]

    with patch("smtplib.SMTP", return_value=mock_smtp):
        destination = EmailDestination()

        config = EmailDestinationConfig(
            type="email",
            from_email="test@gmail.com",
            password="123",
            to_email="target@gmail.com",
        )

        sync_options = SyncOptions(
            rate_limit=RateLimitConfig(requests_per_second=100),
            retry=RetryConfig(max_attempts=2),
        )

        records = [{"name": "Saprol"}]

        result = destination.load(records, config, sync_options)

        assert result.success == 1