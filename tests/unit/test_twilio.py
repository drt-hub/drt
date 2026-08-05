from unittest.mock import patch

import httpx
import pytest

from drt.config.models import RetryConfig, SyncOptions, TwilioDestinationConfig
from drt.destinations.twilio import TwilioDestination


class DummyResponse:
    def __init__(self, status_code=200, json_data=None, text="ok"):
        self.status_code = status_code
        self._json_data = json_data or {}
        self.text = text

    def raise_for_status(self):
        if 400 <= self.status_code < 600:
            raise httpx.HTTPStatusError(
                message="error",
                request=httpx.Request("POST", "https://test"),
                response=self,
            )

    def json(self):
        if isinstance(self._json_data, Exception):
            raise self._json_data
        return self._json_data


@pytest.fixture
def base_config():
    return TwilioDestinationConfig(
        type="twilio",
        account_sid="AC123",
        auth_token="token123",
        from_number="+1234567890",
        to_template="{{ row.phone }}",
        message_template="Hi {{ row.name }}",
    )


@pytest.fixture
def sync_options():
    return SyncOptions(
        retry=RetryConfig(max_attempts=1),
    )


# ----------------------------
# 1. SUCCESS PATH
# ----------------------------
def test_successful_send(base_config):
    records = [{"phone": "+19876543210", "name": "Alice"}]

    with patch("httpx.Client.post", return_value=DummyResponse(200, {"sid": "SM123"})):
        result = TwilioDestination().load(records, base_config, SyncOptions())

    assert result.success == 1
    assert result.failed == 0


# ----------------------------
# 2. INVALID FROM NUMBER (hard fail before loop)
# ----------------------------
def test_invalid_from_number():
    config = TwilioDestinationConfig(
        type="twilio",
        account_sid="AC123",
        auth_token="token123",
        from_number="12345",  # invalid
        to_template="{{ row.phone }}",
        message_template="Hi",
    )

    with pytest.raises(ValueError, match="Invalid from_number"):
        TwilioDestination().load([{"phone": "+1234567890"}], config, SyncOptions())


# ----------------------------
# 3. INVALID TO NUMBER (now EXPECTS FAILURE)
# ----------------------------
def test_invalid_to_number(base_config):
    records = [{"phone": "bad-number", "name": "Alice"}]

    with pytest.raises(ValueError, match="Invalid to_number"):
        TwilioDestination().load(records, base_config, SyncOptions())


# ----------------------------
# 4. HTTP ERROR (propagates via retry → HTTPStatusError)
# ----------------------------
def test_twilio_http_error(base_config):
    records = [{"phone": "+19876543210", "name": "Alice"}]

    with patch(
        "httpx.Client.post",
        return_value=DummyResponse(400, {"error_message": "Bad request"}, "Bad request"),
    ):
        with pytest.raises(httpx.HTTPStatusError):
            TwilioDestination().load(records, base_config, SyncOptions())


# ----------------------------
# 5. JSON DECODE ERROR
# ----------------------------
def test_twilio_json_decode_error(base_config):
    records = [{"phone": "+19876543210", "name": "Alice"}]

    bad_response = DummyResponse(200, json_data=Exception("bad json"), text="not json")

    with patch("httpx.Client.post", return_value=bad_response):
        with pytest.raises(ValueError, match="Invalid Twilio response"):
            TwilioDestination().load(records, base_config, SyncOptions())


# ----------------------------
# 6. RETRY PATH
# ----------------------------
def test_retry_logic_triggered(base_config):
    records = [{"phone": "+19876543210", "name": "Alice"}]

    success_response = DummyResponse(200, {"sid": "SM123"})

    with patch(
        "httpx.Client.post",
        side_effect=[
            DummyResponse(500, text="server error"),
            success_response,
        ],
    ):
        result = TwilioDestination().load(records, base_config, SyncOptions())

    assert result.success == 1