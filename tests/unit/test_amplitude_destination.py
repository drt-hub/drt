"""Unit tests for Amplitude destination."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import httpx
import pytest
from pytest_httpserver import HTTPServer

from drt.config.models import (
    AmplitudeDestinationConfig,
    RateLimitConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.amplitude import AmplitudeDestination


def _config(**overrides: object) -> AmplitudeDestinationConfig:
    data: dict[str, object] = {
        "type": "amplitude",
        "api_key": "test-api-key",
        "endpoint": "identify",
    }
    data.update(overrides)
    return AmplitudeDestinationConfig(**data)


def _options(**overrides: object) -> SyncOptions:
    data: dict[str, object] = {
        "rate_limit": RateLimitConfig(requests_per_second=0),
        "retry": RetryConfig(max_attempts=1, initial_backoff=0.0, backoff_multiplier=1.0),
        "on_error": "skip",
    }
    data.update(overrides)
    return SyncOptions(**data)


def _response(status_code: int = 200, text: str = "{}") -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.text = text
    response.raise_for_status.return_value = None
    return response


def _http_error(status_code: int, text: str) -> httpx.HTTPStatusError:
    response = httpx.Response(
        status_code=status_code,
        text=text,
        request=httpx.Request("POST", "https://api2.amplitude.com/identify"),
    )
    return httpx.HTTPStatusError(
        message=f"HTTP {status_code}",
        request=response.request,
        response=response,
    )


class TestAmplitudeDestinationConfig:
    def test_valid_identify_config(self) -> None:
        config = _config()
        assert config.type == "amplitude"
        assert config.endpoint == "identify"
        assert config.batch_size == 1000

    def test_valid_event_config(self) -> None:
        config = _config(endpoint="event", event_type="sync_event")
        assert config.event_type == "sync_event"

    def test_missing_api_key_raises(self) -> None:
        with pytest.raises(ValueError, match="api_key"):
            AmplitudeDestinationConfig(type="amplitude", api_key_env=None)

    def test_event_endpoint_requires_event_type(self) -> None:
        with pytest.raises(ValueError, match="event_type"):
            _config(endpoint="event")

    def test_batch_size_clamped(self) -> None:
        config = AmplitudeDestinationConfig(
            type="amplitude",
            api_key="key",
            batch_size=5000,
        )
        assert config.batch_size == 1000

        config_too_small = AmplitudeDestinationConfig(
            type="amplitude",
            api_key="key",
            batch_size=0,
        )
        assert config_too_small.batch_size == 1


class TestAmplitudeIdentifyLoad:
    def test_identify_success(self) -> None:
        records = [
            {"user_id": "user-10001", "plan": "enterprise", "ltv": 1200},
            {"user_id": "user-10002", "plan": "starter"},
        ]
        config = _config(endpoint="identify")

        with patch("drt.destinations.amplitude.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = _response()

            result = AmplitudeDestination().load(records, config, _options())

        assert result.success == 2
        assert result.failed == 0
        mock_client.post.assert_called_once()
        _, kwargs = mock_client.post.call_args
        assert kwargs["data"]["api_key"] == "test-api-key"
        identifications = json.loads(kwargs["data"]["identification"])
        assert identifications[0]["user_id"] == "user-10001"
        assert identifications[0]["user_properties"] == {"plan": "enterprise", "ltv": 1200}
        assert "plan" not in identifications[0]

    def test_properties_template_merge(self) -> None:
        record = {"user_id": "user-10001", "plan": "legacy", "ltv": 99}
        config = _config(
            properties_template='{"plan": "{{ row.plan }}", "ltv_tier": "high"}',
        )

        with patch("drt.destinations.amplitude.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = _response()

            result = AmplitudeDestination().load([record], config, _options())

        assert result.success == 1
        _, kwargs = mock_client.post.call_args
        identifications = json.loads(kwargs["data"]["identification"])
        assert identifications[0]["user_properties"] == {
            "plan": "legacy",
            "ltv": 99,
            "ltv_tier": "high",
        }

    def test_missing_user_and_device_id(self) -> None:
        config = _config()
        result = AmplitudeDestination().load([{"plan": "free"}], config, _options())
        assert result.success == 0
        assert result.failed == 1
        assert "user_id" in result.row_errors[0].error_message

    def test_identify_batch_split(self) -> None:
        records = [{"user_id": f"user-{i:05d}", "score": i} for i in range(1001)]
        config = _config(batch_size=1000)

        with patch("drt.destinations.amplitude.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = _response()

            result = AmplitudeDestination().load(records, config, _options())

        assert result.success == 1001
        assert mock_client.post.call_count == 2
        calls = mock_client.post.call_args_list
        first_batch = json.loads(calls[0].kwargs["data"]["identification"])
        second_batch = json.loads(calls[1].kwargs["data"]["identification"])
        assert len(first_batch) == 1000
        assert len(second_batch) == 1

    def test_identify_httpserver(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/identify").respond_with_data("{}", status=200)
        config = _config(
            api_key="live-key",
            endpoint="identify",
        )
        records = [{"user_id": "user-10001", "segment": "vip"}]

        with patch(
            "drt.destinations.amplitude._AMPLITUDE_HOSTS",
            {"default": httpserver.url_for(""), "eu": httpserver.url_for("")},
        ):
            result = AmplitudeDestination().load(records, config, _options())

        assert result.success == 1
        assert result.failed == 0


class TestAmplitudeEventLoad:
    def test_event_success_with_field(self) -> None:
        records = [
            {
                "user_id": "user-10001",
                "event_name": "feature_used",
                "feature": "export",
            },
        ]
        config = _config(endpoint="event", event_type_field="event_name")

        with patch("drt.destinations.amplitude.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = _response()

            result = AmplitudeDestination().load(records, config, _options())

        assert result.success == 1
        _, kwargs = mock_client.post.call_args
        body = kwargs["json"]
        assert body["api_key"] == "test-api-key"
        assert body["events"][0]["event_type"] == "feature_used"
        assert body["events"][0]["event_properties"] == {"feature": "export"}
        assert "insert_id" in body["events"][0]

    def test_event_constant_event_type(self) -> None:
        records = [{"user_id": "user-10001", "value": 1}]
        config = _config(endpoint="event", event_type="warehouse_sync")

        with patch("drt.destinations.amplitude.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = _response()

            result = AmplitudeDestination().load(records, config, _options())

        assert result.success == 1
        body = mock_client.post.call_args.kwargs["json"]
        assert body["events"][0]["event_type"] == "warehouse_sync"

    def test_event_missing_event_type_row_error(self) -> None:
        records = [{"user_id": "user-10001"}]
        config = _config(endpoint="event", event_type_field="event_name")

        result = AmplitudeDestination().load(records, config, _options())
        assert result.failed == 1
        assert "event_type" in result.row_errors[0].error_message

    def test_event_min_id_length_options(self) -> None:
        records = [{"user_id": "abc", "event_name": "click"}]
        config = _config(
            endpoint="event",
            event_type_field="event_name",
            min_id_length=1,
        )

        with patch("drt.destinations.amplitude.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = _response()

            AmplitudeDestination().load(records, config, _options())

        body = mock_client.post.call_args.kwargs["json"]
        assert body["options"] == {"min_id_length": 1}

    def test_http_error_records_row_errors(self) -> None:
        records = [{"user_id": "user-10001", "plan": "pro"}]
        config = _config()

        with patch("drt.destinations.amplitude.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.side_effect = _http_error(400, "bad request")

            result = AmplitudeDestination().load(records, config, _options())

        assert result.success == 0
        assert result.failed == 1
        assert result.row_errors[0].http_status == 400

    def test_on_error_fail_stops_after_batch_failure(self) -> None:
        records = [
            {"user_id": "user-10001", "plan": "a"},
            {"user_id": "user-10002", "plan": "b"},
        ]
        config = _config(batch_size=1)

        with patch("drt.destinations.amplitude.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.side_effect = _http_error(500, "server error")

            result = AmplitudeDestination().load(
                records,
                config,
                _options(on_error="fail"),
            )

        assert result.failed == 1
        assert mock_client.post.call_count == 1

    def test_empty_records(self) -> None:
        result = AmplitudeDestination().load([], _config(), _options())
        assert result.success == 0
        assert result.failed == 0

    def test_missing_api_key_at_load(self) -> None:
        config = AmplitudeDestinationConfig(
            type="amplitude",
            api_key_env="MISSING_AMPLITUDE_KEY",
        )
        with pytest.raises(ValueError, match="api_key"):
            AmplitudeDestination().load(
                [{"user_id": "user-10001"}],
                config,
                _options(),
            )
