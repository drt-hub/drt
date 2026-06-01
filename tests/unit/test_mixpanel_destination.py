"""Unit tests for the Mixpanel destination (people_set + import_events)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from drt.config.models import (
    MixpanelDestinationConfig,
    RateLimitConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.mixpanel import MixpanelDestination


def _people_config(**overrides: object) -> MixpanelDestinationConfig:
    data: dict[str, object] = {
        "type": "mixpanel",
        "endpoint": "people_set",
        "project_token": "test-token",
        "distinct_id_field": "user_id",
    }
    data.update(overrides)
    return MixpanelDestinationConfig(**data)


def _event_config(**overrides: object) -> MixpanelDestinationConfig:
    data: dict[str, object] = {
        "type": "mixpanel",
        "endpoint": "import_events",
        "project_id": "1234567",
        "service_account_username": "svc",
        "service_account_secret": "secret",
        "distinct_id_field": "user_id",
        "event_name": "signup_completed",
    }
    data.update(overrides)
    return MixpanelDestinationConfig(**data)


def _options(**overrides: object) -> SyncOptions:
    data: dict[str, object] = {
        "rate_limit": RateLimitConfig(requests_per_second=0),
        "retry": RetryConfig(max_attempts=1, initial_backoff=0.0, backoff_multiplier=1.0),
        "on_error": "skip",
    }
    data.update(overrides)
    return SyncOptions(**data)


def _response(status_code: int = 200, text: str = "1") -> MagicMock:
    response = MagicMock()
    response.status_code = status_code
    response.text = text
    response.raise_for_status.return_value = None
    return response


def _http_error(status_code: int, text: str) -> httpx.HTTPStatusError:
    response = httpx.Response(
        status_code=status_code,
        text=text,
        request=httpx.Request("POST", "https://api.mixpanel.com/engage"),
    )
    return httpx.HTTPStatusError(
        message=f"HTTP {status_code}", request=response.request, response=response
    )


def _records(n: int) -> list[dict[str, Any]]:
    return [{"user_id": f"u{i}", "plan": "pro", "source": "web"} for i in range(n)]


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


class TestConfigValidation:
    def test_people_requires_token(self) -> None:
        with pytest.raises(ValueError, match="project_token"):
            MixpanelDestinationConfig(
                type="mixpanel", endpoint="people_set", project_token_env=None
            )

    def test_import_requires_project_id(self) -> None:
        with pytest.raises(ValueError, match="project_id"):
            MixpanelDestinationConfig(
                type="mixpanel",
                endpoint="import_events",
                service_account_username="u",
                service_account_secret="s",
                event_name="x",
            )

    def test_import_requires_event_name(self) -> None:
        with pytest.raises(ValueError, match="event_name"):
            MixpanelDestinationConfig(
                type="mixpanel",
                endpoint="import_events",
                project_id="1",
                service_account_username="u",
                service_account_secret="s",
            )

    def test_batch_size_clamped_to_2000(self) -> None:
        assert _people_config(batch_size=9000).batch_size == 2000
        assert _people_config(batch_size=0).batch_size == 1


# ---------------------------------------------------------------------------
# people_set (/engage)
# ---------------------------------------------------------------------------


class TestPeopleSet:
    def test_profile_payload_shape(self) -> None:
        config = _people_config()
        with patch("drt.destinations.mixpanel.httpx.Client") as mock_cls:
            client = MagicMock()
            mock_cls.return_value.__enter__.return_value = client
            client.post.return_value = _response()

            result = MixpanelDestination().load(_records(2), config, _options())

        assert result.success == 2
        assert result.failed == 0
        _, kwargs = client.post.call_args
        url = client.post.call_args[0][0] if client.post.call_args[0] else kwargs.get("url")
        assert url == "https://api.mixpanel.com/engage"
        # people_set carries auth in the record ($token), so no HTTP auth is sent
        assert "auth" not in kwargs
        body = kwargs["json"]
        assert body[0]["$token"] == "test-token"
        assert body[0]["$distinct_id"] == "u0"
        assert body[0]["$set"] == {"plan": "pro", "source": "web"}
        assert "user_id" not in body[0]["$set"]

    def test_eu_residency_host(self) -> None:
        config = _people_config(region="eu")
        with patch("drt.destinations.mixpanel.httpx.Client") as mock_cls:
            client = MagicMock()
            mock_cls.return_value.__enter__.return_value = client
            client.post.return_value = _response()

            MixpanelDestination().load(_records(1), config, _options())

        url = client.post.call_args[0][0]
        assert url == "https://api-eu.mixpanel.com/engage"

    def test_missing_distinct_id_is_row_error(self) -> None:
        config = _people_config()
        result = MixpanelDestination().load([{"plan": "pro"}], config, _options())
        assert result.success == 0
        assert result.failed == 1
        assert "distinct_id" in result.row_errors[0].error_message


# ---------------------------------------------------------------------------
# import_events (/import)
# ---------------------------------------------------------------------------


class TestImportEvents:
    def test_event_payload_shape(self) -> None:
        config = _event_config()
        with patch("drt.destinations.mixpanel.httpx.Client") as mock_cls:
            client = MagicMock()
            mock_cls.return_value.__enter__.return_value = client
            client.post.return_value = _response()

            result = MixpanelDestination().load(_records(1), config, _options())

        assert result.success == 1
        _, kwargs = client.post.call_args
        url = client.post.call_args[0][0]
        assert url == "https://api.mixpanel.com/import"
        assert kwargs["auth"] == ("svc", "secret")
        assert kwargs["params"] == {"project_id": "1234567"}
        event = kwargs["json"][0]
        assert event["event"] == "signup_completed"
        props = event["properties"]
        assert props["distinct_id"] == "u0"
        assert "time" in props
        assert "$insert_id" in props

    def test_insert_id_is_deterministic(self) -> None:
        config = _event_config(time_field="event_time")
        rows = [{"user_id": "u1", "event_time": 1700000000, "plan": "pro"}]
        captured: list[Any] = []

        def run() -> None:
            with patch("drt.destinations.mixpanel.httpx.Client") as mock_cls:
                client = MagicMock()
                mock_cls.return_value.__enter__.return_value = client
                client.post.return_value = _response()
                MixpanelDestination().load(rows, config, _options())
                captured.append(client.post.call_args[1]["json"][0]["properties"]["$insert_id"])

        run()
        run()
        assert captured[0] == captured[1], "same row must yield the same $insert_id"

    def test_time_field_used(self) -> None:
        config = _event_config(time_field="event_time")
        rows = [{"user_id": "u1", "event_time": 1700000000}]
        with patch("drt.destinations.mixpanel.httpx.Client") as mock_cls:
            client = MagicMock()
            mock_cls.return_value.__enter__.return_value = client
            client.post.return_value = _response()
            MixpanelDestination().load(rows, config, _options())

        assert client.post.call_args[1]["json"][0]["properties"]["time"] == 1700000000


# ---------------------------------------------------------------------------
# Batching + error handling (shared)
# ---------------------------------------------------------------------------


class TestBatchingAndErrors:
    def test_batch_size_splits_requests(self) -> None:
        config = _people_config(batch_size=2)
        with patch("drt.destinations.mixpanel.httpx.Client") as mock_cls:
            client = MagicMock()
            mock_cls.return_value.__enter__.return_value = client
            client.post.return_value = _response()

            result = MixpanelDestination().load(_records(5), config, _options())

        assert result.success == 5
        assert client.post.call_count == 3  # ceil(5/2)

    def test_http_error_records_row_errors(self) -> None:
        config = _people_config(batch_size=1)
        with patch("drt.destinations.mixpanel.httpx.Client") as mock_cls:
            client = MagicMock()
            mock_cls.return_value.__enter__.return_value = client
            client.post.side_effect = _http_error(400, "bad request")

            result = MixpanelDestination().load(_records(2), config, _options())

        assert result.success == 0
        assert result.failed == 2
        assert result.row_errors[0].http_status == 400

    def test_empty_records(self) -> None:
        result = MixpanelDestination().load([], _people_config(), _options())
        assert result.success == 0
        assert result.failed == 0


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
