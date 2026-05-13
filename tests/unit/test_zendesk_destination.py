"""Unit tests for Zendesk destination."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from drt.config.models import RateLimitConfig, RetryConfig, SyncOptions, ZendeskDestinationConfig
from drt.destinations.zendesk import ZendeskDestination


def _config(**overrides: object) -> ZendeskDestinationConfig:
    data: dict[str, object] = {
        "type": "zendesk",
        "subdomain": "demo",
        "email": "bot@example.com",
        "api_token": "token-123",
    }
    data.update(overrides)
    return ZendeskDestinationConfig(**data)


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
        request=httpx.Request("POST", "https://demo.zendesk.com/api/v2/test"),
    )
    return httpx.HTTPStatusError(
        message=f"HTTP {status_code}",
        request=response.request,
        response=response,
    )


class TestZendeskDestination:
    def test_user_bulk_upsert_success(self) -> None:
        records = [
            {
                "zendesk_user_id": 101,
                "email": "alice@example.com",
                "name": "Alice",
                "health_score": 91,
                "user_fields": {"segment": "strategic"},
            },
            {
                "zendesk_user_id": 102,
                "email": "bob@example.com",
                "name": "Bob",
                "health_score": 82,
            },
        ]
        config = _config(
            object="user",
            id_field="zendesk_user_id",
            custom_fields_template='{"health_score": "{{ row.health_score }}"}',
        )

        with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = _response()

            result = ZendeskDestination().load(records, config, _options())

        assert result.success == 2
        assert result.failed == 0
        mock_client.post.assert_called_once()
        args, kwargs = mock_client.post.call_args
        assert args[0] == "https://demo.zendesk.com/api/v2/users/create_or_update_many.json"
        assert kwargs["json"]["users"][0]["id"] == 101
        assert "zendesk_user_id" not in kwargs["json"]["users"][0]
        assert kwargs["json"]["users"][0]["user_fields"] == {
            "health_score": "91",
            "segment": "strategic",
        }

    def test_user_bulk_splits_batches_at_zendesk_limit(self) -> None:
        records = [
            {"external_id": f"user-{index}", "email": f"user-{index}@example.com"}
            for index in range(101)
        ]

        with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = _response()

            result = ZendeskDestination().load(records, _config(), _options())

        assert result.success == 101
        assert result.failed == 0
        assert mock_client.post.call_count == 2
        first_call = mock_client.post.call_args_list[0]
        second_call = mock_client.post.call_args_list[1]
        assert len(first_call.kwargs["json"]["users"]) == 100
        assert len(second_call.kwargs["json"]["users"]) == 1

    def test_organization_upsert_success(self) -> None:
        record = {
            "zendesk_organization_id": 555,
            "name": "Acme Ltd",
            "tier": "enterprise",
        }
        config = _config(
            object="organization",
            id_field="zendesk_organization_id",
            custom_fields_template='{"plan_tier": "{{ row.tier }}"}',
        )

        with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = _response()

            result = ZendeskDestination().load([record], config, _options())

        assert result.success == 1
        assert result.failed == 0
        args, kwargs = mock_client.post.call_args
        assert args[0] == "https://demo.zendesk.com/api/v2/organizations/create_or_update.json"
        organization = kwargs["json"]["organization"]
        assert organization["id"] == 555
        assert organization["organization_fields"] == {"plan_tier": "enterprise"}
        assert "zendesk_organization_id" not in organization

    def test_env_credentials_are_resolved(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("ZENDESK_SUBDOMAIN", "envdemo")
        monkeypatch.setenv("ZENDESK_EMAIL", "envbot@example.com")
        monkeypatch.setenv("ZENDESK_API_TOKEN", "env-token")
        config = _config(
            subdomain=None,
            email=None,
            api_token=None,
            subdomain_env="ZENDESK_SUBDOMAIN",
            email_env="ZENDESK_EMAIL",
            api_token_env="ZENDESK_API_TOKEN",
        )

        with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = _response()

            result = ZendeskDestination().load([{"email": "alice@example.com"}], config, _options())

        assert result.success == 1
        args, _ = mock_client.post.call_args
        assert args[0] == "https://envdemo.zendesk.com/api/v2/users/create_or_update_many.json"

    def test_missing_credentials_raise(self) -> None:
        config = _config(subdomain=None, email=None, api_token=None)

        with pytest.raises(ValueError, match="ZENDESK_SUBDOMAIN"):
            ZendeskDestination().load([{"email": "alice@example.com"}], config, _options())

    def test_custom_fields_template_error_records_row_error(self) -> None:
        config = _config(custom_fields_template='{"missing": "{{ row.missing }}"}')

        with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client

            result = ZendeskDestination().load([{"email": "alice@example.com"}], config, _options())

        assert result.success == 0
        assert result.failed == 1
        assert "Template error" in result.row_errors[0].error_message
        mock_client.post.assert_not_called()

    def test_custom_fields_template_must_render_json_object(self) -> None:
        config = _config(custom_fields_template='["not", "an", "object"]')

        with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client

            result = ZendeskDestination().load([{"email": "alice@example.com"}], config, _options())

        assert result.failed == 1
        assert "must render a JSON object" in result.row_errors[0].error_message
        mock_client.post.assert_not_called()

    def test_user_template_error_with_on_error_fail_stops_before_post(self) -> None:
        config = _config(custom_fields_template='{"missing": "{{ row.missing }}"}')

        with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client

            result = ZendeskDestination().load(
                [{"email": "alice@example.com"}, {"email": "bob@example.com"}],
                config,
                _options(on_error="fail"),
            )

        assert result.failed == 1
        mock_client.post.assert_not_called()

    def test_http_error_marks_each_row_in_failed_user_batch(self) -> None:
        records = [
            {"email": "alice@example.com"},
            {"email": "bob@example.com"},
        ]

        with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.side_effect = _http_error(429, "rate limited")

            result = ZendeskDestination().load(records, _config(), _options())

        assert result.success == 0
        assert result.failed == 2
        assert [error.http_status for error in result.row_errors] == [429, 429]

    def test_on_error_fail_stops_after_first_user_batch(self) -> None:
        records = [
            {"external_id": f"user-{index}", "email": f"user-{index}@example.com"}
            for index in range(101)
        ]

        with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.side_effect = _http_error(400, "bad request")

            result = ZendeskDestination().load(
                records,
                _config(),
                _options(on_error="fail"),
            )

        assert result.success == 0
        assert result.failed == 100
        assert mock_client.post.call_count == 1

    def test_generic_user_batch_error_marks_batch_failed(self) -> None:
        records = [
            {"external_id": f"user-{index}", "email": f"user-{index}@example.com"}
            for index in range(101)
        ]

        with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            response = _response()
            response.raise_for_status.side_effect = RuntimeError("boom")
            mock_client.post.return_value = response

            result = ZendeskDestination().load(
                records,
                _config(),
                _options(on_error="fail"),
            )

        assert result.failed == 100
        assert mock_client.post.call_count == 1
        assert result.row_errors[0].error_message == "boom"

    def test_organization_template_error_skips_row(self) -> None:
        config = _config(
            object="organization",
            custom_fields_template='{"missing": "{{ row.missing }}"}',
        )

        with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client

            result = ZendeskDestination().load(
                [{"name": "Broken Org"}, {"name": "Also Broken"}],
                config,
                _options(),
            )

        assert result.failed == 2
        mock_client.post.assert_not_called()

    def test_organization_template_error_with_on_error_fail_stops(self) -> None:
        config = _config(
            object="organization",
            custom_fields_template='{"missing": "{{ row.missing }}"}',
        )

        with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client

            result = ZendeskDestination().load(
                [{"name": "Broken Org"}, {"name": "Also Broken"}],
                config,
                _options(on_error="fail"),
            )

        assert result.failed == 1
        mock_client.post.assert_not_called()

    def test_organization_http_error_fail_stops(self) -> None:
        config = _config(object="organization")

        with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.side_effect = _http_error(400, "bad request")

            result = ZendeskDestination().load(
                [{"name": "Org 1"}, {"name": "Org 2"}],
                config,
                _options(on_error="fail"),
            )

        assert result.failed == 1
        assert result.row_errors[0].http_status == 400
        assert mock_client.post.call_count == 1

    def test_organization_generic_error_fail_stops(self) -> None:
        config = _config(object="organization")

        with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            response = _response()
            response.raise_for_status.side_effect = RuntimeError("boom")
            mock_client.post.return_value = response

            result = ZendeskDestination().load(
                [{"name": "Org 1"}, {"name": "Org 2"}],
                config,
                _options(on_error="fail"),
            )

        assert result.failed == 1
        assert result.row_errors[0].error_message == "boom"
        assert mock_client.post.call_count == 1

    def test_rate_limiter_called_per_request(self) -> None:
        records = [
            {"external_id": f"user-{index}", "email": f"user-{index}@example.com"}
            for index in range(101)
        ]

        with patch("drt.destinations.zendesk.RateLimiter") as mock_limiter_cls:
            limiter = MagicMock()
            mock_limiter_cls.return_value = limiter
            with patch("drt.destinations.zendesk.httpx.Client") as mock_client_cls:
                mock_client = MagicMock()
                mock_client_cls.return_value.__enter__.return_value = mock_client
                mock_client.post.return_value = _response()

                ZendeskDestination().load(records, _config(), _options())

        assert limiter.acquire.call_count == 2
