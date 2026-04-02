"""Unit tests for LineNotifyDestination — httpx mocked via unittest.mock."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx

from drt.config.models import (
    LineNotifyDestinationConfig,
    RateLimitConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.base import SyncResult
from drt.destinations.line_notify import LineNotifyDestination, _LINE_NOTIFY_API

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sync_options(max_attempts: int = 1) -> SyncOptions:
    return SyncOptions(
        batch_size=10,
        rate_limit=RateLimitConfig(requests_per_second=1000),
        retry=RetryConfig(
            max_attempts=max_attempts, initial_backoff=0.0, backoff_multiplier=1.0
        ),
        on_error="skip",
    )


def _dest_config(
    token: str = "test_token", token_env: str | None = None
) -> LineNotifyDestinationConfig:
    return LineNotifyDestinationConfig(
        type="line_notify",
        token=token,
        token_env=token_env,
        message_template="{{ row.name }}: {{ row.email }}",
    )


def _make_response(status_code: int, json: dict | None = None) -> httpx.Response:
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.text = ""
    if json:
        import json as _json

        response._content = _json.dumps(json).encode()
    if status_code >= 400:
        response.raise_for_status.side_effect = httpx.HTTPStatusError(
            message=f"HTTP {status_code}",
            request=MagicMock(),
            response=response,
        )
    else:
        response.raise_for_status.return_value = None
    return response


# ---------------------------------------------------------------------------
# Success cases
# ---------------------------------------------------------------------------


class TestLineNotifyDestinationSuccess:
    def test_all_records_succeed(self) -> None:
        records = [{"name": "Alice", "email": "alice@example.com"}]
        config = _dest_config(token="my_token")
        options = _sync_options()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = _make_response(200, {"status": 200})

            dest = LineNotifyDestination()
            result = dest.load(records, config, options)

        assert result.success == 1
        assert result.failed == 0
        assert result.row_errors == []
        mock_client.post.assert_called_once()
        call_kwargs = mock_client.post.call_args
        assert call_kwargs.kwargs["headers"]["Authorization"] == "Bearer my_token"
        assert call_kwargs.kwargs["data"]["message"] == "Alice: alice@example.com"

    def test_token_from_env(self) -> None:
        records = [{"name": "Bob", "email": "bob@example.com"}]
        config = LineNotifyDestinationConfig(
            type="line_notify",
            token_env="LINE_NOTIFY_TOKEN",
            message_template="{{ row.name }}: {{ row.email }}",
        )
        options = _sync_options()

        with patch.dict("os.environ", {"LINE_NOTIFY_TOKEN": "env_token"}):
            with patch("httpx.Client") as mock_client_cls:
                mock_client = MagicMock()
                mock_client_cls.return_value.__enter__.return_value = mock_client
                mock_client.post.return_value = _make_response(200, {"status": 200})

                dest = LineNotifyDestination()
                result = dest.load(records, config, options)

        assert result.success == 1
        assert (
            mock_client.post.call_args.kwargs["headers"]["Authorization"]
            == "Bearer env_token"
        )


class TestLineNotifyDestinationFailure:
    def test_http_error_marks_record_failed(self) -> None:
        records = [{"name": "Carol", "email": "carol@example.com"}]
        config = _dest_config(token="bad_token")
        options = _sync_options()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = _make_response(401, {"status": 401, "message": "Invalid token"})

            dest = LineNotifyDestination()
            result = dest.load(records, config, options)

        assert result.success == 0
        assert result.failed == 1
        assert len(result.row_errors) == 1
        assert result.row_errors[0].http_status == 401

    def test_missing_token_raises_value_error(self) -> None:
        records = [{"name": "Dave", "email": "dave@example.com"}]
        config = LineNotifyDestinationConfig(type="line_notify", token=None, token_env=None)
        options = _sync_options()

        dest = LineNotifyDestination()
        try:
            dest.load(records, config, options)
            assert False, "Expected ValueError"
        except ValueError as e:
            assert "token" in str(e).lower()

    def test_default_message_template(self) -> None:
        records = [{"id": 1, "value": "test"}]
        config = LineNotifyDestinationConfig(type="line_notify", token="tok")
        options = _sync_options()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = _make_response(200, {"status": 200})

            dest = LineNotifyDestination()
            result = dest.load(records, config, options)

        assert result.success == 1
        assert mock_client.post.call_args.kwargs["data"]["message"] == "{'id': 1, 'value': 'test'}"
