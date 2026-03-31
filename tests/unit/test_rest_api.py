"""Unit tests for RestApiDestination — httpx mocked via pytest-mock / unittest.mock."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx

from drt.config.models import (
    RateLimitConfig,
    RestApiDestinationConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.base import SyncResult
from drt.destinations.rest_api import RestApiDestination

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sync_options(max_attempts: int = 1) -> SyncOptions:
    return SyncOptions(
        batch_size=10,
        rate_limit=RateLimitConfig(requests_per_second=1000),
        retry=RetryConfig(max_attempts=max_attempts, initial_backoff=0.0, backoff_multiplier=1.0),
        on_error="skip",
    )


def _dest_config(url: str = "https://api.example.com/webhook") -> RestApiDestinationConfig:
    return RestApiDestinationConfig(
        type="rest_api",
        url=url,
        method="POST",
        headers={},
    )


def _make_response(status_code: int, text: str = "") -> httpx.Response:
    response = MagicMock(spec=httpx.Response)
    response.status_code = status_code
    response.text = text
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


class TestRestApiDestinationSuccess:
    def test_all_records_succeed(self) -> None:
        records = [{"id": 1}, {"id": 2}, {"id": 3}]
        config = _dest_config()
        options = _sync_options()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = _make_response(200, "OK")

            dest = RestApiDestination()
            result = dest.load(records, config, options)

        assert result.success == 3
        assert result.failed == 0
        assert result.row_errors == []

    def test_returns_detailed_sync_result(self) -> None:
        config = _dest_config()
        options = _sync_options()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = _make_response(200, "OK")

            result = RestApiDestination().load([{"id": 1}], config, options)

        assert isinstance(result, SyncResult)


# ---------------------------------------------------------------------------
# Failure cases — row_errors populated
# ---------------------------------------------------------------------------


class TestRestApiDestinationRowErrors:
    def test_http_422_creates_row_error(self) -> None:
        records = [{"id": 1, "email": "not-an-email"}]
        config = _dest_config()
        options = _sync_options()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = _make_response(
                422, '{"error": "invalid email format"}'
            )

            result = RestApiDestination().load(records, config, options)

        assert result.failed == 1
        assert result.success == 0
        assert len(result.row_errors) == 1
        row_err = result.row_errors[0]
        assert row_err.batch_index == 0
        assert row_err.http_status == 422
        assert "invalid email format" in row_err.error_message

    def test_http_429_creates_row_error(self) -> None:
        records = [{"id": 1}]
        config = _dest_config()
        options = _sync_options(max_attempts=1)

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = _make_response(429, "Too Many Requests")

            result = RestApiDestination().load(records, config, options)

        assert result.failed == 1
        row_err = result.row_errors[0]
        assert row_err.http_status == 429
        assert row_err.batch_index == 0

    def test_batch_index_matches_record_position(self) -> None:
        # First record succeeds, second and third fail
        records = [{"id": 0}, {"id": 1}, {"id": 2}]
        config = _dest_config()
        options = _sync_options()

        responses = [
            _make_response(200, "OK"),
            _make_response(422, "bad"),
            _make_response(500, "err"),
        ]

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.side_effect = responses

            result = RestApiDestination().load(records, config, options)

        assert result.success == 1
        assert result.failed == 2
        assert len(result.row_errors) == 2
        assert result.row_errors[0].batch_index == 1
        assert result.row_errors[1].batch_index == 2

    def test_record_preview_is_json_and_truncated(self) -> None:
        # Large record — preview must be JSON-formatted and at most 200 chars
        record = {"key": "x" * 300}
        records = [record]
        config = _dest_config()
        options = _sync_options()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = _make_response(422, "bad request")

            result = RestApiDestination().load(records, config, options)

        assert len(result.row_errors) == 1
        preview = result.row_errors[0].record_preview
        # Must be at most 200 chars
        assert len(preview) <= 200
        # Must start with JSON object opener (comes from json.dumps)
        assert preview.startswith("{")

    def test_row_error_has_timestamp(self) -> None:
        records = [{"id": 1}]
        config = _dest_config()
        options = _sync_options()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = _make_response(500, "server error")

            result = RestApiDestination().load(records, config, options)

        assert result.row_errors[0].timestamp  # non-empty
        assert "T" in result.row_errors[0].timestamp  # ISO8601

    def test_errors_backward_compat_from_row_errors(self) -> None:
        records = [{"id": 1}, {"id": 2}]
        config = _dest_config()
        options = _sync_options()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = _make_response(500, "server error")

            result = RestApiDestination().load(records, config, options)

        # errors property should be a flat list of error_message strings
        assert isinstance(result.errors, list)
        assert len(result.errors) == 2
        assert all(isinstance(e, str) for e in result.errors)

    def test_non_http_exception_creates_row_error(self) -> None:
        records = [{"id": 1}]
        config = _dest_config()
        options = _sync_options()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.side_effect = ConnectionError("network unreachable")

            result = RestApiDestination().load(records, config, options)

        assert result.failed == 1
        row_err = result.row_errors[0]
        assert row_err.http_status is None
        assert "network unreachable" in row_err.error_message
