"""Unit tests for RestApiDestination — httpx mocked via pytest-mock / unittest.mock."""

from __future__ import annotations

import json
import logging
from unittest.mock import MagicMock, patch

import httpx
import pytest

from drt.config.models import (
    OffsetPaginationConfig,
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
        assert hasattr(result, "row_errors")


class TestRestApiDestinationBatchMode:
    def test_empty_batch_returns_before_opening_client(self) -> None:
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows | tojson_safe }}",
        )

        with patch("httpx.Client") as mock_client_cls:
            result = RestApiDestination().load([], config, _sync_options())

        assert result == SyncResult()
        mock_client_cls.assert_not_called()

    def test_sends_sub_chunks_and_acquires_once_per_request(self) -> None:
        records = [{"id": i} for i in range(5)]
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template='{"records": {{ rows | tojson_safe }}}',
            max_records_per_request=2,
        )
        limiter = MagicMock()

        with (
            patch("httpx.Client") as mock_client_cls,
            patch(
                "drt.destinations.rest_api.resolve_rate_limiter",
                return_value=limiter,
            ),
        ):
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = _make_response(200, "OK")

            result = RestApiDestination().load(records, config, _sync_options())

        assert result.success == 5
        assert result.failed == 0
        assert mock_client.request.call_count == 3
        assert limiter.acquire.call_count == 3
        payloads = [
            json.loads(call.kwargs["content"].decode())
            for call in mock_client.request.call_args_list
        ]
        assert payloads == [
            {"records": records[:2]},
            {"records": records[2:4]},
            {"records": records[4:]},
        ]

    def test_none_max_records_sends_whole_engine_chunk_once(self) -> None:
        records = [{"id": 1}, {"id": 2}, {"id": 3}]
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows | tojson_safe }}",
        )

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = _make_response(200, "OK")

            result = RestApiDestination().load(records, config, _sync_options())

        assert result.success == 3
        assert mock_client.request.call_count == 1
        content = mock_client.request.call_args.kwargs["content"]
        assert json.loads(content.decode()) == records

    def test_http_error_without_error_path_fails_whole_sub_chunk(self) -> None:
        records = [{"id": 1}, {"id": 2}]
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows | tojson_safe }}",
        )

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = _make_response(422, "x" * 600)

            result = RestApiDestination().load(records, config, _sync_options())

        assert result.success == 0
        assert result.failed == 2
        assert [error.batch_index for error in result.row_errors] == [0, 1]
        assert all(error.http_status == 422 for error in result.row_errors)
        assert all(len(error.error_message) == 500 for error in result.row_errors)

    def test_error_path_maps_second_sub_chunk_to_global_indexes(self) -> None:
        records = [{"id": i} for i in range(4)]
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows | tojson_safe }}",
            max_records_per_request=2,
            error_path="data.results",
        )
        failed_response = _make_response(422, '{"data": {"results": []}}')
        failed_response.json.return_value = {
            "data": {"results": [None, {"message": "fourth record failed"}]}
        }

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.side_effect = [
                _make_response(200, "OK"),
                failed_response,
            ]

            result = RestApiDestination().load(records, config, _sync_options())

        assert result.success == 3
        assert result.failed == 1
        assert [error.batch_index for error in result.row_errors] == [3]
        assert result.row_errors[0].record_preview == '{"id": 3}'
        assert result.row_errors[0].error_message == "fourth record failed"

    def test_mismatched_error_path_fails_whole_chunk_and_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        records = [{"id": 1}, {"id": 2}]
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows | tojson_safe }}",
            error_path="results",
        )
        response = _make_response(422, '{"results": [null]}')
        response.json.return_value = {"results": [None]}

        with patch("httpx.Client") as mock_client_cls, caplog.at_level(logging.WARNING):
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = response

            result = RestApiDestination().load(records, config, _sync_options())

        assert result.failed == 2
        assert [error.batch_index for error in result.row_errors] == [0, 1]
        assert "error_path 'results' did not match the response shape" in caplog.text

    def test_successful_response_does_not_consult_error_path(self) -> None:
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows | tojson_safe }}",
            error_path="results",
        )
        response = _make_response(200, "not json")
        response.json.side_effect = AssertionError("response JSON must not be read")

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = response

            result = RestApiDestination().load([{"id": 1}], config, _sync_options())

        assert result.success == 1
        response.json.assert_not_called()

    def test_error_path_message_mapping_contract(self) -> None:
        records = [{"id": i} for i in range(6)]
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows | tojson_safe }}",
            error_path="results",
        )
        response = _make_response(422, "per-item failures")
        response.json.return_value = {
            "results": [
                None,
                {"error": "first", "message": "not selected"},
                {"message": "second"},
                {"error_message": "third"},
                {"detail": "fallback"},
                42,
            ]
        }

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = response

            result = RestApiDestination().load(records, config, _sync_options())

        assert result.success == 1
        assert result.failed == 5
        assert [error.error_message for error in result.row_errors] == [
            "first",
            "second",
            "third",
            "{'detail': 'fallback'}",
            "42",
        ]

    def test_invalid_json_error_mapping_falls_back_to_whole_chunk(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows | tojson_safe }}",
            error_path="results",
        )
        response = _make_response(422, "not json")
        response.json.side_effect = json.JSONDecodeError("invalid", "", 0)

        with patch("httpx.Client") as mock_client_cls, caplog.at_level(logging.WARNING):
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = response

            result = RestApiDestination().load([{"id": 1}], config, _sync_options())

        assert result.failed == 1
        assert "did not match the response shape" in caplog.text

    def test_missing_error_path_falls_back_to_whole_chunk(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows | tojson_safe }}",
            error_path="data.results",
        )
        response = _make_response(422, '{"data": {}}')
        response.json.return_value = {"data": {}}

        with patch("httpx.Client") as mock_client_cls, caplog.at_level(logging.WARNING):
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = response

            result = RestApiDestination().load([{"id": 1}], config, _sync_options())

        assert result.failed == 1
        assert "did not match the response shape" in caplog.text

    def test_batch_request_retries_as_one_request_unit(self) -> None:
        records = [{"id": 1}, {"id": 2}]
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows | tojson_safe }}",
        )
        limiter = MagicMock()

        with (
            patch("httpx.Client") as mock_client_cls,
            patch(
                "drt.destinations.rest_api.resolve_rate_limiter",
                return_value=limiter,
            ),
        ):
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.side_effect = [
                _make_response(503, "busy"),
                _make_response(200, "OK"),
            ]

            result = RestApiDestination().load(records, config, _sync_options(max_attempts=2))

        assert result.success == 2
        assert mock_client.request.call_count == 2
        assert limiter.acquire.call_count == 1

    def test_non_http_failure_marks_whole_sub_chunk_failed(self) -> None:
        records = [{"id": 1}, {"id": 2}]
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows | tojson_safe }}",
        )

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.side_effect = ConnectionError("network unreachable")

            result = RestApiDestination().load(records, config, _sync_options())

        assert result.failed == 2
        assert [error.batch_index for error in result.row_errors] == [0, 1]
        assert all("network unreachable" in e.error_message for e in result.row_errors)

    def test_on_error_fail_stops_after_non_http_failure(self) -> None:
        records = [{"id": i} for i in range(4)]
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows | tojson_safe }}",
            max_records_per_request=2,
        )
        options = _sync_options()
        options.on_error = "fail"

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.side_effect = ConnectionError("network unreachable")

            result = RestApiDestination().load(records, config, options)

        assert result.failed == 2
        assert mock_client.request.call_count == 1

    def test_on_error_fail_continues_when_error_path_maps_all_successes(self) -> None:
        records = [{"id": i} for i in range(4)]
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows | tojson_safe }}",
            max_records_per_request=2,
            error_path="results",
        )
        mapped_success = _make_response(422, "mapped")
        mapped_success.json.return_value = {"results": [None, None]}
        options = _sync_options()
        options.on_error = "fail"

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.side_effect = [mapped_success, _make_response(200, "OK")]

            result = RestApiDestination().load(records, config, options)

        assert result.success == 4
        assert result.failed == 0
        assert mock_client.request.call_count == 2

    def test_on_error_fail_stops_after_first_failing_sub_chunk(self) -> None:
        records = [{"id": i} for i in range(4)]
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows | tojson_safe }}",
            max_records_per_request=2,
            error_path="results",
        )
        response = _make_response(422, "partial failure")
        response.json.return_value = {"results": [None, "bad record"]}
        options = _sync_options()
        options.on_error = "fail"

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = response

            result = RestApiDestination().load(records, config, options)

        assert result.success == 1
        assert result.failed == 1
        assert mock_client.request.call_count == 1

    def test_template_error_fails_sub_chunk_without_request(self) -> None:
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows[0].missing }}",
        )

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client

            result = RestApiDestination().load([{"id": 1}, {"id": 2}], config, _sync_options())

        assert result.failed == 2
        assert mock_client.request.call_count == 0

    def test_non_undefined_template_error_still_fails_sub_chunk(self) -> None:
        """render_template() only normalizes Jinja's UndefinedError to
        ValueError -- a ZeroDivisionError from row arithmetic must be caught
        too, or on_error: skip stops being effective for it (#763 precedent)."""
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ 1 / rows[0].zero }}",
        )

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client

            result = RestApiDestination().load([{"zero": 0}], config, _sync_options())

        assert result.failed == 1
        assert mock_client.request.call_count == 0

    def test_on_error_fail_stops_after_batch_template_error(self) -> None:
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/batch",
            body_mode="batch",
            batch_template="{{ rows[0].missing }}",
            max_records_per_request=1,
        )
        options = _sync_options()
        options.on_error = "fail"

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client

            result = RestApiDestination().load([{"id": 1}, {"id": 2}], config, options)

        assert result.failed == 1
        assert mock_client.request.call_count == 0


class TestRestApiDestinationBatchConfig:
    def test_batch_mode_requires_batch_template(self) -> None:
        with pytest.raises(ValueError, match="batch_template.*body_mode"):
            RestApiDestinationConfig(
                type="rest_api",
                url="https://api.example.com/batch",
                body_mode="batch",
            )

    def test_batch_mode_rejects_body_template(self) -> None:
        with pytest.raises(ValueError, match="body_template.*batch_template"):
            RestApiDestinationConfig(
                type="rest_api",
                url="https://api.example.com/batch",
                body_mode="batch",
                body_template="{{ row }}",
                batch_template="{{ rows }}",
            )

    def test_record_mode_rejects_batch_template(self) -> None:
        with pytest.raises(ValueError, match="batch_template.*body_mode"):
            RestApiDestinationConfig(
                type="rest_api",
                url="https://api.example.com/batch",
                batch_template="{{ rows }}",
            )

    @pytest.mark.parametrize("value", [0, -1])
    def test_max_records_per_request_must_be_positive(self, value: int) -> None:
        with pytest.raises(ValueError, match="max_records_per_request.*at least 1"):
            RestApiDestinationConfig(
                type="rest_api",
                url="https://api.example.com/batch",
                max_records_per_request=value,
            )


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

        # row_errors should contain error details for each failed row
        assert len(result.row_errors) == 2
        assert all(isinstance(e.error_message, str) for e in result.row_errors)

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


# ---------------------------------------------------------------------------
# Per-destination retry override (#277)
# ---------------------------------------------------------------------------


class TestRestApiDestinationRetryOverride:
    def test_destination_retry_overrides_sync_retry(self) -> None:
        """destination.retry takes precedence over sync.retry."""
        # destination.retry says 5 attempts; sync.retry says 2.
        # On 503 → destination override should drive 5 attempts.
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/x",
            method="POST",
            retry=RetryConfig(max_attempts=5, initial_backoff=0.0, backoff_multiplier=1.0),
        )
        options = SyncOptions(
            rate_limit=RateLimitConfig(requests_per_second=1000),
            retry=RetryConfig(max_attempts=2, initial_backoff=0.0, backoff_multiplier=1.0),
            on_error="skip",
        )

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = _make_response(503, "busy")

            RestApiDestination().load([{"id": 1}], config, options)

        # 5 actual request calls (destination override), not 2 (sync default).
        assert mock_client.request.call_count == 5

    def test_sync_retry_used_when_no_destination_override(self) -> None:
        """Without destination.retry, sync.retry is used as before."""
        config = _dest_config()  # no retry field set
        options = _sync_options(max_attempts=3)

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = _make_response(503, "busy")

            RestApiDestination().load([{"id": 1}], config, options)

        assert mock_client.request.call_count == 3


class TestRestApiOnErrorFail:
    """on_error=fail must stop processing after the first failure (#365)."""

    def test_on_error_fail_stops_after_first_http_error(self) -> None:
        records = [{"id": 1}, {"id": 2}, {"id": 3}]
        config = _dest_config()
        options = SyncOptions(
            batch_size=10,
            rate_limit=RateLimitConfig(requests_per_second=1000),
            retry=RetryConfig(max_attempts=1, initial_backoff=0.0, backoff_multiplier=1.0),
            on_error="fail",
        )

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = _make_response(500, "internal error")

            result = RestApiDestination().load(records, config, options)

        assert result.failed == 1
        assert result.success == 0
        # Only the first record was attempted (one HTTP request, one retry-attempt)
        assert mock_client.request.call_count == 1

    def test_on_error_fail_stops_after_template_error(self) -> None:
        records = [
            {"id": 1, "name": "ok"},
            {"id": 2},  # missing 'name' — would trigger template error
            {"id": 3},
        ]
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/webhook",
            method="POST",
            body_template='{"name": "{{ row.required_field_that_does_not_exist }}"}',
        )
        options = SyncOptions(
            batch_size=10,
            rate_limit=RateLimitConfig(requests_per_second=1000),
            retry=RetryConfig(max_attempts=1),
            on_error="fail",
        )

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            # If template error stops processing, no HTTP requests should be made
            mock_client.request.return_value = _make_response(200, "OK")

            result = RestApiDestination().load(records, config, options)

        # Template error on first record should immediately fail-out.
        # Only 1 row attempted — no HTTP since template fails before request.
        assert result.failed == 1
        assert result.success == 0
        assert mock_client.request.call_count == 0

    def test_non_undefined_template_error_is_caught_as_row_error(self) -> None:
        """Same gap as batch mode's non-UndefinedError template test: a
        ZeroDivisionError from row arithmetic must respect on_error, not
        abort load() outright."""
        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/webhook",
            method="POST",
            body_template="{{ 1 / row.zero }}",
        )
        options = _sync_options()

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client

            result = RestApiDestination().load([{"zero": 0}], config, options)

        assert result.failed == 1
        assert result.success == 0
        assert mock_client.request.call_count == 0


# ---------------------------------------------------------------------------
# Shared limiter bucket across both call sites (#769)
# ---------------------------------------------------------------------------


class TestSharedRateLimiterBucket:
    """``rest_api`` builds two limiters; both must land in one bucket (#769).

    ``load()`` paces the write loop and ``fetch_paginated()`` paces the
    pagination reads. Before the registry these were independent instances, so
    one host saw up to 2x the configured rate from a single sync — and
    ``--threads N`` multiplied that again. The key is ``rest_api:<netloc>``, so
    sharing falls out of the key rather than any coordination between the two
    methods. Verified rather than assumed, per the plan.
    """

    def test_both_call_sites_resolve_to_one_limiter(self) -> None:
        from drt.destinations.rate_limiter import resolve_rate_limiter

        config = RestApiDestinationConfig(type="rest_api", url="https://api.example.com/v1/users")
        options = _sync_options()

        first = resolve_rate_limiter(config, options)
        second = resolve_rate_limiter(config, options)

        assert first is second

    def test_differing_paths_on_one_host_share_a_bucket(self) -> None:
        """The write URL and a pagination URL differ by path, not host — the
        vendor quota is per host, so they must not get separate buckets."""
        from drt.destinations.rate_limiter import resolve_rate_limiter

        write = RestApiDestinationConfig(type="rest_api", url="https://api.example.com/v1/users")
        read = RestApiDestinationConfig(type="rest_api", url="https://api.example.com/v2/orders")
        options = _sync_options()

        assert resolve_rate_limiter(write, options) is resolve_rate_limiter(read, options)

    def test_different_hosts_do_not_share_a_bucket(self) -> None:
        """The converse guard: unrelated APIs must not throttle each other."""
        from drt.destinations.rate_limiter import resolve_rate_limiter

        a = RestApiDestinationConfig(type="rest_api", url="https://api.example.com/v1/users")
        b = RestApiDestinationConfig(type="rest_api", url="https://api.other.com/v1/users")
        options = _sync_options()

        assert resolve_rate_limiter(a, options) is not resolve_rate_limiter(b, options)

    def test_load_and_fetch_paginated_use_the_same_instance(self) -> None:
        """End-to-end through the real code paths rather than the helper: both
        methods must receive the same limiter object for one config."""
        from drt.destinations import rate_limiter as rl_module

        config = RestApiDestinationConfig(
            type="rest_api",
            url="https://api.example.com/webhook",
            method="POST",
            pagination=OffsetPaginationConfig(type="offset", max_pages=1),
        )
        options = _sync_options()
        seen: list[object] = []
        real = rl_module.resolve_rate_limiter

        def spy(*args, **kwargs):
            limiter = real(*args, **kwargs)
            seen.append(limiter)
            return limiter

        with patch("drt.destinations.rest_api.resolve_rate_limiter", side_effect=spy):
            with patch("httpx.Client") as mock_client_cls:
                mock_client = MagicMock()
                mock_client_cls.return_value.__enter__.return_value = mock_client
                mock_client.request.return_value = _make_response(200, "OK")
                mock_client.get.return_value = _make_response(200, "[]")

                dest = RestApiDestination()
                dest.load([{"id": 1}], config, options)
                dest.fetch_paginated(config, {}, options)

        assert len(seen) == 2, "both call sites must go through the registry"
        assert seen[0] is seen[1], "one host, one bucket"
