"""Unit tests for Linear destination."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from drt.config.models import (
    BearerAuth,
    LinearDestinationConfig,
    RateLimitConfig,
    SyncOptions,
)
from drt.destinations.linear import LinearDestination
from drt.destinations.row_errors import RowError


def _config(**overrides):
    defaults = {
        "type": "linear",
        "team_id_env": "LINEAR_TEAM_ID",
        "title_template": "Issue: {{ row.metric }}",
        "description_template": "Value: {{ row.value }}",
        "label_ids": [],
        "assignee_id": None,
        "auth": BearerAuth(type="bearer", token="test-api-key"),
    }
    defaults.update(overrides)
    return LinearDestinationConfig(**defaults)


def _options(**overrides):
    defaults = {
        "mode": "full",
        "batch_size": 100,
        "on_error": "skip",
        "rate_limit": RateLimitConfig(requests_per_second=0),
    }
    defaults.update(overrides)
    return SyncOptions(**defaults)


RECORDS = [
    {"metric": "cpu_usage", "value": 90},
    {"metric": "memory_usage", "value": 80},
]


@pytest.fixture
def mock_linear_client(monkeypatch):
    monkeypatch.setenv("LINEAR_TEAM_ID", "team123")

    with patch("drt.destinations.linear.httpx.Client") as mock_client_cls:
        mock_client = MagicMock()
        mock_client_cls.return_value.__enter__ = MagicMock(return_value=mock_client)
        mock_client_cls.return_value.__exit__ = MagicMock(return_value=False)

        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json.return_value = {"data": {"issueCreate": {"success": True}}}
        mock_client.post.return_value = mock_response

        yield mock_client, mock_response


class TestLinearDestination:
    def test_successful_issue_creation(self, mock_linear_client):
        mock_client, _ = mock_linear_client

        dest = LinearDestination()
        result = dest.load(RECORDS, _config(), _options())

        assert result.success == 2
        assert result.failed == 0
        assert result.row_errors == []
        assert mock_client.post.call_count == 2

    def test_template_rendering_error(self, mock_linear_client):
        dest = LinearDestination()
        config = _config(title_template="{{ row.nonexistent }}")

        result = dest.load(RECORDS, config, _options())

        assert result.success == 0
        assert result.failed == 2
        assert all(isinstance(err, RowError) for err in result.row_errors)

    def test_linear_api_failure(self, mock_linear_client):
        mock_client, mock_response = mock_linear_client
        mock_response.json.return_value = {"data": {"issueCreate": {"success": False}}}

        dest = LinearDestination()
        result = dest.load(RECORDS, _config(), _options())

        assert result.success == 0
        assert result.failed == 2
        assert all("Linear issue creation failed" in err.error_message for err in result.row_errors)

    def test_http_error(self, mock_linear_client):
        mock_client, _ = mock_linear_client

        error_response = MagicMock()
        error_response.status_code = 500
        error_response.text = "Server Error"

        mock_client.post.side_effect = httpx.HTTPStatusError(
            "500", request=MagicMock(), response=error_response
        )

        dest = LinearDestination()
        result = dest.load(RECORDS, _config(), _options())

        assert result.failed == 2
        assert result.row_errors[0].http_status == 500

    def test_on_error_fail_stops(self, mock_linear_client):
        mock_client, _ = mock_linear_client

        error_response = MagicMock()
        error_response.status_code = 400
        error_response.text = "Bad Request"

        mock_client.post.side_effect = httpx.HTTPStatusError(
            "400", request=MagicMock(), response=error_response
        )

        dest = LinearDestination()
        result = dest.load(RECORDS, _config(), _options(on_error="fail"))

        # Only 1 record processed — on_error="fail" stops after first failure
        assert result.failed == 1
        assert mock_client.post.call_count == 1

    def test_missing_api_key(self, monkeypatch):
        monkeypatch.setenv("LINEAR_TEAM_ID", "team123")

        dest = LinearDestination()
        config = _config(auth=BearerAuth(type="bearer", token=None, token_env=None))

        with pytest.raises(ValueError, match="LINEAR_API_KEY"):
            dest.load(RECORDS, config, _options())

    def test_rate_limiter_called_per_record(self, mock_linear_client):
        with patch("drt.destinations.linear.RateLimiter") as mock_rl_cls:
            mock_rl = MagicMock()
            mock_rl_cls.return_value = mock_rl

            dest = LinearDestination()
            dest.load(RECORDS, _config(), _options())

            assert mock_rl.acquire.call_count == 2
