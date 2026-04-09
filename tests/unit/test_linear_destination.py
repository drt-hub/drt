import os
import pytest
from unittest.mock import patch, MagicMock
from httpx import Response, HTTPStatusError

from drt.config.models import LinearDestinationConfig, SyncOptions, RateLimitConfig, BearerAuth
from drt.destinations.linear import LinearDestination
from drt.destinations.row_errors import RowError


# --- Correct fixture for LinearDestinationConfig ---
@pytest.fixture
def mock_config():
    return LinearDestinationConfig(
        type="linear",
        team_id_env="LINEAR_TEAM_ID",
        title_template="Issue: {{ row.metric }}",
        description_template="Value: {{ row.value }}",
        label_ids=[],
        assignee_id=None,
        auth=BearerAuth(type="bearer", token_env="LINEAR_API_KEY"),
    )


# --- Records to load ---
@pytest.fixture
def mock_records():
    return [
        {"metric": "cpu_usage", "value": 90},
        {"metric": "memory_usage", "value": 80},
    ]


# --- SyncOptions fixture ---
@pytest.fixture
def sync_options():
    return SyncOptions(rate_limit=RateLimitConfig(requests_per_second=5))


# --- Test: successful creation ---
def test_successful_issue_creation(mock_config, mock_records, sync_options):
    os.environ["LINEAR_API_KEY"] = "key123"
    os.environ["LINEAR_TEAM_ID"] = "team123"

    dest = LinearDestination()

    with patch("drt.destinations.linear.httpx.Client.post") as mock_post:
        mock_response = MagicMock()
        mock_response.json.return_value = {"data": {"issueCreate": {"success": True}}}
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response

        result = dest.load(mock_records, mock_config, sync_options)

        assert result.success == 2
        assert result.failed == 0
        assert result.row_errors == []


# --- Test: template rendering error ---
def test_template_rendering_error(mock_records, sync_options):
    os.environ["LINEAR_API_KEY"] = "key123"
    os.environ["LINEAR_TEAM_ID"] = "team123"

    bad_config = LinearDestinationConfig(
        type="linear",
        team_id_env="LINEAR_TEAM_ID",
        title_template="{{ row.nonexistent }}",  # this will fail
        description_template="Some description",
        label_ids=[],
        assignee_id=None,
        auth=BearerAuth(type="bearer", token_env="LINEAR_API_KEY"),
    )

    dest = LinearDestination()
    result = dest.load(mock_records, bad_config, sync_options)

    assert result.success == 0
    assert result.failed == 2
    assert all(isinstance(err, RowError) for err in result.row_errors)
    assert all("Template rendering error" in err.error_message for err in result.row_errors)


# --- Test: HTTP error triggers retry ---
def test_http_error_retry(mock_config, mock_records, sync_options):
    os.environ["LINEAR_API_KEY"] = "key123"
    os.environ["LINEAR_TEAM_ID"] = "team123"

    dest = LinearDestination()

    with patch("drt.destinations.linear.httpx.Client.post") as mock_post:
        # first attempt fails with HTTPStatusError (500), second succeeds
        mock_response_fail = MagicMock()
        mock_response_fail.response = Response(500)
        mock_response_fail.raise_for_status.side_effect = HTTPStatusError(
            "500 Server Error",
            request=MagicMock(),
            response=mock_response_fail.response,
        )

        mock_response_success = MagicMock()
        mock_response_success.raise_for_status.return_value = None
        mock_response_success.json.return_value = {"data": {"issueCreate": {"success": True}}}

        # alternate fail/success for each record
        mock_post.side_effect = [mock_response_fail, mock_response_success] * len(mock_records)

        result = dest.load(mock_records, mock_config, sync_options)

        # all records should eventually succeed
        assert result.success == len(mock_records)
        assert result.failed == 0
        assert result.row_errors == []


# --- Test: Linear API returns failure (issue not created) ---
def test_linear_issue_creation_failure(mock_config, mock_records, sync_options):
    os.environ["LINEAR_API_KEY"] = "key123"
    os.environ["LINEAR_TEAM_ID"] = "team123"

    dest = LinearDestination()

    with patch("drt.destinations.linear.httpx.Client.post") as mock_post:
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"data": {"issueCreate": {"success": False}}}
        mock_post.return_value = mock_response

        result = dest.load(mock_records, mock_config, sync_options)

        assert result.success == 0
        assert result.failed == 2
        assert all("Linear issue creation failed" in err.error_message for err in result.row_errors)


# --- Test: Rate limiter is called for each record ---
def test_rate_limiter_called(mock_config, mock_records, sync_options):
    os.environ["LINEAR_API_KEY"] = "key123"
    os.environ["LINEAR_TEAM_ID"] = "team123"

    dest = LinearDestination()

    with patch("drt.destinations.linear.RateLimiter.acquire") as mock_acquire:
        with patch("drt.destinations.linear.httpx.Client.post") as mock_post:
            mock_response = MagicMock()
            mock_response.raise_for_status.return_value = None
            mock_response.json.return_value = {"data": {"issueCreate": {"success": True}}}
            mock_post.return_value = mock_response

            result = dest.load(mock_records, mock_config, sync_options)

            # RateLimiter.acquire should be called once per record
            assert mock_acquire.call_count == len(mock_records)
            assert result.success == 2