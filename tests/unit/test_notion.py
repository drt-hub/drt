import json
import pytest
import httpx
from unittest.mock import patch, MagicMock

from drt.destinations.notion import NotionDestination
from drt.config.models import (
    NotionDestinationConfig,
    SyncOptions,
    RetryConfig,
    RateLimitConfig,
    BearerAuth,
)


@pytest.fixture
def config():
    return NotionDestinationConfig(
        type="notion",
        database_id="db123",
        properties_template='{"Name": {"title": [{"text": {"content": "{{ row.name }}"}}]}}',
        auth=BearerAuth(type="bearer", token="test-token"),
    )


@pytest.fixture
def sync_options():
    return SyncOptions(
        retry=RetryConfig(max_attempts=2),
        rate_limit=RateLimitConfig(requests_per_second=100),
    )


def make_response(status=200):
    response = MagicMock(spec=httpx.Response)
    response.status_code = status
    response.text = "error"
    response.raise_for_status.side_effect = (
        None if status < 400 else httpx.HTTPStatusError("err", request=None, response=response)
    )
    return response


# ---------------------------------------------------------
# Tests
# ---------------------------------------------------------


@patch("httpx.Client.post")
def test_successful_insert(mock_post, config, sync_options):
    mock_post.return_value = make_response(200)

    dest = NotionDestination()
    result = dest.load([{"name": "Alice"}], config, sync_options)

    assert result.success == 1
    assert result.failed == 0


@patch("httpx.Client.post")
def test_retry_then_success(mock_post, config, sync_options):
    mock_post.side_effect = [
        make_response(500),
        make_response(200),
    ]

    dest = NotionDestination()
    result = dest.load([{"name": "Retry"}], config, sync_options)

    assert result.success == 1
    assert mock_post.call_count >= 2


@patch("httpx.Client.post")
def test_max_retries_failure(mock_post, config, sync_options):
    mock_post.return_value = make_response(500)

    dest = NotionDestination()
    result = dest.load([{"name": "Fail"}], config, sync_options)

    assert result.failed == 1
    assert result.success == 0


def test_invalid_template(config, sync_options):
    config.properties_template = "{ invalid json }"

    dest = NotionDestination()
    result = dest.load([{"name": "Bad"}], config, sync_options)

    assert result.failed == 1
    assert "properties_template error" in result.row_errors[0].error_message


def test_missing_token(sync_options):
    config = NotionDestinationConfig(
        type="notion",
        database_id="db123",
        properties_template="{}",
        auth=BearerAuth(type="bearer"),
    )

    dest = NotionDestination()

    with pytest.raises(ValueError):
        dest.load([{}], config, sync_options)


def test_missing_database_id(sync_options):
    with pytest.raises(ValueError):
        NotionDestinationConfig(
            type="notion",
            properties_template="{}",
            auth=BearerAuth(type="bearer", token="x"),
        )
