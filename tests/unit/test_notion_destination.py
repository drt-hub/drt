"""Unit tests for the Notion destination."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from drt.config.models import BearerAuth, NotionDestinationConfig, RateLimitConfig, SyncOptions
from drt.destinations.notion import NotionDestination


@pytest.fixture
def config():
    return NotionDestinationConfig(
        type="notion",
        database_id="test_db_123",
        properties_template='{"Name": {"title": [{"text": {"content": "{{ row.name }}"}}]}}',
        auth=BearerAuth(type="bearer", token="secret_token"),
    )


@pytest.fixture
def sync_options():
    return SyncOptions(rate_limit=RateLimitConfig(requests_per_second=100))


@pytest.fixture
def destination():
    return NotionDestination()


def test_missing_database_id():
    """Should raise error if no database_id is provided."""
    with pytest.raises(ValueError, match="database_id"):
        dest = NotionDestination()
        dest.load(
            [{"name": "Alice"}],
            NotionDestinationConfig(
                type="notion",
                properties_template="{}",
                auth=BearerAuth(type="bearer", token="secret"),
            ),
            SyncOptions(),
        )


def test_missing_token():
    """Should raise error if no auth token is provided."""
    with pytest.raises(ValueError, match="token"):
        dest = NotionDestination()
        dest.load(
            [{"name": "Alice"}],
            NotionDestinationConfig(
                type="notion",
                database_id="test_db",
                properties_template="{}",
                auth=BearerAuth(type="bearer"),
            ),
            SyncOptions(),
        )


@patch("drt.destinations.notion.httpx.Client")
def test_successful_sync(MockClient, config, sync_options, destination):
    """Should sync records successfully."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    MockClient.return_value.__enter__.return_value.post.return_value = mock_response

    records = [{"name": "Alice"}, {"name": "Bob"}]
    result = destination.load(records, config, sync_options)

    assert result.success == 2
    assert result.failed == 0
    assert MockClient.return_value.__enter__.return_value.post.call_count == 2


@patch("drt.destinations.notion.httpx.Client")
def test_sync_failure(MockClient, config, sync_options, destination):
    """Should handle HTTP errors gracefully."""
    error_response = MagicMock()
    error_response.status_code = 400
    error_response.text = '{"message": "Invalid property"}'
    http_error = httpx.HTTPStatusError("Bad Request", request=MagicMock(), response=error_response)

    MockClient.return_value.__enter__.return_value.post.side_effect = http_error

    records = [{"name": "Alice"}]
    result = destination.load(records, config, sync_options)

    assert result.success == 0
    assert result.failed == 1
    assert len(result.row_errors) == 1


@patch("drt.destinations.notion.httpx.Client")
def test_template_rendering_error(MockClient, config, sync_options, destination):
    """Should handle Jinja2 template errors gracefully."""
    config_bad_template = config.model_copy()
    config_bad_template.properties_template = "NOT JSON"

    records = [{"name": "Alice"}]
    result = destination.load(records, config_bad_template, sync_options)

    assert result.failed == 1
    assert len(result.row_errors) == 1
    assert "Expecting" in result.row_errors[0].error_message
