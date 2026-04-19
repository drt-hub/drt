"""Tests for REST API destination pagination support (feature #260)."""

import json
from unittest.mock import MagicMock, patch

import pytest

from drt.config.models import (
    CursorPaginationConfig,
    LinkHeaderPaginationConfig,
    OffsetPaginationConfig,
    RestApiDestinationConfig,
    SyncOptions,
)
from drt.destinations.rest_api import RestApiDestination


@pytest.fixture
def rest_api_destination():
    """Create a REST API destination instance."""
    return RestApiDestination()


@pytest.fixture
def base_config():
    """Create a base REST API config with no pagination."""
    return RestApiDestinationConfig(
        type="rest_api",
        url="https://api.example.com/contacts",
        method="POST",
        headers={},
        auth=None,
        pagination=None,
    )


@pytest.fixture
def sync_options():
    """Create a sync options instance."""
    return SyncOptions(mode="full")


class TestExtractNextLink:
    """Tests for _extract_next_link static method (RFC 5988 Link header parsing)."""

    def test_extract_next_link_standard_format(self, rest_api_destination):
        """Parse standard RFC 5988 Link header format."""
        link_header = (
            '<https://api.example.com?page=2>; rel="next", '
            '<https://api.example.com?page=50>; rel="last"'
        )
        result = rest_api_destination._extract_next_link(link_header)
        assert result == "https://api.example.com?page=2"

    def test_extract_next_link_no_next(self, rest_api_destination):
        """Return None when rel='next' not present."""
        link_header = '<https://api.example.com?page=50>; rel="last"'
        result = rest_api_destination._extract_next_link(link_header)
        assert result is None

    def test_extract_next_link_empty_header(self, rest_api_destination):
        """Return None for empty link header."""
        result = rest_api_destination._extract_next_link("")
        assert result is None

    def test_extract_next_link_single_quotes(self, rest_api_destination):
        """Handle single quotes in rel attribute."""
        link_header = "<https://api.example.com?page=2>; rel='next'"
        result = rest_api_destination._extract_next_link(link_header)
        assert result == "https://api.example.com?page=2"

    def test_extract_next_link_with_parameters(self, rest_api_destination):
        """Handle URLs with query parameters."""
        link_header = (
            '<https://api.example.com/contacts?offset=50&limit=25&filter=active>; rel="next"'
        )
        result = rest_api_destination._extract_next_link(link_header)
        assert result == "https://api.example.com/contacts?offset=50&limit=25&filter=active"


class TestFetchPaginatedOffsetBased:
    """Tests for offset-based pagination strategy."""

    def test_fetch_paginated_offset_single_page(
        self, rest_api_destination, base_config, sync_options
    ):
        """Fetch single page with offset pagination."""
        config = RestApiDestinationConfig(
            **{
                **base_config.model_dump(),
                "pagination": OffsetPaginationConfig(
                    type="offset",
                    limit=10,
                    offset_param="offset",
                    limit_param="limit",
                    max_pages=100,
                ),
            }
        )

        # Mock httpx.Client
        mock_response = MagicMock()
        mock_response.json.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        mock_response.headers = {}

        with patch("drt.destinations.rest_api.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = mock_response

            result = rest_api_destination.fetch_paginated(config, {}, sync_options)

            assert len(result) == 2
            assert result[0]["id"] == 1
            assert result[1]["id"] == 2

    def test_fetch_paginated_offset_multiple_pages(
        self, rest_api_destination, base_config, sync_options
    ):
        """Fetch multiple pages with offset pagination."""
        config = RestApiDestinationConfig(
            **{
                **base_config.model_dump(),
                "pagination": OffsetPaginationConfig(
                    type="offset",
                    limit=2,
                    offset_param="offset",
                    limit_param="limit",
                    max_pages=100,
                ),
            }
        )

        # Mock responses for 2 pages
        mock_response_1 = MagicMock()
        mock_response_1.json.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        mock_response_1.headers = {}

        mock_response_2 = MagicMock()
        mock_response_2.json.return_value = [
            {"id": 3, "name": "Charlie"},
            {"id": 4, "name": "Diana"},
        ]
        mock_response_2.headers = {}

        # No more records on page 3
        mock_response_3 = MagicMock()
        mock_response_3.json.return_value = []
        mock_response_3.headers = {}

        with patch("drt.destinations.rest_api.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value.__enter__.return_value = mock_client
            mock_client.request.side_effect = [
                mock_response_1,
                mock_response_2,
                mock_response_3,
            ]

            result = rest_api_destination.fetch_paginated(config, {}, sync_options)

            assert len(result) == 4
            assert result[0]["id"] == 1
            assert result[3]["id"] == 4

    def test_fetch_paginated_offset_respects_max_pages(
        self, rest_api_destination, base_config, sync_options
    ):
        """Respect max_pages limit."""
        config = RestApiDestinationConfig(
            **{
                **base_config.model_dump(),
                "pagination": OffsetPaginationConfig(
                    type="offset",
                    limit=2,
                    offset_param="offset",
                    limit_param="limit",
                    max_pages=2,  # Only fetch 2 pages max
                ),
            }
        )

        # Mock 3 pages of responses
        mock_responses = [
            MagicMock(json=MagicMock(return_value=[{"id": i}, {"id": i + 1}]), headers={})
            for i in range(1, 7, 2)
        ]

        with patch("drt.destinations.rest_api.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value.__enter__.return_value = mock_client
            mock_client.request.side_effect = mock_responses

            result = rest_api_destination.fetch_paginated(config, {}, sync_options)

            # Should only have 2 pages worth of data
            assert len(result) == 4
            assert mock_client.request.call_count == 2


class TestFetchPaginatedCursorBased:
    """Tests for cursor-based pagination strategy."""

    def test_fetch_paginated_cursor_single_page(
        self, rest_api_destination, base_config, sync_options
    ):
        """Fetch single page with cursor pagination."""
        config = RestApiDestinationConfig(
            **{
                **base_config.model_dump(),
                "pagination": CursorPaginationConfig(
                    type="cursor",
                    limit=10,
                    cursor_param="after",
                    cursor_field="next_cursor",
                    max_pages=100,
                ),
            }
        )

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [{"id": 1, "name": "Alice"}],
            "next_cursor": None,  # No more pages
        }
        mock_response.headers = {}

        with patch("drt.destinations.rest_api.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = mock_response

            result = rest_api_destination.fetch_paginated(config, {}, sync_options)

            assert len(result) == 1
            assert result[0]["id"] == 1

    def test_fetch_paginated_cursor_multiple_pages(
        self, rest_api_destination, base_config, sync_options
    ):
        """Fetch multiple pages with cursor pagination."""
        config = RestApiDestinationConfig(
            **{
                **base_config.model_dump(),
                "pagination": CursorPaginationConfig(
                    type="cursor",
                    limit=2,
                    cursor_param="after",
                    cursor_field="next_cursor",
                    max_pages=100,
                ),
            }
        )

        # Page 1
        mock_response_1 = MagicMock()
        mock_response_1.json.return_value = {
            "data": [{"id": 1}, {"id": 2}],
            "next_cursor": "cursor_page2",
        }
        mock_response_1.headers = {}

        # Page 2
        mock_response_2 = MagicMock()
        mock_response_2.json.return_value = {
            "data": [{"id": 3}, {"id": 4}],
            "next_cursor": None,  # Last page
        }
        mock_response_2.headers = {}

        with patch("drt.destinations.rest_api.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value.__enter__.return_value = mock_client
            mock_client.request.side_effect = [mock_response_1, mock_response_2]

            result = rest_api_destination.fetch_paginated(config, {}, sync_options)

            assert len(result) == 4
            assert result[0]["id"] == 1
            assert result[3]["id"] == 4


class TestFetchPaginatedLinkHeader:
    """Tests for Link header-based pagination strategy."""

    def test_fetch_paginated_link_header_single_page(
        self, rest_api_destination, base_config, sync_options
    ):
        """Fetch single page with Link header pagination."""
        config = RestApiDestinationConfig(
            **{
                **base_config.model_dump(),
                "pagination": LinkHeaderPaginationConfig(
                    type="link_header",
                    max_pages=100,
                ),
            }
        )

        mock_response = MagicMock()
        mock_response.json.return_value = [{"id": 1, "name": "Alice"}]
        mock_response.headers = {"link": ""}  # No next link

        with patch("drt.destinations.rest_api.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = mock_response

            result = rest_api_destination.fetch_paginated(config, {}, sync_options)

            assert len(result) == 1
            assert result[0]["id"] == 1

    def test_fetch_paginated_link_header_multiple_pages(
        self, rest_api_destination, base_config, sync_options
    ):
        """Fetch multiple pages with Link header pagination."""
        config = RestApiDestinationConfig(
            **{
                **base_config.model_dump(),
                "pagination": LinkHeaderPaginationConfig(
                    type="link_header",
                    max_pages=100,
                ),
            }
        )

        # Page 1
        mock_response_1 = MagicMock()
        mock_response_1.json.return_value = [{"id": 1}, {"id": 2}]
        mock_response_1.headers = {"link": '<https://api.example.com/contacts?page=2>; rel="next"'}

        # Page 2
        mock_response_2 = MagicMock()
        mock_response_2.json.return_value = [{"id": 3}, {"id": 4}]
        mock_response_2.headers = {}  # No next link

        with patch("drt.destinations.rest_api.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value.__enter__.return_value = mock_client
            mock_client.request.side_effect = [mock_response_1, mock_response_2]

            result = rest_api_destination.fetch_paginated(config, {}, sync_options)

            assert len(result) == 4
            assert result[0]["id"] == 1
            assert result[3]["id"] == 4


class TestFetchPaginatedResponseFormats:
    """Tests for different response body formats."""

    def test_fetch_paginated_array_response(self, rest_api_destination, base_config, sync_options):
        """Handle response as raw array."""
        config = RestApiDestinationConfig(
            **{
                **base_config.model_dump(),
                "pagination": OffsetPaginationConfig(
                    type="offset",
                    limit=10,
                    offset_param="offset",
                    limit_param="limit",
                    max_pages=100,
                ),
            }
        )

        mock_response = MagicMock()
        mock_response.json.return_value = [{"id": 1}, {"id": 2}]
        mock_response.headers = {}

        with patch("drt.destinations.rest_api.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = mock_response

            result = rest_api_destination.fetch_paginated(config, {}, sync_options)

            assert len(result) == 2

    def test_fetch_paginated_records_key_response(
        self, rest_api_destination, base_config, sync_options
    ):
        """Handle response with 'records' key."""
        config = RestApiDestinationConfig(
            **{
                **base_config.model_dump(),
                "pagination": OffsetPaginationConfig(
                    type="offset",
                    limit=10,
                    offset_param="offset",
                    limit_param="limit",
                    max_pages=100,
                ),
            }
        )

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "records": [{"id": 1}, {"id": 2}],
            "page": 1,
        }
        mock_response.headers = {}

        with patch("drt.destinations.rest_api.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = mock_response

            result = rest_api_destination.fetch_paginated(config, {}, sync_options)

            assert len(result) == 2

    def test_fetch_paginated_data_key_response(
        self, rest_api_destination, base_config, sync_options
    ):
        """Handle response with 'data' key."""
        config = RestApiDestinationConfig(
            **{
                **base_config.model_dump(),
                "pagination": OffsetPaginationConfig(
                    type="offset",
                    limit=10,
                    offset_param="offset",
                    limit_param="limit",
                    max_pages=100,
                ),
            }
        )

        mock_response = MagicMock()
        mock_response.json.return_value = {
            "data": [{"id": 1}, {"id": 2}],
            "meta": {"total": 2},
        }
        mock_response.headers = {}

        with patch("drt.destinations.rest_api.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = mock_response

            result = rest_api_destination.fetch_paginated(config, {}, sync_options)

            assert len(result) == 2


class TestFetchPaginatedErrorHandling:
    """Tests for error handling during pagination."""

    def test_fetch_paginated_no_pagination_returns_empty(
        self, rest_api_destination, base_config, sync_options
    ):
        """Return empty list if pagination is None."""
        config = base_config  # No pagination configured

        result = rest_api_destination.fetch_paginated(config, {}, sync_options)

        assert result == []

    def test_fetch_paginated_http_error_stops_pagination(
        self, rest_api_destination, base_config, sync_options
    ):
        """Stop pagination gracefully on HTTP error."""
        config = RestApiDestinationConfig(
            **{
                **base_config.model_dump(),
                "pagination": OffsetPaginationConfig(
                    type="offset",
                    limit=10,
                    offset_param="offset",
                    limit_param="limit",
                    max_pages=100,
                ),
            }
        )

        # First response succeeds, second fails
        mock_response_1 = MagicMock()
        mock_response_1.json.return_value = [{"id": 1}]
        mock_response_1.headers = {}

        import httpx

        mock_response_2 = MagicMock(spec=httpx.Response)
        mock_response_2.raise_for_status.side_effect = httpx.HTTPStatusError(
            "404", request=MagicMock(), response=MagicMock()
        )

        with patch("drt.destinations.rest_api.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value.__enter__.return_value = mock_client
            mock_client.request.side_effect = [mock_response_1, mock_response_2]

            result = rest_api_destination.fetch_paginated(config, {}, sync_options)

            # Should have first page's data and stop
            assert len(result) == 1

    def test_fetch_paginated_json_decode_error_stops(
        self, rest_api_destination, base_config, sync_options
    ):
        """Stop pagination gracefully on JSON decode error."""
        config = RestApiDestinationConfig(
            **{
                **base_config.model_dump(),
                "pagination": OffsetPaginationConfig(
                    type="offset",
                    limit=10,
                    offset_param="offset",
                    limit_param="limit",
                    max_pages=100,
                ),
            }
        )

        mock_response = MagicMock()
        mock_response.json.side_effect = json.JSONDecodeError("msg", "doc", 0)
        mock_response.headers = {}

        with patch("drt.destinations.rest_api.httpx.Client") as mock_client_class:
            mock_client = MagicMock()
            mock_client_class.return_value.__enter__.return_value = mock_client
            mock_client.request.return_value = mock_response

            result = rest_api_destination.fetch_paginated(config, {}, sync_options)

            # Should return empty list on error
            assert result == []
