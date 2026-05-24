"""REST API Source — extracts data from HTTP endpoints."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from typing import Any, cast

import httpx
from pydantic import TypeAdapter

from drt._http_utils import extract_next_link
from drt.config.credentials import ProfileConfig, RestApiProfile
from drt.config.models import (
    AuthConfig,
    CursorPaginationConfig,
    LinkHeaderPaginationConfig,
    OffsetPaginationConfig,
    PaginationConfig,
)
from drt.destinations.auth import AuthHandler
from drt.sources.base import Source

logger = logging.getLogger("drt")


class RestApiSource(Source):
    """Extract records from a REST API endpoint."""

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        """Extract records from the configured REST endpoint.

        The ``query`` argument is part of the ``Source`` Protocol (used by SQL
        sources for the SELECT statement) and is **ignored** here — for REST,
        the endpoint URL and pagination strategy come from ``config`` (the
        ``RestApiProfile`` block in ``profiles.yml``).
        """
        assert isinstance(config, RestApiProfile)

        auth_config: AuthConfig | None = None
        if config.auth:
            auth_config = TypeAdapter(AuthConfig).validate_python(config.auth)

        pagination_config: PaginationConfig | None = None
        if config.pagination:
            pagination_config = TypeAdapter(PaginationConfig).validate_python(config.pagination)

        auth_headers = AuthHandler(auth_config).get_headers()
        headers = {**auth_headers}

        # Setup httpx client
        with httpx.Client(timeout=30.0) as client:
            page = 0
            next_url: str | None = config.url
            next_cursor: str | None = None

            while True:
                # Build URL with pagination params
                request_params: dict[str, str] | None = None

                if isinstance(pagination_config, OffsetPaginationConfig):
                    offset = page * pagination_config.limit
                    request_params = {
                        pagination_config.offset_param: str(offset),
                        pagination_config.limit_param: str(pagination_config.limit),
                    }
                    url_with_params = config.url
                elif isinstance(pagination_config, CursorPaginationConfig):
                    url_with_params = config.url
                    request_params = {
                        pagination_config.limit_param: str(pagination_config.limit),
                    }
                    if page > 0:
                        if next_cursor:
                            request_params[pagination_config.cursor_param] = next_cursor
                        else:
                            break
                elif isinstance(pagination_config, LinkHeaderPaginationConfig):
                    url_with_params = next_url or config.url
                    request_params = None
                else:
                    url_with_params = config.url
                    if page > 0:
                        break  # No pagination, only 1 page

                response = client.request(
                    method="GET",
                    url=url_with_params,
                    headers=headers,
                    params=request_params,
                )
                response.raise_for_status()

                # Extract records from response
                data = response.json()

                # Apply result_path if configured
                records = self._extract_records(data, config.result_path)

                if not records:
                    break

                yield from records

                # Determine next page
                if isinstance(pagination_config, OffsetPaginationConfig):
                    # Stop if fewer records than limit (no next page)
                    if len(records) < pagination_config.limit:
                        break
                elif isinstance(pagination_config, CursorPaginationConfig):
                    # Extract next cursor from response
                    if isinstance(data, dict):
                        next_cursor = data.get(pagination_config.cursor_field)
                        if not next_cursor:
                            break
                    else:
                        break
                elif isinstance(pagination_config, LinkHeaderPaginationConfig):
                    # Parse Link header for next URL
                    link_header = response.headers.get("link", "")
                    next_url = extract_next_link(link_header)
                    if not next_url:
                        break

                page += 1

                if pagination_config and getattr(pagination_config, "max_pages", None):
                    if page >= getattr(pagination_config, "max_pages"):
                        break

    def _extract_records(self, data: Any, result_path: str | None) -> list[dict[str, Any]]:
        """Extract records array from JSON data using optional dot notation."""
        if not result_path:
            # Default behavior if no result_path:
            if isinstance(data, list):
                return cast(list[dict[str, Any]], data)
            if isinstance(data, dict) and "records" in data:
                return cast(list[dict[str, Any]], data["records"])
            if isinstance(data, dict) and "data" in data:
                items = data["data"]
                if isinstance(items, list):
                    return cast(list[dict[str, Any]], items)
            if isinstance(data, dict):
                logger.debug(
                    "REST source: no records array found in response; "
                    "wrapping single dict as one record (keys=%s)",
                    list(data.keys()),
                )
                return [data]
            return []

        # Simple dot notation resolution
        current = data
        for part in result_path.split("."):
            if isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return []

        if isinstance(current, list):
            return cast(list[dict[str, Any]], current)
        if isinstance(current, dict):
            return [current]
        return []

    def test_connection(self, config: ProfileConfig) -> bool:
        assert isinstance(config, RestApiProfile)

        auth_config: AuthConfig | None = None
        if config.auth:
            auth_config = TypeAdapter(AuthConfig).validate_python(config.auth)

        auth_headers = AuthHandler(auth_config).get_headers()

        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.request("GET", config.url, headers=auth_headers)
                response.raise_for_status()
                return True
        except Exception:
            return False
