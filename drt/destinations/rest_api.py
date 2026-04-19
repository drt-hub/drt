"""Generic REST API destination — Phase 2 implementation.

Features:
  - Auth header injection (Bearer, API Key, Basic) via AuthHandler
  - Token-bucket rate limiting via RateLimiter
  - Exponential backoff retry via with_retry
  - Row-level error tracking via SyncResult
  - Pagination support (offset, cursor, link header)
"""

from __future__ import annotations

import json
import re
from typing import Any

import httpx

from drt.config.models import (
    CursorPaginationConfig,
    DestinationConfig,
    LinkHeaderPaginationConfig,
    OffsetPaginationConfig,
    RestApiDestinationConfig,
    SyncOptions,
)
from drt.destinations.auth import AuthHandler
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template


class RestApiDestination:
    """Send records to any REST API endpoint."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, RestApiDestinationConfig)
        result = SyncResult()
        auth_headers = AuthHandler(config.auth).get_headers()
        headers = {**config.headers, **auth_headers}
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()

                body: dict[str, Any] | str
                if config.body_template:
                    try:
                        body = render_template(config.body_template, record)
                    except ValueError as e:
                        result.row_errors.append(
                            RowError(
                                batch_index=i,
                                record_preview=json.dumps(record)[:200],
                                http_status=None,
                                error_message=f"Template error: {e}",
                            )
                        )
                        result.failed += 1
                        continue
                else:
                    body = record

                def do_request(
                    _body: dict[str, Any] | str = body,
                    _headers: dict[str, Any] = headers,
                ) -> httpx.Response:
                    response = client.request(
                        method=config.method,
                        url=config.url,
                        headers=_headers,
                        json=_body if isinstance(_body, dict) else None,
                        content=_body.encode() if isinstance(_body, str) else None,
                    )
                    response.raise_for_status()
                    return response

                try:
                    with_retry(do_request, sync_options.retry)
                    result.success += 1
                except httpx.HTTPStatusError as e:
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(record)[:200],
                            http_status=e.response.status_code,
                            error_message=e.response.text[:500],
                        )
                    )
                    result.failed += 1
                except Exception as e:
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(record)[:200],
                            http_status=None,
                            error_message=str(e),
                        )
                    )
                    result.failed += 1

        # Return as SyncResult-compatible object
        return result

    def fetch_paginated(
        self,
        config: RestApiDestinationConfig,
        auth_headers: dict[str, str],
        sync_options: SyncOptions,
    ) -> list[dict[str, Any]]:
        """Fetch all pages of data from a paginated REST API endpoint.

        Args:
            config: REST API destination config with pagination settings.
            auth_headers: Authorization headers from AuthHandler.
            sync_options: Sync options for retry and rate limiting.

        Returns:
            Flattened list of all records from all pages.
        """
        if not config.pagination:
            return []

        all_records: list[dict[str, Any]] = []
        headers = {**config.headers, **auth_headers}
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)
        pagination = config.pagination

        with httpx.Client(timeout=30.0) as client:
            page = 0
            next_url: str | None = config.url
            next_cursor: str | None = None

            while page < pagination.max_pages and (next_url or next_cursor or page == 0):
                rate_limiter.acquire()

                # Build URL with pagination params
                request_params: dict[str, str] | None = None
                if isinstance(pagination, OffsetPaginationConfig):
                    offset = page * pagination.limit
                    request_params = {
                        pagination.offset_param: str(offset),
                        pagination.limit_param: str(pagination.limit),
                    }
                    url_with_params = config.url
                elif isinstance(pagination, CursorPaginationConfig):
                    url_with_params = config.url
                    request_params = {
                        pagination.limit_param: str(pagination.limit),
                    }
                    if page > 0:
                        if next_cursor:
                            request_params[pagination.cursor_param] = next_cursor
                        else:
                            break
                elif isinstance(pagination, LinkHeaderPaginationConfig):
                    url_with_params = next_url or config.url
                    request_params = None
                else:
                    break

                try:

                    def do_request(
                        _url: str = url_with_params,
                        _headers: dict[str, Any] = headers,
                        _method: str = config.method,
                        _params: dict[str, str] | None = request_params,
                    ) -> httpx.Response:
                        response = client.request(
                            method=_method,
                            url=_url,
                            headers=_headers,
                            params=_params,
                        )
                        response.raise_for_status()
                        return response

                    response = with_retry(do_request, sync_options.retry)

                    # Extract records from response
                    data = response.json()
                    if isinstance(data, list):
                        all_records.extend(data)
                    elif isinstance(data, dict) and "records" in data:
                        all_records.extend(data["records"])
                    elif isinstance(data, dict) and "data" in data:
                        items = data["data"]
                        if isinstance(items, list):
                            all_records.extend(items)

                    # Determine next page
                    if isinstance(pagination, OffsetPaginationConfig):
                        # Stop if fewer records than limit (no next page)
                        page_count = len(all_records) - (page * pagination.limit)
                        if page_count < pagination.limit:
                            break
                    elif isinstance(pagination, CursorPaginationConfig):
                        # Extract next cursor from response
                        if isinstance(data, dict):
                            next_cursor = data.get(pagination.cursor_field)
                            if not next_cursor:
                                break
                        else:
                            break
                    elif isinstance(pagination, LinkHeaderPaginationConfig):
                        # Parse Link header for next URL
                        link_header = response.headers.get("link", "")
                        next_url = self._extract_next_link(link_header)
                        if not next_url:
                            break

                    page += 1

                except (httpx.HTTPStatusError, json.JSONDecodeError, KeyError):
                    # Stop pagination on error
                    break

        return all_records

    @staticmethod
    def _extract_next_link(link_header: str) -> str | None:
        """Extract next URL from Link header.

        RFC 5988 format: <https://api.example.com?page=2>; rel="next"
        Returns the URL with rel="next".
        """
        links = link_header.split(",")
        for link in links:
            if re.search(r'rel\s*=\s*["\']next["\']', link, re.IGNORECASE):
                # Extract URL between < and >
                match = re.search(r"<([^>]+)>", link)
                if match:
                    return match.group(1)
        return None
