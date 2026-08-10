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
import logging
from collections.abc import Iterator
from typing import Any

import httpx

from drt._http_utils import extract_next_link
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
from drt.destinations.rate_limiter import RateLimiter, resolve_rate_limiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template

logger = logging.getLogger(__name__)


def _resolve_dotted_path(value: Any, path: str) -> Any:
    """Resolve a dotted-key path through dictionaries only."""
    current = value
    for key in path.split("."):
        if not isinstance(current, dict) or key not in current:
            raise KeyError(key)
        current = current[key]
    return current


def _batch_error_message(error: Any) -> str:
    if isinstance(error, dict):
        for key in ("error", "message", "error_message"):
            if key in error:
                return str(error[key])
    return str(error)


def _chunk_records(
    records: list[dict[str, Any]], max_records: int | None
) -> Iterator[tuple[int, list[dict[str, Any]]]]:
    """Yield destination-local HTTP chunks with offsets into ``records``."""
    request_size = max_records or len(records)
    for offset in range(0, len(records), request_size):
        yield offset, records[offset : offset + request_size]


class RestApiDestination:
    """Send records to any REST API endpoint.

    Batch mode treats every 2xx response as success. Per-item errors embedded
    in a 2xx body (including HTTP 207-style payloads returned as 2xx) are not
    inspected.
    """

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, RestApiDestinationConfig)
        result = SyncResult()
        if not records:
            return result

        auth_headers = AuthHandler(config.auth).get_headers()
        headers = {**config.headers, **auth_headers}
        rate_limiter = resolve_rate_limiter(config, sync_options, limiter_factory=RateLimiter)
        retry_config = resolve_retry(config.retry, sync_options)

        with httpx.Client(timeout=30.0) as client:
            if config.body_mode == "batch":
                for offset, sub_chunk in _chunk_records(records, config.max_records_per_request):
                    rate_limiter.acquire()

                    try:
                        assert config.batch_template is not None
                        batch_body = render_template(config.batch_template, rows=sub_chunk)
                    except ValueError as e:
                        self._fail_chunk(
                            result,
                            sub_chunk,
                            offset,
                            http_status=None,
                            error_message=f"Template error: {e}",
                        )
                        if sync_options.on_error == "fail":
                            return result
                        continue

                    def do_batch_request(
                        _body: str = batch_body,
                        _headers: dict[str, Any] = headers,
                    ) -> httpx.Response:
                        response = client.request(
                            method=config.method,
                            url=config.url,
                            headers=_headers,
                            content=_body.encode(),
                        )
                        response.raise_for_status()
                        return response

                    try:
                        with_retry(do_batch_request, retry_config)
                        result.success += len(sub_chunk)
                    except httpx.HTTPStatusError as e:
                        failed_before = result.failed
                        self._handle_batch_http_error(
                            result,
                            sub_chunk,
                            offset,
                            e.response,
                            config.error_path,
                        )
                        if sync_options.on_error == "fail" and result.failed > failed_before:
                            return result
                    except Exception as e:
                        self._fail_chunk(
                            result,
                            sub_chunk,
                            offset,
                            http_status=None,
                            error_message=str(e),
                        )
                        if sync_options.on_error == "fail":
                            return result

                return result

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
                                record_preview=json.dumps(record, default=str)[:200],
                                http_status=None,
                                error_message=f"Template error: {e}",
                            )
                        )
                        result.failed += 1
                        if sync_options.on_error == "fail":
                            return result
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
                    with_retry(do_request, retry_config)
                    result.success += 1
                except httpx.HTTPStatusError as e:
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(record, default=str)[:200],
                            http_status=e.response.status_code,
                            error_message=e.response.text[:500],
                        )
                    )
                    result.failed += 1
                    if sync_options.on_error == "fail":
                        return result
                except Exception as e:
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(record, default=str)[:200],
                            http_status=None,
                            error_message=str(e),
                        )
                    )
                    result.failed += 1
                    if sync_options.on_error == "fail":
                        return result

        # Return as SyncResult-compatible object
        return result

    @staticmethod
    def _fail_chunk(
        result: SyncResult,
        records: list[dict[str, Any]],
        offset: int,
        *,
        http_status: int | None,
        error_message: str,
    ) -> None:
        for local_index, record in enumerate(records):
            result.row_errors.append(
                RowError(
                    batch_index=offset + local_index,
                    record_preview=json.dumps(record, default=str)[:200],
                    http_status=http_status,
                    error_message=error_message,
                )
            )
        result.failed += len(records)

    def _handle_batch_http_error(
        self,
        result: SyncResult,
        records: list[dict[str, Any]],
        offset: int,
        response: httpx.Response,
        error_path: str | None,
    ) -> None:
        if error_path is None:
            self._fail_chunk(
                result,
                records,
                offset,
                http_status=response.status_code,
                error_message=response.text[:500],
            )
            return

        try:
            errors = _resolve_dotted_path(response.json(), error_path)
            if not isinstance(errors, list) or len(errors) != len(records):
                raise ValueError("error list is missing or has the wrong length")
        except (json.JSONDecodeError, KeyError, TypeError, ValueError):
            logger.warning(
                "REST API batch error_path %r did not match the response shape; "
                "marking the whole request chunk as failed",
                error_path,
            )
            self._fail_chunk(
                result,
                records,
                offset,
                http_status=response.status_code,
                error_message=response.text[:500],
            )
            return

        for local_index, (record, error) in enumerate(zip(records, errors, strict=True)):
            if error is None:
                result.success += 1
                continue
            result.row_errors.append(
                RowError(
                    batch_index=offset + local_index,
                    record_preview=json.dumps(record, default=str)[:200],
                    http_status=response.status_code,
                    error_message=_batch_error_message(error),
                )
            )
            result.failed += 1

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
        rate_limiter = resolve_rate_limiter(config, sync_options, limiter_factory=RateLimiter)
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

                    response = with_retry(do_request, resolve_retry(config.retry, sync_options))

                    # Extract records from response
                    records_count_before = len(all_records)
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
                        page_record_count = len(all_records) - records_count_before
                        if page_record_count < pagination.limit:
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
                        next_url = extract_next_link(link_header)
                        if not next_url:
                            break

                    page += 1

                except (httpx.HTTPStatusError, json.JSONDecodeError, KeyError):
                    # Stop pagination on error
                    break

        return all_records
