"""Generic REST API destination — Phase 2 implementation.

Features:
  - Auth header injection (Bearer, API Key, Basic) via AuthHandler
  - Token-bucket rate limiting via RateLimiter
  - Exponential backoff retry via with_retry
  - Row-level error tracking via DetailedSyncResult
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from drt.config.models import RestApiDestinationConfig, SyncOptions
from drt.destinations.auth import AuthHandler
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import DetailedSyncResult, RowError
from drt.templates.renderer import render_template


class RestApiDestination:
    """Send records to any REST API endpoint."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: RestApiDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        result = DetailedSyncResult()
        auth_headers = AuthHandler(config.auth).get_headers()
        headers = {**config.headers, **auth_headers}
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)

        with httpx.Client() as client:
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
        # DetailedSyncResult has all SyncResult fields + row_errors
        return result  # type: ignore[return-value]
