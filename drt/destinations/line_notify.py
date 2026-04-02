"""LINE Notify destination — https://notify-bot.line.me/

Sends messages to LINE Notify via personal access token.
Supports plain text messages via Jinja2 templates.

No extra dependencies required (uses httpx from core).

Example sync YAML:

    destination:
      type: line_notify
      token_env: LINE_NOTIFY_TOKEN
      message_template: "New signup: {{ row.name }} ({{ row.email }})"
"""

from __future__ import annotations

import os
from typing import Any

import httpx

from drt.config.models import DestinationConfig, LineNotifyDestinationConfig, RetryConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template

_DEFAULT_RETRY = RetryConfig(
    max_attempts=3,
    initial_backoff=1.0,
    retryable_status_codes=(429, 500, 502, 503, 504),
)

_LINE_NOTIFY_API = "https://notify-api.line.me/api/notify"


class LineNotifyDestination:
    """Send records as LINE Notify messages via personal access token."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, LineNotifyDestinationConfig)
        token = config.token or (
            os.environ.get(config.token_env) if config.token_env else None
        )
        if not token:
            raise ValueError(
                "LINE Notify destination: provide 'token' or set 'token_env'."
            )

        result = SyncResult()
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()
                try:
                    rendered = render_template(config.message_template, record)

                    def do_post() -> httpx.Response:
                        response = client.post(
                            _LINE_NOTIFY_API,
                            data={"message": rendered},
                            headers={"Authorization": f"Bearer {token}"},
                        )
                        response.raise_for_status()
                        return response

                    with_retry(do_post, _DEFAULT_RETRY)
                    result.success += 1
                except httpx.HTTPStatusError as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=str(record)[:200],
                            http_status=e.response.status_code,
                            error_message=e.response.text[:500],
                        )
                    )
                except Exception as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=str(record)[:200],
                            http_status=None,
                            error_message=str(e),
                        )
                    )

        return result
