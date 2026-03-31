"""Slack destination — Incoming Webhooks.

Sends messages to a Slack channel via Incoming Webhook URL.
Supports plain text and Block Kit payloads via Jinja2 templates.

No extra dependencies required (uses httpx from core).

Example sync YAML:

    destination:
      type: slack
      webhook_url_env: SLACK_WEBHOOK_URL
      message_template: "New signup: {{ row.name }} ({{ row.email }})"

Block Kit example:

    destination:
      type: slack
      webhook_url_env: SLACK_WEBHOOK_URL
      block_kit: true
      message_template: |
        {
          "blocks": [
            {
              "type": "section",
              "text": {
                "type": "mrkdwn",
                "text": "*New user:* {{ row.name }}\n{{ row.email }}"
              }
            }
          ]
        }
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx

from drt.config.models import DestinationConfig, RetryConfig, SlackDestinationConfig, SyncOptions
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


class SlackDestination:
    """Send records as Slack messages via Incoming Webhook."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, SlackDestinationConfig)
        webhook_url = config.webhook_url or (
            os.environ.get(config.webhook_url_env) if config.webhook_url_env else None
        )
        if not webhook_url:
            raise ValueError("Slack destination: provide 'webhook_url' or set 'webhook_url_env'.")
        resolved_url: str = webhook_url

        result = SyncResult()
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()
                try:
                    rendered = render_template(config.message_template, record)
                    if config.block_kit:
                        payload = json.loads(rendered)
                    else:
                        payload = {"text": rendered}

                    def do_post(
                        _url: str = resolved_url,
                        _payload: dict[str, Any] = payload,
                    ) -> httpx.Response:
                        response = client.post(_url, json=_payload)
                        response.raise_for_status()
                        return response

                    with_retry(do_post, _DEFAULT_RETRY)
                    result.success += 1
                except httpx.HTTPStatusError as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(record)[:200],
                            http_status=e.response.status_code,
                            error_message=e.response.text[:500],
                        )
                    )
                except Exception as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(record)[:200],
                            http_status=None,
                            error_message=str(e),
                        )
                    )

        return result
