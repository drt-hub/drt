"""Microsoft Teams destination — Incoming Webhook.

Sends messages to a Teams channel via Incoming Webhook URL.
Supports plain text and Adaptive Card payloads via Jinja2 templates.

No extra dependencies required (uses httpx from core).

Example sync YAML:

    destination:
      type: teams
      webhook_url_env: TEAMS_WEBHOOK_URL
      message_template: "New signup: {{ row.name }} ({{ row.email }})"

Adaptive Card example:

    destination:
      type: teams
      webhook_url_env: TEAMS_WEBHOOK_URL
      adaptive_card: true
      message_template: |
        {
          "type": "AdaptiveCard",
          "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
          "version": "1.4",
          "body": [
            {
              "type": "TextBlock",
              "text": "New user: {{ row.name }}",
              "weight": "Bolder"
            }
          ]
        }
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx

from drt.config.models import DestinationConfig, RetryConfig, SyncOptions, TeamsDestinationConfig
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


class TeamsDestination:
    """Send records as Microsoft Teams messages via Incoming Webhook."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, TeamsDestinationConfig)
        webhook_url = config.webhook_url or (
            os.environ.get(config.webhook_url_env) if config.webhook_url_env else None
        )
        if not webhook_url:
            raise ValueError(
                "Teams destination: provide 'webhook_url' or set 'webhook_url_env'."
            )

        result = SyncResult()
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()
                try:
                    rendered = render_template(config.message_template, record)
                    if config.adaptive_card:
                        card = json.loads(rendered)
                        # Teams webhook expects Adaptive Card wrapped in an attachment
                        payload = {
                            "type": "message",
                            "attachments": [
                                {
                                    "contentType": "application/vnd.microsoft.card.adaptive",
                                    "content": card,
                                }
                            ],
                        }
                    else:
                        payload = {"text": rendered}

                    _url = webhook_url
                    _payload = payload

                    def do_post() -> httpx.Response:
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
