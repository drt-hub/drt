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
import logging
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, SlackDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter, resolve_rate_limiter
from drt.destinations.retry import (
    DeliveryUnknownError,
    OperationSafety,
    RetryDirective,
    handle_blind_delivery_unknown,
    resolve_retry,
    with_retry,
)
from drt.destinations.row_errors import record_row_error
from drt.templates.renderer import render_template

logger = logging.getLogger(__name__)


class SlackDestination:
    """Send records as Slack messages via Incoming Webhook."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, SlackDestinationConfig)
        if not records:
            return SyncResult()

        webhook_url = resolve_env(config.webhook_url, config.webhook_url_env)
        if not webhook_url:
            raise ValueError("Slack destination: provide 'webhook_url' or set 'webhook_url_env'.")

        result = SyncResult()
        rate_limiter = resolve_rate_limiter(config, sync_options, limiter_factory=RateLimiter)
        retry_config = resolve_retry(config.retry, sync_options)

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()
                try:
                    rendered = render_template(config.message_template, record)
                    if config.block_kit:
                        payload = json.loads(rendered)
                    else:
                        payload = {"text": rendered}

                    _url = webhook_url
                    _payload = payload

                    def do_post() -> httpx.Response:
                        response = client.post(_url, json=_payload)
                        response.raise_for_status()
                        return response

                    def on_delivery_unknown(error: Exception, attempt: int) -> RetryDirective:
                        return handle_blind_delivery_unknown(
                            error,
                            attempt,
                            retry_creates=config.retry_creates,
                            max_attempts=retry_config.max_attempts,
                            destination="Slack",
                            operation="incoming-webhook POST",
                            logger=logger,
                        )

                    with_retry(
                        do_post,
                        retry_config,
                        operation=OperationSafety.BLIND_CREATE,
                        on_delivery_unknown=on_delivery_unknown,
                    )
                    result.success += 1
                except DeliveryUnknownError as e:
                    record_row_error(
                        result,
                        i,
                        json.dumps(record, default=str)[:200],
                        e,
                        http_status=e.http_status,
                        error_message=str(e),
                    )
                    if sync_options.on_error == "fail":
                        break
                except httpx.HTTPStatusError as e:
                    record_row_error(
                        result,
                        i,
                        json.dumps(record, default=str)[:200],
                        e,
                        http_status=e.response.status_code,
                        error_message=e.response.text[:500],
                    )
                    if sync_options.on_error == "fail":
                        break
                except Exception as e:
                    record_row_error(
                        result,
                        i,
                        json.dumps(record, default=str)[:200],
                        e,
                    )
                    if sync_options.on_error == "fail":
                        break

        return result
