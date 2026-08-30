"""Discord destination — Webhook Integration.

Sends messages to a Discord channel via Webhook URL.
Supports plain text messages and Discord embeds via Jinja2 templates.

No extra dependencies required (uses httpx from core).

Example sync YAML:

    destination:
      type: discord
      webhook_url_env: DISCORD_WEBHOOK_URL
      message_template: "New signup: {{ row.name }} ({{ row.email }})"

Embed example:

    destination:
      type: discord
      webhook_url_env: DISCORD_WEBHOOK_URL
      embeds: true
      message_template: |
        {
          "embeds": [
            {
              "title": "{{ row.title }}",
              "description": "{{ row.description }}",
              "color": 3447003
            }
          ]
        }
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, DiscordDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter, resolve_rate_limiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import record_row_error
from drt.templates.renderer import render_template


class DiscordDestination:
    """Send records as Discord messages via Webhook."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, DiscordDestinationConfig)
        if not records:
            return SyncResult()

        webhook_url = resolve_env(config.webhook_url, config.webhook_url_env)
        if not webhook_url:
            raise ValueError("Discord destination: provide 'webhook_url' or set 'webhook_url_env'.")

        result = SyncResult()
        rate_limiter = resolve_rate_limiter(config, sync_options, limiter_factory=RateLimiter)
        retry_config = resolve_retry(config.retry, sync_options)

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()
                try:
                    rendered = render_template(config.message_template, record)
                    if config.embeds:
                        payload = json.loads(rendered)
                    else:
                        payload = {"content": rendered}

                    _url = webhook_url
                    _payload = payload

                    def do_post() -> httpx.Response:
                        response = client.post(_url, json=_payload)
                        response.raise_for_status()
                        return response

                    with_retry(do_post, retry_config)
                    result.success += 1
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
