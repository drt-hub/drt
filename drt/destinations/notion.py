"""Notion destination — append rows to a Notion database.

Creates pages in a Notion database via the Notion API.
Each record becomes a page with properties rendered from a Jinja2 template.

Requires: NOTION_TOKEN (internal integration token with database write access).

Example sync YAML:

    destination:
      type: notion
      database_id: "abc123def456"
      properties_template: |
        {
          "Name": {"title": [{"text": {"content": "{{ row.name }}"}}]},
          "Email": {"email": "{{ row.email }}"},
          "Revenue": {"number": {{ row.revenue }}},
          "Status": {"select": {"name": "{{ row.status }}"}}
        }
      auth:
        type: bearer
        token_env: NOTION_TOKEN
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import (
    DestinationConfig,
    NotionDestinationConfig,
    SyncOptions,
)
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import resolve_rate_limiter
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

_NOTION_API = "https://api.notion.com/v1"
_NOTION_VERSION = "2022-06-28"


class NotionDestination:
    """Append rows to a Notion database via the Notion API."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, NotionDestinationConfig)
        token = resolve_env(config.auth.token, config.auth.token_env)
        if not token:
            raise ValueError(
                "Notion destination: set NOTION_TOKEN env var "
                "or provide auth.token_env in the sync config."
            )

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Notion-Version": _NOTION_VERSION,
        }
        pages_url = f"{_NOTION_API}/pages"
        result = SyncResult()
        # Notion rate limit: ~3 req/s for integrations. Capped at resolution
        # time so the shared bucket is created already clamped.
        rate_limiter = resolve_rate_limiter(config, sync_options, max_requests_per_second=3)
        retry_config = resolve_retry(config.retry, sync_options)

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()

                # Build properties dict from template or raw record
                if config.properties_template:
                    try:
                        rendered = render_template(config.properties_template, record)
                        properties = json.loads(rendered)
                    except (ValueError, json.JSONDecodeError) as e:
                        record_row_error(
                            result,
                            i,
                            json.dumps(record, default=str)[:200],
                            e,
                            error_message=f"properties_template error: {e}",
                        )
                        if sync_options.on_error == "fail":
                            return result
                        continue
                else:
                    # Without a template, map simple scalar values to rich text
                    properties = {
                        key: {"rich_text": [{"text": {"content": str(val)}}]}
                        for key, val in record.items()
                        if val is not None
                    }

                payload: dict[str, Any] = {
                    "parent": {"database_id": config.database_id},
                    "properties": properties,
                }

                def do_create(
                    _url: str = pages_url,
                    _headers: dict[str, Any] = headers,
                    _payload: dict[str, Any] = payload,
                ) -> httpx.Response:
                    response = client.post(_url, json=_payload, headers=_headers)
                    response.raise_for_status()
                    return response

                def on_delivery_unknown(error: Exception, attempt: int) -> RetryDirective:
                    return handle_blind_delivery_unknown(
                        error,
                        attempt,
                        retry_creates=config.retry_creates,
                        max_attempts=retry_config.max_attempts,
                        destination="Notion",
                        operation="page create",
                        logger=logger,
                    )

                try:
                    with_retry(
                        do_create,
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
                        return result
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
                        return result
                except Exception as e:
                    record_row_error(
                        result,
                        i,
                        json.dumps(record, default=str)[:200],
                        e,
                    )
                    if sync_options.on_error == "fail":
                        return result

        return result
