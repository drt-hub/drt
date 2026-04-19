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
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import (
    DestinationConfig,
    NotionDestinationConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template

_NOTION_API = "https://api.notion.com/v1"
_NOTION_VERSION = "2022-06-28"
_DEFAULT_RETRY = RetryConfig(
    max_attempts=3,
    initial_backoff=1.0,
    retryable_status_codes=(429, 500, 502, 503, 504),
)


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
        # Notion rate limit: ~3 req/s for integrations
        rate_limiter = RateLimiter(min(sync_options.rate_limit.requests_per_second, 3))

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()

                # Build properties dict from template or raw record
                if config.properties_template:
                    try:
                        rendered = render_template(config.properties_template, record)
                        properties = json.loads(rendered)
                    except (ValueError, json.JSONDecodeError) as e:
                        result.failed += 1
                        result.row_errors.append(
                            RowError(
                                batch_index=i,
                                record_preview=json.dumps(record, default=str)[:200],
                                http_status=None,
                                error_message=f"properties_template error: {e}",
                            )
                        )
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

                try:
                    with_retry(do_create, _DEFAULT_RETRY)
                    result.success += 1
                except httpx.HTTPStatusError as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(record, default=str)[:200],
                            http_status=e.response.status_code,
                            error_message=e.response.text[:500],
                        )
                    )
                except Exception as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(record, default=str)[:200],
                            http_status=None,
                            error_message=str(e),
                        )
                    )

        return result
