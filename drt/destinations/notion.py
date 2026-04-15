"""Notion destination — Append rows to a Notion database.

Appends records as new pages in a Notion database.
Uses the Notion API to create pages with properties populated from a Jinja2 template.

Requires: NOTION_TOKEN (Integration token with read/write access).

Example sync YAML:

    destination:
      type: notion
      database_id_env: NOTION_DATABASE_ID
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
from drt.config.models import DestinationConfig, NotionDestinationConfig, RetryConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template

_NOTION_API = "https://api.notion.com/v1/pages"
_DEFAULT_RETRY = RetryConfig(
    max_attempts=3,
    initial_backoff=1.0,
    retryable_status_codes=(429, 500, 502, 503, 504),
)


class NotionDestination:
    """Append records as pages to a Notion database."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, NotionDestinationConfig)

        database_id = resolve_env(config.database_id, config.database_id_env)
        if not database_id:
            raise ValueError(
                "Notion destination: set database_id or database_id_env in the sync config."
            )

        token = resolve_env(config.auth.token, config.auth.token_env)
        if not token:
            raise ValueError(
                "Notion destination: set auth.token or auth.token_env in the sync config."
            )

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "Notion-Version": "2022-06-28",
        }

        result = SyncResult()
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()

                # Build properties dict via Jinja2 template
                if config.properties_template:
                    try:
                        rendered = render_template(config.properties_template, record)
                        properties = json.loads(rendered)
                    except (ValueError, json.JSONDecodeError) as e:
                        result.failed += 1
                        result.row_errors.append(
                            RowError(
                                batch_index=i,
                                record_preview=json.dumps(record)[:200],
                                http_status=None,
                                error_message=f"properties_template error: {e}",
                            )
                        )
                        continue
                else:
                    # Fallback: treat row values as properties (requires matching keys)
                    properties = {
                        k: {"rich_text": [{"text": {"content": str(v)}}]} for k, v in record.items()
                    }

                payload = {
                    "parent": {"database_id": database_id},
                    "properties": properties,
                }

                def create_page(
                    _url: str = _NOTION_API,
                    _headers: dict[str, Any] = headers,
                    _payload: dict[str, Any] = payload,
                ) -> httpx.Response:
                    response = client.post(_url, json=_payload, headers=_headers)
                    response.raise_for_status()
                    return response

                try:
                    with_retry(create_page, _DEFAULT_RETRY)
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
