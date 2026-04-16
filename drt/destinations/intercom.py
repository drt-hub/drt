"""Intercom destination — Create or update contacts/leads.

Uses Intercom REST API v2.0 to upsert contacts.

Docs:
https://developers.intercom.com/intercom-api-reference/reference/create-contact
https://developers.intercom.com/intercom-api-reference/reference/update-contact

Auth:
- Bearer token (INTERCOM_TOKEN)

Example sync YAML:

    destination:
      type: intercom
      auth:
        type: bearer
        token_env: INTERCOM_TOKEN
      properties_template: |
        {
          "email": "{{ row.email }}",
          "name": "{{ row.name }}",
          "custom_attributes": {
            "plan": "{{ row.plan }}"
          }
        }
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx

from drt.config.models import (
    DestinationConfig,
    IntercomDestinationConfig,
    SyncOptions,
)
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template


class IntercomDestination:
    """Send records as Intercom contacts (create/update)."""

    BASE_URL = "https://api.intercom.io/contacts"

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, IntercomDestinationConfig)

        token = config.auth.token or (
            os.environ.get(config.auth.token_env) if config.auth.token_env else None
        )

        if not token:
            raise ValueError(
                "Intercom destination: missing bearer token (auth.token or INTERCOM_TOKEN env)."
            )

        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

        result = SyncResult()
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)
        retry_config = sync_options.retry

        with httpx.Client(timeout=30.0, headers=headers) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()

                try:
                    rendered = render_template(config.properties_template, record)

                    try:
                        payload = json.loads(rendered)
                    except json.JSONDecodeError as e:
                        raise ValueError(f"Invalid Intercom JSON payload: {e}")

                    def do_request() -> httpx.Response:
                        response = client.post(self.BASE_URL, json=payload)
                        response.raise_for_status()
                        return response

                    with_retry(do_request, retry_config)

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
                    if sync_options.on_error == "fail":
                        raise

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
                    if sync_options.on_error == "fail":
                        raise

        return result