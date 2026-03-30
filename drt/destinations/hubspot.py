"""HubSpot destination — Contacts, Deals, Companies upsert.

Upserts records into HubSpot CRM objects using the HubSpot v3 API.
Deduplicates by a configurable ID property (default: email for contacts).

Requires: HUBSPOT_TOKEN (Private App token with CRM write scope).

Example sync YAML — contacts:

    destination:
      type: hubspot
      object_type: contacts
      id_property: email
      properties_template: |
        {
          "email": "{{ row.email }}",
          "firstname": "{{ row.first_name }}",
          "lastname": "{{ row.last_name }}",
          "company": "{{ row.company }}",
          "lifecyclestage": "lead"
        }
      auth:
        type: bearer
        token_env: HUBSPOT_TOKEN

Example sync YAML — deals:

    destination:
      type: hubspot
      object_type: deals
      id_property: dealname
      properties_template: |
        {
          "dealname": "{{ row.deal_name }}",
          "amount": "{{ row.amount }}",
          "dealstage": "{{ row.stage }}"
        }
      auth:
        type: bearer
        token_env: HUBSPOT_TOKEN
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import HubSpotDestinationConfig, RetryConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import DetailedSyncResult, RowError
from drt.templates.renderer import render_template

_HUBSPOT_API = "https://api.hubapi.com/crm/v3/objects"
_DEFAULT_RETRY = RetryConfig(
    max_attempts=3,
    initial_backoff=1.0,
    retryable_status_codes=(429, 500, 502, 503, 504),
)


class HubSpotDestination:
    """Upsert records into HubSpot CRM via the v3 Objects API."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: HubSpotDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        token = resolve_env(config.auth.token, config.auth.token_env)
        if not token:
            raise ValueError(
                "HubSpot destination: set HUBSPOT_TOKEN env var "
                "or provide auth.token_env in the sync config."
            )

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        upsert_url = f"{_HUBSPOT_API}/{config.object_type}"
        result = DetailedSyncResult()
        # HubSpot rate limit: 100 req/10s for private apps
        rate_limiter = RateLimiter(
            min(sync_options.rate_limit.requests_per_second, 9)
        )

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()

                # Build properties dict
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
                    properties = record

                payload = {
                    "properties": properties,
                    "idProperty": config.id_property,
                }

                def do_upsert(
                    _url: str = upsert_url,
                    _headers: dict[str, Any] = headers,
                    _payload: dict[str, Any] = payload,
                ) -> httpx.Response:
                    # HubSpot upsert: POST with idProperty deduplicates
                    response = client.post(_url, json=_payload, headers=_headers)
                    # 409 Conflict = already exists, update instead
                    if response.status_code == 409:
                        id_value = _payload["properties"].get(config.id_property)
                        patch_url = f"{_url}/{id_value}?idProperty={config.id_property}"
                        response = client.patch(
                            patch_url,
                            json={"properties": _payload["properties"]},
                            headers=_headers,
                        )
                    response.raise_for_status()
                    return response

                try:
                    with_retry(do_upsert, _DEFAULT_RETRY)
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

        return result  # type: ignore[return-value]
