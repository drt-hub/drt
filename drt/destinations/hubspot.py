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
from drt.config.models import DestinationConfig, HubSpotDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template

_HUBSPOT_API = "https://api.hubapi.com/crm/v3/objects"


class HubSpotDestination:
    """Upsert records into HubSpot CRM via the v3 Objects API."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, HubSpotDestinationConfig)
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
        policy = sync_options.match_policy  # #757 — upsert | update_only | create_only
        result = SyncResult()
        # HubSpot rate limit: 100 req/10s for private apps
        rate_limiter = RateLimiter(min(sync_options.rate_limit.requests_per_second, 9))

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
                                record_preview=json.dumps(record, default=str)[:200],
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

                def do_write(
                    _url: str = upsert_url,
                    _headers: dict[str, Any] = headers,
                    _payload: dict[str, Any] = payload,
                ) -> httpx.Response | None:
                    """Write one record, honouring ``match_policy`` (#757).

                    HubSpot's create-vs-update outcome is the status code, which
                    gives exact skip detection without a separate lookup:

                    - ``update_only``: PATCH by ``idProperty`` directly. A 404
                      means no such record — skip, never create.
                    - ``create_only``: POST only. A 409 means it already exists
                      — skip, never overwrite.
                    - ``upsert`` (default): POST, then PATCH on 409 (unchanged).

                    Returns ``None`` when the record was skipped by the policy.
                    """
                    id_value = _payload["properties"].get(config.id_property)
                    patch_url = f"{_url}/{id_value}?idProperty={config.id_property}"

                    if policy == "update_only":
                        response = client.patch(
                            patch_url,
                            json={"properties": _payload["properties"]},
                            headers=_headers,
                        )
                        if response.status_code == 404:
                            return None  # no matching record — skip
                        response.raise_for_status()
                        return response

                    # create_only + upsert both POST first (idProperty dedupes).
                    response = client.post(_url, json=_payload, headers=_headers)
                    if response.status_code == 409:  # already exists
                        if policy == "create_only":
                            return None  # exists — skip, don't overwrite
                        response = client.patch(
                            patch_url,
                            json={"properties": _payload["properties"]},
                            headers=_headers,
                        )
                    response.raise_for_status()
                    return response

                try:
                    retry_config = resolve_retry(config.retry, sync_options)
                    written = with_retry(do_write, retry_config)
                    if written is None:
                        result.skipped += 1
                        result.skipped_no_match += 1  # #757 — policy declined
                    else:
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
                    if sync_options.on_error == "fail":
                        break
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
                    if sync_options.on_error == "fail":
                        break

        return result

    def supported_match_policies(self) -> frozenset[str]:
        """HubSpot honours all three ``match_policy`` values (#757).

        The v3 Objects API's POST-409 / PATCH-404 responses map exactly onto
        create-only / update-only, so skips are detected without a separate
        existence lookup. Declaring this makes the engine's
        ``MatchPolicyCapable`` guard accept a non-default policy here.
        """
        return frozenset({"upsert", "update_only", "create_only"})
