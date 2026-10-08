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
import re
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import (
    DestinationConfig,
    IntercomDestinationConfig,
    SyncOptions,
)
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter, RateLimiterBackend, resolve_rate_limiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import record_row_error
from drt.templates.renderer import render_template


class IntercomDestination:
    """Send records as Intercom contacts (create/update)."""

    BASE_URL = "https://api.intercom.io/contacts"
    SEARCH_URL = f"{BASE_URL}/search"

    @staticmethod
    def _conflicting_contact_id(response: httpx.Response) -> tuple[bool, str | None]:
        """Return whether a 409 is Intercom's duplicate-contact response and its ID."""
        if response.status_code != 409:
            return False, None
        try:
            errors = response.json().get("errors", [])
        except (AttributeError, ValueError):
            return False, None
        conflict_found = False
        for error in errors:
            if not isinstance(error, dict) or error.get("code") != "conflict":
                continue
            conflict_found = True
            message = error.get("message")
            if isinstance(message, str):
                match = re.search(r"\bid=([A-Za-z0-9_-]+)\b", message)
                if match:
                    return True, match.group(1)
        return conflict_found, None

    def _find_contact_id(
        self,
        client: httpx.Client,
        payload: dict[str, Any],
        rate_limiter: RateLimiterBackend,
    ) -> str | None:
        """Resolve one existing contact for ``update_only`` without creating it."""
        direct_id = payload.get("id")
        if direct_id not in (None, ""):
            return str(direct_id)

        filters = [
            {"field": field, "operator": "=", "value": str(payload[field])}
            for field in ("external_id", "email")
            if payload.get(field) not in (None, "")
        ]
        if not filters:
            raise ValueError(
                "Intercom destination: sync.match_policy: update_only requires "
                "properties_template to render a non-empty id, external_id, or email."
            )

        query: dict[str, Any]
        if len(filters) == 1:
            query = filters[0]
        else:
            query = {"operator": "OR", "value": filters}

        rate_limiter.acquire()
        response = client.post(
            self.SEARCH_URL,
            json={"query": query, "pagination": {"per_page": 2}},
        )
        response.raise_for_status()
        try:
            data = response.json()["data"]
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError("Invalid Intercom contact-search response: missing data list.") from e
        if not isinstance(data, list):
            raise ValueError("Invalid Intercom contact-search response: data is not a list.")

        contact_ids: set[str] = set()
        for contact in data:
            if not isinstance(contact, dict) or contact.get("id") in (None, ""):
                raise ValueError("Invalid Intercom contact-search response: contact is missing id.")
            contact_ids.add(str(contact["id"]))
        if len(contact_ids) > 1:
            raise ValueError(
                "Intercom destination: update_only matched multiple contacts; "
                "render a unique id or external_id instead of an ambiguous email."
            )
        return next(iter(contact_ids), None)

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, IntercomDestinationConfig)
        if not records:
            return SyncResult()

        from drt.config.models import BearerAuth

        assert isinstance(config.auth, BearerAuth)
        token = resolve_env(config.auth.token, config.auth.token_env)

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
        rate_limiter = resolve_rate_limiter(config, sync_options, limiter_factory=RateLimiter)
        retry_config = resolve_retry(config.retry, sync_options)
        policy = sync_options.match_policy

        with httpx.Client(timeout=30.0, headers=headers) as client:
            for i, record in enumerate(records):
                try:
                    rendered = render_template(config.properties_template, record)

                    try:
                        payload = json.loads(rendered)
                    except json.JSONDecodeError as e:
                        raise ValueError(f"Invalid Intercom JSON payload: {e}")
                    if not isinstance(payload, dict):
                        raise ValueError("Invalid Intercom JSON payload: expected an object.")

                    def do_request() -> httpx.Response | None:
                        if policy == "update_only":
                            contact_id = self._find_contact_id(client, payload, rate_limiter)
                            if contact_id is None:
                                return None
                            rate_limiter.acquire()
                            response = client.put(
                                f"{self.BASE_URL}/{contact_id}",
                                json={key: value for key, value in payload.items() if key != "id"},
                            )
                            if response.status_code == 404:
                                return None
                            response.raise_for_status()
                            return response

                        rate_limiter.acquire()
                        response = client.post(self.BASE_URL, json=payload)
                        is_conflict, contact_id = self._conflicting_contact_id(response)
                        if is_conflict and policy == "create_only":
                            return None
                        if is_conflict and policy == "upsert":
                            if contact_id is None:
                                raise ValueError(
                                    "Invalid Intercom conflict response: "
                                    "missing existing contact id."
                                )
                            rate_limiter.acquire()
                            response = client.put(
                                f"{self.BASE_URL}/{contact_id}",
                                json={key: value for key, value in payload.items() if key != "id"},
                            )
                        response.raise_for_status()
                        return response

                    written = with_retry(do_request, retry_config)
                    if written is None:
                        result.skipped += 1
                        result.skipped_no_match += 1
                    else:
                        result.success += 1

                except httpx.HTTPStatusError as e:
                    record_row_error(
                        result,
                        i,
                        str(record)[:200],
                        e,
                        http_status=e.response.status_code,
                        error_message=e.response.text[:500],
                    )
                    if sync_options.on_error == "fail":
                        raise

                except Exception as e:
                    record_row_error(
                        result,
                        i,
                        str(record)[:200],
                        e,
                    )
                    if sync_options.on_error == "fail":
                        raise

        return result

    def supported_match_policies(self) -> frozenset[str]:
        """Intercom honours all three ``match_policy`` values (#757)."""
        return frozenset({"upsert", "update_only", "create_only"})
