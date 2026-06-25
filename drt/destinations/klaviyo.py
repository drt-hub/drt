"""Klaviyo destination — upsert profiles via the Klaviyo v3 API.

Syncs DWH customer rows into Klaviyo profiles (the common Reverse-ETL pattern:
push LTV / churn-risk / segment attributes to the marketing platform). Each row
is upserted by **email**:

1. ``POST /api/profiles/`` to create.
2. On ``409`` (the profile already exists), the existing id is read from the
   error's ``meta.duplicate_profile_id`` and the profile is updated with
   ``PATCH /api/profiles/{id}/``.
3. If ``list_id`` is set, the profile is added to that Klaviyo list.

Auth is an API key (``Authorization: Klaviyo-API-Key <key>``) plus the
``revision`` header. No extra dependencies beyond core ``httpx``. Per-record
calls — set ``sync.rate_limit`` to respect Klaviyo's limit (75 req/s).

Example sync YAML:

    destination:
      type: klaviyo
      api_key_env: KLAVIYO_API_KEY
      email_field: email
      properties_template: |
        {"ltv_segment": "{{ row.ltv_segment }}", "plan": "{{ row.plan }}"}
      list_id_env: KLAVIYO_LIST_ID   # optional

``sync.mode: mirror`` / event tracking are not implemented — follow-ups.
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import (
    DestinationConfig,
    KlaviyoDestinationConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template

_BASE = "https://a.klaviyo.com/api"


class KlaviyoDestination:
    """Upsert records into Klaviyo profiles."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, KlaviyoDestinationConfig)
        if not records:
            return SyncResult()

        api_key = resolve_env(config.api_key, config.api_key_env)
        if not api_key:
            raise ValueError(
                "Klaviyo destination: provide api_key or set the env var "
                f"named in api_key_env ({config.api_key_env!r})."
            )
        list_id = resolve_env(config.list_id, config.list_id_env)
        headers = {
            "Authorization": f"Klaviyo-API-Key {api_key}",
            "revision": config.revision,
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        retry_config = resolve_retry(config.retry, sync_options)
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)
        result = SyncResult()

        with httpx.Client(timeout=30.0) as client:
            for index, record in enumerate(records):
                try:
                    rate_limiter.acquire()
                    self._upsert(client, config, headers, record, list_id, retry_config)
                    result.success += 1
                except httpx.HTTPStatusError as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=index,
                            record_preview=str(record)[:200],
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
                            batch_index=index,
                            record_preview=str(record)[:200],
                            http_status=None,
                            error_message=str(e),
                        )
                    )
                    if sync_options.on_error == "fail":
                        break

        return result

    def _upsert(
        self,
        client: httpx.Client,
        config: KlaviyoDestinationConfig,
        headers: dict[str, str],
        record: dict[str, Any],
        list_id: str | None,
        retry_config: RetryConfig,
    ) -> None:
        email = record.get(config.email_field)
        if email is None or str(email).strip() == "":
            raise ValueError(f"Row missing email field {config.email_field!r}.")

        attributes: dict[str, Any] = {"email": str(email)}
        properties = self._properties(record, config)
        if properties:
            attributes["properties"] = properties

        def _create() -> httpx.Response:
            resp = client.post(
                f"{_BASE}/profiles/",
                headers=headers,
                json={"data": {"type": "profile", "attributes": attributes}},
            )
            if resp.status_code == 409:
                return resp  # existing profile — handled below, outside retry
            resp.raise_for_status()
            return resp

        resp = with_retry(_create, retry_config)

        if resp.status_code == 409:
            profile_id = _duplicate_id(resp)
            if not profile_id:
                resp.raise_for_status()  # can't recover the id — surface the 409

            def _patch() -> httpx.Response:
                r = client.patch(
                    f"{_BASE}/profiles/{profile_id}/",
                    headers=headers,
                    json={
                        "data": {
                            "type": "profile",
                            "id": profile_id,
                            "attributes": attributes,
                        }
                    },
                )
                r.raise_for_status()
                return r

            with_retry(_patch, retry_config)
        else:
            profile_id = _created_id(resp)

        if list_id and profile_id:
            self._add_to_list(client, headers, list_id, profile_id, retry_config)

    def _add_to_list(
        self,
        client: httpx.Client,
        headers: dict[str, str],
        list_id: str,
        profile_id: str,
        retry_config: RetryConfig,
    ) -> None:
        def _post() -> httpx.Response:
            r = client.post(
                f"{_BASE}/lists/{list_id}/relationships/profiles/",
                headers=headers,
                json={"data": [{"type": "profile", "id": profile_id}]},
            )
            r.raise_for_status()
            return r

        with_retry(_post, retry_config)

    @staticmethod
    def _properties(
        record: dict[str, Any], config: KlaviyoDestinationConfig
    ) -> dict[str, Any]:
        if config.properties_template:
            rendered = render_template(config.properties_template, record)
            parsed = json.loads(rendered)
            return parsed if isinstance(parsed, dict) else {}
        return {
            k: v
            for k, v in record.items()
            if k != config.email_field and v is not None
        }

    def test_connection(self, config: DestinationConfig) -> None:
        """Test connectivity by listing accounts (a cheap authenticated GET)."""
        assert isinstance(config, KlaviyoDestinationConfig)
        api_key = resolve_env(config.api_key, config.api_key_env)
        if not api_key:
            raise ValueError("Klaviyo destination: missing api_key.")
        headers = {
            "Authorization": f"Klaviyo-API-Key {api_key}",
            "revision": config.revision,
            "Accept": "application/json",
        }
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(f"{_BASE}/accounts/", headers=headers)
            resp.raise_for_status()


def _duplicate_id(resp: httpx.Response) -> str | None:
    try:
        errors = resp.json().get("errors", [])
        for err in errors:
            dup = err.get("meta", {}).get("duplicate_profile_id")
            if dup:
                return str(dup)
    except Exception:
        return None
    return None


def _created_id(resp: httpx.Response) -> str | None:
    try:
        return str(resp.json()["data"]["id"])
    except Exception:
        return None
