"""Zendesk destination — users and organizations upsert.

Syncs rows into Zendesk Support through the Ticketing API.

Example sync YAML — users:

    destination:
      type: zendesk
      subdomain_env: ZENDESK_SUBDOMAIN
      email_env: ZENDESK_EMAIL
      api_token_env: ZENDESK_API_TOKEN
      object: user
      id_field: zendesk_user_id
      custom_fields_template: |
        {
          "health_score": "{{ row.health_score }}",
          "plan": "{{ row.plan }}"
        }

Example sync YAML — organizations:

    destination:
      type: zendesk
      subdomain: example
      email_env: ZENDESK_EMAIL
      api_token_env: ZENDESK_API_TOKEN
      object: organization
      id_field: zendesk_organization_id
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, RetryConfig, SyncOptions, ZendeskDestinationConfig
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template

_ZENDESK_API_TEMPLATE = "https://{subdomain}.zendesk.com/api/v2"
_ZENDESK_MAX_USER_BATCH_SIZE = 100
_ZENDESK_MAX_REQUESTS_PER_SECOND = 11
_USER_CUSTOM_FIELDS_KEY = "user_fields"
_ORGANIZATION_CUSTOM_FIELDS_KEY = "organization_fields"
_CANONICAL_ID_FIELDS = {"id", "external_id"}


class ZendeskDestination:
    """Upsert Zendesk users or organizations from sync records."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, ZendeskDestinationConfig)

        credentials = _resolve_credentials(config)
        base_url = _base_url(credentials["subdomain"])
        auth = httpx.BasicAuth(
            username=f"{credentials['email']}/token",
            password=credentials["api_token"],
        )
        headers = {"Content-Type": "application/json"}
        retry_config = resolve_retry(config.retry, sync_options)
        rate_limiter = RateLimiter(
            min(sync_options.rate_limit.requests_per_second, _ZENDESK_MAX_REQUESTS_PER_SECOND)
        )
        result = SyncResult()

        with httpx.Client(timeout=30.0) as client:
            if config.object == "user":
                self._load_users(
                    records,
                    client=client,
                    config=config,
                    sync_options=sync_options,
                    base_url=base_url,
                    auth=auth,
                    headers=headers,
                    rate_limiter=rate_limiter,
                    result=result,
                    retry_config=retry_config,
                )
            else:
                self._load_organizations(
                    records,
                    client=client,
                    config=config,
                    sync_options=sync_options,
                    base_url=base_url,
                    auth=auth,
                    headers=headers,
                    rate_limiter=rate_limiter,
                    result=result,
                    retry_config=retry_config,
                )

        return result

    def _load_users(
        self,
        records: list[dict[str, Any]],
        *,
        client: httpx.Client,
        config: ZendeskDestinationConfig,
        sync_options: SyncOptions,
        base_url: str,
        auth: httpx.BasicAuth,
        headers: dict[str, str],
        rate_limiter: RateLimiter,
        result: SyncResult,
        retry_config: RetryConfig,
    ) -> None:
        indexed_payloads: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
        for index, record in enumerate(records):
            try:
                payload = _build_zendesk_object(record, config, _USER_CUSTOM_FIELDS_KEY)
                indexed_payloads.append((index, record, payload))
            except Exception as exc:
                _add_row_error(result, index, record, None, str(exc))
                if sync_options.on_error == "fail":
                    return

        url = f"{base_url}/users/create_or_update_many.json"
        for chunk in _chunks(indexed_payloads, _ZENDESK_MAX_USER_BATCH_SIZE):
            users = [payload for _, _, payload in chunk]

            def do_post(_users: list[dict[str, Any]] = users) -> httpx.Response:
                response = client.post(
                    url,
                    json={"users": _users},
                    auth=auth,
                    headers=headers,
                )
                response.raise_for_status()
                return response

            try:
                rate_limiter.acquire()
                with_retry(do_post, retry_config)
                result.success += len(chunk)
            except httpx.HTTPStatusError as exc:
                for index, record, _ in chunk:
                    _add_row_error(
                        result,
                        index,
                        record,
                        exc.response.status_code,
                        exc.response.text[:500],
                    )
                if sync_options.on_error == "fail":
                    break
            except Exception as exc:
                for index, record, _ in chunk:
                    _add_row_error(result, index, record, None, str(exc))
                if sync_options.on_error == "fail":
                    break

    def _load_organizations(
        self,
        records: list[dict[str, Any]],
        *,
        client: httpx.Client,
        config: ZendeskDestinationConfig,
        sync_options: SyncOptions,
        base_url: str,
        auth: httpx.BasicAuth,
        headers: dict[str, str],
        rate_limiter: RateLimiter,
        result: SyncResult,
        retry_config: RetryConfig,
    ) -> None:
        url = f"{base_url}/organizations/create_or_update.json"
        for index, record in enumerate(records):
            try:
                organization = _build_zendesk_object(
                    record,
                    config,
                    _ORGANIZATION_CUSTOM_FIELDS_KEY,
                )
            except Exception as exc:
                _add_row_error(result, index, record, None, str(exc))
                if sync_options.on_error == "fail":
                    break
                continue

            def do_post(_organization: dict[str, Any] = organization) -> httpx.Response:
                response = client.post(
                    url,
                    json={"organization": _organization},
                    auth=auth,
                    headers=headers,
                )
                response.raise_for_status()
                return response

            try:
                rate_limiter.acquire()
                with_retry(do_post, retry_config)
                result.success += 1
            except httpx.HTTPStatusError as exc:
                _add_row_error(
                    result,
                    index,
                    record,
                    exc.response.status_code,
                    exc.response.text[:500],
                )
                if sync_options.on_error == "fail":
                    break
            except Exception as exc:
                _add_row_error(result, index, record, None, str(exc))
                if sync_options.on_error == "fail":
                    break


def _resolve_credentials(config: ZendeskDestinationConfig) -> dict[str, str]:
    subdomain = resolve_env(config.subdomain, config.subdomain_env)
    email = resolve_env(config.email, config.email_env)
    api_token = resolve_env(config.api_token, config.api_token_env)
    missing = []
    if not subdomain:
        missing.append("ZENDESK_SUBDOMAIN")
    if not email:
        missing.append("ZENDESK_EMAIL")
    if not api_token:
        missing.append("ZENDESK_API_TOKEN")
    if missing:
        raise ValueError(
            "Zendesk destination: provide subdomain/email/api_token "
            f"or set env vars: {', '.join(missing)}."
        )
    assert subdomain is not None
    assert email is not None
    assert api_token is not None
    return {
        "subdomain": subdomain.strip(),
        "email": email,
        "api_token": api_token,
    }


def _base_url(subdomain: str) -> str:
    return _ZENDESK_API_TEMPLATE.format(subdomain=subdomain).rstrip("/")


def _build_zendesk_object(
    record: dict[str, Any],
    config: ZendeskDestinationConfig,
    custom_fields_key: str,
) -> dict[str, Any]:
    payload = dict(record)
    if config.id_field:
        id_value = record.get(config.id_field)
        should_copy_to_id = (
            _has_value(id_value)
            and "id" not in payload
            and config.id_field not in _CANONICAL_ID_FIELDS
        )
        if should_copy_to_id:
            payload["id"] = id_value
        if config.id_field not in _CANONICAL_ID_FIELDS:
            payload.pop(config.id_field, None)

    custom_fields = _render_custom_fields(config, record)
    if custom_fields:
        existing = payload.get(custom_fields_key)
        if isinstance(existing, dict):
            payload[custom_fields_key] = {**existing, **custom_fields}
        else:
            payload[custom_fields_key] = custom_fields

    return payload


def _render_custom_fields(
    config: ZendeskDestinationConfig,
    record: dict[str, Any],
) -> dict[str, Any]:
    if not config.custom_fields_template:
        return {}

    rendered = render_template(config.custom_fields_template, record)
    data = json.loads(rendered)
    if not isinstance(data, dict):
        raise ValueError("custom_fields_template must render a JSON object.")
    return data


def _has_value(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _chunks(
    records: list[tuple[int, dict[str, Any], dict[str, Any]]],
    size: int,
) -> Iterator[list[tuple[int, dict[str, Any], dict[str, Any]]]]:
    for start in range(0, len(records), size):
        yield records[start : start + size]


def _add_row_error(
    result: SyncResult,
    index: int,
    record: dict[str, Any],
    http_status: int | None,
    message: str,
) -> None:
    result.failed += 1
    result.row_errors.append(
        RowError(
            batch_index=index,
            record_preview=json.dumps(record, default=str)[:200],
            http_status=http_status,
            error_message=message,
        )
    )
