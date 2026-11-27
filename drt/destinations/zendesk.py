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
import logging
from collections.abc import Callable, Iterator
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, RetryConfig, SyncOptions, ZendeskDestinationConfig
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter, RateLimiterBackend, resolve_rate_limiter
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
        # The vendor ceiling is passed as a cap so it clamps the resolved rate
        # before the shared bucket is created; _load_users / _load_organizations
        # receive that already-capped limiter as a parameter, unchanged.
        rate_limiter = resolve_rate_limiter(
            config,
            sync_options,
            max_requests_per_second=_ZENDESK_MAX_REQUESTS_PER_SECOND,
            limiter_factory=RateLimiter,
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
        rate_limiter: RateLimiterBackend,
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
                    result.skipped += len(records) - 1
                    return

        url = f"{base_url}/users/create_or_update_many.json"
        for chunk_start, chunk in enumerate(
            _chunks(indexed_payloads, _ZENDESK_MAX_USER_BATCH_SIZE)
        ):
            users = [payload for _, _, payload in chunk]
            remaining = len(indexed_payloads) - (
                chunk_start * _ZENDESK_MAX_USER_BATCH_SIZE + len(chunk)
            )

            def do_post(_users: list[dict[str, Any]] = users) -> httpx.Response:
                response = client.post(
                    url,
                    json={"users": _users},
                    auth=auth,
                    headers=headers,
                )
                response.raise_for_status()
                return response

            operation, on_unknown = _replay_policy(
                users,
                _has_user_identity,
                config,
                retry_config,
                "user create-or-update",
                "email or external_id",
            )
            try:
                rate_limiter.acquire()
                with_retry(
                    do_post,
                    retry_config,
                    operation=operation,
                    on_delivery_unknown=on_unknown,
                )
                result.success += len(chunk)
            except DeliveryUnknownError as exc:
                for index, record, _ in chunk:
                    _add_row_error(result, index, record, exc.http_status, str(exc))
                if sync_options.on_error == "fail":
                    result.skipped += remaining
                    break
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
                    result.skipped += remaining
                    break
            except Exception as exc:
                for index, record, _ in chunk:
                    _add_row_error(result, index, record, None, str(exc))
                if sync_options.on_error == "fail":
                    result.skipped += remaining
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
        rate_limiter: RateLimiterBackend,
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
                    result.skipped += len(records) - index - 1
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

            operation, on_unknown = _replay_policy(
                [organization],
                _has_organization_identity,
                config,
                retry_config,
                "organization create-or-update",
                "id or external_id",
            )
            try:
                rate_limiter.acquire()
                with_retry(
                    do_post,
                    retry_config,
                    operation=operation,
                    on_delivery_unknown=on_unknown,
                )
                result.success += 1
            except DeliveryUnknownError as exc:
                _add_row_error(result, index, record, exc.http_status, str(exc))
                if sync_options.on_error == "fail":
                    result.skipped += len(records) - index - 1
                    break
            except httpx.HTTPStatusError as exc:
                _add_row_error(
                    result,
                    index,
                    record,
                    exc.response.status_code,
                    exc.response.text[:500],
                )
                if sync_options.on_error == "fail":
                    result.skipped += len(records) - index - 1
                    break
            except Exception as exc:
                _add_row_error(result, index, record, None, str(exc))
                if sync_options.on_error == "fail":
                    result.skipped += len(records) - index - 1
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
            _is_zendesk_id(id_value)
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


def _replay_policy(
    payloads: list[dict[str, Any]],
    has_identity: Callable[[dict[str, Any]], bool],
    config: ZendeskDestinationConfig,
    retry_config: RetryConfig,
    operation_label: str,
    identity_label: str,
) -> tuple[OperationSafety, Callable[[Exception, int], RetryDirective] | None]:
    """Keyed when every payload carries an identity Zendesk matches on, else blind."""
    if all(has_identity(payload) for payload in payloads):
        return OperationSafety.KEYED, None

    def on_delivery_unknown(error: Exception, attempt: int) -> RetryDirective:
        return handle_blind_delivery_unknown(
            error,
            attempt,
            retry_creates=config.retry_creates,
            max_attempts=retry_config.max_attempts,
            destination="Zendesk",
            operation=f"{operation_label} without valid {identity_label}",
            logger=logger,
        )

    return OperationSafety.BLIND_CREATE, on_delivery_unknown


def _has_non_blank_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_zendesk_id(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value > 0
    if isinstance(value, str):
        stripped = value.strip()
        return stripped.isdecimal() and int(stripped) > 0
    return False


def _has_user_identity(payload: dict[str, Any]) -> bool:
    # create_or_update_many documents only email and external_id as match keys.
    return _has_non_blank_string(payload.get("email")) or _has_non_blank_string(
        payload.get("external_id")
    )


def _has_organization_identity(payload: dict[str, Any]) -> bool:
    # create_or_update documents id and external_id, but explicitly not name.
    return _is_zendesk_id(payload.get("id")) or _has_non_blank_string(payload.get("external_id"))


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
    record_row_error(
        result,
        index,
        json.dumps(record, default=str)[:200],
        Exception(message),
        http_status=http_status,
        error_message=message,
    )
