"""Meta Conversions API destination.

Sends warehouse conversion events to a Meta Pixel's synchronous ``/events``
edge. Meta accepts at most 1000 events per request, so records are transformed
and uploaded in batches rather than one request per row.

Example::

    destination:
      type: meta_conversions
      pixel_id: "123456789012345"
      event_name: Purchase
      event_id_field: event_id  # Required so retries can be deduplicated.
"""

from __future__ import annotations

import hashlib
import re
import time
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import (
    DestinationConfig,
    MetaConversionsDestinationConfig,
    SyncOptions,
)
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter, resolve_rate_limiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import record_preview, record_row_error

_BASE_URL = "https://graph.facebook.com"
_MAX_EVENTS_PER_REQUEST = 1000


class MetaConversionsDestination:
    """Upload server-side conversion events to a Meta Pixel."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, MetaConversionsDestinationConfig)
        if not records:
            return SyncResult()

        access_token = resolve_env(config.access_token, config.access_token_env)
        if not access_token:
            raise ValueError(
                "Meta Conversions destination: provide access_token or set the env var "
                f"named in access_token_env ({config.access_token_env!r})."
            )

        result = SyncResult()
        prepared: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
        for index, record in enumerate(records):
            try:
                prepared.append((index, record, _build_event(record, config)))
            except (TypeError, ValueError) as exc:
                record_row_error(result, index, record_preview(record), exc)
                if sync_options.on_error == "fail":
                    break

        if not prepared:
            return result

        retry_config = resolve_retry(config.retry, sync_options)
        rate_limiter = resolve_rate_limiter(
            config, sync_options, limiter_factory=RateLimiter
        )
        url = f"{_BASE_URL}/{config.api_version}/{config.pixel_id}/events"

        with httpx.Client(timeout=60.0) as client:
            for start in range(0, len(prepared), _MAX_EVENTS_PER_REQUEST):
                batch = prepared[start : start + _MAX_EVENTS_PER_REQUEST]
                payload = {"data": [event for _, _, event in batch]}

                def _post() -> httpx.Response:
                    # Each retry is a real outbound request and must consume a
                    # limiter slot (#1048), not just the first attempt.
                    rate_limiter.acquire()
                    response = client.post(
                        url,
                        params={"access_token": access_token},
                        headers={"Content-Type": "application/json"},
                        json=payload,
                    )
                    response.raise_for_status()
                    return response

                try:
                    response = with_retry(_post, retry_config)
                    response_data = response.json()
                    events_received = (
                        response_data.get("events_received")
                        if isinstance(response_data, dict)
                        else None
                    )
                    if (
                        not isinstance(events_received, int)
                        or isinstance(events_received, bool)
                        or events_received != len(batch)
                    ):
                        raise ValueError(
                            "Meta Conversions API acknowledged "
                            f"{events_received} of {len(batch)} events; the response does "
                            "not identify individual failures."
                        )
                    result.success += len(batch)
                except Exception as exc:
                    status = (
                        exc.response.status_code
                        if isinstance(exc, httpx.HTTPStatusError)
                        else None
                    )
                    if isinstance(exc, httpx.HTTPStatusError):
                        message = (
                            "Meta Conversions API error: "
                            f"{exc.response.status_code} {exc.response.text[:500]}"
                        )
                    else:
                        message = f"Meta Conversions error: {exc}"
                    result.errors.append(message)
                    for index, record, _event in batch:
                        record_row_error(
                            result,
                            index,
                            record_preview(record),
                            exc,
                            http_status=status,
                            error_message=message,
                        )
                    if sync_options.on_error == "fail":
                        break

        return result


def _build_event(
    record: dict[str, Any], config: MetaConversionsDestinationConfig
) -> dict[str, Any]:
    event_name = _nonempty(config.event_name)
    if config.event_name_field is not None:
        event_name = _nonempty(record.get(config.event_name_field))
        if event_name is None:
            raise ValueError(f"Row missing event name field {config.event_name_field!r}.")

    event: dict[str, Any] = {
        "event_name": event_name,
        "event_time": _event_time(record, config),
        "action_source": config.action_source,
    }
    _copy_optional(event, "event_id", record, config.event_id_field)
    _copy_optional(
        event,
        "event_source_url",
        record,
        config.event_source_url_field,
    )

    user_data: dict[str, Any] = {}
    if config.email_field is not None:
        email = _nonempty(record.get(config.email_field))
        if email is not None:
            user_data["em"] = [_hash_email(email)]
    if config.phone_field is not None:
        phone = _nonempty(record.get(config.phone_field))
        if phone is not None:
            normalized_phone = _normalize_phone(phone)
            if normalized_phone:
                user_data["ph"] = [_hash_phone(phone)]

    # Meta explicitly requires these browser identifiers to remain plain text.
    _copy_optional(
        user_data,
        "client_ip_address",
        record,
        config.client_ip_address_field,
    )
    _copy_optional(
        user_data,
        "client_user_agent",
        record,
        config.client_user_agent_field,
    )
    _copy_optional(user_data, "fbc", record, config.fbc_field)
    _copy_optional(user_data, "fbp", record, config.fbp_field)
    if user_data:
        event["user_data"] = user_data

    if config.value_field is not None:
        value = record.get(config.value_field)
        if value is not None:
            event["custom_data"] = {
                "value": float(value),
                "currency": config.currency,
            }

    return event


def _event_time(
    record: dict[str, Any], config: MetaConversionsDestinationConfig
) -> int:
    if config.event_time_field is None:
        return int(time.time())
    value = record.get(config.event_time_field)
    if value is None:
        raise ValueError(f"Row missing event time field {config.event_time_field!r}.")
    timestamp = int(value)
    if isinstance(value, float) and not value.is_integer():
        raise ValueError("event_time must be a Unix timestamp in whole seconds.")
    return timestamp


def _copy_optional(
    target: dict[str, Any],
    target_field: str,
    record: dict[str, Any],
    source_field: str | None,
) -> None:
    if source_field is None:
        return
    value = record.get(source_field)
    if value is None or (isinstance(value, str) and value == ""):
        return
    target[target_field] = value


def _nonempty(value: Any) -> Any | None:
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        return value or None
    return value


def _sha256(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _normalize_email(value: Any) -> str:
    return str(value).strip().lower()


def _hash_email(value: Any) -> str:
    return _sha256(_normalize_email(value))


def _normalize_phone(value: Any) -> str:
    return re.sub(r"[^0-9]", "", str(value))


def _hash_phone(value: Any) -> str:
    return _sha256(_normalize_phone(value))
