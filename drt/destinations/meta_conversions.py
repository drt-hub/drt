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
      event_source_url_field: page_url  # Required for website events.
      client_user_agent_field: user_agent  # Required for website events.
      email_field: email        # At least one customer identifier is required.
"""

from __future__ import annotations

import hashlib
import json
import math
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
_MAX_EVENT_AGE_SECONDS = 604_800


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
                        headers={
                            "Authorization": f"Bearer {access_token}",
                            "Content-Type": "application/json",
                        },
                        json=payload,
                    )
                    response.raise_for_status()
                    return response

                try:
                    response = with_retry(
                        _post,
                        retry_config,
                        retry_on=_is_meta_transient_error,
                    )
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
    event_id_field = config.event_id_field
    assert event_id_field is not None
    event_id = _nonempty(record.get(event_id_field))
    if event_id is None:
        raise ValueError(f"Row missing event id field {event_id_field!r}.")
    if isinstance(event_id, bool) or not isinstance(event_id, (str, int, float)):
        raise ValueError(
            f"Row event id field {event_id_field!r} must be a string or a plain "
            f"number, not a container or boolean; got {type(event_id).__name__}."
        )
    event_id = str(event_id)
    event["event_id"] = event_id
    if config.action_source == "website":
        event_source_url_field = config.event_source_url_field
        assert event_source_url_field is not None
        event_source_url = _nonempty(record.get(event_source_url_field))
        if event_source_url is None:
            raise ValueError(
                f"Row missing event source URL field {event_source_url_field!r} "
                "required for action_source 'website'."
            )
        event["event_source_url"] = event_source_url
    else:
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
    if config.action_source == "website":
        client_user_agent_field = config.client_user_agent_field
        assert client_user_agent_field is not None
        client_user_agent = _nonempty(record.get(client_user_agent_field))
        if client_user_agent is None:
            raise ValueError(
                f"Row missing client user agent field {client_user_agent_field!r} "
                "required for action_source 'website'."
            )
        user_data["client_user_agent"] = client_user_agent
    else:
        _copy_optional(
            user_data,
            "client_user_agent",
            record,
            config.client_user_agent_field,
        )
    _copy_optional(user_data, "fbc", record, config.fbc_field)
    _copy_optional(user_data, "fbp", record, config.fbp_field)
    if not user_data:
        raise ValueError(
            "Row missing customer information; at least one configured user_data "
            "field must resolve to a non-empty value."
        )
    event["user_data"] = user_data

    if config.value_field is not None:
        raw_value = record.get(config.value_field)
        if raw_value is not None:
            value = float(raw_value)
            if not math.isfinite(value):
                raise ValueError(
                    f"custom_data.value {value!r} is not JSON-serializable."
                )
            event["custom_data"] = {
                "value": value,
                "currency": config.currency,
            }

    try:
        json.dumps(event, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Row event_id {event_id!r} contains a value that cannot be sent to Meta: "
            f"{exc}"
        ) from exc

    return event


def _is_meta_transient_error(exc: Exception) -> bool:
    """Return whether Meta explicitly marks an HTTP 400 response transient."""
    if not isinstance(exc, httpx.HTTPStatusError) or exc.response.status_code != 400:
        return False
    try:
        response_data = exc.response.json()
    except (TypeError, ValueError):
        return False
    if not isinstance(response_data, dict):
        return False
    error = response_data.get("error")
    return isinstance(error, dict) and error.get("is_transient") is True


def _event_time(
    record: dict[str, Any], config: MetaConversionsDestinationConfig
) -> int:
    current_time = time.time()
    if config.event_time_field is None:
        timestamp = int(current_time)
    else:
        value = record.get(config.event_time_field)
        if value is None:
            raise ValueError(f"Row missing event time field {config.event_time_field!r}.")
        timestamp = int(value)
        if isinstance(value, float) and not value.is_integer():
            raise ValueError("event_time must be a Unix timestamp in whole seconds.")

    cutoff = current_time - _MAX_EVENT_AGE_SECONDS
    if timestamp < cutoff:
        raise ValueError(
            f"event_time timestamp {timestamp} is older than Meta's seven-day "
            f"cutoff {cutoff}."
        )
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
    if not isinstance(value, str):
        raise ValueError(
            f"email must be a string; got {type(value).__name__}."
        )
    return value.strip().lower()


def _hash_email(value: Any) -> str:
    return _sha256(_normalize_email(value))


def _normalize_phone(value: Any) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        raise ValueError(
            "phone must be a string or integer, not a float, container, or boolean; "
            f"got {type(value).__name__}."
        )
    return re.sub(r"[^0-9]", "", str(value))


def _hash_phone(value: Any) -> str:
    return _sha256(_normalize_phone(value))
