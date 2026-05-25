"""Amplitude destination — sync user properties and events via HTTP APIs.

Uses Amplitude Identify API for user properties and HTTP V2 API for events.
No extra dependencies beyond core httpx.

Example sync YAML — identify (user properties):

    destination:
      type: amplitude
      api_key_env: AMPLITUDE_API_KEY
      endpoint: identify
      user_id_field: user_id
      properties_template: |
        {"ltv_segment": "{{ row.ltv_segment }}", "plan": "{{ row.plan }}"}

Example sync YAML — events:

    destination:
      type: amplitude
      api_key_env: AMPLITUDE_API_KEY
      endpoint: event
      user_id_field: user_id
      event_type_field: event_name
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import (
    AmplitudeDestinationConfig,
    DestinationConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template

_AMPLITUDE_HOSTS = {
    "default": "https://api2.amplitude.com",
    "eu": "https://api.eu.amplitude.com",
}

_RESERVED_ROW_KEYS = frozenset(
    {
        "user_id",
        "device_id",
        "event_type",
        "time",
        "insert_id",
        "event_id",
        "event_properties",
        "user_properties",
        "groups",
        "group_properties",
        "app_version",
        "platform",
        "os_name",
        "os_version",
        "device_brand",
        "device_manufacturer",
        "device_model",
        "carrier",
        "country",
        "region",
        "city",
        "dma",
        "language",
        "price",
        "quantity",
        "revenue",
        "productId",
        "revenueType",
        "location_lat",
        "location_lng",
        "ip",
        "idfa",
        "idfv",
        "adid",
        "android_id",
        "event_id",
    }
)


class AmplitudeDestination:
    """Send sync records to Amplitude Identify or HTTP V2 event APIs."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, AmplitudeDestinationConfig)
        if not records:
            return SyncResult()

        api_key = resolve_env(config.api_key, config.api_key_env)
        if not api_key:
            raise ValueError(
                "Amplitude destination: provide api_key or set the env var "
                f"named in api_key_env ({config.api_key_env!r})."
            )

        base_url = _AMPLITUDE_HOSTS[config.region]
        if config.endpoint == "identify":
            url = f"{base_url}/identify"
        else:
            url = f"{base_url}/2/httpapi"

        retry_config = resolve_retry(config.retry, sync_options)
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)
        result = SyncResult()

        indexed_payloads: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
        for index, record in enumerate(records):
            try:
                payload = _build_payload(record, config)
                indexed_payloads.append((index, record, payload))
            except Exception as e:
                _add_row_error(result, index, record, None, str(e))
                if sync_options.on_error == "fail":
                    return result

        if not indexed_payloads:
            return result

        request_options: dict[str, Any] | None = None
        if config.min_id_length is not None:
            request_options = {"min_id_length": config.min_id_length}

        with httpx.Client(timeout=30.0) as client:
            for chunk in _chunks(indexed_payloads, config.batch_size):
                payloads = [payload for _, _, payload in chunk]
                try:
                    rate_limiter.acquire()
                    if config.endpoint == "identify":
                        _post_identify(
                            client,
                            url,
                            api_key,
                            payloads,
                            retry_config,
                        )
                    else:
                        _post_events(
                            client,
                            url,
                            api_key,
                            payloads,
                            retry_config,
                            request_options,
                        )
                    result.success += len(chunk)
                except httpx.HTTPStatusError as e:
                    for index, record, _ in chunk:
                        _add_row_error(
                            result,
                            index,
                            record,
                            e.response.status_code,
                            e.response.text[:500],
                        )
                    if sync_options.on_error == "fail":
                        break
                except Exception as e:
                    for index, record, _ in chunk:
                        _add_row_error(result, index, record, None, str(e))
                    if sync_options.on_error == "fail":
                        break

        return result


def _build_payload(record: dict[str, Any], config: AmplitudeDestinationConfig) -> dict[str, Any]:
    user_id = _field_value(record, config.user_id_field)
    device_id = _field_value(record, config.device_id_field) if config.device_id_field else None

    if not _has_value(user_id) and not _has_value(device_id):
        raise ValueError("Row must include user_id or device_id.")

    payload: dict[str, Any] = {}
    if _has_value(user_id):
        payload["user_id"] = str(user_id)
    if _has_value(device_id):
        payload["device_id"] = str(device_id)

    if config.endpoint == "event":
        event_type = config.event_type
        if config.event_type_field:
            event_type = record.get(config.event_type_field)
        if not _has_value(event_type):
            raise ValueError("Row must include event_type.")
        payload["event_type"] = str(event_type)

        if config.time_field:
            time_value = record.get(config.time_field)
            if _has_value(time_value):
                payload["time"] = time_value

        insert_id = _resolve_insert_id(record, config, str(event_type))
        if insert_id:
            payload["insert_id"] = insert_id

        properties_key = "event_properties"
    else:
        properties_key = "user_properties"

    properties = _row_properties(record, config)
    if properties:
        payload[properties_key] = properties

    if config.properties_template:
        template_data = _render_properties_template(config.properties_template, record)
        _merge_template_into_payload(payload, template_data, properties_key)

    return payload


def _merge_template_into_payload(
    payload: dict[str, Any],
    template_data: dict[str, Any],
    properties_key: str,
) -> None:
    other_key = "event_properties" if properties_key == "user_properties" else "user_properties"
    extra = dict(template_data)
    nested = extra.pop(properties_key, None)
    if isinstance(nested, dict):
        existing = payload.get(properties_key)
        if isinstance(existing, dict):
            payload[properties_key] = {**existing, **nested}
        else:
            payload[properties_key] = nested
    other_nested = extra.pop(other_key, None)
    if isinstance(other_nested, dict):
        existing = payload.get(other_key)
        if isinstance(existing, dict):
            payload[other_key] = {**existing, **other_nested}
        else:
            payload[other_key] = other_nested
    if extra:
        existing = payload.get(properties_key)
        if isinstance(existing, dict):
            payload[properties_key] = {**existing, **extra}
        else:
            payload[properties_key] = extra


def _row_properties(record: dict[str, Any], config: AmplitudeDestinationConfig) -> dict[str, Any]:
    excluded = _excluded_fields(config)
    properties: dict[str, Any] = {}
    for key, value in record.items():
        if key in excluded or key in _RESERVED_ROW_KEYS:
            continue
        if value is None:
            continue
        properties[key] = value
    return properties


def _excluded_fields(config: AmplitudeDestinationConfig) -> set[str]:
    excluded = {config.user_id_field}
    if config.device_id_field:
        excluded.add(config.device_id_field)
    if config.event_type_field:
        excluded.add(config.event_type_field)
    if config.time_field:
        excluded.add(config.time_field)
    if config.insert_id_field:
        excluded.add(config.insert_id_field)
    return excluded


def _field_value(record: dict[str, Any], field: str | None) -> Any:
    if not field:
        return None
    return record.get(field)


def _has_value(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _resolve_insert_id(
    record: dict[str, Any],
    config: AmplitudeDestinationConfig,
    event_type: str,
) -> str | None:
    if config.insert_id_field:
        value = record.get(config.insert_id_field)
        if _has_value(value):
            return str(value)
    existing = record.get("insert_id")
    if _has_value(existing):
        return str(existing)
    canonical = json.dumps(record, sort_keys=True, default=str)
    digest = hashlib.sha256(f"{canonical}:{event_type}".encode()).hexdigest()
    return digest[:32]


def _render_properties_template(template: str, record: dict[str, Any]) -> dict[str, Any]:
    rendered = render_template(template, record)
    data = json.loads(rendered)
    if not isinstance(data, dict):
        raise ValueError("properties_template must render a JSON object.")
    return data


def _post_identify(
    client: httpx.Client,
    url: str,
    api_key: str,
    identifications: list[dict[str, Any]],
    retry_config: RetryConfig,
) -> None:
    def do_post() -> httpx.Response:
        response = client.post(
            url,
            data={
                "api_key": api_key,
                "identification": json.dumps(identifications),
            },
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        response.raise_for_status()
        return response

    with_retry(do_post, retry_config)


def _post_events(
    client: httpx.Client,
    url: str,
    api_key: str,
    events: list[dict[str, Any]],
    retry_config: RetryConfig,
    options: dict[str, Any] | None,
) -> None:
    body: dict[str, Any] = {"api_key": api_key, "events": events}
    if options:
        body["options"] = options

    def do_post() -> httpx.Response:
        response = client.post(url, json=body)
        response.raise_for_status()
        return response

    with_retry(do_post, retry_config)


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
