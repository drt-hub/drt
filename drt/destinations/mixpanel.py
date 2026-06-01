"""Mixpanel destination — sync user profiles and events via HTTP APIs.

Two endpoints, selected by ``endpoint`` in the config:

* ``people_set``    -> ``/engage#profile-set``. Sets user-profile
  properties. Authenticated by the project token, which Mixpanel
  carries inside each record (no auth header).
* ``import_events`` -> ``/import``. Ingests events. Authenticated by a
  service account (HTTP Basic) plus the numeric ``project_id`` query
  parameter.

Both batch up to 2000 records per request (Mixpanel's limit) and
support EU data residency (``api-eu.mixpanel.com``). No extra
dependencies beyond core httpx.

Docs:
* https://developer.mixpanel.com/reference/profile-set
* https://developer.mixpanel.com/reference/import-events

Example sync YAML — profile set:

    destination:
      type: mixpanel
      endpoint: people_set
      project_token_env: MIXPANEL_TOKEN
      distinct_id_field: user_id
      properties_template: |
        {"plan": "{{ row.plan }}", "signup_source": "{{ row.source }}"}

Example sync YAML — event import:

    destination:
      type: mixpanel
      endpoint: import_events
      project_id: "1234567"
      service_account_username_env: MIXPANEL_SA_USERNAME
      service_account_secret_env: MIXPANEL_SA_SECRET
      distinct_id_field: user_id
      event_name: signup_completed
      time_field: event_time
"""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Iterator
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import (
    DestinationConfig,
    MixpanelDestinationConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template

_MIXPANEL_HOSTS = {
    "default": "https://api.mixpanel.com",
    "eu": "https://api-eu.mixpanel.com",
}

# Keys consumed by mapping config rather than passed through as properties.
_PEOPLE_RESERVED = frozenset({"$token", "$distinct_id", "$set"})


class MixpanelDestination:
    """Send sync records to Mixpanel's /engage (profiles) or /import (events) APIs."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, MixpanelDestinationConfig)
        if not records:
            return SyncResult()

        base_url = _MIXPANEL_HOSTS[config.region]
        retry_config = resolve_retry(config.retry, sync_options)
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)
        result = SyncResult()

        if config.endpoint == "people_set":
            self._load_people(
                records, config, sync_options, base_url, retry_config, rate_limiter, result
            )
        else:
            self._load_events(
                records, config, sync_options, base_url, retry_config, rate_limiter, result
            )

        return result

    # -- /engage#profile-set -------------------------------------------------

    def _load_people(
        self,
        records: list[dict[str, Any]],
        config: MixpanelDestinationConfig,
        sync_options: SyncOptions,
        base_url: str,
        retry_config: RetryConfig,
        rate_limiter: RateLimiter,
        result: SyncResult,
    ) -> None:
        token = resolve_env(config.project_token, config.project_token_env)
        if not token:
            raise ValueError(
                "Mixpanel destination (people_set): provide project_token or set the "
                f"env var named in project_token_env ({config.project_token_env!r})."
            )
        url = f"{base_url}/engage"

        indexed = self._build_payloads(
            records, config, result, sync_options, lambda r: _build_profile(r, config, token)
        )
        if not indexed:
            return

        with httpx.Client(timeout=30.0) as client:
            for chunk in _chunks(indexed, config.batch_size):
                payloads = [p for _, _, p in chunk]
                self._post_chunk(
                    chunk,
                    result,
                    sync_options,
                    rate_limiter,
                    lambda: _post_json(client, url, payloads, retry_config),
                )

    # -- /import -------------------------------------------------------------

    def _load_events(
        self,
        records: list[dict[str, Any]],
        config: MixpanelDestinationConfig,
        sync_options: SyncOptions,
        base_url: str,
        retry_config: RetryConfig,
        rate_limiter: RateLimiter,
        result: SyncResult,
    ) -> None:
        username = resolve_env(config.service_account_username, config.service_account_username_env)
        secret = resolve_env(config.service_account_secret, config.service_account_secret_env)
        if not username or not secret:
            raise ValueError(
                "Mixpanel destination (import_events): provide service account "
                "username + secret (or their *_env vars)."
            )
        url = f"{base_url}/import"
        auth = (username, secret)
        params = {"project_id": str(config.project_id)}

        indexed = self._build_payloads(
            records, config, result, sync_options, lambda r: _build_event(r, config)
        )
        if not indexed:
            return

        with httpx.Client(timeout=30.0) as client:
            for chunk in _chunks(indexed, config.batch_size):
                payloads = [p for _, _, p in chunk]
                self._post_chunk(
                    chunk,
                    result,
                    sync_options,
                    rate_limiter,
                    lambda: _post_json(
                        client, url, payloads, retry_config, auth=auth, params=params
                    ),
                )

    # -- shared --------------------------------------------------------------

    def _build_payloads(
        self,
        records: list[dict[str, Any]],
        config: MixpanelDestinationConfig,
        result: SyncResult,
        sync_options: SyncOptions,
        builder: Any,
    ) -> list[tuple[int, dict[str, Any], dict[str, Any]]]:
        indexed: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
        for index, record in enumerate(records):
            try:
                indexed.append((index, record, builder(record)))
            except Exception as e:  # noqa: BLE001 - row-level error capture
                _add_row_error(result, index, record, None, str(e))
                if sync_options.on_error == "fail":
                    return []
        return indexed

    def _post_chunk(
        self,
        chunk: list[tuple[int, dict[str, Any], dict[str, Any]]],
        result: SyncResult,
        sync_options: SyncOptions,
        rate_limiter: RateLimiter,
        do_post: Any,
    ) -> bool:
        """POST one chunk; record success/row-errors. Returns False to stop."""
        try:
            rate_limiter.acquire()
            do_post()
            result.success += len(chunk)
        except httpx.HTTPStatusError as e:
            for index, record, _ in chunk:
                _add_row_error(result, index, record, e.response.status_code, e.response.text[:500])
            if sync_options.on_error == "fail":
                return False
        except Exception as e:  # noqa: BLE001 - chunk-level error capture
            for index, record, _ in chunk:
                _add_row_error(result, index, record, None, str(e))
            if sync_options.on_error == "fail":
                return False
        return True


# ---------------------------------------------------------------------------
# Payload builders
# ---------------------------------------------------------------------------


def _build_profile(
    record: dict[str, Any], config: MixpanelDestinationConfig, token: str
) -> dict[str, Any]:
    distinct_id = record.get(config.distinct_id_field)
    if not _has_value(distinct_id):
        raise ValueError(f"Row must include distinct_id field {config.distinct_id_field!r}.")

    properties = _row_properties(record, config, reserved=_PEOPLE_RESERVED)
    if config.properties_template:
        properties.update(_render_properties_template(config.properties_template, record))

    return {
        "$token": token,
        "$distinct_id": str(distinct_id),
        "$set": properties,
    }


def _build_event(record: dict[str, Any], config: MixpanelDestinationConfig) -> dict[str, Any]:
    distinct_id = record.get(config.distinct_id_field)
    if not _has_value(distinct_id):
        raise ValueError(f"Row must include distinct_id field {config.distinct_id_field!r}.")

    event_name = config.event_name
    if config.event_name_field:
        event_name = record.get(config.event_name_field)
    if not _has_value(event_name):
        raise ValueError("Row must include an event name.")

    event_time = int(time.time())
    if config.time_field:
        time_value = record.get(config.time_field)
        if _has_value(time_value):
            event_time = int(str(time_value))

    properties: dict[str, Any] = {
        "distinct_id": str(distinct_id),
        "time": event_time,
        "$insert_id": _resolve_insert_id(record, config, str(event_name)),
    }
    properties.update(_row_properties(record, config, reserved=frozenset()))
    if config.properties_template:
        properties.update(_render_properties_template(config.properties_template, record))

    return {"event": str(event_name), "properties": properties}


# ---------------------------------------------------------------------------
# Helpers (mirrors amplitude.py idioms)
# ---------------------------------------------------------------------------


def _row_properties(
    record: dict[str, Any], config: MixpanelDestinationConfig, reserved: frozenset[str]
) -> dict[str, Any]:
    excluded = {config.distinct_id_field}
    for f in (config.event_name_field, config.time_field, config.insert_id_field):
        if f:
            excluded.add(f)
    out: dict[str, Any] = {}
    for key, value in record.items():
        if key in excluded or key in reserved or value is None:
            continue
        out[key] = value
    return out


def _has_value(value: Any) -> bool:
    return value is not None and str(value).strip() != ""


def _resolve_insert_id(
    record: dict[str, Any], config: MixpanelDestinationConfig, event_name: str
) -> str:
    if config.insert_id_field:
        value = record.get(config.insert_id_field)
        if _has_value(value):
            return str(value)
    # Deterministic so a re-run of the same sync does not double-count events.
    canonical = json.dumps(record, sort_keys=True, default=str)
    return hashlib.sha256(f"{canonical}:{event_name}".encode()).hexdigest()[:32]


def _render_properties_template(template: str, record: dict[str, Any]) -> dict[str, Any]:
    rendered = render_template(template, record)
    data = json.loads(rendered)
    if not isinstance(data, dict):
        raise ValueError("properties_template must render a JSON object.")
    return data


def _post_json(
    client: httpx.Client,
    url: str,
    payload: list[dict[str, Any]],
    retry_config: RetryConfig,
    auth: tuple[str, str] | None = None,
    params: dict[str, str] | None = None,
) -> None:
    def do_post() -> httpx.Response:
        kwargs: dict[str, Any] = {"json": payload}
        if auth is not None:
            kwargs["auth"] = auth
        if params is not None:
            kwargs["params"] = params
        response = client.post(url, **kwargs)
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
