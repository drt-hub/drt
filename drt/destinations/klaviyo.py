"""Klaviyo destination — upsert profiles or track events via the Klaviyo API.

Syncs DWH customer rows into Klaviyo profiles (the common Reverse-ETL pattern:
push LTV / churn-risk / segment attributes to the marketing platform). Each row
is upserted by **email**:

1. ``POST /api/profiles/`` to create.
2. On ``409`` (the profile already exists), the existing id is read from the
   error's ``meta.duplicate_profile_id`` and the profile is updated with
   ``PATCH /api/profiles/{id}/``.
3. If ``list_id`` is set, the profile is added to that Klaviyo list.

With ``endpoint: event``, each row is instead sent to ``POST /api/events/``
with an email-identified profile, a metric name, properties, a required stable
``unique_id``, and any configured ``time`` / ``value`` fields.

Auth is an API key (``Authorization: Klaviyo-API-Key <key>``) plus the
``revision`` header. No extra dependencies beyond core ``httpx``. Per-record
calls — set ``sync.rate_limit`` to respect the selected endpoint's limits
(profiles: 75 req/s burst, 700/minute steady; events: 350 req/s burst,
3500/minute steady).

Example sync YAML — profile upsert:

    destination:
      type: klaviyo
      api_key_env: KLAVIYO_API_KEY
      email_field: email
      properties_template: |
        {"ltv_segment": "{{ row.ltv_segment }}", "plan": "{{ row.plan }}"}
      list_id_env: KLAVIYO_LIST_ID   # optional

Example sync YAML — event tracking:

    destination:
      type: klaviyo
      api_key_env: KLAVIYO_API_KEY
      endpoint: event
      email_field: email
      metric_name_field: event_name
      time_field: occurred_at
      value_field: value
      unique_id_field: event_id  # required stable deduplication key

``sync.mode: mirror`` is not implemented — follow-up.
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime
from decimal import Decimal
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
from drt.destinations.rate_limiter import (
    RateLimiter,
    RateLimiterBackend,
    resolve_rate_limiter,
)
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import record_row_error
from drt.templates.renderer import render_template

_BASE = "https://a.klaviyo.com/api"


class KlaviyoDestination:
    """Upsert records into Klaviyo profiles or send Klaviyo events."""

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
        rate_limiter = resolve_rate_limiter(config, sync_options, limiter_factory=RateLimiter)
        result = SyncResult()

        with httpx.Client(timeout=30.0) as client:
            for index, record in enumerate(records):
                try:
                    if config.endpoint == "event":
                        self._send_event(
                            client,
                            config,
                            headers,
                            record,
                            retry_config,
                            rate_limiter,
                        )
                    else:
                        rate_limiter.acquire()
                        self._upsert(client, config, headers, record, list_id, retry_config)
                    result.success += 1
                except httpx.HTTPStatusError as e:
                    record_row_error(
                        result,
                        index,
                        str(record)[:200],
                        e,
                        http_status=e.response.status_code,
                        error_message=e.response.text[:500],
                    )
                    if sync_options.on_error == "fail":
                        break
                except Exception as e:
                    record_row_error(
                        result,
                        index,
                        str(record)[:200],
                        e,
                    )
                    if sync_options.on_error == "fail":
                        break

        return result

    def _send_event(
        self,
        client: httpx.Client,
        config: KlaviyoDestinationConfig,
        headers: dict[str, str],
        record: dict[str, Any],
        retry_config: RetryConfig,
        rate_limiter: RateLimiterBackend,
    ) -> None:
        email = record.get(config.email_field)
        if email is None or str(email).strip() == "":
            raise ValueError(f"Row missing email field {config.email_field!r}.")

        metric_name: Any = config.metric_name
        if config.metric_name_field:
            metric_name = record.get(config.metric_name_field)
        if metric_name is None or str(metric_name).strip() == "":
            raise ValueError("Row missing Klaviyo event metric name.")

        attributes: dict[str, Any] = {
            "properties": self._properties(record, config),
            "metric": {
                "data": {
                    "type": "metric",
                    "attributes": {"name": str(metric_name)},
                }
            },
            "profile": {
                "data": {
                    "type": "profile",
                    "attributes": {"email": str(email)},
                }
            },
        }
        if config.time_field:
            time_value = record.get(config.time_field)
            if time_value is not None:
                if isinstance(time_value, datetime):
                    time_value = time_value.isoformat()
                elif isinstance(time_value, date):
                    raise ValueError(
                        f"Klaviyo event time field {config.time_field!r} must use a "
                        "TIMESTAMP/DATETIME-typed source column, not a DATE-typed "
                        "source column."
                    )
                attributes["time"] = time_value
        if config.value_field:
            value = record.get(config.value_field)
            if value is not None:
                attributes["value"] = float(value)
        if config.unique_id_field is not None:
            unique_id = record.get(config.unique_id_field)
            if unique_id is None or str(unique_id).strip() == "":
                raise ValueError(f"Row missing unique_id field {config.unique_id_field!r}.")
            attributes["unique_id"] = str(unique_id)

        payload = {"data": {"type": "event", "attributes": attributes}}

        def _post() -> httpx.Response:
            rate_limiter.acquire()
            response = client.post(
                f"{_BASE}/events/",
                headers=headers,
                json=payload,
            )
            response.raise_for_status()
            return response

        with_retry(_post, retry_config)

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
    def _properties(record: dict[str, Any], config: KlaviyoDestinationConfig) -> dict[str, Any]:
        if config.properties_template:
            rendered = render_template(config.properties_template, record)
            parsed = json.loads(rendered)
            return parsed if isinstance(parsed, dict) else {}
        excluded_fields = {config.email_field}
        if config.endpoint == "event":
            for field in (
                config.time_field,
                config.value_field,
                config.unique_id_field,
                config.metric_name_field,
            ):
                if field:
                    excluded_fields.add(field)
        return {
            k: _json_safe(v)
            for k, v in record.items()
            if k not in excluded_fields and v is not None
        }

    def test_connection(self, config: DestinationConfig) -> None:
        """Test whether Klaviyo accepts the configured private API key.

        Klaviyo has no scope-free identity endpoint, so this probes accounts
        and accepts its documented ``permission_denied`` response as proof
        that authentication succeeded for a more narrowly scoped key.
        """
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
            # A permission_denied 403 means the key was authenticated but lacks
            # accounts:read, which is expected for the minimal write-only key.
            if resp.status_code == 403 and _permission_denied(resp):
                return
            resp.raise_for_status()


def _decimal_to_json_safe(value: Decimal) -> float:
    """Convert a Decimal to a JSON number without silently losing precision.

    A blind ``float(value)`` corrupts values a ``float`` can't represent
    exactly (e.g. large integer-valued Decimals beyond 2**53) and can
    overflow extreme Decimals to +/-inf, which JSON's own spec forbids.
    Falling back to a string representation for those cases was considered
    and rejected: it would make the *same* warehouse DECIMAL column emit a
    mix of JSON number and string values depending on the individual row,
    which can silently change how Klaviyo evaluates that property in
    segments. Instead, a value that can't round-trip exactly through
    ``float`` raises, surfacing as a per-row error like any other malformed
    input rather than silently changing type or precision.
    """
    if not value.is_finite():
        raise ValueError(f"Decimal value {value!r} is not finite and cannot be sent to Klaviyo.")
    as_float = float(value)
    if math.isinf(as_float) or Decimal(str(as_float)) != value:
        raise ValueError(
            f"Decimal value {value!r} cannot be represented exactly as a JSON number "
            "without precision loss."
        )
    return as_float


def _json_safe(value: Any) -> Any:
    """Recursively coerce a warehouse-driver value into a JSON-serializable one.

    Common driver return types (``Decimal``, ``date``, ``datetime``) aren't
    JSON-serializable and can appear nested inside dict/list-typed columns
    (e.g. JSON/STRUCT columns), not just at the top level.
    """
    if isinstance(value, Decimal):
        return _decimal_to_json_safe(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    return value


def _permission_denied(resp: httpx.Response) -> bool:
    try:
        errors = resp.json().get("errors", [])
        return any(err.get("code") == "permission_denied" for err in errors)
    except Exception:
        return False


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
