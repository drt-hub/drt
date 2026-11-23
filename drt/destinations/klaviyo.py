"""Klaviyo destination — upsert profiles or track events via the Klaviyo API.

Syncs DWH customer rows into Klaviyo profiles (the common Reverse-ETL pattern:
push LTV / churn-risk / segment attributes to the marketing platform). Normal
upserts use Klaviyo's email-keyed ``POST /api/profile-import`` endpoint. The
``create_only`` and ``update_only`` match policies use create/conflict and exact
email lookup plus ID-addressed update respectively. If ``list_id`` is set, the
profile is added to that Klaviyo list.

With ``endpoint: event``, each row is instead sent to ``POST /api/events/``
with an email-identified profile, a metric name, properties, a required stable
``unique_id``, and any configured ``time`` / ``value`` fields. Optional
``backfill: true`` (#1075) suppresses live flow/automation triggers for a
historical replay (a first full sync, or a cursor-override re-processing old
rows) so existing flows don't re-send customer-facing messages for events
that already happened; the event still counts toward metrics/segmentation.

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
import logging
import math
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
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
from drt.destinations.retry import (
    DeliveryUnknownError,
    OperationSafety,
    RetryDirective,
    resolve_retry,
    with_retry,
)
from drt.destinations.row_errors import record_row_error
from drt.templates.renderer import render_template

_BASE = "https://a.klaviyo.com/api"
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _ProfileWriteOutcome:
    profile_id: str | None = None
    skipped: bool = False


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
                        if sync_options.match_policy != "upsert":
                            raise ValueError(
                                "Klaviyo destination: sync.match_policy applies only to "
                                "endpoint: profile."
                            )
                        self._send_event(
                            client,
                            config,
                            headers,
                            record,
                            retry_config,
                            rate_limiter,
                        )
                    else:
                        written = self._write_profile(
                            client,
                            config,
                            headers,
                            record,
                            list_id,
                            sync_options.match_policy,
                            retry_config,
                            rate_limiter,
                        )
                        if not written:
                            result.skipped += 1
                            result.skipped_no_match += 1
                            continue
                    result.success += 1
                except DeliveryUnknownError as e:
                    record_row_error(
                        result,
                        index,
                        str(record)[:200],
                        e,
                        http_status=e.http_status,
                        error_message=str(e),
                    )
                    if sync_options.on_error == "fail":
                        break
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
        if config.backfill:
            attributes["backfill"] = True

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

        with_retry(_post, retry_config, operation=OperationSafety.KEYED)

    @staticmethod
    def _search_profile_ids(
        client: httpx.Client,
        headers: dict[str, str],
        email: str,
        retry_config: RetryConfig,
        rate_limiter: RateLimiterBackend,
        *,
        list_id: str | None = None,
    ) -> list[str]:
        """Find at most two profiles by exact email, optionally within one list."""
        path = (
            f"{_BASE}/lists/{list_id}/relationships/profiles/"
            if list_id is not None
            else f"{_BASE}/profiles/"
        )
        escaped_email = email.replace("\\", "\\\\").replace('"', '\\"')

        def _get() -> httpx.Response:
            rate_limiter.acquire()
            response = client.get(
                path,
                headers=headers,
                params={"filter": f'equals(email,"{escaped_email}")', "page[size]": 2},
            )
            response.raise_for_status()
            return response

        response = with_retry(_get, retry_config, operation=OperationSafety.IDEMPOTENT)
        try:
            data = response.json()["data"]
        except (KeyError, TypeError, ValueError) as e:
            raise ValueError("Invalid Klaviyo profile-search response: missing data list.") from e
        if not isinstance(data, list):
            raise ValueError("Invalid Klaviyo profile-search response: data is not a list.")

        profile_ids: list[str] = []
        for profile in data:
            if not isinstance(profile, dict) or profile.get("id") in (None, ""):
                raise ValueError("Invalid Klaviyo profile-search response: profile is missing id.")
            profile_id = str(profile["id"])
            if profile_id not in profile_ids:
                profile_ids.append(profile_id)
        return profile_ids

    @staticmethod
    def _patch_profile(
        client: httpx.Client,
        headers: dict[str, str],
        profile_id: str,
        attributes: dict[str, Any],
        retry_config: RetryConfig,
        rate_limiter: RateLimiterBackend,
        *,
        missing_is_skip: bool = False,
    ) -> bool:
        """Retry an update addressed to one fixed Klaviyo profile ID."""

        def _patch() -> bool:
            rate_limiter.acquire()
            response = client.patch(
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
            if missing_is_skip and response.status_code == 404:
                return False
            response.raise_for_status()
            return True

        return with_retry(_patch, retry_config, operation=OperationSafety.IDEMPOTENT)

    def _create_profile(
        self,
        client: httpx.Client,
        config: KlaviyoDestinationConfig,
        headers: dict[str, str],
        email: str,
        attributes: dict[str, Any],
        retry_config: RetryConfig,
        rate_limiter: RateLimiterBackend,
    ) -> _ProfileWriteOutcome:
        """Create once for ``create_only``, reconciling before any blind replay."""

        def _create() -> _ProfileWriteOutcome:
            rate_limiter.acquire()
            response = client.post(
                f"{_BASE}/profiles/",
                headers=headers,
                json={"data": {"type": "profile", "attributes": attributes}},
            )
            if response.status_code == 409:
                return _ProfileWriteOutcome(skipped=True)
            response.raise_for_status()
            return _ProfileWriteOutcome(profile_id=_created_id(response))

        def _reconcile(error: Exception, attempt: int) -> _ProfileWriteOutcome | RetryDirective:
            profile_ids = self._search_profile_ids(
                client,
                headers,
                email,
                retry_config,
                rate_limiter,
            )
            if len(profile_ids) > 1:
                raise ValueError(
                    "Klaviyo destination: profile-create delivery is unknown and exact "
                    f"email reconciliation matched multiple profiles; profile ids: "
                    f"{profile_ids}. Refusing to guess or create another profile."
                ) from error
            if profile_ids:
                return _ProfileWriteOutcome(profile_id=profile_ids[0])
            if config.retry_creates and attempt < retry_config.max_attempts:
                logger.warning(
                    "Klaviyo retry_creates=true: retrying delivery-unknown POST /profiles "
                    "after exact email reconciliation found no match (attempt %d/%d); "
                    "duplicate profile risk.",
                    attempt,
                    retry_config.max_attempts,
                )
                return RetryDirective.RETRY
            raise DeliveryUnknownError(
                "Klaviyo profile-create delivery is unknown: the request may have committed, "
                "but an exact email reconciliation search found no profile. Klaviyo results "
                "can lag after asynchronous writes, so drt refused a second POST. Verify "
                "Klaviyo before manual replay; destination.retry_creates: true restores "
                "blind retries with duplicate risk.",
                cause=error,
                attempt=attempt,
            ) from error

        return with_retry(
            _create,
            retry_config,
            operation=OperationSafety.RECONCILIABLE_CREATE,
            on_delivery_unknown=_reconcile,
        )

    def _write_profile(
        self,
        client: httpx.Client,
        config: KlaviyoDestinationConfig,
        headers: dict[str, str],
        record: dict[str, Any],
        list_id: str | None,
        match_policy: str,
        retry_config: RetryConfig,
        rate_limiter: RateLimiterBackend,
    ) -> bool:
        email = record.get(config.email_field)
        if email is None or str(email).strip() == "":
            raise ValueError(f"Row missing email field {config.email_field!r}.")

        attributes: dict[str, Any] = {"email": str(email)}
        properties = self._properties(record, config)
        if properties:
            attributes["properties"] = properties

        email_text = str(email)
        profile_id: str | None
        if match_policy == "update_only":
            profile_ids = self._search_profile_ids(
                client, headers, email_text, retry_config, rate_limiter
            )
            if len(profile_ids) > 1:
                raise ValueError(
                    "Klaviyo destination: update_only matched multiple profiles by exact "
                    f"email; profile ids: {profile_ids}."
                )
            if not profile_ids:
                return False
            written = self._patch_profile(
                client,
                headers,
                profile_ids[0],
                attributes,
                retry_config,
                rate_limiter,
                missing_is_skip=True,
            )
            if not written:
                return False
            profile_id = profile_ids[0]
        elif match_policy == "create_only":
            outcome = self._create_profile(
                client,
                config,
                headers,
                email_text,
                attributes,
                retry_config,
                rate_limiter,
            )
            if outcome.skipped:
                return False
            profile_id = outcome.profile_id
        else:

            def _upsert() -> httpx.Response:
                rate_limiter.acquire()
                response = client.post(
                    f"{_BASE}/profile-import",
                    headers=headers,
                    json={"data": {"type": "profile", "attributes": attributes}},
                )
                response.raise_for_status()
                return response

            response = with_retry(
                _upsert,
                retry_config,
                operation=OperationSafety.KEYED,
            )
            profile_id = _created_id(response)

        if list_id and profile_id:
            self._add_to_list(
                client,
                config,
                headers,
                list_id,
                profile_id,
                email_text,
                retry_config,
                rate_limiter,
            )
        return True

    def _add_to_list(
        self,
        client: httpx.Client,
        config: KlaviyoDestinationConfig,
        headers: dict[str, str],
        list_id: str,
        profile_id: str,
        email: str,
        retry_config: RetryConfig,
        rate_limiter: RateLimiterBackend,
    ) -> None:
        def _post() -> httpx.Response:
            rate_limiter.acquire()
            response = client.post(
                f"{_BASE}/lists/{list_id}/relationships/profiles/",
                headers=headers,
                json={"data": [{"type": "profile", "id": profile_id}]},
            )
            response.raise_for_status()
            return response

        def _reconcile(error: Exception, attempt: int) -> httpx.Response | RetryDirective:
            profile_ids = self._search_profile_ids(
                client,
                headers,
                email,
                retry_config,
                rate_limiter,
                list_id=list_id,
            )
            if len(profile_ids) > 1:
                raise ValueError(
                    "Klaviyo destination: list-membership delivery is unknown and exact "
                    f"email reconciliation matched multiple profiles; profile ids: "
                    f"{profile_ids}. Refusing to guess or add the relationship again."
                ) from error
            if profile_ids == [profile_id]:
                return httpx.Response(204)
            if profile_ids:
                raise ValueError(
                    "Klaviyo destination: list-membership reconciliation returned profile "
                    f"{profile_ids[0]!r}, expected {profile_id!r}; refusing to replay."
                ) from error
            if config.retry_creates and attempt < retry_config.max_attempts:
                logger.warning(
                    "Klaviyo retry_creates=true: retrying delivery-unknown list-membership "
                    "POST after exact email reconciliation found no match (attempt %d/%d); "
                    "duplicate relationship risk.",
                    attempt,
                    retry_config.max_attempts,
                )
                return RetryDirective.RETRY
            raise DeliveryUnknownError(
                "Klaviyo list-membership delivery is unknown: the request may have committed, "
                "but an exact list lookup found no profile. Klaviyo read results can lag, so "
                "drt refused a second POST. Verify list membership before manual replay; "
                "destination.retry_creates: true restores blind retries with duplicate risk.",
                cause=error,
                attempt=attempt,
            ) from error

        with_retry(
            _post,
            retry_config,
            operation=OperationSafety.RECONCILIABLE_CREATE,
            on_delivery_unknown=_reconcile,
        )

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
        properties = {
            k: _json_safe(v)
            for k, v in record.items()
            if k not in excluded_fields and v is not None
        }
        # Defensive backstop: normalize the known warehouse-driver types
        # above, but also verify the *result* against the exact encoder
        # httpx uses (allow_nan=False) rather than trusting that list is
        # exhaustive — an unhandled type surfaces here as a clear per-row
        # error instead of a cryptic failure deep inside request encoding.
        try:
            json.dumps(properties, allow_nan=False)
        except (TypeError, ValueError) as e:
            raise ValueError(f"Klaviyo properties are not JSON-serializable: {e}") from e
        return properties

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

    def supported_match_policies(self) -> frozenset[str]:
        """Profile writes support upsert, update-only and create-only semantics."""
        return frozenset({"upsert", "update_only", "create_only"})


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

    Common driver return types (``Decimal``, ``date``/``datetime``/``time``,
    non-finite ``float``) aren't JSON-serializable — or, for NaN/infinity,
    are only serializable via a non-standard extension `httpx` explicitly
    disables (``allow_nan=False``) — and any of these can appear nested
    inside dict/list-typed columns (e.g. JSON/STRUCT columns), not just at
    the top level.
    """
    if isinstance(value, Decimal):
        return _decimal_to_json_safe(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, time):
        return value.isoformat()
    if isinstance(value, timedelta):
        # e.g. PyMySQL returns MySQL TIME columns as timedelta (which can be
        # negative or exceed 24h) rather than time — total_seconds() keeps
        # sign and sub-second precision without an arbitrary string format.
        return value.total_seconds()
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError(f"Float value {value!r} is not finite and cannot be sent to Klaviyo.")
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


def _created_id(resp: httpx.Response) -> str | None:
    try:
        return str(resp.json()["data"]["id"])
    except Exception:
        return None
