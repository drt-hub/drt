"""HubSpot destination — Contacts, Deals, Companies upsert.

Upserts records into HubSpot CRM objects using the HubSpot v3 API.
Deduplicates by a configurable ID property (default: email for contacts).

Requires: HUBSPOT_TOKEN (Private App token with CRM write scope).

Example sync YAML — contacts:

    destination:
      type: hubspot
      object_type: contacts
      id_property: email
      properties_template: |
        {
          "email": "{{ row.email }}",
          "firstname": "{{ row.first_name }}",
          "lastname": "{{ row.last_name }}",
          "company": "{{ row.company }}",
          "lifecyclestage": "lead"
        }
      auth:
        type: bearer
        token_env: HUBSPOT_TOKEN

Example sync YAML — deals:

    destination:
      type: hubspot
      object_type: deals
      id_property: dealname
      properties_template: |
        {
          "dealname": "{{ row.deal_name }}",
          "amount": "{{ row.amount }}",
          "dealstage": "{{ row.stage }}"
        }
      auth:
        type: bearer
        token_env: HUBSPOT_TOKEN
"""

from __future__ import annotations

import hashlib
import json
import logging
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, HubSpotDestinationConfig, RetryConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiterBackend, resolve_rate_limiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import record_row_error
from drt.templates.renderer import render_template

_HUBSPOT_API = "https://api.hubapi.com/crm/v3/objects"
_HUBSPOT_PROPERTIES_API = "https://api.hubapi.com/crm/v3/properties"

logger = logging.getLogger(__name__)


def _is_empty(value: Any) -> bool:
    """Match the engine diff's fill-empty rule for HubSpot property values."""
    return value is None or (isinstance(value, str) and value.strip(" ") == "")


class HubSpotDestination:
    """Upsert records into HubSpot CRM via the v3 Objects API."""

    identifiers_case_insensitive = False

    def __init__(self) -> None:
        # ``None`` means the best-effort lookup was unavailable or unusable, so
        # later engine batches do not repeatedly call or warn for the same
        # object type and resolved token. The token itself is never retained.
        self._property_names: dict[tuple[str, str], frozenset[str] | None] = {}

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, HubSpotDestinationConfig)
        token = resolve_env(config.auth.token, config.auth.token_env)
        if not token:
            raise ValueError(
                "HubSpot destination: set HUBSPOT_TOKEN env var "
                "or provide auth.token_env in the sync config."
            )

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        upsert_url = f"{_HUBSPOT_API}/{config.object_type}"
        batch_read_url = f"{upsert_url}/batch/read"
        policy = sync_options.match_policy
        result = SyncResult()
        rate_limiter = resolve_rate_limiter(config, sync_options, max_requests_per_second=9)
        retry_config = resolve_retry(config.retry, sync_options)

        with httpx.Client(timeout=30.0) as client:
            if not sync_options.uses_fill_empty:
                # Preserve the original path exactly: acquire and render each
                # record in sequence, so an earlier fatal HTTP error prevents a
                # later record's template from being evaluated.
                for i, record in enumerate(records):
                    rate_limiter.acquire()
                    if config.properties_template:
                        try:
                            rendered = render_template(config.properties_template, record)
                            properties = json.loads(rendered)
                        except (ValueError, json.JSONDecodeError) as e:
                            record_row_error(
                                result,
                                i,
                                json.dumps(record, default=str)[:200],
                                e,
                                error_message=f"properties_template error: {e}",
                            )
                            continue
                    else:
                        properties = record

                    payload = {"properties": properties, "idProperty": config.id_property}

                    def do_write(
                        _url: str = upsert_url,
                        _headers: dict[str, str] = headers,
                        _payload: dict[str, Any] = payload,
                    ) -> httpx.Response | None:
                        id_value = _payload["properties"].get(config.id_property)
                        patch_url = f"{_url}/{id_value}?idProperty={config.id_property}"
                        if policy == "update_only":
                            response = client.patch(
                                patch_url,
                                json={"properties": _payload["properties"]},
                                headers=_headers,
                            )
                            if response.status_code == 404:
                                return None
                            response.raise_for_status()
                            return response

                        response = client.post(_url, json=_payload, headers=_headers)
                        if response.status_code == 409:
                            if policy == "create_only":
                                return None
                            response = client.patch(
                                patch_url,
                                json={"properties": _payload["properties"]},
                                headers=_headers,
                            )
                        response.raise_for_status()
                        return response

                    try:
                        written = with_retry(do_write, retry_config)
                        if written is None:
                            result.skipped += 1
                            result.skipped_no_match += 1
                        else:
                            result.success += 1
                    except httpx.HTTPStatusError as e:
                        record_row_error(
                            result,
                            i,
                            json.dumps(record, default=str)[:200],
                            e,
                            http_status=e.response.status_code,
                            error_message=e.response.text[:500],
                        )
                        if sync_options.on_error == "fail":
                            break
                    except Exception as e:
                        record_row_error(result, i, json.dumps(record, default=str)[:200], e)
                        if sync_options.on_error == "fail":
                            break
                return result

            self._validate_write_policy_overrides(
                client,
                headers,
                config,
                sync_options,
                rate_limiter,
                retry_config,
                hashlib.sha256(token.encode("utf-8")).hexdigest(),
            )

            stop = False
            # Preserve successful writes across this load's 100-record read
            # chunks. Separate engine batches necessarily rely on HubSpot's next
            # read observing the preceding write.
            written_current_by_id: dict[str, dict[str, Any]] = {}
            for chunk_start in range(0, len(records), 100):
                chunk: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
                for offset, record in enumerate(records[chunk_start : chunk_start + 100]):
                    i = chunk_start + offset
                    if config.properties_template:
                        try:
                            rendered = render_template(config.properties_template, record)
                            properties = json.loads(rendered)
                            if not isinstance(properties, dict):
                                raise ValueError("properties_template must render a JSON object")
                        except (ValueError, json.JSONDecodeError) as e:
                            record_row_error(
                                result,
                                i,
                                json.dumps(record, default=str)[:200],
                                e,
                                error_message=f"properties_template error: {e}",
                            )
                            if sync_options.on_error == "fail":
                                stop = True
                                break
                            continue
                    else:
                        properties = record
                    chunk.append((i, record, properties))

                if stop:
                    break
                if not chunk:
                    continue

                fill_properties = sorted(
                    {
                        property_name
                        for _, _, properties in chunk
                        for property_name in sync_options.fill_empty_columns(properties)
                        if property_name != config.id_property
                    }
                )
                current_by_id: dict[str, dict[str, Any]] = {}
                if fill_properties:
                    rate_limiter.acquire()
                    try:
                        current_by_id = with_retry(
                            lambda: self._read_current_properties(
                                client,
                                batch_read_url,
                                headers,
                                config.id_property,
                                fill_properties,
                                [properties.get(config.id_property) for _, _, properties in chunk],
                            ),
                            retry_config,
                        )
                    except Exception as e:
                        http_status = (
                            e.response.status_code if isinstance(e, httpx.HTTPStatusError) else None
                        )
                        error_message = (
                            e.response.text[:500] if isinstance(e, httpx.HTTPStatusError) else None
                        )
                        failed_rows = chunk[:1] if sync_options.on_error == "fail" else chunk
                        for i, record, _ in failed_rows:
                            record_row_error(
                                result,
                                i,
                                json.dumps(record, default=str)[:200],
                                e,
                                http_status=http_status,
                                error_message=error_message,
                            )
                        if sync_options.on_error == "fail":
                            break
                        continue

                    chunk_id_keys = {
                        str(properties.get(config.id_property)) for _, _, properties in chunk
                    }
                    for id_key, written_properties in written_current_by_id.items():
                        if id_key in chunk_id_keys:
                            current_by_id.setdefault(id_key, {}).update(written_properties)

                for i, record, properties in chunk:
                    id_key = str(properties.get(config.id_property))
                    exists = id_key in current_by_id if fill_properties else False
                    write_properties = properties
                    if fill_properties and exists:
                        if policy == "create_only":
                            result.skipped += 1
                            result.skipped_no_match += 1
                            continue

                        fill_for_record = sync_options.fill_empty_columns(properties) - {
                            config.id_property
                        }
                        kept = (
                            fill_for_record
                            if id_key in written_current_by_id
                            else {
                                property_name
                                for property_name in fill_for_record
                                if not _is_empty(current_by_id[id_key].get(property_name))
                            }
                        )
                        result.kept_values += len(kept)
                        if kept:
                            write_properties = {
                                name: value
                                for name, value in properties.items()
                                if name not in kept
                            }
                        if set(write_properties) <= {config.id_property}:
                            result.skipped += 1
                            continue
                    elif fill_properties and not exists and policy == "update_only":
                        result.skipped += 1
                        result.skipped_no_match += 1
                        continue

                    payload = {
                        "properties": write_properties,
                        "idProperty": config.id_property,
                    }
                    rate_limiter.acquire()

                    def do_fill_write(
                        _url: str = upsert_url,
                        _headers: dict[str, str] = headers,
                        _payload: dict[str, Any] = payload,
                        _exists: bool = exists,
                    ) -> httpx.Response | None:
                        patch_url = (
                            f"{_url}/{_payload['properties'].get(config.id_property)}"
                            f"?idProperty={config.id_property}"
                        )
                        if _exists:
                            response = client.patch(
                                patch_url,
                                json={"properties": _payload["properties"]},
                                headers=_headers,
                            )
                            response.raise_for_status()
                            return response
                        if policy == "update_only":
                            response = client.patch(
                                patch_url,
                                json={"properties": _payload["properties"]},
                                headers=_headers,
                            )
                            if response.status_code == 404:
                                return None
                            response.raise_for_status()
                            return response

                        response = client.post(_url, json=_payload, headers=_headers)
                        if response.status_code == 409:
                            if policy == "create_only":
                                return None
                            if fill_properties:
                                raise ValueError(
                                    "HubSpot fill_empty: record appeared between read and write; "
                                    "re-run"
                                )
                            response = client.patch(
                                patch_url,
                                json={"properties": _payload["properties"]},
                                headers=_headers,
                            )
                        response.raise_for_status()
                        return response

                    try:
                        written = with_retry(do_fill_write, retry_config)
                        if written is None:
                            result.skipped += 1
                            result.skipped_no_match += 1
                        else:
                            result.success += 1
                            written_current_by_id.setdefault(id_key, {}).update(write_properties)
                            if fill_properties:
                                current_by_id.setdefault(id_key, {}).update(write_properties)
                    except httpx.HTTPStatusError as e:
                        record_row_error(
                            result,
                            i,
                            json.dumps(record, default=str)[:200],
                            e,
                            http_status=e.response.status_code,
                            error_message=e.response.text[:500],
                        )
                        if sync_options.on_error == "fail":
                            stop = True
                            break
                    except Exception as e:
                        record_row_error(result, i, json.dumps(record, default=str)[:200], e)
                        if sync_options.on_error == "fail":
                            stop = True
                            break
                if stop:
                    break

        return result

    def _validate_write_policy_overrides(
        self,
        client: httpx.Client,
        headers: dict[str, str],
        config: HubSpotDestinationConfig,
        sync_options: SyncOptions,
        rate_limiter: RateLimiterBackend,
        retry_config: RetryConfig,
        token_digest: str,
    ) -> None:
        """Validate override names once per object type and token when schemas are exposed."""
        if not sync_options.write_policy_overrides:
            return

        object_type = config.object_type
        cache_key = (object_type, token_digest)
        if cache_key not in self._property_names:
            rate_limiter.acquire()

            def fetch_properties() -> frozenset[str]:
                response = client.get(f"{_HUBSPOT_PROPERTIES_API}/{object_type}", headers=headers)
                response.raise_for_status()
                body = response.json()
                if not isinstance(body, dict) or not isinstance(body.get("results"), list):
                    raise ValueError("HubSpot properties response has no usable results list")
                names: list[str] = []
                for item in body["results"]:
                    if (
                        not isinstance(item, dict)
                        or not isinstance(item.get("name"), str)
                        or not item["name"]
                    ):
                        raise ValueError("HubSpot properties response contains an invalid property")
                    names.append(item["name"])
                if not names:
                    raise ValueError("HubSpot properties response contains no properties")
                return frozenset(names)

            try:
                self._property_names[cache_key] = with_retry(fetch_properties, retry_config)
            except Exception as exc:
                self._property_names[cache_key] = None
                logger.warning(
                    "HubSpot property-name validation skipped for %s: %s", object_type, exc
                )

        property_names = self._property_names[cache_key]
        if property_names is None:
            return
        unseen = sync_options.unseen_write_policy_overrides(property_names)
        if unseen:
            raise ValueError(
                f"sync.write_policy_overrides names property/properties {unseen} that are not "
                f"properties of the HubSpot {object_type} object; check the spelling."
            )

    @staticmethod
    def _read_current_properties(
        client: httpx.Client,
        url: str,
        headers: dict[str, Any],
        id_property: str,
        property_names: list[str],
        id_values: list[Any],
    ) -> dict[str, dict[str, Any]]:
        """Read current fill-policy values for at most 100 HubSpot objects.

        The request/response boundary is kept in one helper so a future
        propertiesWithHistory preservation policy (#1237) can extend the same
        batch read. HTTP 207 is a successful partial result only when every
        missing input has a correlated ``OBJECT_NOT_FOUND`` error.
        """
        if len(id_values) > 100:
            raise ValueError("HubSpot batch read accepts at most 100 inputs")
        response = client.post(
            url,
            json={
                "idProperty": id_property,
                # The id property is requested too: HubSpot answers a custom
                # ``idProperty`` lookup with the internal object ID in ``id``, so
                # the returned property value is what ties a result to its input.
                "properties": sorted({*property_names, id_property}),
                "propertiesWithHistory": [],
                "inputs": [{"id": value} for value in id_values],
            },
            headers=headers,
        )
        if response.status_code not in {200, 207}:
            response.raise_for_status()
            raise httpx.HTTPStatusError(
                f"Unexpected HubSpot batch read status {response.status_code}",
                request=response.request,
                response=response,
            )
        body = response.json()
        if not isinstance(body, dict):
            raise ValueError("HubSpot batch read response must be an object")
        if body.get("status") != "COMPLETE":
            raise ValueError("HubSpot batch read response status must be COMPLETE")
        results = body.get("results")
        if not isinstance(results, list):
            raise ValueError("HubSpot batch read response must contain a 'results' list")
        requested_ids = {str(value) for value in id_values}
        # HubSpot normalises some identifiers (emails come back lower-cased), so
        # fall back to a case-insensitive match, but only where it is unambiguous.
        folded: dict[str, str | None] = {}
        for requested in requested_ids:
            key = requested.casefold()
            folded[key] = requested if key not in folded else None
        current: dict[str, dict[str, Any]] = {}
        for item in results:
            if not isinstance(item, dict) or "id" not in item:
                raise ValueError("HubSpot batch read returned an invalid result")
            properties = item.get("properties", {})
            if not isinstance(properties, dict):
                raise ValueError("HubSpot batch read result 'properties' must be an object")
            # HubSpot object responses conventionally expose the internal
            # object ID in ``id`` even when the request used a custom unique
            # ``idProperty``. Prefer that property's returned value when it is
            # present, while retaining the documented/requested-ID response
            # shape supported by existing accounts and mocks. If neither can
            # be correlated, fail closed rather than treating an existing
            # record as new and POSTing an unfiltered payload.
            property_id = properties.get(id_property)
            candidates = [property_id, item["id"]]
            matched_id = next(
                (str(candidate) for candidate in candidates if str(candidate) in requested_ids),
                None,
            )
            if matched_id is None:
                matched_id = next(
                    (
                        folded[str(candidate).casefold()]
                        for candidate in candidates
                        if candidate is not None and folded.get(str(candidate).casefold())
                    ),
                    None,
                )
            if matched_id is None:
                raise ValueError(
                    "HubSpot batch read result could not be correlated to its requested "
                    f"{id_property!r} value"
                )
            if matched_id in current:
                raise ValueError("HubSpot batch read returned duplicate results for one input")
            current[matched_id] = properties

        errors = body.get("errors", [])
        if not isinstance(errors, list):
            raise ValueError("HubSpot batch read response 'errors' must be a list")
        num_errors = body.get("numErrors", len(errors))
        if not isinstance(num_errors, int) or isinstance(num_errors, bool):
            raise ValueError("HubSpot batch read response 'numErrors' must be an integer")
        if num_errors != len(errors):
            raise ValueError("HubSpot batch read response error count is inconsistent")

        missing_ids: set[str] = set()
        for error in errors:
            if not isinstance(error, dict) or error.get("category") != "OBJECT_NOT_FOUND":
                raise ValueError("HubSpot batch read reported an error other than OBJECT_NOT_FOUND")
            context = error.get("context")
            context_ids = context.get("ids") if isinstance(context, dict) else None
            if not isinstance(context_ids, list) or not context_ids:
                raise ValueError(
                    "HubSpot OBJECT_NOT_FOUND error must identify at least one requested input"
                )
            for context_id in context_ids:
                requested = str(context_id)
                if requested not in requested_ids:
                    requested = folded.get(requested.casefold()) or ""
                if requested not in requested_ids:
                    raise ValueError(
                        "HubSpot OBJECT_NOT_FOUND error references an unrequested input"
                    )
                if requested in current:
                    raise ValueError("HubSpot batch read response has conflicting input outcomes")
                missing_ids.add(requested)

        unresolved = requested_ids - set(current) - missing_ids
        if unresolved:
            raise ValueError(
                "HubSpot batch read omitted requested inputs without OBJECT_NOT_FOUND errors"
            )
        return current

    def supported_match_policies(self) -> frozenset[str]:
        """HubSpot honours all three ``match_policy`` values (#757).

        The v3 Objects API's POST-409 / PATCH-404 responses map exactly onto
        create-only / update-only, so skips are detected without a separate
        existence lookup. Declaring this makes the engine's
        ``MatchPolicyCapable`` guard accept a non-default policy here.
        """
        return frozenset({"upsert", "update_only", "create_only"})

    def supported_write_policies(self) -> frozenset[str]:
        """HubSpot honours ``fill_empty`` through a fail-closed batch read (#1238)."""
        return frozenset({"overwrite", "fill_empty"})
