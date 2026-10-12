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

import json
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, HubSpotDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import resolve_rate_limiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import record_row_error
from drt.templates.renderer import render_template

_HUBSPOT_API = "https://api.hubapi.com/crm/v3/objects"


def _is_empty(value: Any) -> bool:
    """Match the engine diff's fill-empty rule for HubSpot property values."""
    return value is None or (isinstance(value, str) and value.strip(" ") == "")


class HubSpotDestination:
    """Upsert records into HubSpot CRM via the v3 Objects API."""

    identifiers_case_insensitive = False

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
        policy = sync_options.match_policy  # #757 — upsert | update_only | create_only
        result = SyncResult()
        # HubSpot rate limit: 100 req/10s for private apps. Passed as a cap so
        # it clamps the resolved rate before the shared bucket is created — a
        # destination-level override cannot exceed what HubSpot publishes.
        rate_limiter = resolve_rate_limiter(config, sync_options, max_requests_per_second=9)
        retry_config = resolve_retry(config.retry, sync_options)

        # Render the complete load call before any HTTP request. Override names
        # are validated against this union, rather than one 100-record read
        # chunk, so a sparse property is not mistaken for a typo after an
        # earlier chunk has already been written.
        prepared: list[tuple[int, dict[str, Any], dict[str, Any]]] = []
        rendered_property_names: set[str] = set()
        for i, record in enumerate(records):
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
                    continue
            else:
                properties = record

            prepared.append((i, record, properties))
            rendered_property_names.update(properties)

        unseen = sync_options.unseen_write_policy_overrides(
            rendered_property_names,
            case_insensitive=self.identifiers_case_insensitive,
        )
        if unseen:
            raise ValueError(
                f"sync.write_policy_overrides names property/properties {unseen} that are not "
                "properties rendered by any record; check the spelling."
            )

        with httpx.Client(timeout=30.0) as client:
            stop = False
            # Carries successful writes across 100-record read chunks. Even if
            # HubSpot's next read is briefly stale, a repeated ID in this same
            # load still sees what the first writer just filled.
            written_current_by_id: dict[str, dict[str, Any]] = {}
            for chunk_start in range(0, len(prepared), 100):
                chunk = prepared[chunk_start : chunk_start + 100]
                fill_properties = sorted(
                    {
                        property_name
                        for _, _, properties in chunk
                        for property_name in sync_options.fill_empty_columns(
                            properties,
                            case_insensitive=self.identifiers_case_insensitive,
                        )
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
                    id_value = properties.get(config.id_property)
                    id_key = str(id_value)
                    exists = id_key in current_by_id if fill_properties else False
                    write_properties = properties

                    if fill_properties and exists:
                        if policy == "create_only":
                            result.skipped += 1
                            result.skipped_no_match += 1
                            continue

                        fill_for_record = sync_options.fill_empty_columns(
                            properties,
                            case_insensitive=self.identifiers_case_insensitive,
                        ) - {config.id_property}
                        kept = {
                            property_name
                            for property_name in fill_for_record
                            if not _is_empty(current_by_id[id_key].get(property_name))
                        }
                        result.kept_values += len(kept)
                        if kept:
                            write_properties = {
                                property_name: value
                                for property_name, value in properties.items()
                                if property_name not in kept
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

                    def do_write(
                        _url: str = upsert_url,
                        _headers: dict[str, Any] = headers,
                        _payload: dict[str, Any] = payload,
                        _exists: bool = exists,
                    ) -> httpx.Response | None:
                        """Write one record, honouring match and write policies.

                        A fill-policy batch read makes existing rows safe to PATCH
                        directly. Missing rows still follow the original POST-first
                        path, including its 409 race fallback; that POST always gets
                        the full, unfiltered properties.
                        """
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

                        # create_only + upsert both POST first (idProperty dedupes).
                        response = client.post(_url, json=_payload, headers=_headers)
                        if response.status_code == 409:  # already exists
                            if policy == "create_only":
                                return None  # exists — skip, don't overwrite
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
                            result.skipped_no_match += 1  # #757 — policy declined
                        else:
                            result.success += 1
                            if fill_properties:
                                current = current_by_id.setdefault(id_key, {})
                                current.update(write_properties)
                                written_current_by_id.setdefault(id_key, {}).update(
                                    write_properties
                                )
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
                        record_row_error(
                            result,
                            i,
                            json.dumps(record, default=str)[:200],
                            e,
                        )
                        if sync_options.on_error == "fail":
                            stop = True
                            break
                if stop:
                    break

        return result

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
        batch read. HTTP 207 is a successful partial result: IDs absent from
        ``results`` are treated as objects that do not exist yet.
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
        results = body.get("results", [])
        if not isinstance(results, list):
            raise ValueError("HubSpot batch read response 'results' must be a list")
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
            current[matched_id] = properties
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
