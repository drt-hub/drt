"""Pipedrive destination: search by match_field, then create or update (#1183).

Pipedrive has no native upsert, so each record is looked up first
(``GET /persons/search`` with ``exact_match``) and then PATCHed when found or
POSTed when not found. Only ``object: person`` is supported for now.

Row -> payload mapping (columns arrive already renamed by ``sync.field_mappings``):
- ``name``, ``owner_id``, ``org_id``, ``visible_to``, ``label_ids`` pass through;
- ``email`` / ``phone`` become Pipedrive's ``emails`` / ``phones`` lists;
- every other column goes into ``custom_fields``, so ``field_mappings`` must rename it to
  Pipedrive's 40-character field key. An unknown key is rejected by Pipedrive and surfaces
  as a per-row error carrying the API's message; nothing is silently dropped.

The token is sent in the ``x-api-token`` header, never in the URL, so it cannot end up in
request logs. Each row spends about 20 API tokens on the search plus 5 on the write.
Repeated match values within one run are remembered: a second row with the same key
updates the person the first row just created instead of creating a duplicate.
"""

from __future__ import annotations

from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import (
    DestinationConfig,
    PipedriveDestinationConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiterBackend, resolve_rate_limiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import record_preview, record_row_error

_PIPEDRIVE_API = "https://api.pipedrive.com/api/v2"

_PASSTHROUGH_KEYS = frozenset({"name", "owner_id", "org_id", "visible_to", "label_ids"})
_CONTACT_KEYS = {"email": "emails", "emails": "emails", "phone": "phones", "phones": "phones"}


def _keys_for(field: str) -> frozenset[str]:
    """Column names that carry the same data as ``field`` (email/emails, phone/phones)."""
    return frozenset({field, f"{field}s"}) if field in ("email", "phone") else frozenset({field})


def _contact_list(value: Any) -> list[dict[str, Any]]:
    """Turn 'a@b.com' (or a list of them) into Pipedrive's [{value, primary}] shape."""
    values = value if isinstance(value, list) else [value]
    contacts = [
        item if isinstance(item, dict) else {"value": str(item)}
        for item in values
        if item is not None and item != ""
    ]
    if contacts and not any(contact.get("primary") for contact in contacts):
        contacts[0] = {**contacts[0], "primary": True}
    return contacts


def _build_payload(
    record: dict[str, Any], *, creating: bool, skip_field: str | None = None
) -> dict[str, Any]:
    """Map one warehouse row to a Pipedrive person payload.

    On update the match field itself is left out: it already matches, and sending
    ``emails`` / ``phones`` would replace the person's other addresses and numbers.
    """
    skip = _keys_for(skip_field) if skip_field else frozenset()
    payload: dict[str, Any] = {}
    custom_fields: dict[str, Any] = {}
    for key, value in record.items():
        if key in skip:
            continue
        if key in _PASSTHROUGH_KEYS:
            payload[key] = value
        elif key in _CONTACT_KEYS:
            contacts = _contact_list(value)
            if contacts:
                payload[_CONTACT_KEYS[key]] = contacts
        else:
            custom_fields[key] = value
    if custom_fields:
        payload["custom_fields"] = custom_fields
    if creating and not payload.get("name"):
        raise ValueError("Pipedrive: 'name' is required to create a person; map a column to name.")
    return payload


class _PipedriveApi:
    """Thin HTTP wrapper: token header, rate limiting and retries in one place."""

    def __init__(
        self,
        client: httpx.Client,
        base_url: str,
        headers: dict[str, str],
        limiter: RateLimiterBackend,
        retry_config: RetryConfig,
    ) -> None:
        self._client = client
        self._base_url = base_url
        self._headers = headers
        self._limiter = limiter
        self._retry = retry_config

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        self._limiter.acquire()

        def send() -> httpx.Response:
            response = self._client.request(
                method, f"{self._base_url}{path}", headers=self._headers, **kwargs
            )
            response.raise_for_status()
            return response

        return with_retry(send, self._retry)

    def search(self, term: str, field: str) -> list[int]:
        """Ids of persons whose ``field`` exactly equals ``term`` (at most two are fetched)."""
        response = self._request(
            "GET",
            "/persons/search",
            params={"term": term, "fields": field, "exact_match": "true", "limit": 2},
        )
        body = response.json()
        data = body.get("data") if isinstance(body, dict) else None
        items = data.get("items") if isinstance(data, dict) else None
        if not isinstance(items, list):
            raise ValueError(
                "Pipedrive search response has no usable data.items list; "
                "not writing, to avoid creating a duplicate."
            )
        ids: list[int] = []
        for entry in items:
            item = entry.get("item") if isinstance(entry, dict) else None
            if not isinstance(item, dict) or "id" not in item:
                raise ValueError("Pipedrive search response contains an invalid item.")
            ids.append(int(item["id"]))
        return ids

    def create(self, payload: dict[str, Any]) -> int | None:
        """POST a new person; returns its id when the response carries one."""
        body = self._request("POST", "/persons", json=payload).json()
        data = body.get("data") if isinstance(body, dict) else None
        person_id = data.get("id") if isinstance(data, dict) else None
        return int(person_id) if person_id is not None else None

    def update(self, person_id: int, payload: dict[str, Any]) -> None:
        self._request("PATCH", f"/persons/{person_id}", json=payload)


class PipedriveDestination:
    """Create or update Pipedrive persons, matched by an exact search."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, PipedriveDestinationConfig)
        if not records:
            return SyncResult()

        token = resolve_env(config.api_token, config.api_token_env)
        if not token:
            raise ValueError(
                "Pipedrive destination: set the PIPEDRIVE_API_TOKEN env var "
                "(or the variable named by api_token_env)."
            )

        result = SyncResult()
        base_url = (config.base_url or _PIPEDRIVE_API).rstrip("/")
        # match value (case-folded) -> person id, for rows found or created in this run.
        # None means "created in this run, but Pipedrive did not return the id".
        known_ids: dict[str, int | None] = {}
        with httpx.Client(timeout=30.0) as client:
            api = _PipedriveApi(
                client,
                base_url,
                {"x-api-token": token, "Accept": "application/json"},
                resolve_rate_limiter(config, sync_options),
                resolve_retry(config.retry, sync_options),
            )
            for index, record in enumerate(records):
                try:
                    self._load_one(
                        api, config, sync_options.match_policy, record, known_ids, result
                    )
                except httpx.HTTPStatusError as exc:
                    record_row_error(
                        result,
                        index,
                        record_preview(record),
                        exc,
                        http_status=exc.response.status_code,
                        error_message=exc.response.text[:500],
                    )
                    if sync_options.on_error == "fail":
                        break
                except Exception as exc:  # noqa: BLE001 - one bad row must not stop the batch
                    record_row_error(result, index, record_preview(record), exc)
                    if sync_options.on_error == "fail":
                        break
        return result

    @staticmethod
    def _load_one(
        api: _PipedriveApi,
        config: PipedriveDestinationConfig,
        policy: str,
        record: dict[str, Any],
        known_ids: dict[str, int | None],
        result: SyncResult,
    ) -> None:
        raw = record.get(config.match_field)
        term = "" if raw is None else str(raw).strip()
        if not term:
            raise ValueError(
                f"Pipedrive: record has no value for match_field {config.match_field!r}."
            )
        cache_key = term.casefold()

        person_id: int | None = None
        if cache_key in known_ids:
            person_id = known_ids[cache_key]
            if person_id is None:
                raise ValueError(
                    "Pipedrive: an earlier row in this run created this person but the "
                    "response had no id; not writing, to avoid creating a duplicate."
                )
        else:
            matches = api.search(term, config.match_field)
            if len(matches) > 1:
                raise ValueError(
                    f"Pipedrive: more than one person matches {config.match_field} {term!r}; "
                    "refusing to guess which one to update."
                )
            if matches:
                person_id = matches[0]
                known_ids[cache_key] = person_id

        if person_id is None:
            if policy == "update_only":
                result.skipped += 1
                result.skipped_no_match += 1
                return
            known_ids[cache_key] = api.create(_build_payload(record, creating=True))
        else:
            if policy == "create_only":
                result.skipped += 1
                result.skipped_no_match += 1
                return
            payload = _build_payload(record, creating=False, skip_field=config.match_field)
            if not payload:
                result.skipped += 1  # nothing to change besides the match field itself
                return
            api.update(person_id, payload)
        result.success += 1

    def supported_match_policies(self) -> frozenset[str]:
        """Search-first design: all three ``match_policy`` values are natural (#757)."""
        return frozenset({"upsert", "update_only", "create_only"})
