"""Airtable destination — write records into an Airtable base table.

Supports:

- **append** (default) — ``POST /v0/{base_id}/{table}`` with up to 10
  records per request.
- **upsert** — set ``primary_key`` to a field name; uses Airtable's
  ``PATCH`` with ``performUpsert.fieldsToMergeOn`` so existing rows
  matched on that field are updated and the rest inserted.

Airtable caps batch writes at **10 records per request**, so the batch is
chunked accordingly. The row dict keys become Airtable field names — Airtable
is schema-enforcing, so a field absent from the table (or a type mismatch)
surfaces as a row error.

Auth: a personal access token / OAuth token via ``access_token`` /
``access_token_env`` (Bearer). No extra dependencies beyond core ``httpx``.

Example sync YAML:

    destination:
      type: airtable
      access_token_env: AIRTABLE_TOKEN
      base_id: appXXXXXXXXXXXXXX
      table_name: Customers
      primary_key: record_id      # omit for append-only
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import AirtableDestinationConfig, DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import RowError

_BASE_URL = "https://api.airtable.com/v0"
_MAX_BATCH = 10  # Airtable API limit


class AirtableDestination:
    """Write records into an Airtable table (append or upsert)."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, AirtableDestinationConfig)
        if not records:
            return SyncResult()

        token = resolve_env(config.access_token, config.access_token_env)
        if not token:
            raise ValueError(
                "Airtable destination: provide access_token or set the env var "
                f"named in access_token_env ({config.access_token_env!r})."
            )

        url = f"{_BASE_URL}/{config.base_id}/{quote(config.table_name)}"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }
        upsert = config.primary_key is not None
        method = "PATCH" if upsert else "POST"
        retry_config = resolve_retry(config.retry, sync_options)
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)
        result = SyncResult()

        with httpx.Client(timeout=30.0) as client:
            for chunk in _chunks(list(enumerate(records)), _MAX_BATCH):
                body: dict[str, Any] = {
                    "records": [{"fields": rec} for _, rec in chunk]
                }
                if upsert:
                    body["performUpsert"] = {"fieldsToMergeOn": [config.primary_key]}

                try:
                    rate_limiter.acquire()

                    def _send(b: dict[str, Any] = body) -> httpx.Response:
                        resp = client.request(method, url, headers=headers, json=b)
                        resp.raise_for_status()
                        return resp

                    with_retry(_send, retry_config)
                    result.success += len(chunk)
                except httpx.HTTPStatusError as e:
                    self._record_chunk_failure(
                        result, chunk, e.response.status_code, e.response.text[:500]
                    )
                    if sync_options.on_error == "fail":
                        break
                except Exception as e:
                    self._record_chunk_failure(result, chunk, None, str(e))
                    if sync_options.on_error == "fail":
                        break

        return result

    @staticmethod
    def _record_chunk_failure(
        result: SyncResult,
        chunk: list[tuple[int, dict[str, Any]]],
        status: int | None,
        message: str,
    ) -> None:
        for index, record in chunk:
            result.failed += 1
            result.row_errors.append(
                RowError(
                    batch_index=index,
                    record_preview=str(record)[:200],
                    http_status=status,
                    error_message=message,
                )
            )

    def test_connection(self, config: DestinationConfig) -> None:
        """Test connectivity by reading one record from the table."""
        assert isinstance(config, AirtableDestinationConfig)
        token = resolve_env(config.access_token, config.access_token_env)
        if not token:
            raise ValueError("Airtable destination: missing access token.")
        url = f"{_BASE_URL}/{config.base_id}/{quote(config.table_name)}"
        headers = {"Authorization": f"Bearer {token}"}
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(url, headers=headers, params={"maxRecords": 1})
            resp.raise_for_status()


def _chunks(
    items: list[tuple[int, dict[str, Any]]], size: int
) -> Iterator[list[tuple[int, dict[str, Any]]]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]
