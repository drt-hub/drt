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

import logging
from collections.abc import Iterator
from typing import Any
from urllib.parse import quote

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import AirtableDestinationConfig, DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter, resolve_rate_limiter
from drt.destinations.retry import (
    DeliveryUnknownError,
    OperationSafety,
    RetryDirective,
    handle_blind_delivery_unknown,
    resolve_retry,
    with_retry,
)
from drt.destinations.row_errors import record_row_error

_BASE_URL = "https://api.airtable.com/v0"
_MAX_BATCH = 10  # Airtable API limit

logger = logging.getLogger(__name__)


def _has_merge_key(record: dict[str, Any], primary_key: str | None) -> bool:
    """True when *record* has a non-null, non-blank value for the merge field."""
    if primary_key is None:
        return False
    value = record.get(primary_key)
    if value is None or isinstance(value, bool):
        return False
    if isinstance(value, str):
        return bool(value.strip())
    return isinstance(value, (int, float))


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
        rate_limiter = resolve_rate_limiter(config, sync_options, limiter_factory=RateLimiter)
        result = SyncResult()

        chunks = list(_chunks(list(enumerate(records)), _MAX_BATCH))
        with httpx.Client(timeout=30.0) as client:
            for chunk_number, chunk in enumerate(chunks):
                body: dict[str, Any] = {"records": [{"fields": rec} for _, rec in chunk]}
                if upsert:
                    body["performUpsert"] = {"fieldsToMergeOn": [config.primary_key]}

                try:
                    rate_limiter.acquire()

                    def _send(b: dict[str, Any] = body) -> httpx.Response:
                        resp = client.request(method, url, headers=headers, json=b)
                        resp.raise_for_status()
                        return resp

                    on_delivery_unknown = None
                    operation = OperationSafety.KEYED
                    # An upsert is replay-safe only when every row carries a usable merge
                    # value: a missing, None or blank key lets Airtable create a record, so
                    # a replay could create a second one (ADR 0023).
                    if not upsert or not all(
                        _has_merge_key(rec, config.primary_key) for _, rec in chunk
                    ):

                        def on_delivery_unknown(error: Exception, attempt: int) -> RetryDirective:
                            return handle_blind_delivery_unknown(
                                error,
                                attempt,
                                retry_creates=config.retry_creates,
                                max_attempts=retry_config.max_attempts,
                                destination="Airtable",
                                operation="append"
                                if not upsert
                                else "upsert with an incomplete merge key",
                                logger=logger,
                            )

                        operation = OperationSafety.BLIND_CREATE

                    with_retry(
                        _send,
                        retry_config,
                        operation=operation,
                        on_delivery_unknown=on_delivery_unknown,
                    )
                    result.success += len(chunk)
                except DeliveryUnknownError as e:
                    self._record_chunk_failure(result, chunk, e.http_status, str(e))
                    if sync_options.on_error == "fail":
                        _mark_unattempted(result, chunks[chunk_number + 1 :])
                        break
                except httpx.HTTPStatusError as e:
                    self._record_chunk_failure(
                        result, chunk, e.response.status_code, e.response.text[:500]
                    )
                    if sync_options.on_error == "fail":
                        _mark_unattempted(result, chunks[chunk_number + 1 :])
                        break
                except Exception as e:
                    self._record_chunk_failure(result, chunk, None, str(e))
                    if sync_options.on_error == "fail":
                        _mark_unattempted(result, chunks[chunk_number + 1 :])
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
            record_row_error(
                result,
                index,
                str(record)[:200],
                Exception(message),
                http_status=status,
                error_message=message,
            )

    def test_connection(self, config: DestinationConfig) -> None:
        """Test the token with Airtable's scope-free identity endpoint.

        ``GET /v0/meta/whoami`` requires no scopes, so this validates any
        valid token without adding ``data.records:read`` to the documented
        minimal write-only credential.
        """
        assert isinstance(config, AirtableDestinationConfig)
        token = resolve_env(config.access_token, config.access_token_env)
        if not token:
            raise ValueError("Airtable destination: missing access token.")
        headers = {"Authorization": f"Bearer {token}"}
        with httpx.Client(timeout=30.0) as client:
            resp = client.get(f"{_BASE_URL}/meta/whoami", headers=headers)
            resp.raise_for_status()


def _chunks(
    items: list[tuple[int, dict[str, Any]]], size: int
) -> Iterator[list[tuple[int, dict[str, Any]]]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _mark_unattempted(
    result: SyncResult,
    chunks: list[list[tuple[int, dict[str, Any]]]],
) -> None:
    indices = [index for chunk in chunks for index, _ in chunk]
    result.skipped += len(indices)
    result.unattempted.extend(indices)
