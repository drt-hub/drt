"""Elasticsearch / OpenSearch destination — bulk-index records via the ``_bulk`` API.

Syncing DWH data into Elasticsearch (or API-compatible OpenSearch) powers
search UIs and dashboards (Kibana / OpenSearch Dashboards). This
destination indexes each sync batch through the cluster's ``_bulk``
endpoint — one HTTP round-trip per batch — using only core ``httpx`` (no
heavy client library, no extra install).

Document shape: each source row becomes a document. The whole row is the
``_source``; if ``id_field`` is set, that field's value becomes the
document ``_id`` (so re-runs update the same document under
``op_type: index``). Without ``id_field`` the cluster auto-generates ids.

``op_type``:

- ``index`` (default) — upsert: replaces the document if the ``_id``
  already exists.
- ``create`` — insert-only: a row whose ``_id`` already exists fails with
  a 409 and is recorded as a row error (the rest of the batch still
  indexes).

Per-document errors: the ``_bulk`` API returns HTTP 200 even when
individual documents fail (it sets ``errors: true`` and a per-item
``error``). This destination parses the ``items`` array and maps each
failure back to its source row, so ``result.row_errors`` reflects exactly
which rows the cluster rejected — not just transport-level failures.

Authentication: provide an **API key** (``api_key`` / ``api_key_env`` →
``Authorization: ApiKey <key>``) or **HTTP Basic** credentials
(``username_env`` / ``password_env``). Set ``verify_tls: false`` for
self-signed dev clusters.

Requires only core ``httpx``. Example sync YAML:

    destination:
      type: elasticsearch
      url: https://localhost:9200
      api_key_env: ES_API_KEY
      index: customers
      id_field: user_id
      op_type: index
"""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, ElasticsearchDestinationConfig, SyncOptions
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

logger = logging.getLogger(__name__)


class ElasticsearchDestination:
    """Bulk-index records into an Elasticsearch / OpenSearch cluster."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, ElasticsearchDestinationConfig)
        if not records:
            return SyncResult()

        headers = self._auth_headers(config)
        bulk_url = f"{config.url.rstrip('/')}/_bulk"
        retry_config = resolve_retry(config.retry, sync_options)
        rate_limiter = resolve_rate_limiter(config, sync_options, limiter_factory=RateLimiter)
        result = SyncResult()

        chunks = _chunks(list(enumerate(records)), sync_options.batch_size)
        with httpx.Client(timeout=30.0, verify=config.verify_tls) as client:
            for chunk_number, chunk in enumerate(chunks):
                body = _build_bulk_body(chunk, config)
                try:
                    rate_limiter.acquire()

                    def _send(body: str = body) -> httpx.Response:
                        resp = client.post(
                            bulk_url,
                            content=body,
                            headers={**headers, "Content-Type": "application/x-ndjson"},
                        )
                        resp.raise_for_status()
                        return resp

                    operation, on_delivery_unknown = _replay_policy(
                        chunk, config, retry_config.max_attempts
                    )
                    response = with_retry(
                        _send,
                        retry_config,
                        operation=operation,
                        on_delivery_unknown=on_delivery_unknown,
                    )
                except DeliveryUnknownError as e:
                    for index, record in chunk:
                        _add_row_error(result, index, record, e.http_status, str(e))
                    if sync_options.on_error == "fail":
                        _mark_unattempted(result, chunks[chunk_number + 1 :])
                        break
                    continue
                except httpx.HTTPStatusError as e:
                    # Whole-batch failure (auth, 5xx after retries, bad URL).
                    for index, record in chunk:
                        _add_row_error(
                            result, index, record, e.response.status_code, e.response.text[:500]
                        )
                    if sync_options.on_error == "fail":
                        _mark_unattempted(result, chunks[chunk_number + 1 :])
                        break
                    continue
                except Exception as e:
                    for index, record in chunk:
                        _add_row_error(result, index, record, None, str(e))
                    if sync_options.on_error == "fail":
                        _mark_unattempted(result, chunks[chunk_number + 1 :])
                        break
                    continue

                # HTTP 200 — but individual docs may still have failed.
                stop = self._record_item_results(response, chunk, result, sync_options)
                if stop:
                    _mark_unattempted(result, chunks[chunk_number + 1 :])
                    break

        return result

    @staticmethod
    def _record_item_results(
        response: httpx.Response,
        chunk: list[tuple[int, dict[str, Any]]],
        result: SyncResult,
        sync_options: SyncOptions,
    ) -> bool:
        """Map per-document bulk results back to source rows.

        Returns ``True`` when the sync should stop (``on_error: fail`` and a
        document was rejected).
        """
        payload = response.json()
        items = payload.get("items") if isinstance(payload, dict) else None
        # A _bulk 200 always carries one result per submitted document. A missing,
        # non-list or short array is an unverifiable response, not success: rows are
        # failed individually (and honour on_error) instead of silently advancing state.
        if not isinstance(items, list):
            items = []

        any_failed = False
        for item_number, (index, record) in enumerate(chunk):
            # Each item is keyed by the action ("index" / "create"); the value
            # carries status + optional error. Read the single value robustly.
            if item_number >= len(items):
                _add_row_error(
                    result,
                    index,
                    record,
                    None,
                    "bulk response omitted this item",
                )
                any_failed = True
                continue
            item = items[item_number]
            outcome = next(iter(item.values()), None) if isinstance(item, dict) else None
            status = outcome.get("status") if isinstance(outcome, dict) else None
            error = outcome.get("error") if isinstance(outcome, dict) else None
            # Success needs positive evidence: a mapping outcome with a numeric 2xx
            # status and no error. Anything else is an unverifiable result, never a
            # silent success that would let state advance past an unwritten document.
            code = status if isinstance(status, int) and not isinstance(status, bool) else None
            if error is None and code is not None and 200 <= code < 300:
                result.success += 1
                continue
            reason = ""
            if isinstance(error, dict):
                reason = error.get("reason") or error.get("type") or json.dumps(error)[:300]
            elif error is None and not (code is not None and code >= 400):
                reason = "bulk item result was malformed or had no 2xx status"
            _add_row_error(
                result,
                index,
                record,
                code or None,
                reason or "bulk item failed",
            )
            any_failed = True
        return any_failed and sync_options.on_error == "fail"

    @staticmethod
    def _auth_headers(config: ElasticsearchDestinationConfig) -> dict[str, str]:
        api_key = resolve_env(config.api_key, config.api_key_env)
        if api_key:
            return {"Authorization": f"ApiKey {api_key}"}

        username = resolve_env(None, config.username_env) if config.username_env else None
        password = resolve_env(None, config.password_env) if config.password_env else None
        if username and password:
            import base64

            token = base64.b64encode(f"{username}:{password}".encode()).decode()
            return {"Authorization": f"Basic {token}"}

        raise ValueError(
            "Elasticsearch destination: provide api_key / api_key_env, or "
            "both username_env and password_env for HTTP Basic auth."
        )


def _build_bulk_body(
    chunk: list[tuple[int, dict[str, Any]]],
    config: ElasticsearchDestinationConfig,
) -> str:
    """Build the newline-delimited (NDJSON) ``_bulk`` request body.

    Two lines per document: an action/metadata line then the source line.
    The body MUST end with a trailing newline or the cluster rejects it.
    """
    lines: list[str] = []
    for _index, record in chunk:
        action: dict[str, Any] = {"_index": config.index}
        if config.id_field is not None:
            doc_id = record.get(config.id_field)
            if doc_id is not None:
                action["_id"] = str(doc_id)
        lines.append(json.dumps({config.op_type: action}, default=str))
        lines.append(json.dumps(record, default=str))
    return "\n".join(lines) + "\n"


def _chunks(
    items: list[tuple[int, dict[str, Any]]], size: int
) -> list[list[tuple[int, dict[str, Any]]]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _replay_policy(
    chunk: list[tuple[int, dict[str, Any]]],
    config: ElasticsearchDestinationConfig,
    max_attempts: int,
) -> tuple[OperationSafety, Callable[[Exception, int], RetryDirective] | None]:
    """Classify the whole bulk request; one auto-id makes the batch blind."""
    if config.id_field is not None and all(
        record.get(config.id_field) is not None for _, record in chunk
    ):
        return OperationSafety.KEYED, None

    def on_delivery_unknown(error: Exception, attempt: int) -> RetryDirective:
        return handle_blind_delivery_unknown(
            error,
            attempt,
            retry_creates=config.retry_creates,
            max_attempts=max_attempts,
            destination="Elasticsearch/OpenSearch",
            operation="auto-id bulk request",
            logger=logger,
        )

    return OperationSafety.BLIND_CREATE, on_delivery_unknown


def _mark_unattempted(
    result: SyncResult,
    chunks: list[list[tuple[int, dict[str, Any]]]],
) -> None:
    indices = [index for chunk in chunks for index, _ in chunk]
    result.skipped += len(indices)
    result.unattempted.extend(indices)


def _add_row_error(
    result: SyncResult,
    index: int,
    record: dict[str, Any],
    http_status: int | None,
    message: str,
) -> None:
    record_row_error(
        result,
        index,
        json.dumps(record, default=str)[:200],
        Exception(message),
        http_status=http_status,
        error_message=message,
    )
