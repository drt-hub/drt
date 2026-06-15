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
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, ElasticsearchDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import RowError


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
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)
        result = SyncResult()

        with httpx.Client(timeout=30.0, verify=config.verify_tls) as client:
            for chunk in _chunks(list(enumerate(records)), sync_options.batch_size):
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

                    response = with_retry(_send, retry_config)
                except httpx.HTTPStatusError as e:
                    # Whole-batch failure (auth, 5xx after retries, bad URL).
                    for index, record in chunk:
                        _add_row_error(
                            result, index, record, e.response.status_code, e.response.text[:500]
                        )
                    if sync_options.on_error == "fail":
                        break
                    continue
                except Exception as e:
                    for index, record in chunk:
                        _add_row_error(result, index, record, None, str(e))
                    if sync_options.on_error == "fail":
                        break
                    continue

                # HTTP 200 — but individual docs may still have failed.
                stop = self._record_item_results(response, chunk, result, sync_options)
                if stop:
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
        items = payload.get("items", [])

        # Defensive: if the cluster didn't return a per-item array (shouldn't
        # happen on a 200 from _bulk), treat the whole chunk as success.
        if not items:
            result.success += len(chunk)
            return False

        for (index, record), item in zip(chunk, items):
            # Each item is keyed by the action ("index" / "create"); the value
            # carries status + optional error. Read the single value robustly.
            outcome = next(iter(item.values())) if item else {}
            status = outcome.get("status", 0)
            error = outcome.get("error")
            if error is not None or status >= 400:
                reason = ""
                if isinstance(error, dict):
                    reason = error.get("reason") or error.get("type") or json.dumps(error)[:300]
                _add_row_error(result, index, record, status or None, reason or "bulk item failed")
                if sync_options.on_error == "fail":
                    return True
            else:
                result.success += 1
        return False

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
