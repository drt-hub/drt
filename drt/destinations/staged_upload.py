"""Staged Upload destination — async bulk-upload APIs.

Supports APIs that require file upload → job trigger → poll for completion
(e.g. Amazon Marketing Cloud, Salesforce Bulk API 2.0).

Three declarative phases in YAML:
  1. Stage: serialize records to file, upload via HTTP
  2. Trigger: kick off server-side job, extract job ID from response
  3. Poll: check job status until success/failure/timeout

Example sync YAML:

    destination:
      type: staged_upload
      format: csv
      stage:
        url: "https://upload.example.com/files"
        method: POST
        auth:
          type: bearer
          token_env: API_TOKEN
        response_extract:
          upload_id: "uploadId"
      trigger:
        url: "https://api.example.com/jobs"
        method: POST
        body_template: '{"uploadId": "{{ upload_id }}"}'
        auth:
          type: bearer
          token_env: API_TOKEN
        response_extract:
          job_id: "jobId"
      poll:
        url: "https://api.example.com/jobs/{{ job_id }}"
        method: GET
        auth:
          type: bearer
          token_env: API_TOKEN
        status_field: "status"
        success_values: ["SUCCEEDED"]
        failure_values: ["FAILED"]
        interval_seconds: 30
        timeout_seconds: 3600
"""

from __future__ import annotations

import csv
import io
import json
import time
from typing import Any
from urllib.parse import urlparse

import httpx
from jinja2 import BaseLoader, Environment, StrictUndefined
from jinja2.exceptions import UndefinedError

from drt.config.models import (
    DestinationConfig,
    RetryConfig,
    StagedUploadDestinationConfig,
    StagedUploadPhaseConfig,
    StagedUploadPollConfig,
    SyncOptions,
)
from drt.destinations.auth import AuthHandler
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import (
    RateLimiter,
    RateLimiterBackend,
    resolve_rate_limiter,
)
from drt.destinations.retry import resolve_retry, with_retry
from drt.templates.renderer import tojson_safe


def _render(template_str: str, context: dict[str, str]) -> str:
    """Render a Jinja2 template with context variables (not row-scoped)."""
    env = Environment(loader=BaseLoader(), undefined=StrictUndefined)
    env.filters["tojson_safe"] = tojson_safe
    try:
        return env.from_string(template_str).render(**context)
    except UndefinedError as e:
        raise ValueError(f"Template error: {e}") from e


class StagedUploadDestination:
    """Accumulate records, then upload as a file and trigger an async job."""

    def __init__(self) -> None:
        self._records: list[dict[str, Any]] = []

    def stage(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> None:
        """Accumulate records for later upload."""
        self._records.extend(records)

    def finalize(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Upload staged file, trigger job, poll for completion."""
        assert isinstance(config, StagedUploadDestinationConfig)
        result = SyncResult()
        context: dict[str, str] = {}
        record_count = len(self._records)

        # Empty-source short-circuit — no auth, no upload, no trigger, no
        # poll. Mirrors the same guard at the top of
        # SalesforceBulkDestination.finalize(). The engine calls
        # finalize() regardless of whether stage() ever received records,
        # so without this guard a transient empty source produces a
        # zero-row upload + job that wastes the trigger / poll cycle.
        if record_count == 0:
            return result

        try:
            # Phase 1: Stage — serialize and upload file
            file_bytes = self._serialize(config.format)
            stage_resp = self._http_phase(config.stage, context, file_bytes=file_bytes)
            self._extract_values(stage_resp, config.stage.response_extract, context)

            # Phase 2: Trigger — kick off server-side job
            trigger_resp = self._http_phase(config.trigger, context)
            self._extract_values(trigger_resp, config.trigger.response_extract, context)

            # Phase 3: Poll — wait for completion (optional)
            if config.poll is not None:
                retry_config = resolve_retry(config.retry, sync_options)
                poll_url = (
                    _render(config.poll.url, context)
                    if "{{" in config.poll.url
                    else config.poll.url
                )
                # Render before keying (#1068): poll.url can be templated
                # entirely from the trigger response (e.g. a vendor-returned
                # status URL), which the unrendered string — all
                # config.rate_limit_key() ever sees — has no parseable host
                # for. Rendering here, once, gives the registry the real
                # endpoint identity instead of an approximation.
                rate_limiter = resolve_rate_limiter(
                    config,
                    sync_options,
                    limiter_factory=RateLimiter,
                    key_override=f"{config.type}:{urlparse(poll_url).netloc}",
                )
                self._poll(config.poll, context, retry_config, rate_limiter, url=poll_url)

            result.success = record_count
        except Exception as e:
            result.failed = record_count
            result.errors.append(str(e))
        finally:
            self._records.clear()

        return result

    def _serialize(self, fmt: str) -> bytes:
        """Serialize accumulated records to bytes."""
        if not self._records:
            return b""

        if fmt == "csv":
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=self._records[0].keys())
            writer.writeheader()
            writer.writerows(self._records)
            return buf.getvalue().encode("utf-8")

        if fmt == "jsonl":
            lines = [json.dumps(r, ensure_ascii=False) for r in self._records]
            return "\n".join(lines).encode("utf-8")

        # json
        return json.dumps(self._records, ensure_ascii=False).encode("utf-8")

    def _http_phase(
        self,
        phase: StagedUploadPhaseConfig,
        context: dict[str, str],
        file_bytes: bytes | None = None,
    ) -> dict[str, Any]:
        """Execute one HTTP phase (stage or trigger)."""
        url = _render(phase.url, context) if "{{" in phase.url else phase.url
        headers = dict(phase.headers or {})
        if phase.auth:
            headers.update(AuthHandler(phase.auth).get_headers())

        with httpx.Client(timeout=120.0) as client:
            if file_bytes is not None:
                # Stage phase: upload file
                response = client.request(
                    phase.method,
                    url,
                    content=file_bytes,
                    headers=headers,
                )
            elif phase.body_template:
                body = _render(phase.body_template, context)
                response = client.request(
                    phase.method,
                    url,
                    content=body.encode("utf-8"),
                    headers={**headers, "Content-Type": "application/json"},
                )
            else:
                response = client.request(phase.method, url, headers=headers)

            response.raise_for_status()

        try:
            return response.json()  # type: ignore[no-any-return]
        except (json.JSONDecodeError, ValueError):
            return {}

    @staticmethod
    def _extract_values(
        response: dict[str, Any],
        extract: dict[str, str] | None,
        context: dict[str, str],
    ) -> None:
        """Extract values from HTTP response into context dict."""
        if not extract:
            return
        for var_name, json_key in extract.items():
            val = response.get(json_key)
            if val is not None:
                context[var_name] = str(val)

    def _poll(
        self,
        poll_config: StagedUploadPollConfig,
        context: dict[str, str],
        retry_config: RetryConfig,
        rate_limiter: RateLimiterBackend,
        url: str,
    ) -> None:
        """Poll for job completion.

        ``url`` is already rendered by the caller (#1068) — the rate limiter
        it acquired from is keyed off this same rendered URL's host, so
        re-rendering here (Jinja templates are pure, but there's no reason to
        pay for it twice) would only risk the two silently drifting apart.
        """
        headers: dict[str, str] = dict(poll_config.headers or {})
        if poll_config.auth:
            headers.update(AuthHandler(poll_config.auth).get_headers())

        deadline = time.monotonic() + poll_config.timeout_seconds
        status = ""
        retry_poll = poll_config.method.upper() in {"GET", "HEAD"}

        with httpx.Client(timeout=60.0) as client:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise TimeoutError(
                        f"Poll timed out after {poll_config.timeout_seconds}s"
                        f" (last status: {status})"
                    )

                def _send() -> httpx.Response:
                    rate_limiter.acquire()
                    request_remaining = deadline - time.monotonic()
                    response = client.request(
                        poll_config.method,
                        url,
                        headers=headers,
                        timeout=max(0.1, min(60.0, request_remaining)),
                    )
                    response.raise_for_status()
                    return response

                if retry_poll:
                    poll_retry_config = retry_config.model_copy(
                        update={"max_backoff": min(retry_config.max_backoff, remaining)}
                    )
                    response = with_retry(_send, poll_retry_config)
                else:
                    response = _send()
                data = response.json()

                status = str(data.get(poll_config.status_field, ""))

                if time.monotonic() > deadline:
                    raise TimeoutError(
                        f"Poll timed out after {poll_config.timeout_seconds}s"
                        f" (last status: {status})"
                    )
                if status in poll_config.success_values:
                    return
                if status in poll_config.failure_values:
                    raise RuntimeError(f"Job failed with status: {status}")

                time.sleep(poll_config.interval_seconds)
