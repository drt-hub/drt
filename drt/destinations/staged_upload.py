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

import httpx
from jinja2 import BaseLoader, Environment, StrictUndefined
from jinja2.exceptions import UndefinedError

from drt.config.models import (
    DestinationConfig,
    StagedUploadDestinationConfig,
    StagedUploadPhaseConfig,
    StagedUploadPollConfig,
    SyncOptions,
)
from drt.destinations.auth import AuthHandler
from drt.destinations.base import SyncResult


def _render(template_str: str, context: dict[str, str]) -> str:
    """Render a Jinja2 template with context variables (not row-scoped)."""
    env = Environment(loader=BaseLoader(), undefined=StrictUndefined)
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

        try:
            # Phase 1: Stage — serialize and upload file
            file_bytes = self._serialize(config.format)
            stage_resp = self._http_phase(
                config.stage, context, file_bytes=file_bytes
            )
            self._extract_values(
                stage_resp, config.stage.response_extract, context
            )

            # Phase 2: Trigger — kick off server-side job
            trigger_resp = self._http_phase(config.trigger, context)
            self._extract_values(
                trigger_resp, config.trigger.response_extract, context
            )

            # Phase 3: Poll — wait for completion (optional)
            if config.poll is not None:
                self._poll(config.poll, context)

        except Exception as e:
            result.failed = len(self._records)
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
    ) -> None:
        """Poll for job completion."""
        url = (
            _render(poll_config.url, context)
            if "{{" in poll_config.url
            else poll_config.url
        )
        headers: dict[str, str] = dict(poll_config.headers or {})
        if poll_config.auth:
            headers.update(AuthHandler(poll_config.auth).get_headers())

        deadline = time.monotonic() + poll_config.timeout_seconds

        with httpx.Client(timeout=60.0) as client:
            while True:
                response = client.request(
                    poll_config.method, url, headers=headers
                )
                response.raise_for_status()
                data = response.json()

                status = str(data.get(poll_config.status_field, ""))

                if status in poll_config.success_values:
                    return
                if status in poll_config.failure_values:
                    raise RuntimeError(
                        f"Job failed with status: {status}"
                    )
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        f"Poll timed out after {poll_config.timeout_seconds}s"
                        f" (last status: {status})"
                    )

                time.sleep(poll_config.interval_seconds)
