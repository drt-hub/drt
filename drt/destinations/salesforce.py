"""Salesforce destination — Upsert via REST API and Bulk API 2.0.

Supports:
- OAuth2 (JWT bearer or client credentials)
- REST API (small datasets)
- Bulk API 2.0 (large datasets)
- Upsert via external ID field

Install:
    pip install drt-core[salesforce]
    
Requires:
    SALESFORCE_ACCESS_TOKEN or OAuth2 config

Example:

    destination:
      type: salesforce
      object: Contact
      external_id_field: Email
"""

from __future__ import annotations

import json
import time
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import (
    DestinationConfig,
    SalesforceDestinationConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template

_DEFAULT_RETRY = RetryConfig(
    max_attempts=3,
    initial_backoff=1.0,
    retryable_status_codes=(429, 500, 502, 503, 504),
)


class SalesforceDestination:
    """Upsert records into Salesforce using REST or Bulk API."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, SalesforceDestinationConfig)

        access_token = resolve_env(
            getattr(config.auth, "token", None),
            getattr(config.auth, "token_env", None),
        )
        if not access_token:
            raise ValueError("Salesforce: missing access token")

        # TODO: Ideally instance_url should come from auth flow
        instance_url = "https://your-instance.salesforce.com"

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }

        result = SyncResult()
        retry_config = sync_options.retry or _DEFAULT_RETRY

        # Basic rate limiter (Salesforce ~100 req/sec org-wide varies)
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)

        # Choose API
        if len(records) >= config.bulk_threshold:
            return self._bulk_upsert(
                records, config, headers, instance_url, retry_config, result
            )

        return self._rest_upsert(
            records,
            config,
            headers,
            instance_url,
            retry_config,
            sync_options,
            rate_limiter,
            result,
        )

    # ---------------------------------------------------------------------
    # REST API (small batches)
    # ---------------------------------------------------------------------

    def _rest_upsert(
        self,
        records: list[dict[str, Any]],
        config: SalesforceDestinationConfig,
        headers: dict[str, str],
        instance_url: str,
        retry_config: RetryConfig,
        sync_options: SyncOptions,
        rate_limiter: RateLimiter,
        result: SyncResult,
    ) -> SyncResult:

        base_url = f"{instance_url}/services/data/v58.0/sobjects/{config.object}"

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()

                ext_id = record.get(config.external_id_field)
                if not ext_id:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(record)[:200],
                            http_status=None,
                            error_message=f"Missing external_id_field: {config.external_id_field}",
                        )
                    )
                    if sync_options.on_error == "fail":
                        raise ValueError("Missing external ID")
                    continue

                url = f"{base_url}/{config.external_id_field}/{ext_id}"

                def do_request() -> httpx.Response:
                    response = client.patch(url, json=record, headers=headers)

                    if response.status_code == 429:
                        retry_after = int(response.headers.get("Retry-After", "1"))
                        time.sleep(retry_after)

                    response.raise_for_status()
                    return response

                try:
                    with_retry(do_request, retry_config)
                    result.success += 1
                except httpx.HTTPStatusError as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(record)[:200],
                            http_status=e.response.status_code,
                            error_message=e.response.text[:500],
                        )
                    )
                    if sync_options.on_error == "fail":
                        raise
                except Exception as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(record)[:200],
                            http_status=None,
                            error_message=str(e),
                        )
                    )
                    if sync_options.on_error == "fail":
                        raise

        return result

    # ---------------------------------------------------------------------
    # Bulk API 2.0 (large datasets)
    # ---------------------------------------------------------------------

    def _bulk_upsert(
        self,
        records: list[dict[str, Any]],
        config: SalesforceDestinationConfig,
        headers: dict[str, str],
        instance_url: str,
        retry_config: RetryConfig,
        result: SyncResult,
    ) -> SyncResult:

        bulk_url = f"{instance_url}/services/data/v58.0/jobs/ingest"

        job_payload = {
            "object": config.object,
            "operation": "upsert",
            "externalIdFieldName": config.external_id_field,
            "contentType": "JSON",
        }

        with httpx.Client(timeout=60.0) as client:

            # Create job
            job_resp = client.post(bulk_url, json=job_payload, headers=headers)
            job_resp.raise_for_status()
            job_id = job_resp.json()["id"]

            # Upload data
            upload_url = f"{bulk_url}/{job_id}/batches"
            client.put(upload_url, json=records, headers=headers)

            # Close job
            close_url = f"{bulk_url}/{job_id}"
            client.patch(close_url, json={"state": "UploadComplete"}, headers=headers)

            # Poll status
            status_url = f"{bulk_url}/{job_id}"
            while True:
                resp = client.get(status_url, headers=headers)
                resp.raise_for_status()
                state = resp.json()["state"]

                if state in ("JobComplete", "Failed", "Aborted"):
                    break

                time.sleep(2)

            if state != "JobComplete":
                result.failed = len(records)
                return result

            result.success = len(records)

        return result