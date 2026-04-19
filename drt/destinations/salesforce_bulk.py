"""Salesforce Bulk API 2.0 destination.

Implements the StagedDestination protocol: records are accumulated via
stage(), then uploaded as a single CSV job in finalize().

Example sync YAML:

    destination:
      type: salesforce_bulk
      instance_url_env: SF_INSTANCE_URL
      object_name: Contact
      operation: upsert
      external_id_field: External_Id__c
      client_id_env: SF_CLIENT_ID
      client_secret_env: SF_CLIENT_SECRET
      username_env: SF_USERNAME
      password_env: SF_PASSWORD
"""

from __future__ import annotations

import csv
import io
import os
import time
from typing import Any

import httpx

from drt.config.models import DestinationConfig, SalesforceBulkDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError


class SalesforceBulkDestination:
    """Upload records to Salesforce via Bulk API 2.0."""

    def __init__(self) -> None:
        self._records: list[dict[str, Any]] = []

    def stage(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> None:
        """Accumulate records for later bulk upload — no HTTP calls here."""
        self._records.extend(records)

    def finalize(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Run the full Bulk API 2.0 lifecycle: auth → create job → upload
        CSV → close job → poll → fetch errors → return SyncResult."""

        # STEP 0 — assert correct config type
        assert isinstance(config, SalesforceBulkDestinationConfig)

        # STEP 3 (early exit) — nothing to do if no records were staged
        if not self._records:
            return SyncResult(rows_extracted=0)

        # STEP 1 — resolve instance URL
        if config.instance_url_env:
            instance_url = os.environ[config.instance_url_env].rstrip("/")
        else:
            instance_url = (config.instance_url or "").rstrip("/")

        with httpx.Client(timeout=30.0) as client:
            # STEP 2 — OAuth2 username-password flow
            auth_resp = client.post(
                f"{instance_url}/services/oauth2/token",
                data={
                    "grant_type": "password",
                    "client_id": os.environ[config.client_id_env],
                    "client_secret": os.environ[config.client_secret_env],
                    "username": os.environ[config.username_env],
                    "password": os.environ[config.password_env],
                },
            )
            if auth_resp.status_code != 200:
                raise RuntimeError(
                    f"Salesforce auth failed ({auth_resp.status_code}): {auth_resp.text}"
                )
            access_token = auth_resp.json()["access_token"]
            auth_headers = {"Authorization": f"Bearer {access_token}"}

            # STEP 3 — serialize records to CSV
            fieldnames = list(self._records[0].keys())
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
            writer.writeheader()
            writer.writerows(self._records)
            csv_content = buf.getvalue()

            # STEP 4 — create ingest job
            job_resp = client.post(
                f"{instance_url}/services/data/v58.0/jobs/ingest",
                headers={**auth_headers, "Content-Type": "application/json"},
                json={
                    "object": config.object_name,
                    "operation": config.operation,
                    "externalIdFieldName": config.external_id_field,
                    "contentType": "CSV",
                    "lineEnding": "LF",
                },
            )
            if job_resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to create Salesforce job ({job_resp.status_code}): {job_resp.text}"
                )
            job_id = job_resp.json()["id"]

            # STEP 5 — upload CSV
            upload_resp = client.put(
                f"{instance_url}/services/data/v58.0/jobs/ingest/{job_id}/batches",
                headers={**auth_headers, "Content-Type": "text/csv"},
                content=csv_content.encode("utf-8"),
            )
            if upload_resp.status_code != 201:
                raise RuntimeError(
                    f"Failed to upload CSV ({upload_resp.status_code}): {upload_resp.text}"
                )

            # STEP 6 — close job (signal UploadComplete)
            close_resp = client.patch(
                f"{instance_url}/services/data/v58.0/jobs/ingest/{job_id}",
                headers={**auth_headers, "Content-Type": "application/json"},
                json={"state": "UploadComplete"},
            )
            if close_resp.status_code != 200:
                raise RuntimeError(
                    f"Failed to close job ({close_resp.status_code}): {close_resp.text}"
                )

            # STEP 7 — poll for completion
            deadline = time.monotonic() + config.poll_timeout_seconds
            final_state: dict[str, Any] = {}
            while True:
                if time.monotonic() > deadline:
                    raise TimeoutError(
                        f"Salesforce job {job_id} did not complete within "
                        f"{config.poll_timeout_seconds}s"
                    )
                poll_resp = client.get(
                    f"{instance_url}/services/data/v58.0/jobs/ingest/{job_id}",
                    headers=auth_headers,
                )
                final_state = poll_resp.json()
                state = final_state.get("state", "")
                if state == "JobComplete":
                    break
                if state in ("Failed", "Aborted"):
                    raise RuntimeError(f"Salesforce job {job_id} ended with state: {state}")
                time.sleep(config.poll_interval_seconds)

            records_processed = int(final_state.get("numberRecordsProcessed", 0))
            records_failed = int(final_state.get("numberRecordsFailed", 0))

            # STEP 8 — fetch per-record errors if any
            row_errors: list[RowError] = []
            if records_failed > 0:
                failed_resp = client.get(
                    f"{instance_url}/services/data/v58.0/jobs/ingest/{job_id}/failedResults",
                    headers=auth_headers,
                )
                reader = csv.DictReader(io.StringIO(failed_resp.text))
                for row in reader:
                    row_errors.append(
                        RowError(
                            batch_index=0,
                            record_preview=str(row)[:200],
                            http_status=None,
                            error_message=row.get("sf__Error", "unknown error"),
                        )
                    )

        # STEP 9 — return SyncResult
        return SyncResult(
            rows_extracted=len(self._records),
            success=records_processed - records_failed,
            failed=records_failed,
            row_errors=row_errors,
        )
