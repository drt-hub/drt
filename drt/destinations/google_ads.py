"""Google Ads destination — upload offline click conversions.

Sends conversion data to Google Ads via the Conversions Upload API.
Each record should contain a gclid, conversion timestamp, and optionally
a conversion value.

Requires OAuth2 authentication (service account or client credentials)
and a developer token.

Example sync YAML:

    destination:
      type: google_ads
      customer_id: "1234567890"
      conversion_action: "customers/1234567890/conversionActions/987"
      gclid_field: gclid
      conversion_time_field: conversion_time
      conversion_value_field: revenue
      currency_code: USD
      developer_token_env: GOOGLE_ADS_DEVELOPER_TOKEN
      auth:
        type: oauth2_client_credentials
        token_url: "https://oauth2.googleapis.com/token"
        client_id_env: GOOGLE_ADS_CLIENT_ID
        client_secret_env: GOOGLE_ADS_CLIENT_SECRET
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import (
    DestinationConfig,
    GoogleAdsDestinationConfig,
    SyncOptions,
)
from drt.destinations.auth import AuthHandler
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter, resolve_rate_limiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import record_row_error

logger = logging.getLogger(__name__)

_API_VERSION = "v17"
_BASE_URL = "https://googleads.googleapis.com"


def _parse_conversion_upload_errors(
    partial_failure_error: dict[str, Any], conversion_count: int
) -> list[tuple[int, dict[str, Any]]] | None:
    """Map a ``partialFailureError`` to ``(conversions-index, GoogleAdsError)``
    pairs, or ``None`` if the response didn't match the documented shape.

    ``partialFailureError.details`` always contains exactly one ``Any``-packed
    ``GoogleAdsFailure`` object -- regardless of how many conversions failed
    -- not one entry per failure (see the Partial Failure guide:
    https://developers.google.com/google-ads/api/docs/best-practices/partial-failures).
    That object's own ``errors`` array has one ``GoogleAdsError`` per actual
    failure, each carrying ``location.fieldPathElements[].index`` to identify
    which conversion (by position in the request's ``conversions[]``) it
    belongs to. Returning ``None`` on any shape mismatch lets the caller
    degrade to a safe, conservative fallback rather than guessing at counts.
    """
    details = partial_failure_error.get("details")
    if not isinstance(details, list) or not details:
        return None
    errors = details[0].get("errors")
    if not isinstance(errors, list):
        return None

    mapped: list[tuple[int, dict[str, Any]]] = []
    for error in errors:
        if not isinstance(error, dict):
            return None
        field_path = (error.get("location") or {}).get("fieldPathElements") or []
        index = next(
            (el.get("index") for el in field_path if el.get("fieldName") == "conversions"),
            None,
        )
        if not isinstance(index, int) or not (0 <= index < conversion_count):
            return None
        mapped.append((index, error))
    return mapped


class GoogleAdsDestination:
    """Upload offline click conversions to Google Ads."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, GoogleAdsDestinationConfig)
        if not records:
            return SyncResult()

        result = SyncResult()
        rate_limiter = resolve_rate_limiter(config, sync_options, limiter_factory=RateLimiter)
        retry_config = resolve_retry(config.retry, sync_options)

        developer_token = resolve_env(None, config.developer_token_env) or ""
        if not developer_token:
            raise ValueError(f"Google Ads: env var '{config.developer_token_env}' is not set.")

        auth_headers = AuthHandler(config.auth).get_headers()
        headers = {
            **auth_headers,
            "developer-token": developer_token,
            "Content-Type": "application/json",
        }

        # Build conversions payload. conversion_record_indices[j] is the
        # original `records` index that conversions[j] came from -- a record
        # skipped below (missing gclid/conversion_time) never gets a
        # conversions[] entry, so the two lists can diverge and the mapping
        # can't be assumed to be the identity.
        conversions: list[dict[str, Any]] = []
        conversion_record_indices: list[int] = []
        for i, record in enumerate(records):
            gclid = record.get(config.gclid_field)
            conv_time = record.get(config.conversion_time_field)
            if not gclid or not conv_time:
                record_row_error(
                    result,
                    i,
                    json.dumps(record, default=str)[:200],
                    ValueError(),
                    error_message=(
                        f"Missing required field: "
                        f"{config.gclid_field} or "
                        f"{config.conversion_time_field}"
                    ),
                )
                if sync_options.on_error == "fail":
                    break
                continue

            conversion: dict[str, Any] = {
                "gclid": str(gclid),
                "conversionAction": config.conversion_action,
                "conversionDateTime": str(conv_time),
            }
            if config.conversion_value_field:
                val = record.get(config.conversion_value_field)
                if val is not None:
                    conversion["conversionValue"] = float(val)
                    conversion["currencyCode"] = config.currency_code

            conversions.append(conversion)
            conversion_record_indices.append(i)

        if not conversions:
            return result

        url = f"{_BASE_URL}/{_API_VERSION}/customers/{config.customer_id}:uploadClickConversions"
        payload = {
            "conversions": conversions,
            "partialFailure": True,
        }

        rate_limiter.acquire()
        try:
            with httpx.Client(timeout=60.0) as client:

                def do_upload() -> httpx.Response:
                    resp = client.post(url, json=payload, headers=headers)
                    resp.raise_for_status()
                    return resp

                response = with_retry(do_upload, retry_config)

            resp_data = response.json()
            partial_errors = resp_data.get("partialFailureError")
            if partial_errors:
                mapped = _parse_conversion_upload_errors(partial_errors, len(conversions))
                if mapped is None:
                    # Response didn't match the documented shape -- degrade
                    # to a conservative "can't tell which ones failed" rather
                    # than guessing, same shape as rest_api.py's
                    # _handle_batch_http_error's error_path fallback.
                    logger.warning(
                        "Google Ads partialFailureError did not match the "
                        "documented response shape; marking the whole "
                        "upload as failed"
                    )
                    message = str(partial_errors.get("message", "")) or "partial failure"
                    for record_index in conversion_record_indices:
                        record_row_error(
                            result,
                            record_index,
                            json.dumps(records[record_index], default=str)[:200],
                            ValueError(),
                            error_message=message,
                        )
                else:
                    failed_conversion_indices = {idx for idx, _ in mapped}
                    for idx, error in mapped:
                        record_index = conversion_record_indices[idx]
                        record_row_error(
                            result,
                            record_index,
                            json.dumps(records[record_index], default=str)[:200],
                            ValueError(),
                            error_message=str(error.get("message", "")),
                        )
                    result.success += len(conversions) - len(failed_conversion_indices)
            else:
                result.success += len(conversions)

        except httpx.HTTPStatusError as e:
            result.failed += len(conversions)
            result.errors.append(
                f"Google Ads API error: {e.response.status_code} {e.response.text[:500]}"
            )
        except Exception as e:
            result.failed += len(conversions)
            result.errors.append(f"Google Ads error: {e}")

        return result
