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
import os
from typing import Any

import httpx

from drt.config.models import (
    DestinationConfig,
    GoogleAdsDestinationConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.auth import AuthHandler
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import RowError

_API_VERSION = "v17"
_BASE_URL = "https://googleads.googleapis.com"
_DEFAULT_RETRY = RetryConfig(
    max_attempts=3,
    initial_backoff=1.0,
    retryable_status_codes=(429, 500, 502, 503, 504),
)


class GoogleAdsDestination:
    """Upload offline click conversions to Google Ads."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, GoogleAdsDestinationConfig)
        result = SyncResult()
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)
        retry_config = sync_options.retry or _DEFAULT_RETRY

        developer_token = os.environ.get(config.developer_token_env, "")
        if not developer_token:
            raise ValueError(f"Google Ads: env var '{config.developer_token_env}' is not set.")

        auth_headers = AuthHandler(config.auth).get_headers()
        headers = {
            **auth_headers,
            "developer-token": developer_token,
            "Content-Type": "application/json",
        }

        # Build conversions payload
        conversions = []
        for i, record in enumerate(records):
            gclid = record.get(config.gclid_field)
            conv_time = record.get(config.conversion_time_field)
            if not gclid or not conv_time:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=i,
                        record_preview=json.dumps(record, default=str)[:200],
                        http_status=None,
                        error_message=(
                            f"Missing required field: "
                            f"{config.gclid_field} or "
                            f"{config.conversion_time_field}"
                        ),
                    )
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
            # Count partial failures
            partial_errors = resp_data.get("partialFailureError")
            if partial_errors:
                error_details = partial_errors.get("details", [])
                result.failed += len(error_details)
                result.success += len(conversions) - len(error_details)
                for detail in error_details[:10]:
                    result.errors.append(str(detail.get("message", "")))
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
