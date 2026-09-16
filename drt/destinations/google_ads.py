"""Google Ads destination — upload offline click conversions.

Sends conversion data to Google Ads via the Conversions Upload API.
Each record should contain a gclid, conversion timestamp, and optionally
a conversion value.

Requires OAuth2 authentication (service account or client credentials).
A developer token (``developer_token_env``) is sent when configured but no
longer required (#1154) -- Google's Cloud-project-based access model made
the header optional and server-ignored.

``destination.native_idempotency_key`` (#897) is rendered per record and
sent as ClickConversion's documented ``orderId`` dedup field, making
drt's own per-destination retry (#277) safe against Google having already
processed a request that looked like it failed client-side (timeout,
dropped connection): a retry that reuses the same ``orderId`` gets back
``ORDER_ID_ALREADY_IN_USE`` rather than reprocessing it, which this
destination counts as a successful delivery, not a failure.

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
from drt.templates.renderer import render_template

logger = logging.getLogger(__name__)

# v17 sunset 2025-06-04 (drt-hub/drt#1154) -- every request against it fails
# outright. v25 (released 2026-07-22) is the current version with the
# longest remaining runway (sunsets ~August 2027); v22-v24 all sunset within
# a year of writing this. ClickConversion's five fields this destination
# sets (gclid, conversionAction, conversionDateTime, conversionValue,
# currencyCode), the partial-failure error shape this destination parses
# (GoogleAdsFailure/GoogleAdsError/FieldPathElement), and order_id are all
# confirmed byte-identical between v22 and v25's proto sources -- this is a
# version-string bump, not a payload/response shape migration.
_API_VERSION = "v25"
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
    # Exactly one -- per the documented contract above, not "at least one".
    # Silently reading details[0] and ignoring any further entries would
    # drop real errors from an undocumented multi-entry response, wrongly
    # crediting those conversions as delivered (Codex review of PR #1153).
    if not isinstance(details, list) or len(details) != 1:
        return None
    failure = details[0]
    if not isinstance(failure, dict):
        return None
    errors = failure.get("errors")
    # An empty errors list is also treated as unparseable rather than "zero
    # failures" -- a non-null partialFailureError with nothing inside it is
    # itself a shape drt-core has never observed, not a legitimate all-clear.
    if not isinstance(errors, list) or not errors:
        return None

    mapped: list[tuple[int, dict[str, Any]]] = []
    for error in errors:
        if not isinstance(error, dict):
            return None
        location = error.get("location")
        if not isinstance(location, dict):
            return None
        field_path = location.get("fieldPathElements")
        if not isinstance(field_path, list):
            return None
        index: Any = None
        for el in field_path:
            if not isinstance(el, dict):
                return None
            if el.get("fieldName") == "conversions":
                index = el.get("index")
                break
        # bool is an int subclass in Python -- an unvalidated `"index": true`
        # would otherwise be accepted as index 1 and misattribute the error
        # to an arbitrary record instead of hitting the whole-batch fallback.
        if isinstance(index, bool) or not isinstance(index, int):
            return None
        if not (0 <= index < conversion_count):
            return None
        mapped.append((index, error))
    return mapped


def _is_order_id_already_in_use(error: dict[str, Any]) -> bool:
    """A `GoogleAdsError` reporting `ORDER_ID_ALREADY_IN_USE`: "the imported
    event includes an order ID that was previously recorded, so the event
    was not processed" (`ConversionUploadErrorEnum`). Retrying a conversion
    upload with the same ``native_idempotency_key``-derived ``orderId``
    (#897) after a prior run already delivered it looks like this, not like
    a fresh success -- so it is dedup, not failure.

    Deliberately narrower than Google's other dedup signal,
    ``CLICK_CONVERSION_ALREADY_EXISTS`` ("same click and
    ``conversion_date_time`` as an existing conversion"), which stays a
    failure here: that one fires with no explicit opt-in and can describe a
    genuinely distinct conversion that happens to share a click and
    timestamp, so treating it as success would risk silently swallowing a
    real, non-duplicate error. ``ORDER_ID_ALREADY_IN_USE`` only fires when
    the caller explicitly set ``native_idempotency_key``, so a match here
    can only mean drt's own redelivery of a row it already sent.
    """
    error_code = error.get("errorCode")
    return (
        isinstance(error_code, dict)
        and error_code.get("conversionUploadError") == "ORDER_ID_ALREADY_IN_USE"
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
        if not records:
            return SyncResult()

        result = SyncResult()
        retry_config = resolve_retry(config.retry, sync_options)

        developer_token = resolve_env(None, config.developer_token_env)
        # (#1154) Google's September 2026 access-model change made this
        # header optional and server-ignored for every caller, not just new
        # Cloud-project-based adopters -- requiring it client-side is now a
        # purely drt-imposed constraint that would otherwise block anyone
        # onboarding under the new model with no token to give. Sent when
        # configured (harmless for existing setups), omitted rather than
        # raising when absent.
        #
        # rate_limit_key() stays keyed on developer_token_env alone with no
        # further splitting -- see its docstring in destinations_saas.py for
        # why a third review round's attempt to split tokenless configs by
        # OAuth-client identity was also reverted (a per-client key can
        # under-share a quota Google now scopes to the Cloud project, which
        # can own several OAuth clients).
        rate_limiter = resolve_rate_limiter(config, sync_options, limiter_factory=RateLimiter)

        auth_headers = AuthHandler(config.auth).get_headers()
        headers = {
            **auth_headers,
            "Content-Type": "application/json",
        }
        if developer_token:
            headers["developer-token"] = developer_token

        # Build conversions payload. conversion_record_indices[j] is the
        # original `records` index that conversions[j] came from -- a record
        # skipped below (missing gclid/conversion_time) never gets a
        # conversions[] entry, so the two lists can diverge and the mapping
        # can't be assumed to be the identity.
        sync_name = sync_options._sync_name or ""
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

            if config.native_idempotency_key:
                # Content-derived, like rest_api.py's record-mode key: a
                # retry of the same row across separate runs should
                # deduplicate against a previously delivered upload of that
                # exact row (#897) -- see _is_order_id_already_in_use below
                # for how Google's response signals that happened.
                try:
                    conversion["orderId"] = render_template(
                        config.native_idempotency_key, record, sync_name=sync_name
                    )
                except Exception as e:  # noqa: BLE001 — a template can raise
                    # more than render_template()'s own normalized
                    # ValueError (rest_api.py established this precedent);
                    # on_error: skip must stay effective for any of them.
                    record_row_error(
                        result,
                        i,
                        json.dumps(record, default=str)[:200],
                        e,
                        error_message=f"Template error: {e}",
                    )
                    if sync_options.on_error == "fail":
                        break
                    continue

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
            # Field *presence*, not truthiness and not None-ness: a present
            # but empty/malformed partialFailureError (e.g. `{}`, or an
            # explicit `null` from a malformed/intermediary response) is
            # still a signal something is wrong, not a legitimate all-clear
            # -- either a truthiness or a None check would take the success
            # branch below and credit every conversion as delivered (Codex
            # review of PR #1153, rounds 3 and 4).
            if "partialFailureError" in resp_data:
                mapped = (
                    _parse_conversion_upload_errors(partial_errors, len(conversions))
                    if isinstance(partial_errors, dict)
                    else None
                )
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
                    message = "partial failure"
                    if isinstance(partial_errors, dict):
                        message = str(partial_errors.get("message", "")) or message
                    for record_index in conversion_record_indices:
                        record_row_error(
                            result,
                            record_index,
                            json.dumps(records[record_index], default=str)[:200],
                            ValueError(),
                            error_message=message,
                        )
                else:
                    # Group by conversion index first: Google can return more
                    # than one GoogleAdsError for the same conversion, and
                    # recording one RowError per *error* rather than per
                    # conversion would inflate `failed` past the batch size
                    # and enqueue the same record into the DLQ more than once.
                    errors_by_index: dict[int, list[dict[str, Any]]] = {}
                    for idx, error in mapped:
                        errors_by_index.setdefault(idx, []).append(error)
                    dedup_indices = 0
                    for idx, idx_errors in errors_by_index.items():
                        if all(_is_order_id_already_in_use(e) for e in idx_errors):
                            # A previous run (or an earlier retry of this
                            # one) already delivered this exact orderId
                            # (#897 native_idempotency_key) -- Google
                            # recognized the dedup and skipped reprocessing
                            # it, so this is a successful delivery, not a
                            # failure.
                            dedup_indices += 1
                            continue
                        record_index = conversion_record_indices[idx]
                        message = "; ".join(str(e.get("message", "")) for e in idx_errors)
                        record_row_error(
                            result,
                            record_index,
                            json.dumps(records[record_index], default=str)[:200],
                            ValueError(),
                            error_message=message,
                        )
                    if dedup_indices:
                        logger.info(
                            "Google Ads reported %d conversion(s) as already delivered "
                            "(ORDER_ID_ALREADY_IN_USE) -- counted as success, not failure",
                            dedup_indices,
                        )
                    result.success += len(conversions) - (len(errors_by_index) - dedup_indices)
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

    def supports_native_idempotency_key(self, config: DestinationConfig) -> bool:
        """NativeIdempotencyCapable (#897): wired via ClickConversion's
        documented ``orderId`` dedup field -- see ``load()``'s per-record
        rendering and ``_is_order_id_already_in_use`` above."""
        return True
