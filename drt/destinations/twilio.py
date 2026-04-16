"""Twilio destination — Send SMS via Twilio REST API.

Sends one SMS per row using Twilio's Messages API.

Auth:
- Basic Auth using Account SID + Auth Token

Docs:
https://www.twilio.com/docs/messaging/api/message-resource

Example sync YAML:

    destination:
      type: twilio
      account_sid_env: TWILIO_ACCOUNT_SID
      auth_token_env: TWILIO_AUTH_TOKEN
      from_number: "+1234567890"
      to_template: "{{ row.phone }}"
      message_template: "Hi {{ row.name }}, your order #{{ row.order_id }} has shipped!"
"""

from __future__ import annotations

import json
import os
import re
from typing import Any

import httpx

from drt.config.models import (
    DestinationConfig,
    RetryConfig,
    SyncOptions,
    TwilioDestinationConfig,
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

# E.164 format: + followed by 1–15 digits
_E164_REGEX = re.compile(r"^\+[1-9]\d{1,14}$")


def _is_valid_e164(phone: str) -> bool:
    return bool(_E164_REGEX.match(phone))


class TwilioDestination:
    """Send records as SMS messages via Twilio."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, TwilioDestinationConfig)

        account_sid = config.account_sid or (
            os.environ.get(config.account_sid_env) if config.account_sid_env else None
        )
        auth_token = config.auth_token or (
            os.environ.get(config.auth_token_env) if config.auth_token_env else None
        )

        if not account_sid or not auth_token:
            raise ValueError(
                "Twilio destination: provide 'account_sid'/'auth_token' or set env vars."
            )

        # Validate from_number early
        if not _is_valid_e164(config.from_number):
            raise ValueError(
                f"Invalid from_number '{config.from_number}'. Must be E.164 format (e.g. +1234567890)."
            )

        url = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"

        result = SyncResult()
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)
        retry_config = sync_options.retry or _DEFAULT_RETRY

        with httpx.Client(timeout=30.0, auth=(account_sid, auth_token)) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()

                try:
                    to_number = render_template(config.to_template, record)

                    # Validate destination number per row
                    if not _is_valid_e164(to_number):
                        raise ValueError(
                            f"Invalid to_number '{to_number}' (row {i}). Must be E.164 format."
                        )

                    body = render_template(config.message_template, record)

                    payload = {
                        "From": config.from_number,
                        "To": to_number,
                        "Body": body,
                    }

                    def do_post() -> httpx.Response:
                        response = client.post(url, data=payload)
                        response.raise_for_status()
                        return response

                    response = with_retry(do_post, retry_config)

                    # Twilio returns JSON — check for API-level errors
                    try:
                        data = response.json()
                    except Exception:
                        raise ValueError(f"Invalid Twilio response: {response.text[:300]}")

                    if data.get("error_code") or data.get("error_message"):
                        raise ValueError(
                            f"Twilio error {data.get('error_code')}: {data.get('error_message')}"
                        )

                    result.success += 1

                except httpx.HTTPStatusError as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=str(record)[:200],
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
                            record_preview=str(record)[:200],
                            http_status=None,
                            error_message=str(e),
                        )
                    )
                    if sync_options.on_error == "fail":
                        raise

        return result