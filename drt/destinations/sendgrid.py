"""SendGrid destination — Transactional Email Integration.

Sends one email per record using SendGrid's v3 Mail Send API.
Supports subject + body templating via Jinja2.

No extra dependencies required (uses httpx from core).

Example sync YAML:

    destination:
      type: sendgrid
      from_email: "noreply@example.com"
      from_name: "My App"
      subject_template: "Welcome, {{ row.first_name }}!"
      body_template: |
        Hi {{ row.first_name }},

        Thanks for signing up. Your account is ready.
      list_ids: ["abc123"]
      auth:
        type: bearer
        token_env: SENDGRID_API_KEY

SendGrid API reference:
    Endpoint: POST https://api.sendgrid.com/v3/mail/send
    Docs: https://docs.sendgrid.com/api-reference/mail-send/mail-send
    Auth: Authorization: Bearer <SENDGRID_API_KEY>

Example payload:
    {
      "personalizations": [{"to": [{"email": "user@example.com"}]}],
      "from": {"email": "noreply@example.com"},
      "subject": "Welcome!",
      "content": [{"type": "text/plain", "value": "Hi there"}]
    }
"""

from __future__ import annotations

import json
import os
from typing import Any

import httpx

from drt.config.models import (
    SendGridDestinationConfig,
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


class SendGridDestination:
    """Send records as transactional emails via SendGrid."""

    SENDGRID_API_URL = "https://api.sendgrid.com/v3/mail/send"

    def load(
        self,
        records: list[dict[str, Any]],
        config: SendGridDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, SendGridDestinationConfig)

        # --- Auth (explicit, no getattr) ---
        if config.auth.type != "bearer":
            raise ValueError("SendGrid destination requires bearer auth.")

        api_key = config.auth.token or (
            os.environ.get(config.auth.token_env)
            if config.auth.token_env
            else None
        )

        if not api_key:
            raise ValueError(
                "SendGrid destination: missing API key via auth.token or auth.token_env."
            )

        # --- Required fields (already enforced by Pydantic, but kept for clarity) ---
        if not config.from_email:
            raise ValueError("SendGrid destination: 'from_email' is required.")

        result = SyncResult()
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()
                try:
                    # --- Render templates ---
                    subject = render_template(config.subject_template, record)
                    body = render_template(config.body_template, record)

                    # --- Resolve recipient ---
                    to_email = record.get(config.to_email_field)
                    if not to_email:
                        raise ValueError(
                            f"Record missing '{config.to_email_field}' field for recipient."
                        )

                    # --- Build payload (SendGrid spec) ---
                    payload: dict[str, Any] = {
                        "personalizations": [
                            {
                                "to": [{"email": to_email}],
                                "subject": subject,
                            }
                        ],
                        "from": {
                            "email": config.from_email,
                            **({"name": config.from_name} if config.from_name else {}),
                        },
                        "content": [
                            {
                                "type": "text/plain",
                                "value": body,
                            }
                        ],
                    }

                    # --- Optional list_ids (explicit field) ---
                    if config.list_ids:
                        payload["list_ids"] = config.list_ids

                    # --- HTTP send with retry ---
                    def do_post() -> httpx.Response:
                        response = client.post(
                            self.SENDGRID_API_URL,
                            headers=headers,
                            json=payload,
                        )
                        response.raise_for_status()
                        return response

                    with_retry(do_post, _DEFAULT_RETRY)
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
                except httpx.RequestError as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(record)[:200],
                            http_status=None,
                            error_message=f"Request error: {str(e)}",
                        )
                    )
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

        return result
    