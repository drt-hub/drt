"""SendGrid destination — Marketing Contacts upsert + transactional emails.

Supports:
1. Upserting contacts into SendGrid Marketing Contacts.
2. Sending transactional emails per row (e.g. onboarding emails, alerts, billing reminders).

Contacts:
- Upserts contacts using the SendGrid v3 Marketing Contacts API.
- Deduplicates automatically by email.

Transactional Emails:
- Sends one email per row using dynamic templates.
- Useful for onboarding emails, alert notifications, billing reminders, etc.

Requires: SENDGRID_API_KEY (API key with Marketing and/or Mail Send permissions).

Example sync YAML — transactional emails:

    destination:
      type: sendgrid
      from_email: "noreply@example.com"
      from_name: "My App"
      subject_template: "Welcome, {{ row.first_name }}!"
      body_template: |
        Hi {{ row.first_name }},

        Thanks for signing up. Your account is ready.
      auth:
        type: bearer
        token_env: SENDGRID_API_KEY

Example sync YAML — contacts:

    destination:
      type: sendgrid
      list_ids: ["your-list-id"]
      properties_template: |
        {
          "email": "{{ row.email }}",
          "first_name": "{{ row.first_name }}",
          "last_name": "{{ row.last_name }}"
        }
      auth:
        type: bearer
        token_env: SENDGRID_API_KEY
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import SendGridDestinationConfig, RetryConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template


_SENDGRID_CONTACTS_API = "https://api.sendgrid.com/v3/marketing/contacts"
_SENDGRID_MAIL_API = "https://api.sendgrid.com/v3/mail/send"

_DEFAULT_RETRY = RetryConfig(
    max_attempts=3,
    initial_backoff=1.0,
    retryable_status_codes=(429, 500, 502, 503, 504),
)


class SendGridDestination:
    """SendGrid destination supporting contacts + transactional email."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: SendGridDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        token = resolve_env(config.auth.token, config.auth.token_env)
        if not token:
            raise ValueError(
                "SendGrid destination: set SENDGRID_API_KEY env var "
                "or provide auth.token_env in the sync config."
            )

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        result = SyncResult()
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)

        # Detect mode
        is_email_mode = hasattr(config, "subject_template") and hasattr(config, "body_template")

        with httpx.Client(timeout=30.0) as client:

            # =========================
            # 📧 TRANSACTIONAL EMAIL MODE
            # =========================
            if is_email_mode:
                for i, record in enumerate(records):
                    rate_limiter.acquire()

                    try:
                        to_email = record.get("email")
                        if not to_email:
                            raise ValueError("Missing required field: email")

                        subject = render_template(config.subject_template, record)
                        body = render_template(config.body_template, record)

                        payload = {
                            "personalizations": [
                                {
                                    "to": [{"email": to_email}]
                                }
                            ],
                            "from": {
                                "email": config.from_email,
                                **({"name": config.from_name} if getattr(config, "from_name", None) else {}),
                            },
                            "subject": subject,
                            "content": [
                                {
                                    "type": "text/plain",
                                    "value": body,
                                }
                            ],
                        }

                        def send_email() -> httpx.Response:
                            response = client.post(
                                _SENDGRID_MAIL_API,
                                json=payload,
                                headers=headers,
                            )
                            response.raise_for_status()
                            return response

                        with_retry(send_email, _DEFAULT_RETRY)
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

            # =========================
            # 👥 CONTACTS MODE (BATCH)
            # =========================
            else:
                batch_size = min(1000, getattr(sync_options, "batch_size", 1000))

                def chunked(data: list[dict[str, Any]], size: int):
                    for i in range(0, len(data), size):
                        yield i, data[i : i + size]

                for batch_index, batch in chunked(records, batch_size):
                    rate_limiter.acquire()

                    contacts = []
                    row_map = []

                    for i, record in enumerate(batch):
                        global_index = batch_index + i

                        try:
                            if config.properties_template:
                                rendered = render_template(
                                    config.properties_template, record
                                )
                                contact = json.loads(rendered)
                            else:
                                contact = record

                            if "email" not in contact:
                                raise ValueError("Missing required field: email")

                            contacts.append(contact)
                            row_map.append(global_index)

                        except Exception as e:
                            result.failed += 1
                            result.row_errors.append(
                                RowError(
                                    batch_index=global_index,
                                    record_preview=json.dumps(record)[:200],
                                    http_status=None,
                                    error_message=str(e),
                                )
                            )

                    if not contacts:
                        continue

                    payload: dict[str, Any] = {"contacts": contacts}

                    if getattr(config, "list_ids", None):
                        payload["list_ids"] = config.list_ids

                    def upsert_contacts() -> httpx.Response:
                        response = client.put(
                            _SENDGRID_CONTACTS_API,
                            json=payload,
                            headers=headers,
                        )
                        response.raise_for_status()
                        return response

                    try:
                        with_retry(upsert_contacts, _DEFAULT_RETRY)
                        result.success += len(contacts)

                    except httpx.HTTPStatusError as e:
                        for idx in row_map:
                            result.failed += 1
                            result.row_errors.append(
                                RowError(
                                    batch_index=idx,
                                    record_preview="batch item",
                                    http_status=e.response.status_code,
                                    error_message=e.response.text[:500],
                                )
                            )

                    except Exception as e:
                        for idx in row_map:
                            result.failed += 1
                            result.row_errors.append(
                                RowError(
                                    batch_index=idx,
                                    record_preview="batch item",
                                    http_status=None,
                                    error_message=str(e),
                                )
                            )

        return result