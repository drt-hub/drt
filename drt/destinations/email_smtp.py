"""Email SMTP destination — send records as emails via SMTP.

Sends one email per record using Python's standard ``smtplib``.
No extra dependencies required.

Example sync YAML:

    destination:
      type: email_smtp
      host: smtp.gmail.com
      port: 587
      sender: "Alerts <noreply@example.com>"
      recipients:
        - admin@example.com
      subject_template: "New signup: {{ row.name }}"
      body_template: |
        Name:  {{ row.name }}
        Email: {{ row.email }}
      username_env: SMTP_USER
      password_env: SMTP_PASSWORD

For SSL-wrapped connections (port 465) set ``use_tls: false`` and connect
via ``smtplib.SMTP_SSL`` — or keep the default STARTTLS behaviour (port 587).
"""

from __future__ import annotations

import json
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

from drt.config.models import DestinationConfig, EmailSmtpDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template


class EmailSmtpDestination:
    """Send records as emails via SMTP."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, EmailSmtpDestinationConfig)

        username = config.username or (
            os.environ.get(config.username_env) if config.username_env else None
        )
        password = config.password or (
            os.environ.get(config.password_env) if config.password_env else None
        )
        if not username or not password:
            raise ValueError(
                "email_smtp destination: provide 'username'/'username_env' and "
                "'password'/'password_env'."
            )

        result = SyncResult()
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)

        for i, record in enumerate(records):
            rate_limiter.acquire()
            try:
                subject = render_template(config.subject_template, record)
                body = render_template(config.body_template, record)

                msg = MIMEMultipart()
                msg["From"] = config.sender
                msg["To"] = ", ".join(config.recipients)
                msg["Subject"] = subject
                msg.attach(MIMEText(body, "plain"))

                with smtplib.SMTP(config.host, config.port, timeout=30) as server:
                    if config.use_tls:
                        server.starttls()
                    server.login(username, password)
                    server.send_message(msg)

                result.success += 1
            except smtplib.SMTPException as e:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=i,
                        record_preview=json.dumps(record, default=str)[:200],
                        http_status=None,
                        error_message=str(e)[:500],
                    )
                )
            except Exception as e:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=i,
                        record_preview=json.dumps(record, default=str)[:200],
                        http_status=None,
                        error_message=str(e),
                    )
                )

        return result
