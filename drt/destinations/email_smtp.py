from __future__ import annotations

import time
import json
from typing import Any

import smtplib
from email.mime.text import MIMEText

from drt.config.models import DestinationConfig, EmailDestinationConfig, SyncOptions, RetryConfig
from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError
from drt.destinations.rate_limiter import RateLimiter
from drt.templates.renderer import render_template

def smtp_with_retry(send_fn, config: RetryConfig) -> None:
    backoff = config.initial_backoff

    for attempt in range(1, config.max_attempts + 1):
        try:
            return send_fn()

        except smtplib.SMTPException:
            if attempt == config.max_attempts:
                raise

        if attempt < config.max_attempts:
            time.sleep(min(backoff, config.max_backoff))
            backoff *= config.backoff_multiplier


class EmailDestination:

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, EmailDestinationConfig)
        from_email = config.from_email
        password = config.password
        smtp_server = config.smtp_server
        smtp_port = config.smtp_port
        to_email = config.to_email

        result = SyncResult()
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)

        with smtplib.SMTP(smtp_server, smtp_port, timeout=30.0) as connection:
            connection.starttls()
            connection.login(from_email, password)

            recipients = to_email if isinstance(to_email, list) else [to_email]

            for i, record in enumerate(records):
                rate_limiter.acquire()
                try:                    
                    message_rendered = render_template(config.message_template, record)
                    subject = (
                        render_template(config.subject_template, record)
                        if config.subject_template
                        else "Data Reverse Tool"
                    )

                    msg = MIMEText(message_rendered, "plain", "utf-8")
                    msg["Subject"] = subject
                    msg["From"] = from_email
                    msg["To"] = ", ".join(recipients)

                    def send():
                        connection.noop()
                        connection.send_message(msg, to_addrs=recipients)
                    
                    smtp_with_retry(send, sync_options.retry)
                    result.success += 1

                except Exception as e:
                    result.failed += 1
                    result.errors.append(
                        RowError(
                            batch_index= i,
                            record_preview= json.dumps(record)[:200],
                            http_status= None,
                            error_message= str(e)
                        )
                    )
        
        return result