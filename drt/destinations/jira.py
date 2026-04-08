"""Jira destination — create/update issues via Jira REST API v3.

Uses Basic auth (email + API token) with environment-variable configuration.
For each record:
- If issue_id_field exists in row (default: issue_id), update that issue.
- Otherwise, create a new issue.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

import httpx

from drt.config.models import DestinationConfig, JiraDestinationConfig, RetryConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template

logger = logging.getLogger(__name__)

_DEFAULT_RETRY = RetryConfig(
    max_attempts=3,
    initial_backoff=1.0,
    retryable_status_codes=(429, 500, 502, 503, 504),
)


class JiraDestination:
    """Create or update Jira issues from sync records."""

    _client: httpx.Client | None = None
    _config: JiraDestinationConfig | None = None
    _auth: httpx.BasicAuth | None = None

    def create_issue(self, row: dict[str, Any]) -> None:
        """Create a new Jira issue for one row."""
        if self._client is None or self._config is None or self._auth is None:
            raise RuntimeError("JiraDestination is not initialized. Call load() first.")

        client = self._client
        config = self._config
        auth = self._auth
        assert client is not None
        assert config is not None
        assert auth is not None

        project_key = render_template(config.project_key, row)
        issue_type = render_template(config.issue_type, row)
        summary = render_template(config.summary_template, row)
        description = render_template(config.description_template, row)

        payload = {
            "fields": {
                "project": {"key": project_key},
                "issuetype": {"name": issue_type},
                "summary": summary,
                "description": _to_adf(description),
            }
        }
        url = f"{_base_url(config)}/rest/api/3/issue"

        def do_post() -> httpx.Response:
            response = client.post(url, json=payload, auth=auth)
            response.raise_for_status()
            return response

        with_retry(do_post, _DEFAULT_RETRY)

    def update_issue(self, row: dict[str, Any], issue_id: str) -> None:
        """Update an existing Jira issue for one row."""
        if self._client is None or self._config is None or self._auth is None:
            raise RuntimeError("JiraDestination is not initialized. Call load() first.")

        client = self._client
        config = self._config
        auth = self._auth
        assert client is not None
        assert config is not None
        assert auth is not None

        project_key = render_template(config.project_key, row)
        issue_type = render_template(config.issue_type, row)
        summary = render_template(config.summary_template, row)
        description = render_template(config.description_template, row)

        payload = {
            "fields": {
                "project": {"key": project_key},
                "issuetype": {"name": issue_type},
                "summary": summary,
                "description": _to_adf(description),
            }
        }
        url = f"{_base_url(config)}/rest/api/3/issue/{issue_id}"

        def do_put() -> httpx.Response:
            response = client.put(url, json=payload, auth=auth)
            response.raise_for_status()
            return response

        with_retry(do_put, _DEFAULT_RETRY)

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, JiraDestinationConfig)

        base_url = os.environ.get(config.base_url_env)
        email = os.environ.get(config.email_env)
        token = os.environ.get(config.token_env)
        if not base_url:
            raise ValueError(
                f"Jira destination: env var '{config.base_url_env}' is required."
            )
        if not email:
            raise ValueError(
                f"Jira destination: env var '{config.email_env}' is required."
            )
        if not token:
            raise ValueError(
                f"Jira destination: env var '{config.token_env}' is required."
            )

        result = SyncResult()
        rate_limiter = RateLimiter(sync_options.rate_limit.requests_per_second)
        auth = httpx.BasicAuth(username=email, password=token)

        with httpx.Client(timeout=30.0) as client:
            self._client = client
            self._config = config
            self._auth = auth

            for i, row in enumerate(records):
                rate_limiter.acquire()
                issue_id = row.get(config.issue_id_field)

                try:
                    if issue_id is not None and str(issue_id).strip():
                        self.update_issue(row, str(issue_id))
                        logger.info("Jira issue updated: %s", issue_id)
                    else:
                        self.create_issue(row)
                        logger.info("Jira issue created for row index %s", i)
                    result.success += 1
                except httpx.HTTPStatusError as e:
                    result.failed += 1
                    logger.warning("Jira request failed for row %s: %s", i, e)
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(row)[:200],
                            http_status=e.response.status_code,
                            error_message=e.response.text[:500],
                        )
                    )
                except Exception as e:
                    result.failed += 1
                    logger.warning("Jira destination failed for row %s: %s", i, e)
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=json.dumps(row)[:200],
                            http_status=None,
                            error_message=str(e),
                        )
                    )

        self._client = None
        self._config = None
        self._auth = None
        return result


def _base_url(config: JiraDestinationConfig) -> str:
    base_url = os.environ[config.base_url_env].rstrip("/")
    return base_url


def _to_adf(text: str) -> dict[str, Any]:
    """Convert plain text to Atlassian Document Format (ADF)."""
    return {
        "type": "doc",
        "version": 1,
        "content": [
            {
                "type": "paragraph",
                "content": [{"type": "text", "text": text}],
            }
        ],
    }
