from __future__ import annotations

import json
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, LinearDestinationConfig, RetryConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template

_LINEAR_API = "https://api.linear.app/graphql"

# Retry configuration for transient errors
_DEFAULT_RETRY = RetryConfig(
    max_attempts=3,
    initial_backoff=1.0,
    retryable_status_codes=(429, 500, 502, 503, 504),
)

_ISSUE_CREATE_MUTATION = """
mutation IssueCreate($input: IssueCreateInput!) {
  issueCreate(input: $input) {
    success
    issue { id title }
  }
}
"""


class LinearDestination:
    """Create or update Linear issues via the GraphQL API."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, LinearDestinationConfig)
        api_key = resolve_env(config.auth.token, config.auth.token_env)
        team_id = resolve_env(None, config.team_id_env)

        if not api_key:
            raise ValueError(
                "Linear destination: set LINEAR_API_KEY env var "
                "or provide auth.token_env in the sync config."
            )
        if not team_id:
            raise ValueError(
                "Linear destination: set LINEAR_TEAM_ID env var "
                "or provide team_id_env in the sync config."
            )

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        result = SyncResult()
        rate_limiter = RateLimiter(min(sync_options.rate_limit.requests_per_second, 9))

        def create_issue(record: dict[str, Any], batch_index: int):
            """Render and send a Linear issue, with retry."""
            # Render title and description
            try:
                title = render_template(config.title_template, record)
                description = render_template(
                    config.description_template, record
                ) if config.description_template else ""
            except Exception as e:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=batch_index,
                        record_preview=json.dumps(record)[:200],
                        http_status=None,
                        error_message=f"Template rendering error: {e}",
                    )
                )
                return

            payload = {
                "query": _ISSUE_CREATE_MUTATION,
                "variables": {
                    "input": {
                        "teamId": team_id,
                        "title": title,
                        "description": description,
                        "labelIds": config.label_ids or [],
                        "assigneeId": config.assignee_id,
                    }
                },
            }

            def do_post():
                rate_limiter.acquire()
                response = client.post(_LINEAR_API, json=payload, headers=headers)
                # Retry should handle 429/5xx automatically
                response.raise_for_status()
                data = response.json()
                if not data.get("data", {}).get("issueCreate", {}).get("success", False):
                    raise Exception(f"Linear issue creation failed: {data}")
                return data

            try:
                with_retry(do_post, _DEFAULT_RETRY)
                result.success += 1
            except httpx.HTTPStatusError as e:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=batch_index,
                        record_preview=json.dumps(record)[:200],
                        http_status=e.response.status_code,
                        error_message=e.response.text[:500],
                    )
                )
            except Exception as e:
                result.failed += 1
                result.row_errors.append(
                    RowError(
                        batch_index=batch_index,
                        record_preview=json.dumps(record)[:200],
                        http_status=None,
                        error_message=str(e),
                    )
                )

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                create_issue(record, i)

        return result