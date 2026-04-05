"""Linear destination — create or update Linear issues from DWH rows.

Upserts issues into Linear via the GraphQL API.

Requires: LINEAR_API_KEY (Personal API key with write access).

Example sync YAML — creating alerts/issues:

    destination:
      type: linear
      team_id_env: LINEAR_TEAM_ID
      title_template: "[Alert] {{ row.metric }} exceeded threshold"
      description_template: |
        **Value:** {{ row.value }}
        **Threshold:** {{ row.threshold }}
        **Detected at:** {{ row.detected_at }}
      label_ids: []          # optional: list of Linear label UUIDs
      assignee_id: null      # optional: Linear user UUID
      auth:
        type: bearer
        token_env: LINEAR_API_KEY
"""

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
        team_id = resolve_env(config.team_id, config.team_id_env)

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

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()

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
                            batch_index=i,
                            record_preview=json.dumps(record)[:200],
                            http_status=None,
                            error_message=f"Template rendering error: {e}",
                        )
                    )
                    continue

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

                def do_create(_payload=payload):
                    response = client.post(_LINEAR_API, json=_payload, headers=headers)
                    response.raise_for_status()
                    data = response.json()
                    if not data.get("data", {}).get("issueCreate", {}).get("success", False):
                        raise Exception(f"Linear issue creation failed: {data}")
                    return data

                try:
                    with_retry(do_create, _DEFAULT_RETRY)
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

        return result