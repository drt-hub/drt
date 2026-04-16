"""Linear destination — create issues via the Linear GraphQL API.

Sends one issue per record using Linear's GraphQL API.
Supports title + description templating via Jinja2.

No extra dependencies required (uses httpx from core).

Example sync YAML:

    destination:
      type: linear
      team_id_env: LINEAR_TEAM_ID
      title_template: "Alert: {{ row.metric }}"
      description_template: "Value: {{ row.value }}"
      auth:
        type: bearer
        token_env: LINEAR_API_KEY

Linear API reference:
    Endpoint: POST https://api.linear.app/graphql
    Docs: https://developers.linear.app/docs/graphql/working-with-the-graphql-api
    Auth: Authorization: Bearer <LINEAR_API_KEY>
"""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, LinearDestinationConfig, RetryConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import RowError
from drt.templates.renderer import render_template

logger = logging.getLogger(__name__)

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


def _record_preview(row: dict[str, Any]) -> str:
    return json.dumps(row, default=str)[:200]


class LinearDestination:
    """Create Linear issues via the GraphQL API."""

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
                "or provide team_id / team_id_env in the sync config."
            )

        retry_config = sync_options.retry or _DEFAULT_RETRY
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
                    title = render_template(config.title_template, record)
                    description = (
                        render_template(config.description_template, record)
                        if config.description_template
                        else ""
                    )

                    issue_input: dict[str, Any] = {
                        "teamId": team_id,
                        "title": title,
                        "description": description,
                        "labelIds": config.label_ids or [],
                    }
                    if config.assignee_id is not None:
                        issue_input["assigneeId"] = config.assignee_id

                    payload = {
                        "query": _ISSUE_CREATE_MUTATION,
                        "variables": {"input": issue_input},
                    }

                    def do_post(_payload: dict[str, Any] = payload) -> Any:
                        response = client.post(_LINEAR_API, json=_payload, headers=headers)
                        response.raise_for_status()
                        data = response.json()
                        if not data.get("data", {}).get("issueCreate", {}).get("success", False):
                            raise Exception(f"Linear issue creation failed: {data}")
                        return data

                    with_retry(do_post, retry_config)
                    result.success += 1

                except httpx.HTTPStatusError as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=_record_preview(record),
                            http_status=e.response.status_code,
                            error_message=e.response.text[:500],
                        )
                    )
                    if sync_options.on_error == "fail":
                        break
                except httpx.RequestError as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=_record_preview(record),
                            http_status=None,
                            error_message=f"Request error: {e}",
                        )
                    )
                    if sync_options.on_error == "fail":
                        break
                except Exception as e:
                    result.failed += 1
                    result.row_errors.append(
                        RowError(
                            batch_index=i,
                            record_preview=_record_preview(record),
                            http_status=None,
                            error_message=str(e),
                        )
                    )
                    if sync_options.on_error == "fail":
                        break

        return result
