"""GitHub Actions destination — workflow_dispatch trigger.

Triggers a GitHub Actions workflow for each record from the DWH.
Uses the GitHub REST API: POST /repos/{owner}/{repo}/actions/workflows/{id}/dispatches

Use cases:
  - New deployment record in DWH → trigger deploy workflow
  - Approved PR row → trigger review workflow
  - Customer data change → trigger onboarding automation

Requires: GITHUB_TOKEN with `actions: write` permission.

Example sync YAML:

    destination:
      type: github_actions
      owner: myorg
      repo: myapp
      workflow_id: deploy.yml
      ref: main
      inputs_template: |
        {
          "environment": "{{ row.env }}",
          "version": "{{ row.version }}",
          "triggered_by": "drt"
        }
      auth:
        type: bearer
        token_env: GITHUB_TOKEN
"""

from __future__ import annotations

import json
from typing import Any

import httpx

from drt.config.credentials import resolve_env
from drt.config.models import GitHubActionsDestinationConfig, RetryConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import DetailedSyncResult, RowError
from drt.templates.renderer import render_template

_GITHUB_API = "https://api.github.com"
_DEFAULT_RETRY = RetryConfig(
    max_attempts=3,
    initial_backoff=1.0,
    retryable_status_codes=(429, 500, 502, 503, 504),
)


class GitHubActionsDestination:
    """Trigger GitHub Actions workflow_dispatch events from DWH records."""

    def load(
        self,
        records: list[dict[str, Any]],
        config: GitHubActionsDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        token = resolve_env(config.auth.token, config.auth.token_env)
        if not token:
            raise ValueError(
                "GitHub Actions destination: set GITHUB_TOKEN env var "
                "or provide auth.token_env in the sync config."
            )

        url = (
            f"{_GITHUB_API}/repos/{config.owner}/{config.repo}"
            f"/actions/workflows/{config.workflow_id}/dispatches"
        )
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }

        result = DetailedSyncResult()
        # GitHub rate limit: 1000 workflow_dispatch/hour per repo — be conservative
        rate_limiter = RateLimiter(
            min(sync_options.rate_limit.requests_per_second, 5)
        )

        with httpx.Client(timeout=30.0) as client:
            for i, record in enumerate(records):
                rate_limiter.acquire()

                inputs: dict[str, Any] = {}
                if config.inputs_template:
                    try:
                        rendered = render_template(config.inputs_template, record)
                        inputs = json.loads(rendered)
                    except (ValueError, json.JSONDecodeError) as e:
                        result.failed += 1
                        result.row_errors.append(
                            RowError(
                                batch_index=i,
                                record_preview=json.dumps(record)[:200],
                                http_status=None,
                                error_message=f"inputs_template error: {e}",
                            )
                        )
                        continue

                payload: dict[str, Any] = {"ref": config.ref}
                if inputs:
                    payload["inputs"] = inputs

                def do_request(
                    _url: str = url,
                    _headers: dict[str, Any] = headers,
                    _payload: dict[str, Any] = payload,
                ) -> httpx.Response:
                    response = client.post(_url, json=_payload, headers=_headers)
                    response.raise_for_status()
                    return response

                try:
                    with_retry(do_request, _DEFAULT_RETRY)
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

        return result  # type: ignore[return-value]
