#!/usr/bin/env python3
"""Schedule deletion of `sync_completed` events older than the retention window
from the drt PostHog project. Workaround for PostHog Free plan, which doesn't
expose per-event retention shorter than 1 year through the UI. Tracked in #482.

Usage
-----

Local one-shot (dry-run by default — never deletes unless --apply is passed):

    POSTHOG_PERSONAL_API_KEY=phx_xxx \\
      python scripts/posthog-retention-cleanup.py --dry-run

Scheduled cleanup (called from `.github/workflows/telemetry-retention-cleanup.yml`):

    POSTHOG_PERSONAL_API_KEY=phx_xxx \\
      python scripts/posthog-retention-cleanup.py --apply

Environment
-----------

POSTHOG_PERSONAL_API_KEY  (required) Personal API key with `event:write` scope.
                          Generated from PostHog Settings > User > Personal
                          API keys. NOT the project write key (`phc_...`) used
                          by drt-core at runtime to send events.

POSTHOG_PROJECT_ID        Default 175587 (drt project, EU region).

POSTHOG_API_HOST          Default https://eu.posthog.com.

DRT_RETENTION_DAYS        Default 90. The number of days to keep events.
                          Anything strictly older than this is queued for
                          deletion.

DRT_EVENT_NAME            Default sync_completed. Only this event name is
                          targeted; other event types (if drt ever adds any)
                          would need their own entry.

Exit codes
----------

0  Cleanup scheduled (or dry-run report produced) without errors.
1  API / auth / network error — the workflow run will be marked failed.
2  Misconfiguration (missing required env var, invalid argument).

Notes
-----

PostHog's deletion path is asynchronous: this script POSTs to the
`async_deletions` endpoint, which queues the deletion and processes it
on PostHog's side over the next ~24h. Confirm completion via the PostHog
"Data management > Deletions" UI on the project, or via a follow-up GET to
the same endpoint.

The script intentionally does not log the count of events found / deleted
to stdout in a publicly archived form — workflow logs are private to repo
maintainers, but anything we echo would also flow to any LLM observing the
session, so we keep the count behind --verbose. See #482 acceptance for
context on why event volume should not leak.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

DEFAULT_PROJECT_ID = "175587"
DEFAULT_API_HOST = "https://eu.posthog.com"
DEFAULT_RETENTION_DAYS = 90
DEFAULT_EVENT_NAME = "sync_completed"


class CleanupError(Exception):
    """Anything that should fail the workflow run."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Schedule deletion of old drt telemetry events from PostHog.",
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Report what would be deleted without scheduling the deletion.",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="Actually schedule the deletion via PostHog's async_deletions API.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Log the request/response payloads. Off by default to keep "
        "event-volume figures out of archived workflow logs.",
    )
    return parser.parse_args()


def env_or_default(name: str, default: str) -> str:
    return os.environ.get(name) or default


def required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        print(
            f"ERROR: {name} environment variable is required.",
            file=sys.stderr,
        )
        sys.exit(2)
    return value


def cutoff_iso(retention_days: int) -> str:
    """Return the ISO-8601 UTC timestamp older-than-which events are stale."""
    now = datetime.now(timezone.utc)
    return (now - timedelta(days=retention_days)).isoformat()


def post_async_deletion(
    api_host: str,
    project_id: str,
    api_key: str,
    event_name: str,
    cutoff_iso_str: str,
    verbose: bool,
) -> dict:
    """Schedule deletion of events with `event = event_name` and
    `timestamp < cutoff_iso_str`.

    PostHog's async_deletions endpoint accepts a JSON body describing what to
    delete. The exact field names have shifted across PostHog versions; the
    payload below targets the public stable shape documented as of 2026-05
    (Event-type deletion keyed on event name and a max timestamp).

    If PostHog rejects the request shape, the response body is logged to
    stderr (regardless of --verbose) so the failure can be diagnosed without
    a re-run.
    """
    endpoint = f"{api_host}/api/projects/{project_id}/async_deletions/"
    payload = {
        "deletion_type": "Event",
        "key": event_name,
        # Delete everything with timestamp strictly older than cutoff.
        "filters": {"max_timestamp": cutoff_iso_str},
    }
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(  # noqa: S310 — fixed host, not user input
        endpoint,
        data=body,
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "User-Agent": "drt-telemetry-retention-cleanup",
        },
        method="POST",
    )
    if verbose:
        print(f"POST {endpoint}")
        print(f"  payload: {payload}")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:  # noqa: S310
            raw = resp.read()
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        print(f"ERROR: PostHog API returned HTTP {e.code}: {detail}", file=sys.stderr)
        raise CleanupError(f"PostHog HTTP {e.code}") from e
    except urllib.error.URLError as e:
        print(f"ERROR: PostHog API unreachable: {e}", file=sys.stderr)
        raise CleanupError("PostHog unreachable") from e
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = {"raw_response": raw.decode("utf-8", errors="replace")}
    if verbose:
        print(f"  response: {data}")
    return data


def main() -> int:
    args = parse_args()

    api_key = required_env("POSTHOG_PERSONAL_API_KEY")
    project_id = env_or_default("POSTHOG_PROJECT_ID", DEFAULT_PROJECT_ID)
    api_host = env_or_default("POSTHOG_API_HOST", DEFAULT_API_HOST)
    retention_days = int(
        env_or_default("DRT_RETENTION_DAYS", str(DEFAULT_RETENTION_DAYS))
    )
    event_name = env_or_default("DRT_EVENT_NAME", DEFAULT_EVENT_NAME)

    cutoff = cutoff_iso(retention_days)

    if args.dry_run:
        print(
            f"DRY RUN: would schedule deletion of '{event_name}' events with "
            f"timestamp < {cutoff} on PostHog project {project_id} at {api_host}."
        )
        print("Pass --apply to actually schedule the deletion.")
        return 0

    print(
        f"Scheduling deletion of '{event_name}' events older than {cutoff} "
        f"on PostHog project {project_id} ({api_host})."
    )
    try:
        post_async_deletion(
            api_host=api_host,
            project_id=project_id,
            api_key=api_key,
            event_name=event_name,
            cutoff_iso_str=cutoff,
            verbose=args.verbose,
        )
    except CleanupError:
        return 1
    print(
        "Deletion request accepted. PostHog will process it asynchronously "
        "(typically within 24h). Verify in PostHog 'Data management > "
        "Deletions' once complete."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
