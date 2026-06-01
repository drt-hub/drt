"""Empty-batch contract for hardcoded-endpoint API destinations.

Sibling to ``test_destination_empty_batch.py`` (HTTP destinations with
user-configurable URLs — Slack / Discord / Teams / REST API). The
destinations covered here target fixed third-party APIs (HubSpot,
Notion, Linear, etc.) where the endpoint is hardcoded in the
destination implementation, so we can't point them at
``pytest_httpserver`` — instead we patch ``httpx.Client.send`` to
guarantee no network I/O happens on empty input.

Contracts under test (same shape as the HTTP / file / SQL sibling
modules — see #593 / #594 / #595):

1. Each destination satisfies the ``Destination`` Protocol.
2. ``load([], config, sync_options)`` returns
   ``SyncResult(success=0, failed=0, skipped=0)``.
3. ``load([])`` never calls ``httpx.Client.send`` — destinations must
   short-circuit on empty input, never probe their API.

Why the third contract is load-bearing: the engine routinely passes
empty batches when the source produced no rows in a window
(incremental syncs after a quiet period). A destination that opens a
session or hits the API on empty input wastes auth tokens / rate
limits / API quota and breaks dry-run-like flows.

Scope: ``load()``-shape HTTP destinations using ``httpx`` against a
hardcoded SaaS endpoint. Out of scope for this module:

- ``email_smtp`` (uses ``smtplib``, different transport)
- ``google_sheets`` (uses ``googleapiclient.discovery``)
- ``salesforce_bulk`` / ``staged_upload`` (use the ``StagedDestination``
  Protocol with ``stage()`` + a separate trigger step rather than
  ``load()``)

Adding a new destination: append a ``pytest.param(...)`` entry to
``API_DESTINATIONS`` below. If the destination needs env vars set, add
them to the third element of the tuple — they're applied via
``monkeypatch.setenv`` before each parametrised test.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from drt.config.models import (
    AmplitudeDestinationConfig,
    BearerAuth,
    GitHubActionsDestinationConfig,
    GoogleAdsDestinationConfig,
    HubSpotDestinationConfig,
    IntercomDestinationConfig,
    JiraDestinationConfig,
    LinearDestinationConfig,
    MixpanelDestinationConfig,
    NotionDestinationConfig,
    RateLimitConfig,
    SendGridDestinationConfig,
    SyncOptions,
    TwilioDestinationConfig,
    ZendeskDestinationConfig,
)
from drt.destinations.amplitude import AmplitudeDestination
from drt.destinations.base import Destination, SyncResult
from drt.destinations.github_actions import GitHubActionsDestination
from drt.destinations.google_ads import GoogleAdsDestination
from drt.destinations.hubspot import HubSpotDestination
from drt.destinations.intercom import IntercomDestination
from drt.destinations.jira import JiraDestination
from drt.destinations.linear import LinearDestination
from drt.destinations.mixpanel import MixpanelDestination
from drt.destinations.notion import NotionDestination
from drt.destinations.sendgrid import SendGridDestination
from drt.destinations.twilio import TwilioDestination
from drt.destinations.zendesk import ZendeskDestination


def _bearer(token: str = "dummy-token") -> BearerAuth:
    return BearerAuth(type="bearer", token=token)


# Each entry: (destination class, factory → config, env vars dict).
# env vars are applied via monkeypatch.setenv before each test so
# destinations that resolve credentials at load-time find a value to
# read (even though no request ever leaves the process).
API_DESTINATIONS: list[Any] = [
    pytest.param(
        HubSpotDestination,
        lambda: HubSpotDestinationConfig(
            type="hubspot",
            object_type="contacts",
            auth=_bearer(),
        ),
        {},
        id="hubspot",
    ),
    pytest.param(
        JiraDestination,
        lambda: JiraDestinationConfig(
            type="jira",
            base_url_env="JIRA_BASE_URL_TEST",
            email_env="JIRA_EMAIL_TEST",
            token_env="JIRA_TOKEN_TEST",
            project_key="DEMO",
            summary_template="{{ row.title }}",
            description_template="{{ row.body }}",
        ),
        {
            "JIRA_BASE_URL_TEST": "https://example.atlassian.net",
            "JIRA_EMAIL_TEST": "test@example.com",
            "JIRA_TOKEN_TEST": "dummy",
        },
        id="jira",
    ),
    pytest.param(
        LinearDestination,
        lambda: LinearDestinationConfig(
            type="linear",
            team_id="team_abc",
            title_template="{{ row.title }}",
            description_template="{{ row.body }}",
            auth=_bearer(),
        ),
        {},
        id="linear",
    ),
    pytest.param(
        NotionDestination,
        lambda: NotionDestinationConfig(
            type="notion",
            database_id="abc123",
            auth=_bearer(),
        ),
        {},
        id="notion",
    ),
    pytest.param(
        TwilioDestination,
        lambda: TwilioDestinationConfig(
            type="twilio",
            account_sid="AC_dummy",
            auth_token="dummy",
            from_number="+15551234567",
            to_template="{{ row.phone }}",
            message_template="{{ row.message }}",
        ),
        {},
        id="twilio",
    ),
    pytest.param(
        AmplitudeDestination,
        lambda: AmplitudeDestinationConfig(
            type="amplitude",
            api_key="dummy",
        ),
        {},
        id="amplitude",
    ),
    pytest.param(
        MixpanelDestination,
        lambda: MixpanelDestinationConfig(
            type="mixpanel",
            endpoint="people_set",
            project_token="dummy",
        ),
        {},
        id="mixpanel",
    ),
    pytest.param(
        ZendeskDestination,
        lambda: ZendeskDestinationConfig(
            type="zendesk",
            subdomain="example",
            email="test@example.com",
            api_token="dummy",
        ),
        {},
        id="zendesk",
    ),
    pytest.param(
        GoogleAdsDestination,
        lambda: GoogleAdsDestinationConfig(
            type="google_ads",
            customer_id="1234567890",
            conversion_action="customers/1234567890/conversionActions/1",
            developer_token_env="GOOGLE_ADS_DEVELOPER_TOKEN_TEST",
        ),
        {"GOOGLE_ADS_DEVELOPER_TOKEN_TEST": "dummy"},
        id="google_ads",
    ),
    pytest.param(
        SendGridDestination,
        lambda: SendGridDestinationConfig(
            type="sendgrid",
            from_email="noreply@example.com",
            subject_template="{{ row.subject }}",
            body_template="{{ row.body }}",
            auth=_bearer(),
        ),
        {},
        id="sendgrid",
    ),
    pytest.param(
        IntercomDestination,
        lambda: IntercomDestinationConfig(
            type="intercom",
            auth=_bearer(),
            properties_template='{"email": "{{ row.email }}"}',
        ),
        {},
        id="intercom",
    ),
    pytest.param(
        GitHubActionsDestination,
        lambda: GitHubActionsDestinationConfig(
            type="github_actions",
            owner="drt-hub",
            repo="drt",
            workflow_id="deploy.yml",
            auth=_bearer(),
        ),
        {},
        id="github_actions",
    ),
]


@pytest.fixture
def empty_sync_options() -> SyncOptions:
    """Minimal SyncOptions with no rate limit — keeps the test fast."""
    return SyncOptions(
        mode="full",
        batch_size=100,
        on_error="skip",
        rate_limit=RateLimitConfig(requests_per_second=0),
    )


@pytest.fixture
def block_httpx(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
    """Patch ``httpx.Client.send`` (sync + async) to a tripwire that records
    any attempted HTTP call as a captured tuple instead of raising.

    Returns the call log so a test can assert on it directly. Using
    ``AssertionError`` inside the tripwire would be caught and swallowed
    by some destinations' broad ``except Exception`` row-error handlers
    — that would cause the test to PASS while the bug it's meant to
    catch slipped through. Logging-then-asserting at the test level
    sidesteps the swallow path.
    """
    captured: list[tuple[Any, ...]] = []

    def _record(self: Any, request: Any, *args: Any, **kwargs: Any) -> Any:
        captured.append((self, request, args, kwargs))
        # Returning None means destinations that ignore the return
        # value still see "something happened" — but we already
        # recorded the call, so the test assertion will fail
        # regardless.
        return None

    monkeypatch.setattr("httpx.Client.send", _record)
    monkeypatch.setattr("httpx.AsyncClient.send", _record)
    return captured


def _apply_env(monkeypatch: pytest.MonkeyPatch, env_vars: dict[str, str]) -> None:
    for k, v in env_vars.items():
        monkeypatch.setenv(k, v)


@pytest.mark.parametrize("destination_class, config_factory, env_vars", API_DESTINATIONS)
def test_satisfies_destination_protocol(
    destination_class: type,
    config_factory: Callable[[], Any],
    env_vars: dict[str, str],
) -> None:
    """Every API destination satisfies the ``Destination`` Protocol."""
    dest = destination_class()
    assert isinstance(dest, Destination)


@pytest.mark.parametrize("destination_class, config_factory, env_vars", API_DESTINATIONS)
def test_empty_batch_returns_empty_sync_result(
    destination_class: type,
    config_factory: Callable[[], Any],
    env_vars: dict[str, str],
    empty_sync_options: SyncOptions,
    block_httpx: list[tuple[Any, ...]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``load([])`` returns ``SyncResult(success=0, failed=0, skipped=0)``."""
    _apply_env(monkeypatch, env_vars)
    dest = destination_class()
    config = config_factory()

    result = dest.load([], config, empty_sync_options)

    assert isinstance(result, SyncResult)
    assert result.success == 0
    assert result.failed == 0
    assert result.skipped == 0


@pytest.mark.parametrize("destination_class, config_factory, env_vars", API_DESTINATIONS)
def test_empty_batch_makes_no_http_request(
    destination_class: type,
    config_factory: Callable[[], Any],
    env_vars: dict[str, str],
    empty_sync_options: SyncOptions,
    block_httpx: list[tuple[Any, ...]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``load([])`` does not call ``httpx.Client.send``.

    The ``block_httpx`` fixture replaces ``httpx.Client.send`` and
    ``httpx.AsyncClient.send`` with a tripwire that records every
    attempted call. A populated ``block_httpx`` list means a
    destination tried to talk to its API on empty input — that's the
    bug this contract locks against.
    """
    _apply_env(monkeypatch, env_vars)
    dest = destination_class()
    config = config_factory()

    dest.load([], config, empty_sync_options)

    assert len(block_httpx) == 0, (
        f"{destination_class.__name__} made {len(block_httpx)} HTTP "
        "request(s) on empty batch; destinations must short-circuit "
        "when there's nothing to send."
    )
