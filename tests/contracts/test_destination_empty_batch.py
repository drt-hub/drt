"""Empty-batch contract for HTTP destinations.

Invariant: calling ``destination.load([], config, sync_options)`` must:

1. Return a ``SyncResult`` dataclass instance.
2. Report ``success == failed == skipped == 0``.
3. Make **zero** HTTP requests — there is no payload to send.

The third is enforced by spinning up a real ``pytest_httpserver`` and
asserting nothing landed on it. If a destination eagerly opens a
session or makes a probe call on empty input, the test fails.

Scope: HTTP-based destinations (Slack / Discord / Teams / REST API).
SQL and file destinations have different shapes and are out of scope
for this PR — extending the framework to cover them is a follow-up.

Why the contract: the engine routinely passes empty batches when the
source produced no rows in a window (incremental syncs after a quiet
period). A destination that crashes or hits the network on empty
input wastes API quota and breaks dry-run-like flows.

Adding a new HTTP destination: append a ``pytest.param(...)`` entry
to ``HTTP_DESTINATIONS`` below. No other change needed.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest
from pytest_httpserver import HTTPServer

from drt.config.models import (
    DiscordDestinationConfig,
    RateLimitConfig,
    RestApiDestinationConfig,
    SlackDestinationConfig,
    SyncOptions,
    TeamsDestinationConfig,
)
from drt.destinations.base import Destination, SyncResult
from drt.destinations.discord import DiscordDestination
from drt.destinations.rest_api import RestApiDestination
from drt.destinations.slack import SlackDestination
from drt.destinations.teams import TeamsDestination

# Each entry: (destination class, factory taking an HTTPServer → valid config).
# Factories return configs whose URL points at the test's httpserver
# so the destination *could* make a request — the contract is that it
# doesn't, given empty input.
HTTP_DESTINATIONS: list[Any] = [
    pytest.param(
        SlackDestination,
        lambda hs: SlackDestinationConfig(
            type="slack",
            webhook_url=hs.url_for("/slack"),
        ),
        id="slack",
    ),
    pytest.param(
        DiscordDestination,
        lambda hs: DiscordDestinationConfig(
            type="discord",
            webhook_url=hs.url_for("/discord"),
        ),
        id="discord",
    ),
    pytest.param(
        TeamsDestination,
        lambda hs: TeamsDestinationConfig(
            type="teams",
            webhook_url=hs.url_for("/teams"),
        ),
        id="teams",
    ),
    pytest.param(
        RestApiDestination,
        lambda hs: RestApiDestinationConfig(
            type="rest_api",
            url=hs.url_for("/rest"),
            method="POST",
        ),
        id="rest_api",
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


@pytest.mark.parametrize("destination_class, config_factory", HTTP_DESTINATIONS)
def test_satisfies_destination_protocol(
    destination_class: type,
    config_factory: Callable[[HTTPServer], Any],
    httpserver: HTTPServer,
) -> None:
    """Every HTTP destination satisfies the ``Destination`` Protocol."""
    dest = destination_class()
    assert isinstance(dest, Destination)


@pytest.mark.parametrize("destination_class, config_factory", HTTP_DESTINATIONS)
def test_empty_batch_returns_empty_sync_result(
    destination_class: type,
    config_factory: Callable[[HTTPServer], Any],
    httpserver: HTTPServer,
    empty_sync_options: SyncOptions,
) -> None:
    """``load([])`` returns ``SyncResult(success=0, failed=0, skipped=0)``."""
    dest = destination_class()
    config = config_factory(httpserver)

    result = dest.load([], config, empty_sync_options)

    assert isinstance(result, SyncResult)
    assert result.success == 0
    assert result.failed == 0
    assert result.skipped == 0


@pytest.mark.parametrize("destination_class, config_factory", HTTP_DESTINATIONS)
def test_empty_batch_makes_no_http_request(
    destination_class: type,
    config_factory: Callable[[HTTPServer], Any],
    httpserver: HTTPServer,
    empty_sync_options: SyncOptions,
) -> None:
    """``load([])`` does not hit the network.

    ``pytest_httpserver.HTTPServer.log`` records every received request,
    matched or not. We register zero expected requests; if anything
    lands, ``len(log) > 0`` and the assertion fails — a destination
    that opens a connection or probes the URL on empty input is a bug.
    """
    dest = destination_class()
    config = config_factory(httpserver)

    dest.load([], config, empty_sync_options)

    assert len(httpserver.log) == 0, (
        f"{destination_class.__name__} made "
        f"{len(httpserver.log)} HTTP request(s) on empty batch; "
        "destinations must short-circuit when there's nothing to send."
    )
