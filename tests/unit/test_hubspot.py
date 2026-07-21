"""Unit tests for the HubSpot destination — focused on match_policy (#757).

HubSpot's create-vs-update outcome is a status code, so the policy legs are
verified by which HTTP verb runs and how the SyncResult counts:

- ``upsert`` (default): POST, then PATCH on 409.
- ``create_only``: POST only; 409 (exists) -> skipped, never PATCH.
- ``update_only``: PATCH by idProperty; 404 (no match) -> skipped, never POST.

httpx is mocked — no network.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import httpx

from drt.config.models import (
    HubSpotDestinationConfig,
    RateLimitConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.base import MatchPolicyCapable
from drt.destinations.hubspot import HubSpotDestination


def _config(**overrides: Any) -> HubSpotDestinationConfig:
    data: dict[str, Any] = {
        "type": "hubspot",
        "object_type": "contacts",
        "id_property": "email",
        "auth": {"type": "bearer", "token": "test-token"},
    }
    data.update(overrides)
    return HubSpotDestinationConfig(**data)


def _options(match_policy: str = "upsert", **overrides: Any) -> SyncOptions:
    data: dict[str, Any] = {
        "match_policy": match_policy,
        "rate_limit": RateLimitConfig(requests_per_second=0),
        "retry": RetryConfig(max_attempts=1, initial_backoff=0.0, backoff_multiplier=1.0),
        "on_error": "skip",
    }
    data.update(overrides)
    return SyncOptions(**data)


def _response(status_code: int = 200) -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.text = "{}"
    resp.raise_for_status.return_value = None
    return resp


_RECORD = {"email": "a@b.com", "firstname": "Ada"}


# ---------------------------------------------------------------------------
# Capability
# ---------------------------------------------------------------------------


def test_hubspot_declares_match_policy_capability() -> None:
    dest = HubSpotDestination()
    assert isinstance(dest, MatchPolicyCapable)
    assert dest.supported_match_policies() == frozenset(
        {"upsert", "update_only", "create_only"}
    )


# ---------------------------------------------------------------------------
# Default upsert (unchanged behaviour)
# ---------------------------------------------------------------------------


def test_upsert_creates_on_post_success() -> None:
    with (
        patch("httpx.Client.post", return_value=_response(200)) as post,
        patch("httpx.Client.patch") as patch_call,
    ):
        result = HubSpotDestination().load([_RECORD], _config(), _options())

    assert (result.success, result.skipped, result.failed) == (1, 0, 0)
    post.assert_called_once()
    patch_call.assert_not_called()


def test_upsert_patches_on_409() -> None:
    with (
        patch("httpx.Client.post", return_value=_response(409)) as post,
        patch("httpx.Client.patch", return_value=_response(200)) as patch_call,
    ):
        result = HubSpotDestination().load([_RECORD], _config(), _options())

    assert (result.success, result.skipped, result.failed) == (1, 0, 0)
    post.assert_called_once()
    patch_call.assert_called_once()


# ---------------------------------------------------------------------------
# create_only — POST only; 409 => skip
# ---------------------------------------------------------------------------


def test_create_only_skips_existing_and_never_patches() -> None:
    with (
        patch("httpx.Client.post", return_value=_response(409)) as post,
        patch("httpx.Client.patch") as patch_call,
    ):
        result = HubSpotDestination().load(
            [_RECORD], _config(), _options("create_only")
        )

    assert (result.success, result.skipped, result.failed) == (0, 1, 0)
    post.assert_called_once()
    patch_call.assert_not_called()  # existing rows are left untouched


def test_create_only_counts_new_record_as_success() -> None:
    with (
        patch("httpx.Client.post", return_value=_response(201)) as post,
        patch("httpx.Client.patch") as patch_call,
    ):
        result = HubSpotDestination().load(
            [_RECORD], _config(), _options("create_only")
        )

    assert (result.success, result.skipped, result.failed) == (1, 0, 0)
    post.assert_called_once()
    patch_call.assert_not_called()


# ---------------------------------------------------------------------------
# update_only — PATCH by idProperty; 404 => skip; never POST
# ---------------------------------------------------------------------------


def test_update_only_patches_directly_and_never_posts() -> None:
    with (
        patch("httpx.Client.post") as post,
        patch("httpx.Client.patch", return_value=_response(200)) as patch_call,
    ):
        result = HubSpotDestination().load(
            [_RECORD], _config(), _options("update_only")
        )

    assert (result.success, result.skipped, result.failed) == (1, 0, 0)
    post.assert_not_called()  # never creates
    patch_call.assert_called_once()
    # PATCH targets the idProperty URL for this record.
    url = patch_call.call_args.args[0]
    assert "a@b.com" in url and "idProperty=email" in url


def test_update_only_skips_when_no_match() -> None:
    with (
        patch("httpx.Client.post") as post,
        patch("httpx.Client.patch", return_value=_response(404)) as patch_call,
    ):
        result = HubSpotDestination().load(
            [_RECORD], _config(), _options("update_only")
        )

    assert (result.success, result.skipped, result.failed) == (0, 1, 0)
    post.assert_not_called()
    patch_call.assert_called_once()


def test_mixed_batch_update_only_counts_hits_and_skips() -> None:
    # First record matches (200), second doesn't (404).
    with (
        patch("httpx.Client.post") as post,
        patch(
            "httpx.Client.patch",
            side_effect=[_response(200), _response(404)],
        ) as patch_call,
    ):
        result = HubSpotDestination().load(
            [_RECORD, {"email": "missing@x.com"}],
            _config(),
            _options("update_only"),
        )

    assert (result.success, result.skipped, result.failed) == (1, 1, 0)
    assert patch_call.call_count == 2
    post.assert_not_called()
