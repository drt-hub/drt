"""Replay safety for Zendesk create-or-update writes (ADR 0023).

Zendesk matches an existing user on id / external_id / email and an organization on
id / external_id, so a replay of an *identified* row updates the same record. A row
with none of those creates a new record per attempt and is treated as a blind create.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
import pytest

from drt.config.models import RateLimitConfig, RetryConfig, SyncOptions, ZendeskDestinationConfig
from drt.destinations.zendesk import ZendeskDestination
from tests.http_fault_injection import BlindCreateFaultHarness, FaultMode, install_mock_transport

_USERS_PATH = "/api/v2/users/create_or_update_many.json"
_ORGS_PATH = "/api/v2/organizations/create_or_update.json"


def _config(**overrides: object) -> ZendeskDestinationConfig:
    data: dict[str, object] = {
        "type": "zendesk",
        "subdomain": "demo",
        "email": "bot@example.com",
        "api_token": "token-123",
    }
    data.update(overrides)
    return ZendeskDestinationConfig(**data)


def _options(on_error: str = "skip") -> SyncOptions:
    return SyncOptions(
        on_error=on_error,
        retry=RetryConfig(max_attempts=2, initial_backoff=0.0, backoff_multiplier=1.0),
        rate_limit=RateLimitConfig(requests_per_second=0),
    )


def _unexpected(request: httpx.Request, _harness: BlindCreateFaultHarness) -> httpx.Response:
    raise AssertionError(f"unexpected request: {request.method} {request.url}")


def _ok(request: httpx.Request, object_id: int) -> httpx.Response:
    return httpx.Response(200, json={"job_status": {"id": str(object_id)}}, request=request)


def _user_key(payload: dict[str, Any]) -> Any:
    user = payload["users"][0] if "users" in payload else payload
    return user.get("email") or user.get("external_id")


def _org_key(payload: dict[str, Any]) -> Any:
    organization = payload.get("organization", payload)
    return organization.get("external_id")


def _harness(path: str, mode: FaultMode, *, keyed: Any = None) -> BlindCreateFaultHarness:
    return BlindCreateFaultHarness(
        path,
        mode=mode,
        fallback=_unexpected,
        create_response=_ok,
        replay_mode="keyed" if keyed else "blind-default",
        dedup_key=keyed,
    )


@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
@pytest.mark.parametrize("identity", [{"email": "ada@example.com"}, {"external_id": "ext-1"}])
def test_identified_user_replay_updates_the_same_record(
    monkeypatch: pytest.MonkeyPatch, mode: FaultMode, identity: dict[str, str]
) -> None:
    harness = _harness(_USERS_PATH, mode, keyed=_user_key)
    install_mock_transport(monkeypatch, harness)

    result = ZendeskDestination().load([{"name": "Ada", **identity}], _config(), _options())

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 1


@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
@pytest.mark.parametrize("on_error", ["skip", "fail"])
def test_user_without_identity_fails_closed_after_one_delivery(
    monkeypatch: pytest.MonkeyPatch, mode: FaultMode, on_error: str
) -> None:
    harness = _harness(_USERS_PATH, mode)
    install_mock_transport(monkeypatch, harness)

    result = ZendeskDestination().load([{"name": "Ada"}], _config(), _options(on_error))

    assert (result.success, result.failed) == (0, 1)
    assert harness.create_attempts == 1
    assert len(harness.remote_objects) == 1
    message = result.row_errors[0].error_message
    assert "delivery is unknown" in message
    assert "destination.retry_creates: true" in message


def test_one_unidentified_row_makes_the_whole_chunk_blind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _harness(_USERS_PATH, "commit-then-drop")
    install_mock_transport(monkeypatch, harness)
    records = [{"name": "Ada", "email": "ada@example.com"}, {"name": "Grace"}]

    result = ZendeskDestination().load(records, _config(), _options())

    assert (result.success, result.failed) == (0, 2)
    assert harness.create_attempts == 1
    assert [error.batch_index for error in result.row_errors] == [0, 1]


def test_retry_creates_true_restores_the_replay_with_a_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    harness = _harness(_USERS_PATH, "commit-then-drop")
    install_mock_transport(monkeypatch, harness)
    caplog.set_level(logging.WARNING)

    result = ZendeskDestination().load([{"name": "Ada"}], _config(retry_creates=True), _options())

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 2
    assert "retry_creates=true" in caplog.text


def test_429_is_retried_for_an_unidentified_user(monkeypatch: pytest.MonkeyPatch) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, request=request)
        return _ok(request, 1)

    install_mock_transport(monkeypatch, handler)
    result = ZendeskDestination().load([{"name": "Ada"}], _config(), _options())

    assert (result.success, result.failed) == (1, 0)
    assert len(requests) == 2


@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
def test_identified_organization_replay_updates_the_same_record(
    monkeypatch: pytest.MonkeyPatch, mode: FaultMode
) -> None:
    harness = _harness(_ORGS_PATH, mode, keyed=_org_key)
    install_mock_transport(monkeypatch, harness)

    result = ZendeskDestination().load(
        [{"name": "Acme", "external_id": "org-1"}],
        _config(object="organization"),
        _options(),
    )

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 1


def test_organization_identified_only_by_name_is_blind(monkeypatch: pytest.MonkeyPatch) -> None:
    harness = _harness(_ORGS_PATH, "commit-then-drop")
    install_mock_transport(monkeypatch, harness)

    result = ZendeskDestination().load(
        [{"name": "Acme"}], _config(object="organization"), _options()
    )

    assert (result.success, result.failed) == (0, 1)
    assert harness.create_attempts == 1
    assert "delivery is unknown" in result.row_errors[0].error_message
