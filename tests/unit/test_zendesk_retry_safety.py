"""Replay safety for Zendesk create-or-update writes (ADR 0023)."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import httpx
import pytest

import drt.cli._helpers as cli_helpers
from drt.cli.commands.retry import replay_dead_letters
from drt.config.models import (
    RateLimitConfig,
    RetryConfig,
    SyncConfig,
    SyncOptions,
    ZendeskDestinationConfig,
)
from drt.destinations.base import SyncResult
from drt.destinations.zendesk import ZendeskDestination
from drt.state.dlq import DeadLetter, DlqStore
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
    return organization.get("external_id") or organization.get("id")


def _harness(path: str, mode: FaultMode, *, keyed: Any = None) -> BlindCreateFaultHarness:
    return BlindCreateFaultHarness(
        path,
        mode=mode,
        fallback=_unexpected,
        create_response=_ok,
        replay_mode="keyed" if keyed else "blind-default",
        dedup_key=keyed,
    )


def _retry_sync(name: str, destination: ZendeskDestinationConfig, *, batch_size: int) -> SyncConfig:
    return SyncConfig(
        name=name,
        model="ref('zendesk_records')",
        destination=destination,
        sync=SyncOptions(
            batch_size=batch_size,
            on_error="fail",
            retry=RetryConfig(
                max_attempts=2,
                initial_backoff=0.0,
                backoff_multiplier=1.0,
            ),
            rate_limit=RateLimitConfig(requests_per_second=0),
        ),
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
@pytest.mark.parametrize(
    ("record", "config_overrides"),
    [
        ({"name": "Ada", "id": 123}, {}),
        ({"name": "Ada", "warehouse_id": 123}, {"id_field": "warehouse_id"}),
        ({"name": "Ada", "email": 0}, {}),
        ({"name": "Ada", "external_id": False}, {}),
        ({"name": "Ada", "email": "   "}, {}),
        ({"name": "Ada", "external_id": None}, {}),
        ({"name": "Ada", "email": {"nested": "value"}}, {}),
        ({"name": "Ada", "external_id": ["nested"]}, {}),
    ],
    ids=[
        "id-only",
        "custom-id-field",
        "zero",
        "false",
        "whitespace",
        "null",
        "nested-object",
        "nested-list",
    ],
)
def test_user_replay_is_blind_without_a_documented_string_identity(
    monkeypatch: pytest.MonkeyPatch,
    mode: FaultMode,
    record: dict[str, Any],
    config_overrides: dict[str, Any],
) -> None:
    harness = _harness(_USERS_PATH, mode)
    install_mock_transport(monkeypatch, harness)

    result = ZendeskDestination().load([record], _config(**config_overrides), _options())

    assert (result.success, result.failed) == (0, 1)
    assert harness.create_attempts == 1
    assert len(harness.remote_objects) == 1
    assert "delivery is unknown" in result.row_errors[0].error_message


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


@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
@pytest.mark.parametrize("organization_id", [123, "123"], ids=["integer-id", "string-id"])
def test_organization_id_replay_updates_the_same_record(
    monkeypatch: pytest.MonkeyPatch, mode: FaultMode, organization_id: int | str
) -> None:
    harness = _harness(_ORGS_PATH, mode, keyed=_org_key)
    install_mock_transport(monkeypatch, harness)

    result = ZendeskDestination().load(
        [{"name": "Acme", "id": organization_id}],
        _config(object="organization"),
        _options(),
    )

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 1


@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
def test_organization_custom_id_field_replay_updates_the_same_record(
    monkeypatch: pytest.MonkeyPatch, mode: FaultMode
) -> None:
    harness = _harness(_ORGS_PATH, mode, keyed=_org_key)
    install_mock_transport(monkeypatch, harness)

    result = ZendeskDestination().load(
        [{"name": "Acme", "warehouse_id": 123}],
        _config(object="organization", id_field="warehouse_id"),
        _options(),
    )

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 1


@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
@pytest.mark.parametrize(
    ("record", "config_overrides"),
    [
        ({"name": "Acme"}, {}),
        ({"name": "Acme", "id": 0}, {}),
        ({"name": "Acme", "id": False}, {}),
        ({"name": "Acme", "external_id": "   "}, {}),
        ({"name": "Acme", "external_id": None}, {}),
        ({"name": "Acme", "external_id": {"nested": "value"}}, {}),
        ({"name": "Acme", "external_id": ["nested"]}, {}),
        (
            {"name": "Acme", "warehouse_id": {"nested": "value"}},
            {"id_field": "warehouse_id"},
        ),
    ],
    ids=[
        "name-only",
        "zero",
        "false",
        "whitespace",
        "null",
        "nested-object",
        "nested-list",
        "invalid-custom-id-field",
    ],
)
def test_organization_replay_is_blind_without_a_documented_valid_identity(
    monkeypatch: pytest.MonkeyPatch,
    mode: FaultMode,
    record: dict[str, Any],
    config_overrides: dict[str, Any],
) -> None:
    harness = _harness(_ORGS_PATH, mode)
    install_mock_transport(monkeypatch, harness)

    result = ZendeskDestination().load(
        [record], _config(object="organization", **config_overrides), _options()
    )

    assert (result.success, result.failed) == (0, 1)
    assert harness.create_attempts == 1
    assert "delivery is unknown" in result.row_errors[0].error_message


def test_retry_removes_successful_organization_before_later_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    records = [
        {"name": "Org 1", "external_id": "org-1"},
        {"name": "Org 2", "external_id": "org-2"},
        {"name": "Org 3", "external_id": "org-3"},
    ]
    store = DlqStore(tmp_path)
    store.append(
        "zendesk_orgs",
        [DeadLetter(record=record, error_message="old failure") for record in records],
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return _ok(request, 1)
        return httpx.Response(400, text="bad organization", request=request)

    install_mock_transport(monkeypatch, handler)
    destination = ZendeskDestination()
    monkeypatch.setattr(cli_helpers, "get_destination", lambda _sync: destination)

    summary = replay_dead_letters(
        _retry_sync("zendesk_orgs", _config(object="organization"), batch_size=3),
        project_dir=tmp_path,
    )

    assert len(requests) == 2
    assert (summary["succeeded"], summary["still_failing"]) == (1, 2)
    remaining = store.read("zendesk_orgs")
    assert [entry.record["external_id"] for entry in remaining] == ["org-2", "org-3"]
    assert remaining[0].error_message == "bad organization"
    assert remaining[1].error_message == "not attempted: the destination stopped after a failure"


def test_retry_removes_successful_user_chunk_before_later_chunk_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    records = [
        {"name": f"User {index}", "email": f"user-{index}@example.com"} for index in range(250)
    ]
    store = DlqStore(tmp_path)
    store.append(
        "zendesk_users",
        [DeadLetter(record=record, error_message="old failure") for record in records],
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return _ok(request, 1)
        return httpx.Response(400, text="bad user chunk", request=request)

    install_mock_transport(monkeypatch, handler)
    destination = ZendeskDestination()
    monkeypatch.setattr(cli_helpers, "get_destination", lambda _sync: destination)

    summary = replay_dead_letters(
        _retry_sync("zendesk_users", _config(), batch_size=250),
        project_dir=tmp_path,
    )

    assert len(requests) == 2
    assert (summary["succeeded"], summary["still_failing"]) == (100, 150)
    remaining = store.read("zendesk_users")
    assert [entry.record["email"] for entry in remaining] == [
        f"user-{index}@example.com" for index in range(100, 250)
    ]
    assert all(entry.error_message == "bad user chunk" for entry in remaining[:100])
    assert all("not attempted" in entry.error_message for entry in remaining[100:])


def test_failed_first_user_chunk_keeps_unattempted_dlq_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    records = [{"name": f"User {index}"} for index in range(250)]
    store = DlqStore(tmp_path)
    store.append(
        "zendesk_users",
        [DeadLetter(record=record, error_message="old failure") for record in records],
    )
    sync = _retry_sync("zendesk_users", _config(), batch_size=250)
    harness = _harness(_USERS_PATH, "commit-then-drop")
    install_mock_transport(monkeypatch, harness)
    destination = ZendeskDestination()
    captured: list[SyncResult] = []
    real_load = destination.load

    def capture_result(
        retry_records: list[dict[str, Any]],
        config: ZendeskDestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        result = real_load(retry_records, config, sync_options)
        captured.append(result)
        return result

    monkeypatch.setattr(destination, "load", capture_result)
    monkeypatch.setattr(cli_helpers, "get_destination", lambda _sync: destination)

    summary = replay_dead_letters(sync, project_dir=tmp_path)

    assert (captured[0].failed, captured[0].skipped) == (100, 150)
    assert captured[0].unattempted == list(range(100, 250))
    assert harness.create_attempts == 1
    assert summary["still_failing"] == 250
    remaining = store.read("zendesk_users")
    assert len(remaining) == 250
    assert remaining[-1].record == records[-1]
    assert remaining[-1].error_message == "not attempted: the destination stopped after a failure"
