"""Fault-injection coverage for ADR 0023 rollout item 1 message writes."""

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any
from urllib.parse import parse_qs

import httpx
import pytest

from drt.config.models import (
    RateLimitConfig,
    RestApiDestinationConfig,
    RetryConfig,
    SyncConfig,
    SyncOptions,
)
from drt.destinations.discord import DiscordDestination
from drt.destinations.github_actions import GitHubActionsDestination
from drt.destinations.jira import JiraDestination
from drt.destinations.linear import LinearDestination
from drt.destinations.notion import NotionDestination
from drt.destinations.rest_api import RestApiDestination
from drt.destinations.sendgrid import SendGridDestination
from drt.destinations.slack import SlackDestination
from drt.destinations.teams import TeamsDestination
from drt.destinations.twilio import TwilioDestination
from tests.http_fault_injection import (
    BlindCreateFaultHarness,
    FaultMode,
    install_mock_transport,
)

_DESTINATIONS = [
    "rest_api",
    "sendgrid",
    "twilio",
    "slack",
    "discord",
    "teams",
    "notion",
    "linear",
    "jira",
    "github_actions",
]

_TEMPLATE_DESTINATIONS = ["notion", "linear", "jira", "github_actions"]


def _options(on_error: str = "skip") -> SyncOptions:
    return SyncOptions(
        on_error=on_error,
        retry=RetryConfig(max_attempts=2, initial_backoff=0.0, backoff_multiplier=1.0),
        rate_limit=RateLimitConfig(requests_per_second=0),
    )


def _decode_form(request: httpx.Request) -> dict[str, Any]:
    return {key: values[0] for key, values in parse_qs(request.content.decode()).items()}


def _unexpected(request: httpx.Request, _harness: BlindCreateFaultHarness) -> httpx.Response:
    raise AssertionError(f"unexpected request: {request.method} {request.url}")


def _case(
    name: str,
    *,
    retry_creates: bool = False,
) -> tuple[
    Any,
    Any,
    dict[str, Any],
    str,
    Callable[[httpx.Request], dict[str, Any]],
]:
    if name == "rest_api":
        return (
            RestApiDestination(),
            {
                "type": "rest_api",
                "url": "https://fault.test/webhook",
                "retry_creates": retry_creates,
            },
            {"event_id": "evt-1", "message": "hello"},
            "/webhook",
            lambda request: json.loads(request.content),
        )
    if name == "sendgrid":
        return (
            SendGridDestination(),
            {
                "type": "sendgrid",
                "from_email": "noreply@example.com",
                "subject_template": "Hello {{ row.name }}",
                "body_template": "Body for {{ row.name }}",
                "auth": {"type": "bearer", "token": "test-key"},
                "retry_creates": retry_creates,
            },
            {"name": "Ada", "email": "ada@example.com"},
            "/v3/mail/send",
            lambda request: json.loads(request.content),
        )
    if name == "twilio":
        return (
            TwilioDestination(),
            {
                "type": "twilio",
                "account_sid": "AC123",
                "auth_token": "token",
                "from_number": "+15551234567",
                "to_template": "{{ row.phone }}",
                "message_template": "Hello {{ row.name }}",
                "retry_creates": retry_creates,
            },
            {"name": "Ada", "phone": "+15557654321"},
            "/2010-04-01/Accounts/AC123/Messages.json",
            _decode_form,
        )
    if name == "slack":
        return (
            SlackDestination(),
            {
                "type": "slack",
                "webhook_url": "https://fault.test/slack",
                "message_template": "Hello {{ row.name }}",
                "retry_creates": retry_creates,
            },
            {"name": "Ada"},
            "/slack",
            lambda request: json.loads(request.content),
        )
    if name == "discord":
        return (
            DiscordDestination(),
            {
                "type": "discord",
                "webhook_url": "https://fault.test/discord",
                "message_template": "Hello {{ row.name }}",
                "retry_creates": retry_creates,
            },
            {"name": "Ada"},
            "/discord",
            lambda request: json.loads(request.content),
        )
    if name == "teams":
        return (
            TeamsDestination(),
            {
                "type": "teams",
                "webhook_url": "https://fault.test/teams",
                "message_template": "Hello {{ row.name }}",
                "retry_creates": retry_creates,
            },
            {"name": "Ada"},
            "/teams",
            lambda request: json.loads(request.content),
        )
    if name == "notion":
        return (
            NotionDestination(),
            {
                "type": "notion",
                "database_id": "db-1",
                "auth": {"type": "bearer", "token": "test-token"},
                "retry_creates": retry_creates,
            },
            {"name": "Ada"},
            "/v1/pages",
            lambda request: json.loads(request.content),
        )
    if name == "linear":
        return (
            LinearDestination(),
            {
                "type": "linear",
                "team_id": "team-1",
                "title_template": "Issue for {{ row.name }}",
                "description_template": "Body for {{ row.name }}",
                "auth": {"type": "bearer", "token": "test-token"},
                "retry_creates": retry_creates,
            },
            {"name": "Ada"},
            "/graphql",
            lambda request: json.loads(request.content),
        )
    if name == "jira":
        return (
            JiraDestination(),
            {
                "type": "jira",
                "base_url_env": "FAULT_JIRA_BASE_URL",
                "email_env": "FAULT_JIRA_EMAIL",
                "token_env": "FAULT_JIRA_TOKEN",
                "project_key": "OPS",
                "summary_template": "Issue for {{ row.name }}",
                "description_template": "Body for {{ row.name }}",
                "retry_creates": retry_creates,
            },
            {"name": "Ada"},
            "/rest/api/3/issue",
            lambda request: json.loads(request.content),
        )
    if name == "github_actions":
        return (
            GitHubActionsDestination(),
            {
                "type": "github_actions",
                "owner": "acme",
                "repo": "app",
                "workflow_id": "deploy.yml",
                "auth": {"type": "bearer", "token": "test-token"},
                "retry_creates": retry_creates,
            },
            {"name": "Ada"},
            "/repos/acme/app/actions/workflows/deploy.yml/dispatches",
            lambda request: json.loads(request.content),
        )
    raise AssertionError(f"unknown test destination: {name}")


def _load(name: str, retry_creates: bool, on_error: str = "skip") -> tuple[Any, Any, Any]:
    destination, config_data, record, path, decoder = _case(name, retry_creates=retry_creates)
    parsed = SyncConfig.model_validate(
        {
            "name": "retry_safety",
            "model": "events",
            "destination": config_data,
        }
    )
    return destination, parsed.destination, (record, path, decoder, _options(on_error))


def _success_response(request: httpx.Request, object_id: int) -> httpx.Response:
    # One body that every destination in this matrix accepts as a success.
    body = {"sid": f"SM{object_id}", "data": {"issueCreate": {"success": True}}}
    return httpx.Response(201, json=body, request=request)


def _template_error_config(name: str) -> dict[str, Any]:
    _, config, _, _, _ = _case(name)
    templates = {
        "notion": (
            "properties_template",
            '{"Name": {"title": [{"text": {"content": "{{ row.required }}"}}]}}',
        ),
        "linear": ("title_template", "Issue for {{ row.required }}"),
        "jira": ("summary_template", "Issue for {{ row.required }}"),
        "github_actions": (
            "inputs_template",
            '{"environment": "{{ row.required }}"}',
        ),
    }
    field, template = templates[name]
    config[field] = template
    return config


@pytest.fixture(autouse=True)
def _jira_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FAULT_JIRA_BASE_URL", "https://fault.test")
    monkeypatch.setenv("FAULT_JIRA_EMAIL", "ops@example.com")
    monkeypatch.setenv("FAULT_JIRA_TOKEN", "token")


@pytest.mark.parametrize("name", _DESTINATIONS)
def test_retry_creates_is_explicit_destination_policy(name: str) -> None:
    _, default_config, _ = _load(name, False)
    _, opted_in_config, _ = _load(name, True)

    assert default_config.retry_creates is False
    assert opted_in_config.retry_creates is True


@pytest.mark.parametrize("name", _TEMPLATE_DESTINATIONS)
@pytest.mark.parametrize(
    ("on_error", "expected_success", "expected_requests"),
    [("fail", 0, 0), ("skip", 1, 1)],
)
def test_pre_send_template_error_honors_on_error_before_next_record(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    on_error: str,
    expected_success: int,
    expected_requests: int,
) -> None:
    destination, _, _, _, _ = _case(name)
    parsed = SyncConfig.model_validate(
        {
            "name": "template_error_safety",
            "model": "events",
            "destination": _template_error_config(name),
        }
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return _success_response(request, 1)

    install_mock_transport(monkeypatch, handler)
    result = destination.load(
        [{"name": "missing required"}, {"name": "valid", "required": "production"}],
        parsed.destination,
        _options(on_error),
    )

    assert (result.success, result.failed) == (expected_success, 1)
    assert len(requests) == expected_requests


@pytest.mark.parametrize("name", _DESTINATIONS)
@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
@pytest.mark.parametrize("on_error", ["skip", "fail"])
def test_ambiguous_blind_send_fails_closed_after_one_delivery(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    mode: FaultMode,
    on_error: str,
) -> None:
    destination, config, (record, path, decoder, options) = _load(name, False, on_error)
    harness = BlindCreateFaultHarness(
        path,
        mode=mode,
        fallback=_unexpected,
        create_response=_success_response,
        decode_request=decoder,
    )
    install_mock_transport(monkeypatch, harness)

    result = destination.load([record], config, options)

    assert (result.success, result.failed) == (0, 1)
    assert harness.create_attempts == 1
    assert len(harness.remote_objects) == 1
    assert result.row_errors[0].http_status == (503 if mode == "commit-then-5xx" else None)
    assert "delivery is unknown" in result.row_errors[0].error_message
    assert "Assume it may have been delivered" in result.row_errors[0].error_message
    assert "do not auto-resend" in result.row_errors[0].error_message
    assert "destination.retry_creates: true" in result.row_errors[0].error_message


@pytest.mark.parametrize("name", _DESTINATIONS)
def test_retry_creates_true_restores_second_send_with_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    name: str,
) -> None:
    destination, config, (record, path, decoder, options) = _load(name, True)
    harness = BlindCreateFaultHarness(
        path,
        mode="commit-then-drop",
        fallback=_unexpected,
        create_response=_success_response,
        decode_request=decoder,
    )
    install_mock_transport(monkeypatch, harness)
    caplog.set_level(logging.WARNING)

    result = destination.load([record], config, options)

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 2
    assert "retry_creates=true" in caplog.text
    assert "duplicate-message or side-effect risk" in caplog.text


@pytest.mark.parametrize("name", _DESTINATIONS)
def test_429_is_retried_without_retry_creates(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
) -> None:
    destination, config, (record, _path, _decoder, options) = _load(name, False)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, request=request)
        return _success_response(request, 1)

    install_mock_transport(monkeypatch, handler)
    result = destination.load([record], config, options)

    assert (result.success, result.failed) == (1, 0)
    assert len(requests) == 2


@pytest.mark.parametrize("name", _DESTINATIONS)
@pytest.mark.parametrize("error_type", [httpx.ConnectError, httpx.ProxyError])
def test_proven_pre_send_error_is_retried_without_retry_creates(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    error_type: type[httpx.TransportError],
) -> None:
    destination, config, (record, _path, _decoder, options) = _load(name, False)
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            raise error_type("connection failed before send", request=request)
        return _success_response(request, 1)

    install_mock_transport(monkeypatch, handler)
    result = destination.load([record], config, options)

    assert (result.success, result.failed) == (1, 0)
    assert len(requests) == 2


def test_rest_record_native_key_keeps_safe_retry(monkeypatch: pytest.MonkeyPatch) -> None:
    destination, config, (record, path, decoder, options) = _load("rest_api", False)
    config.native_idempotency_key = "{{ row.event_id }}"
    harness = BlindCreateFaultHarness(
        path,
        mode="commit-then-drop",
        fallback=_unexpected,
        create_response=_success_response,
        replay_mode="keyed",
        dedup_key=lambda payload: payload["event_id"],
        decode_request=decoder,
    )
    install_mock_transport(monkeypatch, harness)

    result = destination.load([record], config, options)

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 1
    assert [request.headers["Idempotency-Key"] for request in harness.requests] == [
        "evt-1",
        "evt-1",
    ]


def test_rest_empty_native_key_remains_blind(monkeypatch: pytest.MonkeyPatch) -> None:
    destination, config, (record, path, decoder, options) = _load("rest_api", False)
    config.native_idempotency_key = "{{ row.empty }}"
    record["empty"] = ""
    harness = BlindCreateFaultHarness(
        path,
        mode="commit-then-drop",
        fallback=_unexpected,
        create_response=_success_response,
        decode_request=decoder,
    )
    install_mock_transport(monkeypatch, harness)

    result = destination.load([record], config, options)

    assert (result.success, result.failed) == (0, 1)
    assert harness.create_attempts == 1
    assert len(harness.remote_objects) == 1


def test_rest_batch_generated_key_remains_blind(monkeypatch: pytest.MonkeyPatch) -> None:
    config = RestApiDestinationConfig(
        type="rest_api",
        url="https://fault.test/batch",
        body_mode="batch",
        batch_template='{"records": {{ rows | tojson_safe }}}',
        native_idempotency_key="{{ sync_name }}:{{ request_id }}",
    )
    harness = BlindCreateFaultHarness(
        "/batch",
        mode="commit-then-drop",
        fallback=_unexpected,
        create_response=_success_response,
    )
    install_mock_transport(monkeypatch, harness)

    result = RestApiDestination().load([{"event_id": "evt-1"}], config, _options())

    assert (result.success, result.failed) == (0, 1)
    assert harness.create_attempts == 1
    assert len(harness.remote_objects) == 1
    assert "delivery is unknown" in result.row_errors[0].error_message


def test_rest_batch_retry_creates_restores_second_send_with_warning(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    config = RestApiDestinationConfig(
        type="rest_api",
        url="https://fault.test/batch",
        body_mode="batch",
        batch_template='{"records": {{ rows | tojson_safe }}}',
        retry_creates=True,
    )
    harness = BlindCreateFaultHarness(
        "/batch",
        mode="commit-then-drop",
        fallback=_unexpected,
        create_response=_success_response,
    )
    install_mock_transport(monkeypatch, harness)
    caplog.set_level(logging.WARNING)

    result = RestApiDestination().load([{"event_id": "evt-1"}], config, _options())

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 2
    assert "retry_creates=true" in caplog.text


@pytest.mark.parametrize("failure", ["429", "connect"])
def test_rest_batch_safe_pre_send_failure_is_retried(
    monkeypatch: pytest.MonkeyPatch,
    failure: str,
) -> None:
    config = RestApiDestinationConfig(
        type="rest_api",
        url="https://fault.test/batch",
        body_mode="batch",
        batch_template='{"records": {{ rows | tojson_safe }}}',
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            if failure == "connect":
                raise httpx.ConnectError("connection refused before send", request=request)
            return httpx.Response(429, headers={"Retry-After": "0"}, request=request)
        return _success_response(request, 1)

    install_mock_transport(monkeypatch, handler)
    result = RestApiDestination().load([{"event_id": "evt-1"}], config, _options())

    assert (result.success, result.failed) == (1, 0)
    assert len(requests) == 2
