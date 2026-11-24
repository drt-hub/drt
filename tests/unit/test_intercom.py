"""Unit tests for the Intercom destination, including match_policy (#757).

HTTP calls are mocked; these tests never contact Intercom.
"""

from __future__ import annotations

import json
import logging
from typing import Any
from unittest.mock import patch

import httpx
import pytest

from drt.config.models import (
    BearerAuth,
    IntercomDestinationConfig,
    RateLimitConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.base import MatchPolicyCapable
from drt.destinations.intercom import IntercomDestination
from tests.http_fault_injection import (
    BlindCreateFaultHarness,
    FaultMode,
    install_mock_transport,
)


def _config(**overrides: Any) -> IntercomDestinationConfig:
    data: dict[str, Any] = {
        "type": "intercom",
        "auth": BearerAuth(type="bearer", token="secret"),
        "properties_template": """
        {
            "email": "{{ row.email }}",
            "name": "{{ row.name }}",
            "custom_attributes": {"plan": "{{ row.plan }}"}
        }
        """,
    }
    data.update(overrides)
    return IntercomDestinationConfig(**data)


def _options(match_policy: str = "upsert", **overrides: Any) -> SyncOptions:
    data: dict[str, Any] = {
        "match_policy": match_policy,
        "rate_limit": RateLimitConfig(requests_per_second=0),
        "retry": RetryConfig(max_attempts=1, initial_backoff=0),
        "on_error": "skip",
    }
    data.update(overrides)
    return SyncOptions(**data)


def _response(
    status_code: int = 200,
    *,
    json: dict[str, Any] | None = None,
    method: str = "POST",
    url: str = IntercomDestination.BASE_URL,
) -> httpx.Response:
    request = httpx.Request(method, url)
    if json is None:
        return httpx.Response(status_code, text="ok", request=request)
    return httpx.Response(status_code, json=json, request=request)


def _conflict(contact_id: str = "contact-123") -> httpx.Response:
    return _response(
        409,
        json={
            "type": "error.list",
            "errors": [
                {
                    "code": "conflict",
                    "message": (
                        f"A contact matching those details already exists with id={contact_id}"
                    ),
                }
            ],
        },
    )


_RECORD = {"email": "a@test.com", "name": "Alice", "plan": "pro"}


def _fault_search_response(
    request: httpx.Request, harness: BlindCreateFaultHarness
) -> httpx.Response:
    if request.method == "POST" and request.url.path == "/contacts/search":
        body = json.loads(request.content)
        raw_filters = body["query"].get("value", [body["query"]])
        filters = raw_filters if isinstance(raw_filters, list) else [body["query"]]
        matching_ids = [
            str(contact["id"])
            for contact in harness.remote_objects
            if any(contact.get(item["field"]) == item["value"] for item in filters)
        ]
        return httpx.Response(
            200,
            json={"type": "list", "data": [{"id": value} for value in matching_ids]},
            request=request,
        )
    if request.method == "PUT" and request.url.path.startswith("/contacts/"):
        return httpx.Response(
            200, json={"id": request.url.path.rsplit("/", 1)[-1]}, request=request
        )
    raise AssertionError(f"unexpected request: {request.method} {request.url}")


def _fault_harness(mode: FaultMode) -> BlindCreateFaultHarness:
    return BlindCreateFaultHarness(
        "/contacts",
        mode=mode,
        fallback=_fault_search_response,
        create_response=lambda request, object_id: httpx.Response(
            200,
            json={"id": str(object_id)},
            request=request,
        ),
    )


def test_intercom_declares_match_policy_capability() -> None:
    destination = IntercomDestination()

    assert isinstance(destination, MatchPolicyCapable)
    assert destination.supported_match_policies() == frozenset(
        {"upsert", "update_only", "create_only"}
    )


def test_retry_creates_defaults_false_and_is_explicit_destination_policy() -> None:
    assert _config().retry_creates is False
    assert _config(retry_creates=True).retry_creates is True


def test_upsert_creates_a_new_contact() -> None:
    with (
        patch("httpx.Client.post", return_value=_response()) as post,
        patch("httpx.Client.put") as put,
    ):
        result = IntercomDestination().load([_RECORD], _config(), _options())

    assert (result.success, result.skipped, result.failed) == (1, 0, 0)
    post.assert_called_once()
    put.assert_not_called()


def test_upsert_updates_the_contact_id_from_a_duplicate_response() -> None:
    with (
        patch("httpx.Client.post", return_value=_conflict("existing-7")) as post,
        patch("httpx.Client.put", return_value=_response(method="PUT")) as put,
    ):
        result = IntercomDestination().load([_RECORD], _config(), _options())

    assert (result.success, result.skipped, result.failed) == (1, 0, 0)
    post.assert_called_once()
    assert put.call_args.args[0] == f"{IntercomDestination.BASE_URL}/existing-7"
    assert put.call_args.kwargs["json"]["email"] == "a@test.com"


def test_upsert_finds_the_contact_id_in_a_later_conflict_error() -> None:
    response = _response(
        409,
        json={
            "errors": [
                {"code": "conflict", "message": None},
                {
                    "code": "conflict",
                    "message": "A contact matching those details already exists with id=existing-8",
                },
            ]
        },
    )

    with (
        patch("httpx.Client.post", return_value=response),
        patch("httpx.Client.put", return_value=_response(method="PUT")) as put,
    ):
        result = IntercomDestination().load([_RECORD], _config(), _options())

    assert (result.success, result.skipped, result.failed) == (1, 0, 0)
    assert put.call_args.args[0] == f"{IntercomDestination.BASE_URL}/existing-8"


def test_upsert_does_not_treat_an_unrelated_409_as_a_duplicate() -> None:
    response = _response(
        409,
        json={"errors": [{"code": "another_conflict", "message": "not a duplicate"}]},
    )

    with (
        patch("httpx.Client.post", return_value=response),
        patch("httpx.Client.put") as put,
        pytest.raises(httpx.HTTPStatusError),
    ):
        IntercomDestination().load(
            [_RECORD],
            _config(),
            _options(on_error="fail"),
        )

    put.assert_not_called()


def test_upsert_does_not_treat_malformed_409_json_as_a_duplicate() -> None:
    response = httpx.Response(
        409,
        text="{",
        request=httpx.Request("POST", IntercomDestination.BASE_URL),
    )

    with (
        patch("httpx.Client.post", return_value=response),
        patch("httpx.Client.put") as put,
        pytest.raises(httpx.HTTPStatusError),
    ):
        IntercomDestination().load(
            [_RECORD],
            _config(),
            _options(on_error="fail"),
        )

    put.assert_not_called()


def test_upsert_rejects_a_duplicate_response_without_a_contact_id() -> None:
    response = _response(
        409,
        json={"errors": [{"code": "conflict", "message": "contact already exists"}]},
    )

    with (
        patch("httpx.Client.post", return_value=response),
        patch("httpx.Client.put") as put,
        pytest.raises(ValueError, match="missing existing contact id"),
    ):
        IntercomDestination().load(
            [_RECORD],
            _config(),
            _options(on_error="fail"),
        )

    put.assert_not_called()


def test_create_only_creates_a_new_contact() -> None:
    with (
        patch("httpx.Client.post", return_value=_response()) as post,
        patch("httpx.Client.put") as put,
    ):
        result = IntercomDestination().load(
            [_RECORD],
            _config(),
            _options("create_only"),
        )

    assert (result.success, result.skipped, result.failed) == (1, 0, 0)
    post.assert_called_once()
    put.assert_not_called()


def test_create_only_skips_an_existing_contact_without_updating() -> None:
    with (
        patch("httpx.Client.post", return_value=_conflict()) as post,
        patch("httpx.Client.put") as put,
    ):
        result = IntercomDestination().load(
            [_RECORD],
            _config(),
            _options("create_only"),
        )

    assert (result.success, result.skipped, result.failed) == (0, 1, 0)
    assert result.skipped_no_match == 1
    post.assert_called_once()
    put.assert_not_called()


@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
@pytest.mark.parametrize("match_policy", ["upsert", "create_only"])
def test_create_commit_then_failure_reconciles_without_duplicate(
    monkeypatch: pytest.MonkeyPatch, mode: FaultMode, match_policy: str
) -> None:
    harness = _fault_harness(mode)
    install_mock_transport(monkeypatch, harness)

    result = IntercomDestination().load(
        [_RECORD],
        _config(),
        _options(
            match_policy,
            retry=RetryConfig(max_attempts=2, initial_backoff=0.0),
        ),
    )

    assert (result.success, result.skipped, result.failed) == (1, 0, 0)
    assert harness.create_attempts == 1
    assert len(harness.remote_objects) == 1
    expected_requests = [("POST", "/contacts"), ("POST", "/contacts/search")]
    if match_policy == "upsert":
        expected_requests.append(("PUT", "/contacts/1"))
    assert [(request.method, request.url.path) for request in harness.requests] == expected_requests


def test_create_reconciliation_miss_fails_closed_with_one_remote_contact(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _fault_harness("commit-then-drop")

    def search_stays_stale(
        request: httpx.Request, _harness: BlindCreateFaultHarness
    ) -> httpx.Response:
        if request.url.path == "/contacts/search":
            return httpx.Response(200, json={"data": []}, request=request)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    harness.fallback = search_stays_stale
    install_mock_transport(monkeypatch, harness)

    result = IntercomDestination().load(
        [_RECORD],
        _config(),
        _options(retry=RetryConfig(max_attempts=3, initial_backoff=0.0)),
    )

    assert (result.success, result.skipped, result.failed) == (0, 0, 1)
    assert "delivery is unknown" in result.row_errors[0].error_message
    assert "refused a second POST" in result.row_errors[0].error_message
    assert harness.create_attempts == 1
    assert len(harness.remote_objects) == 1


def test_create_without_a_reconciliation_key_is_blind_and_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _fault_harness("commit-then-drop")
    install_mock_transport(monkeypatch, harness)
    config = _config(properties_template='{"role": "lead", "name": "{{ row.name }}"}')

    result = IntercomDestination().load(
        [_RECORD],
        config,
        _options(retry=RetryConfig(max_attempts=3, initial_backoff=0.0)),
    )

    assert (result.success, result.skipped, result.failed) == (0, 0, 1)
    assert "no external_id or email reconciliation key" in result.row_errors[0].error_message
    assert harness.create_attempts == 1
    assert len(harness.remote_objects) == 1
    assert [(request.method, request.url.path) for request in harness.requests] == [
        ("POST", "/contacts")
    ]


def test_multiple_matches_during_reconciliation_are_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = _fault_harness("commit-then-drop")

    def duplicate_after_commit(
        request: httpx.Request, state: BlindCreateFaultHarness
    ) -> httpx.Response:
        if request.url.path != "/contacts/search":
            raise AssertionError(f"unexpected request: {request.method} {request.url}")
        state.remote_objects.append({"id": 99, "email": "a@test.com"})
        return _fault_search_response(request, state)

    harness.fallback = duplicate_after_commit
    install_mock_transport(monkeypatch, harness)

    result = IntercomDestination().load(
        [_RECORD],
        _config(),
        _options(retry=RetryConfig(max_attempts=3, initial_backoff=0.0)),
    )

    assert (result.success, result.skipped, result.failed) == (0, 0, 1)
    assert "matched multiple contacts" in result.row_errors[0].error_message
    assert "['1', '99']" in result.row_errors[0].error_message
    assert harness.create_attempts == 1


def test_retry_creates_true_restores_blind_replay_with_warning(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    harness = _fault_harness("commit-then-drop")

    def search_stays_stale(
        request: httpx.Request, _harness: BlindCreateFaultHarness
    ) -> httpx.Response:
        if request.url.path == "/contacts/search":
            return httpx.Response(200, json={"data": []}, request=request)
        raise AssertionError(f"unexpected request: {request.method} {request.url}")

    harness.fallback = search_stays_stale
    install_mock_transport(monkeypatch, harness)
    caplog.set_level(logging.WARNING, logger="drt.destinations.intercom")

    result = IntercomDestination().load(
        [_RECORD],
        _config(retry_creates=True),
        _options(retry=RetryConfig(max_attempts=2, initial_backoff=0.0)),
    )

    assert (result.success, result.skipped, result.failed) == (1, 0, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 2
    assert "duplicate contact risk" in caplog.text


def test_429_on_create_is_retried_without_reconciliation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(429, headers={"Retry-After": "0"}, request=request)
        return httpx.Response(200, json={"id": "contact-1"}, request=request)

    install_mock_transport(monkeypatch, handler)
    result = IntercomDestination().load(
        [_RECORD],
        _config(),
        _options(retry=RetryConfig(max_attempts=2, initial_backoff=0.0)),
    )

    assert (result.success, result.failed) == (1, 0)
    assert [(request.method, request.url.path) for request in requests] == [
        ("POST", "/contacts"),
        ("POST", "/contacts"),
    ]


def test_pre_send_connect_error_on_create_is_retried(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            raise httpx.ConnectError("not connected", request=request)
        return httpx.Response(200, json={"id": "contact-1"}, request=request)

    install_mock_transport(monkeypatch, handler)
    result = IntercomDestination().load(
        [_RECORD],
        _config(),
        _options(retry=RetryConfig(max_attempts=2, initial_backoff=0.0)),
    )

    assert (result.success, result.failed) == (1, 0)
    assert len(requests) == 2


def test_update_only_searches_by_rendered_identifiers_then_updates() -> None:
    config = _config(
        properties_template="""
        {
            "external_id": "{{ row.external_id }}",
            "email": "{{ row.email }}",
            "name": "{{ row.name }}"
        }
        """
    )
    record = {"external_id": "warehouse-42", "email": "a@test.com", "name": "Alice"}

    with (
        patch(
            "httpx.Client.post",
            return_value=_response(
                json={"data": [{"id": "contact-42"}]},
                url=IntercomDestination.SEARCH_URL,
            ),
        ) as post,
        patch("httpx.Client.put", return_value=_response(method="PUT")) as put,
    ):
        result = IntercomDestination().load(
            [record],
            config,
            _options("update_only"),
        )

    assert (result.success, result.skipped, result.failed) == (1, 0, 0)
    assert post.call_args.args[0] == IntercomDestination.SEARCH_URL
    assert post.call_args.kwargs["json"] == {
        "query": {
            "operator": "OR",
            "value": [
                {"field": "external_id", "operator": "=", "value": "warehouse-42"},
                {"field": "email", "operator": "=", "value": "a@test.com"},
            ],
        },
        "pagination": {"per_page": 2},
    }
    assert put.call_args.args[0] == f"{IntercomDestination.BASE_URL}/contact-42"


def test_update_only_uses_a_rendered_intercom_id_without_searching() -> None:
    config = _config(properties_template='{"id": "{{ row.id }}", "name": "{{ row.name }}"}')

    with (
        patch("httpx.Client.post") as post,
        patch("httpx.Client.put", return_value=_response(method="PUT")) as put,
    ):
        result = IntercomDestination().load(
            [{"id": "contact-7", "name": "Alice"}],
            config,
            _options("update_only"),
        )

    assert (result.success, result.skipped, result.failed) == (1, 0, 0)
    post.assert_not_called()
    assert put.call_args.args[0] == f"{IntercomDestination.BASE_URL}/contact-7"
    assert put.call_args.kwargs["json"] == {"name": "Alice"}


def test_update_only_skips_when_search_finds_no_contact() -> None:
    with (
        patch(
            "httpx.Client.post",
            return_value=_response(json={"data": []}, url=IntercomDestination.SEARCH_URL),
        ) as post,
        patch("httpx.Client.put") as put,
    ):
        result = IntercomDestination().load(
            [_RECORD],
            _config(),
            _options("update_only"),
        )

    assert (result.success, result.skipped, result.failed) == (0, 1, 0)
    assert result.skipped_no_match == 1
    post.assert_called_once()
    put.assert_not_called()


def test_update_only_skips_when_a_direct_id_no_longer_exists() -> None:
    config = _config(properties_template='{"id": "{{ row.id }}", "name": "{{ row.name }}"}')

    with (
        patch("httpx.Client.post") as post,
        patch("httpx.Client.put", return_value=_response(404, method="PUT")) as put,
    ):
        result = IntercomDestination().load(
            [{"id": "gone", "name": "Alice"}],
            config,
            _options("update_only"),
        )

    assert (result.success, result.skipped, result.failed) == (0, 1, 0)
    assert result.skipped_no_match == 1
    post.assert_not_called()
    put.assert_called_once()


def test_update_only_rejects_an_ambiguous_email_match() -> None:
    search_response = _response(
        json={"data": [{"id": "contact-1"}, {"id": "contact-2"}]},
        url=IntercomDestination.SEARCH_URL,
    )

    with (
        patch("httpx.Client.post", return_value=search_response),
        patch("httpx.Client.put") as put,
        pytest.raises(ValueError, match="matched multiple contacts"),
    ):
        IntercomDestination().load(
            [_RECORD],
            _config(),
            _options("update_only", on_error="fail"),
        )

    put.assert_not_called()


@pytest.mark.parametrize(
    ("search_payload", "error"),
    [
        ({}, "missing data list"),
        ({"data": {}}, "data is not a list"),
        ({"data": [{}]}, "contact is missing id"),
    ],
)
def test_update_only_rejects_malformed_search_results(
    search_payload: dict[str, Any],
    error: str,
) -> None:
    response = _response(json=search_payload, url=IntercomDestination.SEARCH_URL)

    with (
        patch("httpx.Client.post", return_value=response),
        patch("httpx.Client.put") as put,
        pytest.raises(ValueError, match=error),
    ):
        IntercomDestination().load(
            [_RECORD],
            _config(),
            _options("update_only", on_error="fail"),
        )

    put.assert_not_called()


def test_update_only_requires_a_supported_identifier() -> None:
    config = _config(properties_template='{"name": "{{ row.name }}"}')

    with (
        patch("httpx.Client.post") as post,
        patch("httpx.Client.put") as put,
        pytest.raises(ValueError, match="non-empty id, external_id, or email"),
    ):
        IntercomDestination().load(
            [{"name": "Alice"}],
            config,
            _options("update_only", on_error="fail"),
        )

    post.assert_not_called()
    put.assert_not_called()


def test_update_only_counts_hits_and_misses_in_one_batch() -> None:
    search_responses = [
        _response(json={"data": [{"id": "contact-1"}]}, url=IntercomDestination.SEARCH_URL),
        _response(json={"data": []}, url=IntercomDestination.SEARCH_URL),
    ]
    records = [
        {"email": "a@test.com", "name": "Alice", "plan": "pro"},
        {"email": "missing@test.com", "name": "Missing", "plan": "free"},
    ]

    with (
        patch("httpx.Client.post", side_effect=search_responses) as post,
        patch("httpx.Client.put", return_value=_response(method="PUT")) as put,
    ):
        result = IntercomDestination().load(
            records,
            _config(),
            _options("update_only"),
        )

    assert (result.success, result.skipped, result.failed) == (1, 1, 0)
    assert result.skipped_no_match == 1
    assert post.call_count == 2
    put.assert_called_once()


def test_invalid_json_template_fails() -> None:
    config = _config(properties_template="{ invalid json {{ }")

    with pytest.raises(Exception):
        IntercomDestination().load([{"email": "a@test.com"}], config, _options(on_error="fail"))


def test_non_object_json_payload_fails_before_the_http_request() -> None:
    config = _config(properties_template='["{{ row.email }}"]')

    with (
        patch("httpx.Client.post") as post,
        pytest.raises(ValueError, match="expected an object"),
    ):
        IntercomDestination().load([_RECORD], config, _options(on_error="fail"))

    post.assert_not_called()


def test_missing_template_field_fails() -> None:
    with pytest.raises(ValueError, match="Template error"):
        IntercomDestination().load(
            [{"email": "a@test.com"}],
            _config(),
            _options(on_error="fail"),
        )


def test_http_error_is_recorded_and_raised_in_fail_mode() -> None:
    with (
        patch("httpx.Client.post", return_value=_response(400)),
        pytest.raises(httpx.HTTPStatusError),
    ):
        IntercomDestination().load(
            [_RECORD],
            _config(),
            _options(on_error="fail"),
        )
