"""Unit tests for the Pipedrive destination (#1183).

Pipedrive has no native upsert, so the destination searches first
(GET /persons/search with exact_match) and then PATCHes the match or POSTs a
new person. A local HTTP server stands in for Pipedrive: no network, no real token.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pytest_httpserver import HTTPServer

from drt.config.models import (
    PipedriveDestinationConfig,
    RateLimitConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.base import MatchPolicyCapable
from drt.destinations.pipedrive import PipedriveDestination

_RECORD = {"name": "Ada Lovelace", "email": "ada@example.com"}
_SEARCH = "/api/v2/persons/search"
_PERSONS = "/api/v2/persons"


def _config(**overrides: Any) -> PipedriveDestinationConfig:
    data: dict[str, Any] = {"type": "pipedrive", "api_token": "test-token"}
    data.update(overrides)
    return PipedriveDestinationConfig(**data)


def _options(match_policy: str = "upsert", **overrides: Any) -> SyncOptions:
    data: dict[str, Any] = {
        "match_policy": match_policy,
        "rate_limit": RateLimitConfig(requests_per_second=0),
        "retry": RetryConfig(max_attempts=1, initial_backoff=0.0, backoff_multiplier=1.0),
        "on_error": "skip",
    }
    data.update(overrides)
    return SyncOptions(**data)


def _use_server(httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch) -> None:
    import drt.destinations.pipedrive as pipedrive_module

    monkeypatch.setattr(pipedrive_module, "_PIPEDRIVE_API", httpserver.url_for("/api/v2"))


def _search_result(*ids: int) -> dict[str, Any]:
    return {
        "success": True,
        "data": {
            "items": [{"result_score": 1.0, "item": {"id": i, "type": "person"}} for i in ids]
        },
    }


def _calls(httpserver: HTTPServer) -> list[tuple[str, str]]:
    return [(request.method, request.path) for request, _response in httpserver.log]


def test_pipedrive_declares_match_policy_capability() -> None:
    dest = PipedriveDestination()
    assert isinstance(dest, MatchPolicyCapable)
    assert dest.supported_match_policies() == frozenset({"upsert", "update_only", "create_only"})


def test_empty_batch_sends_nothing(httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch) -> None:
    _use_server(httpserver, monkeypatch)
    result = PipedriveDestination().load([], _config(), _options())
    assert (result.success, result.skipped, result.failed) == (0, 0, 0)
    assert httpserver.log == []


def test_missing_token_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PIPEDRIVE_TEST_UNSET_TOKEN", raising=False)
    config = _config(api_token=None, api_token_env="PIPEDRIVE_TEST_UNSET_TOKEN")
    with pytest.raises(ValueError, match="Pipedrive"):
        PipedriveDestination().load([_RECORD], config, _options())


def test_search_uses_exact_match_and_the_token_header(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(_SEARCH, method="GET").respond_with_json(_search_result())
    httpserver.expect_request(_PERSONS, method="POST").respond_with_json(
        {"success": True, "data": {"id": 11}}
    )
    _use_server(httpserver, monkeypatch)

    PipedriveDestination().load([_RECORD], _config(), _options())

    search_request, _ = httpserver.log[0]
    assert search_request.args["term"] == "ada@example.com"
    assert search_request.args["fields"] == "email"
    assert search_request.args["exact_match"] == "true"
    assert search_request.headers["x-api-token"] == "test-token"


def test_creates_person_when_search_finds_nothing(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(_SEARCH, method="GET").respond_with_json(_search_result())
    httpserver.expect_request(_PERSONS, method="POST").respond_with_json(
        {"success": True, "data": {"id": 11}}
    )
    _use_server(httpserver, monkeypatch)

    result = PipedriveDestination().load([_RECORD], _config(), _options())

    assert (result.success, result.skipped, result.failed) == (1, 0, 0)
    assert _calls(httpserver) == [("GET", _SEARCH), ("POST", _PERSONS)]


def test_updates_person_when_search_finds_a_match(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(_SEARCH, method="GET").respond_with_json(_search_result(42))
    httpserver.expect_request(f"{_PERSONS}/42", method="PATCH").respond_with_json(
        {"success": True, "data": {"id": 42}}
    )
    _use_server(httpserver, monkeypatch)

    result = PipedriveDestination().load([_RECORD], _config(), _options())

    assert (result.success, result.skipped, result.failed) == (1, 0, 0)
    assert _calls(httpserver) == [("GET", _SEARCH), ("PATCH", f"{_PERSONS}/42")]


def test_update_only_skips_when_no_match_and_never_creates(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(_SEARCH, method="GET").respond_with_json(_search_result())
    _use_server(httpserver, monkeypatch)

    result = PipedriveDestination().load([_RECORD], _config(), _options("update_only"))

    assert (result.success, result.skipped, result.failed) == (0, 1, 0)
    assert result.skipped_no_match == 1
    assert _calls(httpserver) == [("GET", _SEARCH)]


def test_create_only_skips_existing_person_and_never_updates(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(_SEARCH, method="GET").respond_with_json(_search_result(42))
    _use_server(httpserver, monkeypatch)

    result = PipedriveDestination().load([_RECORD], _config(), _options("create_only"))

    assert (result.success, result.skipped, result.failed) == (0, 1, 0)
    assert result.skipped_no_match == 1
    assert _calls(httpserver) == [("GET", _SEARCH)]


def test_repeated_match_value_in_one_batch_is_searched_only_once(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(_SEARCH, method="GET").respond_with_json(_search_result())
    httpserver.expect_request(_PERSONS, method="POST").respond_with_json(
        {"success": True, "data": {"id": 11}}
    )
    httpserver.expect_request(f"{_PERSONS}/11", method="PATCH").respond_with_json(
        {"success": True, "data": {"id": 11}}
    )
    _use_server(httpserver, monkeypatch)

    records = [_RECORD, {"name": "Ada L.", "email": "ada@example.com"}]
    result = PipedriveDestination().load(records, _config(), _options())

    assert (result.success, result.skipped, result.failed) == (2, 0, 0)
    assert _calls(httpserver) == [
        ("GET", _SEARCH),
        ("POST", _PERSONS),
        ("PATCH", f"{_PERSONS}/11"),
    ]


@pytest.mark.parametrize(
    ("on_error", "expected_failed", "expected_calls"),
    [("skip", 2, 4), ("fail", 1, 2)],
)
def test_write_failure_becomes_a_row_error(
    httpserver: HTTPServer,
    monkeypatch: pytest.MonkeyPatch,
    on_error: str,
    expected_failed: int,
    expected_calls: int,
) -> None:
    httpserver.expect_request(_SEARCH, method="GET").respond_with_json(_search_result())
    httpserver.expect_request(_PERSONS, method="POST").respond_with_json(
        {"success": False, "error": "boom"}, status=500
    )
    _use_server(httpserver, monkeypatch)

    records = [_RECORD, {"name": "Grace Hopper", "email": "grace@example.com"}]
    result = PipedriveDestination().load(records, _config(), _options(on_error=on_error))

    assert (result.success, result.failed) == (0, expected_failed)
    assert all(error.http_status == 500 for error in result.row_errors)
    assert len(httpserver.log) == expected_calls


def _body(httpserver: HTTPServer, index: int) -> dict[str, Any]:
    request, _ = httpserver.log[index]
    return json.loads(request.get_data(as_text=True))  # type: ignore[no-any-return]


def test_create_payload_maps_columns_to_pipedrive_fields(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(_SEARCH, method="GET").respond_with_json(_search_result())
    httpserver.expect_request(_PERSONS, method="POST").respond_with_json(
        {"success": True, "data": {"id": 11}}
    )
    _use_server(httpserver, monkeypatch)

    record = {"name": "Ada", "email": "ada@example.com", "phone": "555", "abc123": "gold"}
    PipedriveDestination().load([record], _config(), _options())

    assert _body(httpserver, 1) == {
        "name": "Ada",
        "emails": [{"value": "ada@example.com", "primary": True}],
        "phones": [{"value": "555", "primary": True}],
        "custom_fields": {"abc123": "gold"},
    }


def test_update_payload_leaves_out_the_match_field(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(_SEARCH, method="GET").respond_with_json(_search_result(42))
    httpserver.expect_request(f"{_PERSONS}/42", method="PATCH").respond_with_json(
        {"success": True, "data": {"id": 42}}
    )
    _use_server(httpserver, monkeypatch)

    PipedriveDestination().load([_RECORD], _config(), _options())

    assert _body(httpserver, 1) == {"name": "Ada Lovelace"}


def test_ambiguous_match_is_a_row_error_and_writes_nothing(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(_SEARCH, method="GET").respond_with_json(_search_result(42, 43))
    _use_server(httpserver, monkeypatch)

    result = PipedriveDestination().load([_RECORD], _config(), _options())

    assert (result.success, result.failed) == (0, 1)
    assert "more than one person" in result.row_errors[0].error_message
    assert _calls(httpserver) == [("GET", _SEARCH)]


def test_create_without_a_name_is_a_row_error(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(_SEARCH, method="GET").respond_with_json(_search_result())
    _use_server(httpserver, monkeypatch)

    result = PipedriveDestination().load([{"email": "ada@example.com"}], _config(), _options())

    assert (result.success, result.failed) == (0, 1)
    assert "'name' is required" in result.row_errors[0].error_message
    assert _calls(httpserver) == [("GET", _SEARCH)]


def test_record_without_the_match_value_is_a_row_error(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    _use_server(httpserver, monkeypatch)

    result = PipedriveDestination().load([{"name": "No Email"}], _config(), _options())

    assert (result.success, result.failed) == (0, 1)
    assert httpserver.log == []


def test_unknown_custom_field_key_is_a_row_error_from_the_api(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(_SEARCH, method="GET").respond_with_json(_search_result())
    httpserver.expect_request(_PERSONS, method="POST").respond_with_json(
        {"success": False, "error": "Unknown custom field key: bogus_key"}, status=400
    )
    _use_server(httpserver, monkeypatch)

    record = {"name": "Ada", "email": "ada@example.com", "bogus_key": "x"}
    result = PipedriveDestination().load([record], _config(), _options())

    assert (result.success, result.failed) == (0, 1)
    assert result.row_errors[0].http_status == 400
    assert "Unknown custom field key" in result.row_errors[0].error_message
    assert _body(httpserver, 1)["custom_fields"] == {"bogus_key": "x"}  # sent, not dropped


@pytest.mark.parametrize(
    ("on_error", "expected_failed", "expected_calls"), [("skip", 2, 2), ("fail", 1, 1)]
)
def test_ambiguous_match_follows_on_error(
    httpserver: HTTPServer,
    monkeypatch: pytest.MonkeyPatch,
    on_error: str,
    expected_failed: int,
    expected_calls: int,
) -> None:
    httpserver.expect_request(_SEARCH, method="GET").respond_with_json(_search_result(42, 43))
    _use_server(httpserver, monkeypatch)

    records = [_RECORD, {"name": "Grace", "email": "grace@example.com"}]
    result = PipedriveDestination().load(records, _config(), _options(on_error=on_error))

    assert (result.success, result.failed) == (0, expected_failed)
    assert len(httpserver.log) == expected_calls
    assert {method for method, _ in _calls(httpserver)} == {"GET"}  # nothing was written


def test_base_url_override_is_used_and_the_token_stays_out_of_the_url(
    httpserver: HTTPServer,
) -> None:
    httpserver.expect_request("/custom/v2/persons/search", method="GET").respond_with_json(
        _search_result()
    )
    httpserver.expect_request("/custom/v2/persons", method="POST").respond_with_json(
        {"success": True, "data": {"id": 11}}
    )
    config = _config(base_url=httpserver.url_for("/custom/v2/"))  # trailing slash is tolerated

    result = PipedriveDestination().load([_RECORD], config, _options())

    assert (result.success, result.failed) == (1, 0)
    assert _calls(httpserver) == [
        ("GET", "/custom/v2/persons/search"),
        ("POST", "/custom/v2/persons"),
    ]
    for request, _response in httpserver.log:
        assert request.headers["x-api-token"] == "test-token"
        assert "api_token" not in request.args


def test_create_response_without_an_id_blocks_a_repeat_row(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(_SEARCH, method="GET").respond_with_json(_search_result())
    httpserver.expect_request(_PERSONS, method="POST").respond_with_json(
        {"success": True, "data": {}}
    )
    _use_server(httpserver, monkeypatch)

    records = [_RECORD, {"name": "Ada L.", "email": "ADA@example.com"}]
    result = PipedriveDestination().load(records, _config(), _options())

    assert (result.success, result.failed) == (1, 1)
    assert "had no id" in result.row_errors[0].error_message
    assert _calls(httpserver) == [("GET", _SEARCH), ("POST", _PERSONS)]  # no duplicate create
