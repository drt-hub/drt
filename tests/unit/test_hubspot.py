"""Unit tests for the HubSpot destination — focused on match_policy (#757).

HubSpot's create-vs-update outcome is a status code, so the policy legs are
verified by which HTTP verb runs and how the SyncResult counts:

- ``upsert`` (default): POST, then PATCH on 409.
- ``create_only``: POST only; 409 (exists) -> skipped, never PATCH.
- ``update_only``: PATCH by idProperty; 404 (no match) -> skipped, never POST.

httpx is mocked — no network.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest
from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Response

from drt.config.models import (
    HubSpotDestinationConfig,
    RateLimitConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.base import MatchPolicyCapable, SyncResult, WritePolicyCapable
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
    if status_code >= 400:
        resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status_code}", request=MagicMock(), response=resp
        )
    else:
        resp.raise_for_status.return_value = None
    return resp


_RECORD = {"email": "a@b.com", "firstname": "Ada"}


# ---------------------------------------------------------------------------
# Capability
# ---------------------------------------------------------------------------


def test_hubspot_declares_match_policy_capability() -> None:
    dest = HubSpotDestination()
    assert isinstance(dest, MatchPolicyCapable)
    assert dest.supported_match_policies() == frozenset({"upsert", "update_only", "create_only"})


def test_hubspot_declares_write_policy_capability() -> None:
    dest = HubSpotDestination()
    assert isinstance(dest, WritePolicyCapable)
    assert dest.identifiers_case_insensitive is False
    assert dest.supported_write_policies() == frozenset({"overwrite", "fill_empty"})


@pytest.mark.parametrize("mode", ["full", "upsert", "incremental"])
def test_hubspot_fill_empty_accepts_upsert_family_modes(mode: str) -> None:
    options = _options(
        write_policy="fill_empty",
        mode=mode,
        **({"cursor_field": "updated_at"} if mode == "incremental" else {}),
    )

    assert options.uses_fill_empty is True


def test_cli_summary_reports_kept_hubspot_properties() -> None:
    from drt.cli.output import print_sync_result

    with patch("drt.cli.output.console") as console:
        print_sync_result(
            "enrich_contacts",
            SyncResult(rows_extracted=1, success=1, kept_values=2),
            elapsed=1.0,
        )

    printed = " ".join(str(call.args[0]) for call in console.print.call_args_list)
    assert "2 properties kept (non-empty)" in printed


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
        result = HubSpotDestination().load([_RECORD], _config(), _options("create_only"))

    assert (result.success, result.skipped, result.failed) == (0, 1, 0)
    assert result.skipped_no_match == 1  # #757 — named subset of skipped
    post.assert_called_once()
    patch_call.assert_not_called()  # existing rows are left untouched


def test_create_only_counts_new_record_as_success() -> None:
    with (
        patch("httpx.Client.post", return_value=_response(201)) as post,
        patch("httpx.Client.patch") as patch_call,
    ):
        result = HubSpotDestination().load([_RECORD], _config(), _options("create_only"))

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
        result = HubSpotDestination().load([_RECORD], _config(), _options("update_only"))

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
        result = HubSpotDestination().load([_RECORD], _config(), _options("update_only"))

    assert (result.success, result.skipped, result.failed) == (0, 1, 0)
    assert result.skipped_no_match == 1
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
    assert result.skipped_no_match == 1
    assert patch_call.call_count == 2
    post.assert_not_called()


# ---------------------------------------------------------------------------
# write_policy: fill_empty (#1238)
# ---------------------------------------------------------------------------


def _use_server(httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch) -> None:
    import drt.destinations.hubspot as hubspot_module

    monkeypatch.setattr(
        hubspot_module,
        "_HUBSPOT_API",
        httpserver.url_for("/crm/v3/objects"),
    )
    monkeypatch.setattr(
        hubspot_module,
        "_HUBSPOT_PROPERTIES_API",
        httpserver.url_for("/crm/v3/properties"),
    )


def _json_requests(httpserver: HTTPServer) -> list[tuple[str, dict[str, Any]]]:
    return [
        (
            request.path,
            json.loads(request.get_data(as_text=True)) if request.get_data() else {},
        )
        for request, _response in httpserver.log
    ]


def _not_found(*ids: str) -> dict[str, Any]:
    return {
        "status": "error",
        "category": "OBJECT_NOT_FOUND",
        "context": {"ids": list(ids)},
    }


def test_batch_read_includes_history_and_accepts_correlated_not_found() -> None:
    client = MagicMock(spec=httpx.Client)
    client.post.return_value = httpx.Response(
        207,
        json={
            "status": "COMPLETE",
            "results": [],
            "numErrors": 1,
            "errors": [_not_found("a@b.com")],
        },
        request=httpx.Request("POST", "https://example.test/batch/read"),
    )

    current = HubSpotDestination._read_current_properties(
        client,
        "https://example.test/batch/read",
        {"Authorization": "Bearer token"},
        "email",
        ["firstname"],
        ["a@b.com"],
    )

    assert current == {}
    assert client.post.call_args.kwargs["json"] == {
        "idProperty": "email",
        "properties": ["email", "firstname"],
        "propertiesWithHistory": [],
        "inputs": [{"id": "a@b.com"}],
    }


def test_batch_read_accepts_one_not_found_error_for_multiple_requested_ids() -> None:
    client = MagicMock(spec=httpx.Client)
    client.post.return_value = httpx.Response(
        207,
        json={
            "status": "COMPLETE",
            "results": [],
            "numErrors": 1,
            "errors": [_not_found("a@b.com", "b@b.com")],
        },
        request=httpx.Request("POST", "https://example.test/batch/read"),
    )

    current = HubSpotDestination._read_current_properties(
        client,
        "https://example.test/batch/read",
        {"Authorization": "Bearer token"},
        "email",
        ["firstname"],
        ["a@b.com", "b@b.com"],
    )

    assert current == {}


def test_batch_read_requires_complete_status_to_be_present() -> None:
    client = MagicMock(spec=httpx.Client)
    client.post.return_value = httpx.Response(
        200,
        json={"results": [{"id": "a@b.com", "properties": {"firstname": "Ada"}}]},
        request=httpx.Request("POST", "https://example.test/batch/read"),
    )

    with pytest.raises(ValueError, match="status must be COMPLETE"):
        HubSpotDestination._read_current_properties(
            client,
            "https://example.test/batch/read",
            {"Authorization": "Bearer token"},
            "email",
            ["firstname"],
            ["a@b.com"],
        )


def test_fill_empty_batch_read_shape_and_100_input_limit(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    def read_handler(request: Any) -> Response:
        body = request.get_json()
        return Response(
            json.dumps(
                {
                    "status": "COMPLETE",
                    "results": [
                        {"id": item["id"], "properties": {"firstname": "already set"}}
                        for item in body["inputs"]
                    ],
                }
            ),
            content_type="application/json",
        )

    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_handler(read_handler)
    _use_server(httpserver, monkeypatch)
    records = [{"email": f"person-{i}@example.com", "firstname": f"Name {i}"} for i in range(101)]

    result = HubSpotDestination().load(
        records,
        _config(),
        _options(write_policy="fill_empty"),
    )

    requests = _json_requests(httpserver)
    assert [path for path, _ in requests] == [
        "/crm/v3/objects/contacts/batch/read",
        "/crm/v3/objects/contacts/batch/read",
    ]
    assert [len(body["inputs"]) for _, body in requests] == [100, 1]
    assert requests[0][1]["idProperty"] == "email"
    # The id property is requested too, so results can be tied to their inputs.
    assert requests[0][1]["properties"] == ["email", "firstname"]
    assert requests[0][1]["propertiesWithHistory"] == []
    assert requests[0][1]["inputs"][0] == {"id": "person-0@example.com"}
    assert (result.success, result.skipped, result.failed, result.kept_values) == (0, 101, 0, 101)


def test_fill_empty_correlates_internal_ids_and_lowercased_emails(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    """HubSpot answers a custom idProperty lookup with its internal ``id`` and returns
    emails lower-cased; the results must still be tied to the requested inputs."""

    def read_handler(request: Any) -> Response:
        return Response(
            json.dumps(
                {
                    "status": "COMPLETE",
                    "results": [
                        {
                            "id": "98765",  # internal object id, not the email
                            "properties": {"email": "mixed.case@example.com", "firstname": "Ann"},
                        }
                    ],
                }
            ),
            content_type="application/json",
        )

    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_handler(read_handler)
    _use_server(httpserver, monkeypatch)

    result = HubSpotDestination().load(
        [{"email": "Mixed.Case@example.com", "firstname": "Source Name"}],
        _config(),
        _options(write_policy="fill_empty"),
    )

    # Correlated (not failed closed): the existing value is kept, nothing written.
    assert (result.success, result.skipped, result.failed, result.kept_values) == (0, 1, 0, 1)
    assert [path for path, _ in _json_requests(httpserver)] == [
        "/crm/v3/objects/contacts/batch/read"
    ]


def test_fill_empty_207_filters_patch_and_posts_missing_record_in_full(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request("/crm/v3/properties/contacts", method="GET").respond_with_json(
        {
            "results": [
                {"name": "email"},
                {"name": "firstname"},
                {"name": "lifecycle_stage"},
            ]
        }
    )
    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json(
        {
            "status": "COMPLETE",
            "results": [
                {
                    "id": "101",
                    "properties": {
                        "email": "existing@example.com",
                        "firstname": "Human edit",
                    },
                }
            ],
            "numErrors": 1,
            "errors": [_not_found("new@example.com")],
        },
        status=207,
    )
    httpserver.expect_request(
        "/crm/v3/objects/contacts/existing@example.com",
        method="PATCH",
        query_string={"idProperty": "email"},
    ).respond_with_json({"id": "1"})
    httpserver.expect_request("/crm/v3/objects/contacts", method="POST").respond_with_json(
        {"id": "2"}, status=201
    )
    _use_server(httpserver, monkeypatch)
    records = [
        {
            "email": "existing@example.com",
            "firstname": "Warehouse guess",
            "lifecycle_stage": "customer",
        },
        {
            "email": "new@example.com",
            "firstname": "New person",
            "lifecycle_stage": "lead",
        },
    ]

    result = HubSpotDestination().load(
        records,
        _config(),
        _options(
            write_policy="fill_empty",
            write_policy_overrides={"lifecycle_stage": "overwrite"},
        ),
    )

    requests = _json_requests(httpserver)
    assert requests[0] == ("/crm/v3/properties/contacts", {})
    assert requests[1][1] == {
        "idProperty": "email",
        "properties": ["email", "firstname"],
        "propertiesWithHistory": [],
        "inputs": [{"id": "existing@example.com"}, {"id": "new@example.com"}],
    }
    assert requests[2][1] == {
        "properties": {
            "email": "existing@example.com",
            "lifecycle_stage": "customer",
        }
    }
    assert requests[3][1] == {
        "properties": records[1],
        "idProperty": "email",
    }
    assert (result.success, result.failed, result.kept_values) == (2, 0, 1)


def test_fill_empty_writes_blank_text_but_keeps_string_zero(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json(
        {
            "status": "COMPLETE",
            "results": [
                {
                    "id": "a@b.com",
                    "properties": {"firstname": "", "lastname": "   ", "score": "0"},
                }
            ],
        }
    )
    httpserver.expect_request(
        "/crm/v3/objects/contacts/a@b.com",
        method="PATCH",
        query_string={"idProperty": "email"},
    ).respond_with_json({"id": "1"})
    _use_server(httpserver, monkeypatch)

    result = HubSpotDestination().load(
        [{"email": "a@b.com", "firstname": "Ada", "lastname": "L", "score": "9"}],
        _config(),
        _options(write_policy="fill_empty"),
    )

    assert _json_requests(httpserver)[1][1] == {
        "properties": {"email": "a@b.com", "firstname": "Ada", "lastname": "L"}
    }
    assert result.kept_values == 1


def test_fill_empty_all_properties_kept_skips_write(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json(
        {
            "status": "COMPLETE",
            "results": [{"id": "a@b.com", "properties": {"firstname": "Human edit"}}],
        }
    )
    _use_server(httpserver, monkeypatch)

    result = HubSpotDestination().load(
        [_RECORD],
        _config(),
        _options(write_policy="fill_empty"),
    )

    assert len(httpserver.log) == 1
    assert (result.success, result.skipped, result.skipped_no_match, result.kept_values) == (
        0,
        1,
        0,
        1,
    )


@pytest.mark.parametrize(
    ("on_error", "expected_failed"),
    [("skip", 2), ("fail", 1)],
)
def test_fill_empty_read_failure_is_fail_closed(
    httpserver: HTTPServer,
    monkeypatch: pytest.MonkeyPatch,
    on_error: str,
    expected_failed: int,
) -> None:
    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json({"message": "unavailable"}, status=503)
    _use_server(httpserver, monkeypatch)

    result = HubSpotDestination().load(
        [_RECORD, {"email": "b@b.com", "firstname": "B"}],
        _config(),
        _options(write_policy="fill_empty", on_error=on_error),
    )

    assert len(httpserver.log) == 1
    assert (result.success, result.failed) == (0, expected_failed)
    assert all(error.http_status == 503 for error in result.row_errors)


@pytest.mark.parametrize(
    "body",
    [
        {},
        {
            "results": [],
            "numErrors": 1,
            "errors": [_not_found("a@b.com")],
        },
        {"status": "COMPLETE", "results": {}},
        {
            "status": "PENDING",
            "results": [],
            "numErrors": 1,
            "errors": [_not_found("a@b.com")],
        },
        {
            "status": "COMPLETE",
            "results": [],
            "numErrors": 1,
            "errors": [
                {
                    "category": "VALIDATION_ERROR",
                    "context": {"ids": ["a@b.com"]},
                }
            ],
        },
        {
            "status": "COMPLETE",
            "results": [],
            "numErrors": 1,
            "errors": [_not_found("a@b.com", "somebody-else@example.com")],
        },
        {"status": "COMPLETE", "results": [], "numErrors": 1, "errors": []},
    ],
)
def test_fill_empty_invalid_batch_read_body_fails_closed(
    httpserver: HTTPServer,
    monkeypatch: pytest.MonkeyPatch,
    body: dict[str, Any],
) -> None:
    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json(body, status=207)
    _use_server(httpserver, monkeypatch)

    result = HubSpotDestination().load(
        [_RECORD],
        _config(),
        _options(write_policy="fill_empty"),
    )

    assert [request.path for request, _ in httpserver.log] == [
        "/crm/v3/objects/contacts/batch/read"
    ]
    assert (result.success, result.failed) == (0, 1)


def test_fill_empty_omitted_input_without_not_found_error_fails_closed(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json({"status": "COMPLETE", "results": []}, status=207)
    _use_server(httpserver, monkeypatch)

    result = HubSpotDestination().load([_RECORD], _config(), _options(write_policy="fill_empty"))

    assert len(httpserver.log) == 1
    assert (result.success, result.failed) == (0, 1)


def test_fill_empty_create_race_is_a_row_error_and_never_patches(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json(
        {
            "status": "COMPLETE",
            "results": [],
            "numErrors": 1,
            "errors": [_not_found("a@b.com")],
        },
        status=207,
    )
    httpserver.expect_request("/crm/v3/objects/contacts", method="POST").respond_with_json(
        {"message": "already exists"}, status=409
    )
    _use_server(httpserver, monkeypatch)

    result = HubSpotDestination().load([_RECORD], _config(), _options(write_policy="fill_empty"))

    assert [request.method for request, _ in httpserver.log] == ["POST", "POST"]
    assert (result.success, result.failed) == (0, 1)
    assert "record appeared between read and write; re-run" in result.row_errors[0].error_message


def test_fill_empty_create_only_still_treats_post_409_as_no_match_skip(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json(
        {
            "status": "COMPLETE",
            "results": [],
            "numErrors": 1,
            "errors": [_not_found("a@b.com")],
        },
        status=207,
    )
    httpserver.expect_request("/crm/v3/objects/contacts", method="POST").respond_with_json(
        {"message": "already exists"}, status=409
    )
    _use_server(httpserver, monkeypatch)

    result = HubSpotDestination().load(
        [_RECORD], _config(), _options("create_only", write_policy="fill_empty")
    )

    assert [request.method for request, _ in httpserver.log] == ["POST", "POST"]
    assert (result.success, result.skipped, result.skipped_no_match, result.failed) == (0, 1, 1, 0)


def test_fill_empty_duplicate_ids_are_first_writer_wins(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json(
        {
            "status": "COMPLETE",
            "results": [{"id": "a@b.com", "properties": {"firstname": None}}],
        }
    )
    httpserver.expect_request(
        "/crm/v3/objects/contacts/a@b.com",
        method="PATCH",
        query_string={"idProperty": "email"},
    ).respond_with_json({"id": "1"})
    _use_server(httpserver, monkeypatch)

    result = HubSpotDestination().load(
        [
            {"email": "a@b.com", "firstname": "first"},
            {"email": "a@b.com", "firstname": "second"},
        ],
        _config(),
        _options(write_policy="fill_empty"),
    )

    assert len(httpserver.log) == 2
    assert _json_requests(httpserver)[1][1]["properties"]["firstname"] == "first"
    assert (result.success, result.skipped, result.kept_values) == (1, 1, 1)


def test_fill_empty_duplicate_after_no_fill_subchunk_uses_first_writer_state(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request("/crm/v3/properties/contacts", method="GET").respond_with_json(
        {"results": [{"name": "email"}, {"name": "firstname"}, {"name": "lastname"}]}
    )
    httpserver.expect_request("/crm/v3/objects/contacts", method="POST").respond_with_json(
        {"id": "created"}, status=201
    )
    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json(
        {
            "status": "COMPLETE",
            "results": [],
            "numErrors": 1,
            "errors": [_not_found("person-0@example.com")],
        },
        status=207,
    )
    _use_server(httpserver, monkeypatch)
    records = [{"email": f"person-{i}@example.com", "firstname": f"Name {i}"} for i in range(100)]
    records.append({"email": "person-0@example.com", "lastname": "Later value"})

    result = HubSpotDestination().load(
        records,
        _config(),
        _options(write_policy_overrides={"lastname": "fill_empty"}),
    )

    paths = [request.path for request, _ in httpserver.log]
    assert paths.count("/crm/v3/objects/contacts") == 100
    assert paths[-1] == "/crm/v3/objects/contacts/batch/read"
    assert (result.success, result.skipped, result.failed, result.kept_values) == (100, 1, 0, 1)


def test_fill_empty_template_error_remains_a_row_error(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json(
        {
            "status": "COMPLETE",
            "results": [],
            "numErrors": 1,
            "errors": [_not_found("good@example.com")],
        },
        status=207,
    )
    httpserver.expect_request("/crm/v3/objects/contacts", method="POST").respond_with_json(
        {"id": "1"}, status=201
    )
    _use_server(httpserver, monkeypatch)
    config = _config(
        properties_template='{"email": "{{ row.email }}", "firstname": {{ row.value }}}'
    )

    result = HubSpotDestination().load(
        [
            {"email": "bad@example.com", "value": "not-json"},
            {"email": "good@example.com", "value": '"Ada"'},
        ],
        config,
        _options(write_policy="fill_empty"),
    )

    assert (result.success, result.failed) == (1, 1)
    assert result.row_errors[0].batch_index == 0
    assert "properties_template error" in result.row_errors[0].error_message
    assert _json_requests(httpserver)[0][1]["inputs"] == [{"id": "good@example.com"}]


def test_write_policy_override_typo_is_rejected_before_write(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request("/crm/v3/properties/contacts", method="GET").respond_with_json(
        {"results": [{"name": "email"}, {"name": "firstname"}]}
    )
    _use_server(httpserver, monkeypatch)

    with pytest.raises(
        ValueError,
        match=r"write_policy_overrides names property/properties.*fristname",
    ):
        HubSpotDestination().load(
            [_RECORD],
            _config(),
            _options(write_policy_overrides={"fristname": "fill_empty"}),
        )

    assert [request.path for request, _ in httpserver.log] == ["/crm/v3/properties/contacts"]


def test_write_policy_override_validation_403_is_skipped_once(
    httpserver: HTTPServer,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    httpserver.expect_request("/crm/v3/properties/contacts", method="GET").respond_with_json(
        {"message": "missing schemas scope"}, status=403
    )
    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json(
        {
            "status": "COMPLETE",
            "results": [{"id": "a@b.com", "properties": {"firstname": "set"}}],
        }
    )
    _use_server(httpserver, monkeypatch)
    destination = HubSpotDestination()
    options = _options(write_policy_overrides={"firstname": "fill_empty"})

    first = destination.load([_RECORD], _config(), options)
    second = destination.load([_RECORD], _config(), options)

    assert (first.skipped, second.skipped) == (1, 1)
    paths = [request.path for request, _ in httpserver.log]
    assert paths.count("/crm/v3/properties/contacts") == 1
    assert paths.count("/crm/v3/objects/contacts/batch/read") == 2
    assert caplog.text.count("property-name validation skipped") == 1


def test_write_policy_override_cache_is_scoped_to_resolved_token(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_ordered_request(
        "/crm/v3/properties/contacts",
        method="GET",
        headers={"Authorization": "Bearer portal-a"},
    ).respond_with_json({"message": "missing schemas scope"}, status=403)
    httpserver.expect_ordered_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json(
        {
            "status": "COMPLETE",
            "results": [{"id": "a@b.com", "properties": {"firstname": "set"}}],
        }
    )
    httpserver.expect_ordered_request(
        "/crm/v3/properties/contacts",
        method="GET",
        headers={"Authorization": "Bearer portal-b"},
    ).respond_with_json({"results": [{"name": "email"}]})
    _use_server(httpserver, monkeypatch)
    destination = HubSpotDestination()
    options = _options(write_policy_overrides={"firstname": "fill_empty"})

    first = destination.load(
        [_RECORD],
        _config(auth={"type": "bearer", "token": "portal-a"}),
        options,
    )
    with pytest.raises(
        ValueError,
        match=r"write_policy_overrides names property/properties.*firstname",
    ):
        destination.load(
            [_RECORD],
            _config(auth={"type": "bearer", "token": "portal-b"}),
            options,
        )

    assert (first.skipped, first.failed) == (1, 0)
    assert [request.path for request, _ in httpserver.log] == [
        "/crm/v3/properties/contacts",
        "/crm/v3/objects/contacts/batch/read",
        "/crm/v3/properties/contacts",
    ]


def _json_requests_with_method(httpserver: HTTPServer, method: str) -> list[tuple[str, Any]]:
    return [
        (request.path, json.loads(request.get_data(as_text=True)))
        for request, _response in httpserver.log
        if request.method == method
    ]


def test_valid_override_absent_from_one_engine_batch_passes_schema_validation(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    httpserver.expect_request("/crm/v3/properties/contacts", method="GET").respond_with_json(
        {"results": [{"name": "email"}, {"name": "firstname"}, {"name": "lastname"}]}
    )
    httpserver.expect_request("/crm/v3/objects/contacts", method="POST").respond_with_json(
        {"id": "1"}, status=201
    )
    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json(
        {
            "status": "COMPLETE",
            "results": [{"id": "b@b.com", "properties": {"lastname": "set"}}],
        }
    )
    httpserver.expect_request("/crm/v3/objects/contacts/b@b.com", method="PATCH").respond_with_json(
        {"id": "2"}
    )
    _use_server(httpserver, monkeypatch)
    destination = HubSpotDestination()
    options = _options(write_policy_overrides={"lastname": "fill_empty"})

    first = destination.load([_RECORD], _config(), options)
    second = destination.load(
        [{"email": "b@b.com", "firstname": "B", "lastname": "Source"}],
        _config(),
        options,
    )

    assert (first.success, first.failed) == (1, 0)
    # lastname is kept (non-empty), but firstname keeps the overwrite policy and is PATCHed.
    assert (second.success, second.failed, second.kept_values) == (1, 0, 1)
    patch = [body for request, body in _json_requests_with_method(httpserver, "PATCH")]
    assert patch == [{"properties": {"email": "b@b.com", "firstname": "B"}}]


def test_default_write_policy_makes_no_batch_read_call(
    httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls = {"post": 0}

    def post_handler(_request: Any) -> Response:
        calls["post"] += 1
        status = 201 if calls["post"] == 1 else 409
        return Response("{}", status=status, content_type="application/json")

    httpserver.expect_request("/crm/v3/objects/contacts", method="POST").respond_with_handler(
        post_handler
    )
    httpserver.expect_request(
        "/crm/v3/objects/contacts/existing@example.com",
        method="PATCH",
        query_string={"idProperty": "email"},
    ).respond_with_json({"id": "1"})
    _use_server(httpserver, monkeypatch)

    result = HubSpotDestination().load(
        [_RECORD, {"email": "existing@example.com", "firstname": "Grace"}],
        _config(),
        _options(write_policy_overrides={"not_a_real_property": "overwrite"}),
    )

    assert [request.path for request, _ in httpserver.log] == [
        "/crm/v3/objects/contacts",
        "/crm/v3/objects/contacts",
        "/crm/v3/objects/contacts/existing@example.com",
    ]
    assert result.success == 2


def test_default_policy_fail_stops_before_later_template_error() -> None:
    config = _config(
        properties_template='{"email": "{{ row.email }}", "firstname": {{ row.value }}}'
    )

    with patch("httpx.Client.post", return_value=_response(400)) as post:
        result = HubSpotDestination().load(
            [
                {"email": "first@example.com", "value": '"Ada"'},
                {"email": "later@example.com", "value": "not-json"},
            ],
            config,
            _options(on_error="fail"),
        )

    post.assert_called_once()
    assert (result.success, result.failed) == (0, 1)
    assert result.row_errors[0].batch_index == 0
    assert "properties_template error" not in result.row_errors[0].error_message


@pytest.mark.parametrize(
    ("match_policy", "read_results", "expected_paths", "expected_no_match"),
    [
        ("update_only", [], ["/crm/v3/objects/contacts/batch/read"], 1),
        (
            "create_only",
            [{"id": "a@b.com", "properties": {"firstname": "existing"}}],
            ["/crm/v3/objects/contacts/batch/read"],
            1,
        ),
        (
            "create_only",
            [],
            ["/crm/v3/objects/contacts/batch/read", "/crm/v3/objects/contacts"],
            0,
        ),
    ],
)
def test_fill_empty_match_policy_interplay(
    httpserver: HTTPServer,
    monkeypatch: pytest.MonkeyPatch,
    match_policy: str,
    read_results: list[dict[str, Any]],
    expected_paths: list[str],
    expected_no_match: int,
) -> None:
    httpserver.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_json(
        {
            "status": "COMPLETE",
            "results": read_results,
            **({"numErrors": 1, "errors": [_not_found("a@b.com")]} if not read_results else {}),
        },
        status=207 if not read_results else 200,
    )
    if match_policy == "create_only" and not read_results:
        httpserver.expect_request("/crm/v3/objects/contacts", method="POST").respond_with_json(
            {"id": "1"}, status=201
        )
    _use_server(httpserver, monkeypatch)

    result = HubSpotDestination().load(
        [_RECORD],
        _config(),
        _options(match_policy, write_policy="fill_empty"),
    )

    assert [request.path for request, _ in httpserver.log] == expected_paths
    assert result.skipped_no_match == expected_no_match
    assert result.success == (1 if match_policy == "create_only" and not read_results else 0)
