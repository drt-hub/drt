"""API-based plan reads for SaaS destinations (#472, ADR 0018) — HubSpot first."""

from __future__ import annotations

import json
from typing import Any

import pytest
from pydantic import ValidationError
from pytest_httpserver import HTTPServer
from werkzeug.wrappers import Response

import drt.destinations.hubspot as hubspot_module
from drt.config.models import (
    HubSpotDestinationConfig,
    RateLimitConfig,
    RetryConfig,
    SyncConfig,
    SyncOptions,
)
from drt.config.sync_options import ApiReadsConfig
from drt.destinations.base import KeyedReadDestination
from drt.engine.diff import compute_diff
from drt.engine.plan import build_plan, render_text


def _config(**overrides: Any) -> HubSpotDestinationConfig:
    data: dict[str, Any] = {
        "type": "hubspot",
        "object_type": "contacts",
        "id_property": "email",
        "auth": {"type": "bearer", "token": "test-token"},
    }
    data.update(overrides)
    return HubSpotDestinationConfig(**data)


def _options(**overrides: Any) -> SyncOptions:
    data: dict[str, Any] = {
        "rate_limit": RateLimitConfig(requests_per_second=0),
        "retry": RetryConfig(max_attempts=1, initial_backoff=0.0, backoff_multiplier=1.0),
    }
    data.update(overrides)
    return SyncOptions(**data)


@pytest.fixture
def hubspot(httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch) -> HTTPServer:
    monkeypatch.setattr(hubspot_module, "_HUBSPOT_API", httpserver.url_for("/crm/v3/objects"))
    return httpserver


def _read_response(found: list[dict[str, Any]], missing: list[str]) -> Response:
    body: dict[str, Any] = {
        "status": "COMPLETE",
        "results": [{"id": str(100 + i), "properties": row} for i, row in enumerate(found)],
        "errors": [],
        "numErrors": 0,
    }
    if missing:
        body["errors"] = [
            {"status": "error", "category": "OBJECT_NOT_FOUND", "context": {"ids": missing}}
        ]
        body["numErrors"] = 1
    return Response(json.dumps(body), status=207 if missing else 200, mimetype="application/json")


def _diff(records: list[dict[str, Any]], **kwargs: Any):
    return compute_diff(
        records,
        _config(**kwargs.pop("config", {})),
        kwargs.pop("options", _options()),
        limit=1000,
        api_reads=kwargs.pop("api_reads", ApiReadsConfig()),
    )


def test_hubspot_implements_the_keyed_read_capability() -> None:
    assert isinstance(hubspot_module.HubSpotDestination(), KeyedReadDestination)


def test_a_plan_classifies_create_update_and_unchanged_from_exact_key_reads(
    hubspot: HTTPServer,
) -> None:
    hubspot.expect_request(
        "/crm/v3/objects/contacts/batch/read", method="POST"
    ).respond_with_handler(
        lambda request: _read_response(
            [
                {"email": "b@example.com", "firstname": "Bea"},
                {"email": "c@example.com", "firstname": "Old"},
            ],
            ["a@example.com"],
        )
    )
    result = _diff(
        [
            {"email": "a@example.com", "firstname": "Ann"},
            {"email": "b@example.com", "firstname": "Bea"},
            {"email": "c@example.com", "firstname": "Cy"},
        ]
    )

    assert result.supported
    assert [row["email"] for row in result.added] == ["a@example.com"]
    assert [new["email"] for _old, new in result.updated] == ["c@example.com"]
    assert DiffChanged(result) == {"firstname"}
    assert result.api_reads == {"calls": 1, "rows": 3}
    [(_, body)] = _requests(hubspot)
    assert body["idProperty"] == "email"
    assert [item["id"] for item in body["inputs"]] == [
        "a@example.com",
        "b@example.com",
        "c@example.com",
    ]
    assert sorted(body["properties"]) == ["email", "firstname"]


def DiffChanged(result: Any) -> set[str]:
    from drt.engine.diff import DiffResult

    [(old, new)] = result.updated
    return set(DiffResult.changed_fields(old, new))


def _requests(server: HTTPServer) -> list[tuple[str, dict[str, Any]]]:
    return [
        (request.path, json.loads(request.get_data(as_text=True)))
        for request, _response in server.log
    ]


def test_values_equal_after_normalisation_are_not_updates(hubspot: HTTPServer) -> None:
    # HubSpot lower-cases emails and stores every property as a string.
    hubspot.expect_request("/crm/v3/objects/contacts/batch/read").respond_with_json(
        {
            "status": "COMPLETE",
            "results": [{"id": "1", "properties": {"email": "a@example.com", "age": "42"}}],
            "errors": [],
            "numErrors": 0,
        }
    )
    result = _diff([{"email": "A@Example.com", "age": 42}])

    assert result.supported
    assert result.added == [] and result.updated == []


def test_the_call_budget_makes_the_plan_unavailable_not_partial(hubspot: HTTPServer) -> None:
    records = [{"email": f"u{i}@example.com"} for i in range(250)]

    result = _diff(records, api_reads=ApiReadsConfig(max_calls=2))

    assert not result.supported
    assert "needs 3 API call(s) for 250 key(s)" in (result.fallback_reason or "")
    assert "max_calls is 2" in (result.fallback_reason or "")
    assert hubspot.log == []  # nothing was read once the budget was known to be exceeded


def test_a_failed_read_is_unavailable_and_names_only_the_error_class(
    hubspot: HTTPServer,
) -> None:
    hubspot.expect_request("/crm/v3/objects/contacts/batch/read").respond_with_data(
        "boom test-token", status=500
    )

    result = _diff([{"email": "a@example.com"}])

    assert not result.supported
    assert result.fallback_reason == "Could not read the destination (HTTPStatusError)"
    assert "test-token" not in (result.fallback_reason or "")


def test_an_ambiguous_batch_read_is_unavailable(hubspot: HTTPServer) -> None:
    # 200 with no result and no OBJECT_NOT_FOUND for the requested key: fail closed.
    hubspot.expect_request("/crm/v3/objects/contacts/batch/read").respond_with_json(
        {"status": "COMPLETE", "results": [], "errors": [], "numErrors": 0}
    )

    result = _diff([{"email": "a@example.com"}])

    assert not result.supported
    assert "Could not read the destination" in (result.fallback_reason or "")


def test_the_properties_template_is_what_is_compared(hubspot: HTTPServer) -> None:
    hubspot.expect_request("/crm/v3/objects/contacts/batch/read").respond_with_json(
        {
            "status": "COMPLETE",
            "results": [{"id": "1", "properties": {"email": "a@example.com", "firstname": "Ann"}}],
            "errors": [],
            "numErrors": 0,
        }
    )
    result = _diff(
        [{"mail": "a@example.com", "name": "Ann"}],
        config={
            "properties_template": '{"email": "{{ row.mail }}", "firstname": "{{ row.name }}"}'
        },
    )

    assert result.supported and result.added == [] and result.updated == []


def test_a_broken_template_makes_the_plan_unavailable() -> None:
    result = _diff(
        [{"mail": "a@example.com"}],
        config={"properties_template": '{"email": {{ row.mail }}'},  # invalid JSON
    )

    assert not result.supported
    assert "properties_template error on source row 0" in (result.fallback_reason or "")


def test_a_row_without_the_destination_key_is_unavailable() -> None:
    result = _diff([{"email": None, "firstname": "x"}])

    assert not result.supported
    assert "no value for the destination key ['email']" in (result.fallback_reason or "")


def test_fill_empty_is_not_planned_by_api_reads_yet() -> None:
    result = _diff([{"email": "a@example.com"}], options=_options(write_policy="fill_empty"))

    assert not result.supported
    assert "fill_empty" in (result.fallback_reason or "")


def test_without_the_opt_in_nothing_changes() -> None:
    result = compute_diff(
        [{"email": "a@example.com"}], _config(), _options(), limit=10, api_reads=None
    )

    assert not result.supported and "True diff not available" in (result.fallback_reason or "")
    assert result.api_reads is None


def test_the_plan_records_the_reads_and_marks_the_comparison_conservative(
    hubspot: HTTPServer,
) -> None:
    hubspot.expect_request("/crm/v3/objects/contacts/batch/read").respond_with_handler(
        lambda request: _read_response([], ["a@example.com"])
    )
    diff = _diff([{"email": "a@example.com"}])

    plan = build_plan(
        diff,
        sync_name="s",
        sync_mode="full",
        match_policy="upsert",
        destination="hubspot (contacts)",
        config_fingerprint="c",
        environment_fingerprint="e",
        plan_key=b"k" * 32,
        drt_version="test",
        key_columns=["email"],
    )

    document = plan.to_dict()
    assert document["api_reads"] == {"calls": 1, "rows": 1, "fidelity": "conservative"}
    assert plan.summary["create"] == 1
    assert "Vendor API read calls: 1" in render_text(plan)


# ---- configuration ----------------------------------------------------------------------


def _sync(destination: dict[str, Any], **extra: Any) -> dict[str, Any]:
    return {"name": "s", "model": "SELECT 1", "destination": destination, **extra}


_HUBSPOT = {"type": "hubspot", "id_property": "email", "auth": {"type": "bearer", "token": "t"}}


def test_the_opt_in_is_accepted_for_hubspot() -> None:
    sync = SyncConfig.model_validate(_sync(_HUBSPOT, plan={"api_reads": {"max_calls": 50}}))
    default = SyncConfig.model_validate(_sync(_HUBSPOT, plan={"api_reads": {}}))

    assert sync.plan is not None and sync.plan.api_reads == ApiReadsConfig(max_calls=50)
    assert default.plan is not None and default.plan.api_reads == ApiReadsConfig(max_calls=200)


def test_the_opt_in_is_rejected_where_it_would_be_silently_ignored() -> None:
    postgres = {
        "type": "postgres",
        "host": "h",
        "dbname": "d",
        "user": "u",
        "password": "p",
        "table": "t",
        "upsert_key": ["id"],
    }
    with pytest.raises(ValidationError, match="not available for destination type 'postgres'"):
        SyncConfig.model_validate(_sync(postgres, plan={"api_reads": {}}))


@pytest.mark.parametrize("mode", ["replace", "mirror"])
def test_deleting_modes_cannot_be_planned_by_api_reads(mode: str) -> None:
    with pytest.raises(ValidationError, match="cannot plan sync.mode replace or mirror"):
        SyncConfig.model_validate(_sync(_HUBSPOT, plan={"api_reads": {}}, sync={"mode": mode}))


def test_unknown_plan_keys_and_a_zero_budget_are_rejected() -> None:
    with pytest.raises(ValidationError):
        SyncConfig.model_validate(_sync(_HUBSPOT, plan={"api_read": {}}))
    with pytest.raises(ValidationError):
        SyncConfig.model_validate(_sync(_HUBSPOT, plan={"api_reads": {"max_calls": 0}}))


# ---- MCP is not an entry point (ADR 0018) --------------------------------------------------


def test_mcp_plan_preflight_refuses_a_sync_that_reads_through_api_calls() -> None:
    from drt.cli._plan_runner import PlanCliError
    from drt.mcp.tools.plan import API_READS_REFUSAL, _refuse_api_reads

    opted_in = SyncConfig.model_validate(_sync(_HUBSPOT, plan={"api_reads": {}}))
    ordinary = SyncConfig.model_validate(_sync(_HUBSPOT))

    with pytest.raises(PlanCliError, match="plan.api_reads spends vendor API quota"):
        _refuse_api_reads(None, None, opted_in)
    assert "ADR 0018" in API_READS_REFUSAL
    _refuse_api_reads(None, None, ordinary)  # no opt-in: untouched


def test_mcp_apply_refuses_a_stored_plan_that_carries_api_reads(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from drt.mcp.tools.apply import apply
    from drt.mcp.tools.plan import API_READS_REFUSAL

    class Ctx:
        project_dir = tmp_path

    monkeypatch.setattr(
        "drt.mcp.tools.plan.load_stored_plan",
        lambda _project, _plan_id: json.dumps({"api_reads": {"calls": 1}}),
    )

    refused = apply(Ctx(), "plan-" + "a" * 16, "alice")  # type: ignore[arg-type]

    assert refused["applied"] is False and refused["error"] == API_READS_REFUSAL


# ---- small branches -------------------------------------------------------------------------


def test_the_plan_key_comes_from_the_destination_when_api_reads_are_on() -> None:
    from drt.cli._plan_runner import _plan_key_columns

    opted_in = SyncConfig.model_validate(_sync(_HUBSPOT, plan={"api_reads": {}}))
    postgres = SyncConfig.model_validate(
        _sync(
            {
                "type": "postgres",
                "host": "h",
                "dbname": "d",
                "user": "u",
                "password": "p",
                "table": "t",
                "upsert_key": ["id", "tenant"],
            }
        )
    )

    assert _plan_key_columns(opted_in) == ["email"]
    assert _plan_key_columns(postgres) == ["id", "tenant"]


def test_a_template_that_is_not_a_json_object_is_unavailable() -> None:
    result = _diff([{"mail": "a@example.com"}], config={"properties_template": '["a"]'})

    assert not result.supported
    assert "must render a JSON object" in (result.fallback_reason or "")


def test_reading_without_a_token_is_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("HUBSPOT_TOKEN", raising=False)
    config = HubSpotDestinationConfig(
        type="hubspot", id_property="email", auth={"type": "bearer", "token_env": "HUBSPOT_TOKEN"}
    )

    result = compute_diff(
        [{"email": "a@example.com"}], config, _options(), limit=10, api_reads=ApiReadsConfig()
    )

    assert not result.supported
    assert result.fallback_reason == "Could not read the destination (ValueError)"


def test_a_destination_without_the_capability_is_unavailable_not_sampled() -> None:
    from drt.config.models import RestApiDestinationConfig

    config = RestApiDestinationConfig(type="rest_api", url="https://example.invalid/x")

    result = compute_diff([{"id": 1}], config, _options(), limit=10, api_reads=ApiReadsConfig())

    assert not result.supported
    assert "cannot be planned by API reads" in (result.fallback_reason or "")


def test_values_are_normalised_the_way_hubspot_stores_them() -> None:
    destination = hubspot_module.HubSpotDestination()

    assert destination.normalize_for_compare("flag", True) == "true"
    assert destination.normalize_for_compare("note", None) == ""
    assert destination.normalize_for_compare("amount", 12.5) == "12.5"
    assert destination.normalize_for_compare("email", "A@B.com") == "a@b.com"
    assert destination.normalize_for_compare("firstname", "Ann") == "Ann"
