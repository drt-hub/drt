"""Fault-injection coverage for ADR 0023 rollout step 5's unkeyed paths."""

from __future__ import annotations

import json
import logging
from typing import Any

import httpx
import pytest

from drt.config.models import (
    AirtableDestinationConfig,
    ElasticsearchDestinationConfig,
    GoogleAdsDestinationConfig,
    RateLimitConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.airtable import AirtableDestination
from drt.destinations.elasticsearch import ElasticsearchDestination
from drt.destinations.google_ads import GoogleAdsDestination
from tests.http_fault_injection import BlindCreateFaultHarness, FaultMode, install_mock_transport


def _options(*, on_error: str = "skip", batch_size: int = 100) -> SyncOptions:
    return SyncOptions(
        on_error=on_error,
        batch_size=batch_size,
        retry=RetryConfig(max_attempts=2, initial_backoff=0.0, backoff_multiplier=1.0),
        rate_limit=RateLimitConfig(requests_per_second=0),
    )


def _unexpected(request: httpx.Request, _harness: BlindCreateFaultHarness) -> httpx.Response:
    raise AssertionError(f"unexpected request: {request.method} {request.url}")


def _airtable_config(**overrides: Any) -> AirtableDestinationConfig:
    data: dict[str, Any] = {
        "type": "airtable",
        "access_token": "token",
        "base_id": "app123",
        "table_name": "Customers",
    }
    data.update(overrides)
    return AirtableDestinationConfig(**data)


def _airtable_response(request: httpx.Request, _object_id: int) -> httpx.Response:
    return httpx.Response(200, json={"records": []}, request=request)


def _airtable_key(payload: dict[str, Any]) -> object:
    return tuple(record["fields"]["warehouse_id"] for record in payload["records"])


@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
def test_airtable_append_delivery_unknown_is_not_replayed(
    monkeypatch: pytest.MonkeyPatch, mode: FaultMode
) -> None:
    harness = BlindCreateFaultHarness(
        "/v0/app123/Customers",
        mode=mode,
        fallback=_unexpected,
        create_response=_airtable_response,
    )
    install_mock_transport(monkeypatch, harness)

    result = AirtableDestination().load([{"name": "Ada"}], _airtable_config(), _options())

    assert (result.success, result.failed) == (0, 1)
    assert harness.create_attempts == 1
    assert len(harness.remote_objects) == 1
    assert "delivery is unknown" in result.row_errors[0].error_message


@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
def test_airtable_upsert_replays_the_identical_keyed_payload(
    monkeypatch: pytest.MonkeyPatch, mode: FaultMode
) -> None:
    harness = BlindCreateFaultHarness(
        "/v0/app123/Customers",
        create_method="PATCH",
        mode=mode,
        fallback=_unexpected,
        create_response=_airtable_response,
        replay_mode="keyed",
        dedup_key=_airtable_key,
    )
    install_mock_transport(monkeypatch, harness)

    result = AirtableDestination().load(
        [{"warehouse_id": "customer-1", "name": "Ada"}],
        _airtable_config(primary_key="warehouse_id"),
        _options(),
    )

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 1
    assert harness.requests[0].content == harness.requests[1].content


def test_airtable_retry_creates_restores_blind_replay(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    harness = BlindCreateFaultHarness(
        "/v0/app123/Customers",
        mode="commit-then-drop",
        fallback=_unexpected,
        create_response=_airtable_response,
    )
    install_mock_transport(monkeypatch, harness)
    caplog.set_level(logging.WARNING)

    result = AirtableDestination().load(
        [{"name": "Ada"}], _airtable_config(retry_creates=True), _options()
    )

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 2
    assert "retry_creates=true" in caplog.text


def _decode_bulk(request: httpx.Request) -> dict[str, Any]:
    lines = [json.loads(line) for line in request.content.decode().strip().splitlines()]
    return {"operations": lines}


def _bulk_response(request: httpx.Request, _object_id: int) -> httpx.Response:
    operation_count = len(request.content.decode().strip().splitlines()) // 2
    return httpx.Response(
        200,
        json={
            "errors": False,
            "items": [{"index": {"status": 201}} for _ in range(operation_count)],
        },
        request=request,
    )


def _bulk_key(payload: dict[str, Any]) -> object:
    actions = payload["operations"][::2]
    return tuple(next(iter(action.values()))["_id"] for action in actions)


def _elasticsearch_config(**overrides: Any) -> ElasticsearchDestinationConfig:
    data: dict[str, Any] = {
        "type": "elasticsearch",
        "url": "https://search.test",
        "index": "customers",
        "api_key": "key",
    }
    data.update(overrides)
    return ElasticsearchDestinationConfig(**data)


@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
def test_elasticsearch_auto_id_delivery_unknown_is_not_replayed(
    monkeypatch: pytest.MonkeyPatch, mode: FaultMode
) -> None:
    harness = BlindCreateFaultHarness(
        "/_bulk",
        mode=mode,
        fallback=_unexpected,
        create_response=_bulk_response,
        decode_request=_decode_bulk,
    )
    install_mock_transport(monkeypatch, harness)

    result = ElasticsearchDestination().load([{"name": "Ada"}], _elasticsearch_config(), _options())

    assert (result.success, result.failed) == (0, 1)
    assert harness.create_attempts == 1
    assert len(harness.remote_objects) == 1
    assert "delivery is unknown" in result.row_errors[0].error_message


def test_one_auto_id_makes_the_whole_elasticsearch_bulk_request_blind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    harness = BlindCreateFaultHarness(
        "/_bulk",
        mode="commit-then-drop",
        fallback=_unexpected,
        create_response=_bulk_response,
        decode_request=_decode_bulk,
    )
    install_mock_transport(monkeypatch, harness)

    result = ElasticsearchDestination().load(
        [{"customer_id": "customer-1"}, {"name": "missing id"}],
        _elasticsearch_config(id_field="customer_id"),
        _options(),
    )

    assert (result.success, result.failed) == (0, 2)
    assert harness.create_attempts == 1
    assert [error.batch_index for error in result.row_errors] == [0, 1]


@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
def test_elasticsearch_id_field_replays_the_identical_keyed_payload(
    monkeypatch: pytest.MonkeyPatch, mode: FaultMode
) -> None:
    harness = BlindCreateFaultHarness(
        "/_bulk",
        mode=mode,
        fallback=_unexpected,
        create_response=_bulk_response,
        replay_mode="keyed",
        dedup_key=_bulk_key,
        decode_request=_decode_bulk,
    )
    install_mock_transport(monkeypatch, harness)

    result = ElasticsearchDestination().load(
        [{"customer_id": "customer-1", "name": "Ada"}],
        _elasticsearch_config(id_field="customer_id"),
        _options(),
    )

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 1
    assert harness.requests[0].content == harness.requests[1].content


def test_elasticsearch_retry_creates_restores_blind_replay(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    harness = BlindCreateFaultHarness(
        "/_bulk",
        mode="commit-then-drop",
        fallback=_unexpected,
        create_response=_bulk_response,
        decode_request=_decode_bulk,
    )
    install_mock_transport(monkeypatch, harness)
    caplog.set_level(logging.WARNING)

    result = ElasticsearchDestination().load(
        [{"name": "Ada"}], _elasticsearch_config(retry_creates=True), _options()
    )

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 2
    assert "retry_creates=true" in caplog.text


def _google_ads_config(**overrides: Any) -> GoogleAdsDestinationConfig:
    data: dict[str, Any] = {
        "type": "google_ads",
        "customer_id": "1234567890",
        "conversion_action": "customers/1234567890/conversionActions/987",
    }
    data.update(overrides)
    return GoogleAdsDestinationConfig(**data)


def _google_ads_response(request: httpx.Request, _object_id: int) -> httpx.Response:
    payload = json.loads(request.content)
    return httpx.Response(
        200,
        json={"results": [{} for _ in payload["conversions"]]},
        request=request,
    )


def _order_id_key(payload: dict[str, Any]) -> object:
    return tuple(conversion["orderId"] for conversion in payload["conversions"])


def _patch_google_ads_base(monkeypatch: pytest.MonkeyPatch) -> str:
    from drt.destinations import google_ads

    monkeypatch.setattr(google_ads, "_BASE_URL", "https://google-ads.test")
    return "/v25/customers/1234567890:uploadClickConversions"


@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
def test_google_ads_without_order_id_delivery_unknown_is_not_replayed(
    monkeypatch: pytest.MonkeyPatch, mode: FaultMode
) -> None:
    path = _patch_google_ads_base(monkeypatch)
    harness = BlindCreateFaultHarness(
        path,
        mode=mode,
        fallback=_unexpected,
        create_response=_google_ads_response,
    )
    install_mock_transport(monkeypatch, harness)

    result = GoogleAdsDestination().load(
        [{"gclid": "click-1", "conversion_time": "2026-01-01 12:00:00"}],
        _google_ads_config(),
        _options(),
    )

    assert (result.success, result.failed) == (0, 1)
    assert harness.create_attempts == 1
    assert len(harness.remote_objects) == 1
    assert "delivery is unknown" in result.row_errors[0].error_message


def test_one_empty_order_id_makes_the_whole_google_ads_request_blind(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    path = _patch_google_ads_base(monkeypatch)
    harness = BlindCreateFaultHarness(
        path,
        mode="commit-then-drop",
        fallback=_unexpected,
        create_response=_google_ads_response,
    )
    install_mock_transport(monkeypatch, harness)

    result = GoogleAdsDestination().load(
        [
            {
                "gclid": "click-1",
                "conversion_time": "2026-01-01 12:00:00",
                "order_id": "order-1",
            },
            {
                "gclid": "click-2",
                "conversion_time": "2026-01-01 12:00:00",
                "order_id": "",
            },
        ],
        _google_ads_config(native_idempotency_key="{{ row.order_id }}"),
        _options(),
    )

    assert (result.success, result.failed) == (0, 2)
    assert harness.create_attempts == 1
    assert [error.batch_index for error in result.row_errors] == [0, 1]


@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
def test_google_ads_order_id_replays_the_identical_keyed_payload(
    monkeypatch: pytest.MonkeyPatch, mode: FaultMode
) -> None:
    path = _patch_google_ads_base(monkeypatch)
    harness = BlindCreateFaultHarness(
        path,
        mode=mode,
        fallback=_unexpected,
        create_response=_google_ads_response,
        replay_mode="keyed",
        dedup_key=_order_id_key,
    )
    install_mock_transport(monkeypatch, harness)

    result = GoogleAdsDestination().load(
        [
            {
                "gclid": "click-1",
                "conversion_time": "2026-01-01 12:00:00",
                "order_id": "order-1",
            }
        ],
        _google_ads_config(native_idempotency_key="{{ row.order_id }}"),
        _options(),
    )

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 1
    assert harness.requests[0].content == harness.requests[1].content


def test_google_ads_retry_creates_restores_blind_replay(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    path = _patch_google_ads_base(monkeypatch)
    harness = BlindCreateFaultHarness(
        path,
        mode="commit-then-drop",
        fallback=_unexpected,
        create_response=_google_ads_response,
    )
    install_mock_transport(monkeypatch, harness)
    caplog.set_level(logging.WARNING)

    result = GoogleAdsDestination().load(
        [{"gclid": "click-1", "conversion_time": "2026-01-01 12:00:00"}],
        _google_ads_config(retry_creates=True),
        _options(),
    )

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 2
    assert "retry_creates=true" in caplog.text


@pytest.mark.parametrize("fault", ["429", "connect"])
@pytest.mark.parametrize("destination", ["airtable", "elasticsearch", "google_ads"])
def test_unkeyed_429_and_connect_errors_still_retry(
    monkeypatch: pytest.MonkeyPatch, destination: str, fault: str
) -> None:
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        if len(requests) == 1:
            if fault == "429":
                return httpx.Response(
                    429,
                    headers={"Retry-After": "0"},
                    request=request,
                )
            raise httpx.ConnectError("connect failed before send", request=request)
        if destination == "elasticsearch":
            return _bulk_response(request, 1)
        if destination == "google_ads":
            return _google_ads_response(request, 1)
        return _airtable_response(request, 1)

    install_mock_transport(monkeypatch, handler)
    if destination == "airtable":
        result = AirtableDestination().load([{"name": "Ada"}], _airtable_config(), _options())
    elif destination == "elasticsearch":
        result = ElasticsearchDestination().load(
            [{"name": "Ada"}], _elasticsearch_config(), _options()
        )
    else:
        _patch_google_ads_base(monkeypatch)
        result = GoogleAdsDestination().load(
            [{"gclid": "click-1", "conversion_time": "2026-01-01 12:00:00"}],
            _google_ads_config(),
            _options(),
        )

    assert (result.success, result.failed) == (1, 0)
    assert len(requests) == 2


def test_fail_fast_marks_only_later_requests_unattempted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, text="bad request", request=request)

    install_mock_transport(monkeypatch, handler)
    records = [{"row": index} for index in range(12)]

    result = AirtableDestination().load(records, _airtable_config(), _options(on_error="fail"))

    assert result.failed == 10
    assert result.skipped == 2
    assert result.unattempted == [10, 11]


def test_elasticsearch_partial_bulk_failure_accounts_for_the_whole_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    responses = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal responses
        responses += 1
        if responses == 1:
            return httpx.Response(
                200,
                json={
                    "errors": True,
                    "items": [
                        {"index": {"status": 400, "error": {"reason": "bad"}}},
                        {"index": {"status": 201}},
                    ],
                },
                request=request,
            )
        return _bulk_response(request, 1)

    install_mock_transport(monkeypatch, handler)
    result = ElasticsearchDestination().load(
        [{"id": 1}, {"id": 2}, {"id": 3}],
        _elasticsearch_config(id_field="id"),
        _options(on_error="fail", batch_size=2),
    )

    assert (result.success, result.failed, result.skipped) == (1, 1, 1)
    assert result.unattempted == [2]
    assert responses == 1


def test_google_ads_fail_fast_marks_later_unbuilt_rows_unattempted() -> None:
    result = GoogleAdsDestination().load(
        [
            {"gclid": "", "conversion_time": "2026-01-01 12:00:00"},
            {"gclid": "click-2", "conversion_time": "2026-01-01 12:00:00"},
        ],
        _google_ads_config(),
        _options(on_error="fail"),
    )

    assert (result.success, result.failed, result.skipped) == (0, 1, 1)
    assert result.unattempted == [1]
