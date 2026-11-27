"""Replay safety of the keyed event destinations (ADR 0023 rollout item 3).

Mixpanel ``/import`` and Amplitude events send a batch in one request and every event
carries an identity the vendor deduplicates on (``$insert_id``, ``insert_id``). After a
lost response drt re-sends the *same* payload, so a replay must leave exactly one event
per identity. Mixpanel profile ``$set`` is idempotent.

Meta Conversions is deliberately NOT covered: its documented ``event_id`` deduplication
is between the browser Pixel and the server API, not between two server submissions.
"""

from __future__ import annotations

import json
import time
from typing import Any

import httpx
import pytest

from drt.config.models import RateLimitConfig, RetryConfig, SyncConfig, SyncOptions
from drt.destinations.amplitude import AmplitudeDestination
from drt.destinations.mixpanel import MixpanelDestination
from drt.destinations.retry import OperationSafety
from tests.http_fault_injection import BlindCreateFaultHarness, FaultMode, install_mock_transport


def _options() -> SyncOptions:
    return SyncOptions(
        on_error="fail",
        retry=RetryConfig(max_attempts=2, initial_backoff=0.0, backoff_multiplier=1.0),
        rate_limit=RateLimitConfig(requests_per_second=0),
    )


def _unexpected(request: httpx.Request, _harness: BlindCreateFaultHarness) -> httpx.Response:
    raise AssertionError(f"unexpected request: {request.method} {request.url}")


def _json_list(request: httpx.Request) -> dict[str, Any]:
    """Mixpanel posts a bare JSON array; wrap it so the harness can store it."""
    return {"items": json.loads(request.content)}


def _case(name: str) -> tuple[Any, dict[str, Any], dict[str, Any], str, Any, Any, Any]:
    """destination, config, record, path, decoder, dedup key, success response factory."""

    def ok(request: httpx.Request, _id: int) -> httpx.Response:
        return httpx.Response(200, json={"code": 200}, request=request)

    if name == "mixpanel_import":
        return (
            MixpanelDestination(),
            {
                "type": "mixpanel",
                "endpoint": "import_events",
                "project_id": "42",
                "service_account_username": "sa",
                "service_account_secret": "secret",
                "event_name": "signup",
            },
            {"distinct_id": "u1", "plan": "pro"},
            "/import",
            _json_list,
            lambda payload: payload["items"][0]["properties"]["$insert_id"],
            ok,
        )
    if name == "mixpanel_people":
        return (
            MixpanelDestination(),
            {"type": "mixpanel", "endpoint": "people_set", "project_token": "tok"},
            {"distinct_id": "u1", "plan": "pro"},
            "/engage",
            _json_list,
            lambda payload: payload["items"][0]["$distinct_id"],
            ok,
        )
    if name == "amplitude_event":
        return (
            AmplitudeDestination(),
            {
                "type": "amplitude",
                "endpoint": "event",
                "api_key": "key",
                "event_type": "signup",
            },
            {"user_id": "u1", "plan": "pro"},
            "/2/httpapi",
            None,
            lambda payload: payload["events"][0]["insert_id"],
            ok,
        )
    raise AssertionError(name)


_CASES = ["mixpanel_import", "mixpanel_people", "amplitude_event"]


def _load(
    name: str, mode: FaultMode
) -> tuple[Any, Any, tuple[dict[str, Any], BlindCreateFaultHarness]]:
    destination, config_data, record, path, decoder, dedup, ok = _case(name)
    config = SyncConfig.model_validate(
        {"name": "events", "model": "events", "destination": config_data}
    ).destination
    kwargs: dict[str, Any] = {"decode_request": decoder} if decoder is not None else {}
    harness = BlindCreateFaultHarness(
        path,
        mode=mode,
        fallback=_unexpected,
        create_response=ok,
        replay_mode="keyed",
        dedup_key=dedup,
        **kwargs,
    )
    return destination, config, (record, harness)


@pytest.mark.parametrize("name", _CASES)
@pytest.mark.parametrize("mode", ["commit-then-drop", "commit-then-5xx"])
def test_lost_response_replay_leaves_one_event_per_identity(
    monkeypatch: pytest.MonkeyPatch, name: str, mode: FaultMode
) -> None:
    destination, config, (record, harness) = _load(name, mode)
    install_mock_transport(monkeypatch, harness)

    result = destination.load([record], config, _options())

    assert (result.success, result.failed) == (1, 0), result.errors
    assert harness.create_attempts == 2
    assert len(harness.remote_objects) == 1


@pytest.mark.parametrize("name", _CASES)
def test_the_replay_resends_the_identical_payload(
    monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    """Dedup only works if attempt 2 carries the same identity *and* the same body."""
    destination, config, (record, harness) = _load(name, "commit-then-drop")
    install_mock_transport(monkeypatch, harness)

    destination.load([record], config, _options())

    first, second = [r for r in harness.requests if r.url.path == harness.create_path]
    assert first.content == second.content


@pytest.mark.parametrize("name", ["mixpanel_import", "amplitude_event"])
def test_derived_event_identity_is_deterministic_across_runs(
    monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    """Without a configured id the identity is a hash of the row: a later run matches."""
    ids: list[Any] = []
    for _ in range(2):
        destination, config, (record, harness) = _load(name, "commit-then-5xx")
        with monkeypatch.context() as scoped:
            install_mock_transport(scoped, harness)
            destination.load([record], config, _options())
        assert harness.dedup_key is not None
        ids.append(harness.dedup_key(harness.remote_objects[0]))

    assert ids[0] == ids[1] and ids[0]


def test_mixpanel_event_without_time_field_uses_load_time(monkeypatch: pytest.MonkeyPatch) -> None:
    """Documented limit: dedup across *runs* needs ``time_field`` (Mixpanel keys on time too)."""
    destination, config, (record, harness) = _load("mixpanel_import", "commit-then-drop")
    install_mock_transport(monkeypatch, harness)
    monkeypatch.setattr(time, "time", lambda: 1_700_000_000.0)

    destination.load([record], config, _options())

    event = harness.remote_objects[0]["items"][0]
    assert event["properties"]["time"] == 1_700_000_000


@pytest.mark.parametrize(
    ("name", "module", "expected"),
    [
        ("mixpanel_import", "drt.destinations.mixpanel", OperationSafety.KEYED),
        ("mixpanel_people", "drt.destinations.mixpanel", OperationSafety.IDEMPOTENT),
        ("amplitude_event", "drt.destinations.amplitude", OperationSafety.KEYED),
    ],
)
def test_each_write_declares_its_replay_safety_class(
    monkeypatch: pytest.MonkeyPatch, name: str, module: str, expected: OperationSafety
) -> None:
    """The fault tests above also pass on the legacy retry loop; this pins the ADR class."""
    import importlib

    seen: list[OperationSafety | None] = []
    target = importlib.import_module(module)
    real = target.with_retry

    def spy(fn: Any, config: Any, *args: Any, **kwargs: Any) -> Any:
        seen.append(kwargs.get("operation"))
        return real(fn, config, *args, **kwargs)

    monkeypatch.setattr(target, "with_retry", spy)
    destination, config, (record, harness) = _load(name, "commit-then-drop")
    install_mock_transport(monkeypatch, harness)

    destination.load([record], config, _options())

    assert seen == [expected]


def _mixpanel(**overrides: Any) -> tuple[Any, Any]:
    destination, config_data, _record, *_rest = _case("mixpanel_import")
    config_data.update(overrides)
    config = SyncConfig.model_validate(
        {"name": "events", "model": "events", "destination": config_data}
    ).destination
    return destination, config


_OVERRIDES = [
    {"distinct_id": "u1", "$insert_id": ""},
    {"distinct_id": "u1", "$insert_id": 7},
    {"distinct_id": "u1", "$insert_id": "   "},
]


@pytest.mark.parametrize("record", _OVERRIDES, ids=["empty", "non-string", "blank"])
def test_an_unusable_overridden_key_is_sent_as_before_but_not_classified_keyed(
    monkeypatch: pytest.MonkeyPatch, record: dict[str, Any]
) -> None:
    """No new rejection (vendor validation stays the vendor's); the batch just isn't KEYED."""
    import drt.destinations.mixpanel as target

    seen: list[Any] = []
    real = target.with_retry

    def spy(fn: Any, config: Any, *args: Any, **kwargs: Any) -> Any:
        seen.append(kwargs.get("operation"))
        return real(fn, config, *args, **kwargs)

    monkeypatch.setattr(target, "with_retry", spy)
    destination, config = _mixpanel()
    harness = BlindCreateFaultHarness(
        "/import",
        mode="commit-then-drop",
        fallback=_unexpected,
        create_response=lambda r, _i: httpx.Response(200, json={}, request=r),
        decode_request=_json_list,
    )
    install_mock_transport(monkeypatch, harness)

    result = destination.load([record], config, _options())

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2
    assert seen == [None]


def test_template_nulling_the_insert_id_is_not_classified_keyed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import drt.destinations.mixpanel as target

    seen: list[Any] = []
    real = target.with_retry

    def spy(fn: Any, config: Any, *args: Any, **kwargs: Any) -> Any:
        seen.append(kwargs.get("operation"))
        return real(fn, config, *args, **kwargs)

    monkeypatch.setattr(target, "with_retry", spy)
    destination, config = _mixpanel(properties_template='{"$insert_id": null}')
    harness = BlindCreateFaultHarness(
        "/import",
        mode="commit-then-5xx",
        fallback=_unexpected,
        create_response=lambda r, _i: httpx.Response(200, json={}, request=r),
        decode_request=_json_list,
    )
    install_mock_transport(monkeypatch, harness)

    result = destination.load([{"distinct_id": "u1"}], config, _options())

    assert (result.success, result.failed) == (1, 0)
    assert seen == [None]


def test_a_valid_override_from_a_column_is_accepted_and_replay_safe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, config = _mixpanel()
    harness = BlindCreateFaultHarness(
        "/import",
        mode="commit-then-drop",
        fallback=_unexpected,
        create_response=lambda r, _i: httpx.Response(200, json={}, request=r),
        replay_mode="keyed",
        dedup_key=lambda payload: payload["items"][0]["properties"]["$insert_id"],
        decode_request=_json_list,
    )
    install_mock_transport(monkeypatch, harness)

    result = destination.load([{"distinct_id": "u1", "$insert_id": "order-9"}], config, _options())

    assert (result.success, result.failed) == (1, 0)
    assert harness.create_attempts == 2 and len(harness.remote_objects) == 1
    assert harness.remote_objects[0]["items"][0]["properties"]["$insert_id"] == "order-9"


def test_a_null_passthrough_column_is_dropped_and_the_derived_id_stays(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    destination, config = _mixpanel()
    harness = BlindCreateFaultHarness(
        "/import",
        mode="commit-then-drop",
        fallback=_unexpected,
        create_response=lambda r, _i: httpx.Response(200, json={}, request=r),
        replay_mode="keyed",
        dedup_key=lambda payload: payload["items"][0]["properties"]["$insert_id"],
        decode_request=_json_list,
    )
    install_mock_transport(monkeypatch, harness)

    result = destination.load([{"distinct_id": "u1", "$insert_id": None}], config, _options())

    assert (result.success, result.failed) == (1, 0)
    assert len(harness.remote_objects[0]["items"][0]["properties"]["$insert_id"]) == 32
