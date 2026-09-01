"""Tests for the Meta Conversions API destination (#1054)."""

from __future__ import annotations

import io
import logging
from unittest.mock import MagicMock, patch

import httpx
import pytest
from pydantic import ValidationError

from drt.cli._logging import _configure_json_logging
from drt.config.models import (
    MetaConversionsDestinationConfig,
    RateLimitConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.meta_conversions import (
    MetaConversionsDestination,
    _hash_email,
    _hash_phone,
    _normalize_phone,
)

EMAIL_HASH = "973dfe463ec85785f5f95af5ba3906eedb2d931c24e69824a89ea65dba4e813b"
PHONE_HASH = "c6a349dfaaf5c3a368d3135014cc1bc7aebf18f654f313f9c1d0b018a897b209"


def _config(**overrides: object) -> MetaConversionsDestinationConfig:
    values: dict[str, object] = {
        "type": "meta_conversions",
        "pixel_id": "123456789",
        "access_token": "meta-token",
        "event_name": "Purchase",
        "event_id_field": "event_id",
    }
    values.update(overrides)
    return MetaConversionsDestinationConfig.model_validate(values)


@pytest.mark.parametrize("email", ["test@example.com", "  Test@Example.com  "])
def test_email_hashing_normalizes_before_sha256(email: str) -> None:
    assert _hash_email(email) == EMAIL_HASH


@pytest.mark.parametrize("phone", ["14155551234", "+1 (415) 555-1234"])
def test_phone_hashing_normalizes_before_sha256(phone: str) -> None:
    assert _normalize_phone(phone) == "14155551234"
    assert _hash_phone(phone) == PHONE_HASH


def test_request_uses_query_param_auth_and_expected_payload() -> None:
    config = _config(
        event_time_field="occurred_at",
        event_id_field="id",
        event_source_url_field="url",
        email_field="email",
        phone_field="phone",
        client_ip_address_field="ip",
        client_user_agent_field="ua",
        fbc_field="fbc",
        fbp_field="fbp",
        value_field="amount",
    )
    record = {
        "occurred_at": 1_633_552_688,
        "id": "event.id.123",
        "url": "http://example.com/product/123",
        "email": "  Test@Example.com  ",
        "phone": "+1 (415) 555-1234",
        "ip": "192.19.9.9",
        "ua": "test ua",
        "fbc": "fb.1.click",
        "fbp": "fb.1.browser",
        "amount": 100.2,
    }

    response = httpx.Response(
        200,
        json={"events_received": 1, "messages": [], "fbtrace_id": "trace"},
        request=httpx.Request("POST", "https://graph.facebook.com/events"),
    )
    with patch("drt.destinations.meta_conversions.httpx.Client") as client_class:
        client = client_class.return_value.__enter__.return_value
        client.post.return_value = response
        result = MetaConversionsDestination().load([record], config, SyncOptions())

    assert result.success == 1
    request = client.post.call_args
    assert request.args[0] == "https://graph.facebook.com/v25.0/123456789/events"
    assert request.kwargs["params"] == {"access_token": "meta-token"}
    assert "Authorization" not in request.kwargs["headers"]
    event = request.kwargs["json"]["data"][0]
    assert event == {
        "event_name": "Purchase",
        "event_time": 1_633_552_688,
        "event_id": "event.id.123",
        "event_source_url": "http://example.com/product/123",
        "action_source": "website",
        "user_data": {
            "em": [EMAIL_HASH],
            "ph": [PHONE_HASH],
            "client_ip_address": "192.19.9.9",
            "client_user_agent": "test ua",
            "fbc": "fb.1.click",
            "fbp": "fb.1.browser",
        },
        "custom_data": {"value": 100.2, "currency": "USD"},
    }


def test_json_logging_does_not_emit_query_param_access_token() -> None:
    token = "SUPERSECRET_META_TOKEN"
    stream = io.StringIO()
    root = logging.root
    httpx_logger = logging.getLogger("httpx")
    previous_handlers = root.handlers[:]
    previous_root_level = root.level
    previous_httpx_level = httpx_logger.level
    real_client = httpx.Client
    transport = httpx.MockTransport(
        lambda _request: httpx.Response(200, json={"events_received": 1})
    )

    try:
        _configure_json_logging()
        root.handlers[0].setStream(stream)
        with patch(
            "drt.destinations.meta_conversions.httpx.Client",
            side_effect=lambda **kwargs: real_client(transport=transport, **kwargs),
        ):
            result = MetaConversionsDestination().load(
                [{"event_id": "conversion-1"}],
                _config(access_token=token),
                SyncOptions(),
            )
    finally:
        root.handlers = previous_handlers
        root.setLevel(previous_root_level)
        httpx_logger.setLevel(previous_httpx_level)

    assert result.success == 1
    assert token not in stream.getvalue()


def test_action_source_defaults_to_website() -> None:
    assert _config().action_source == "website"


def test_rate_limit_key_is_shared_per_pixel_without_token_material() -> None:
    first = _config(access_token="first")
    same_pixel = _config(access_token="second")
    other_pixel = _config(pixel_id="987654321", access_token="first")

    assert first.rate_limit_key() == same_pixel.rate_limit_key()
    assert first.rate_limit_key() == "meta_conversions:123456789"
    assert other_pixel.rate_limit_key() != first.rate_limit_key()
    assert "first" not in first.rate_limit_key()


def test_event_time_defaults_to_current_unix_seconds() -> None:
    response = httpx.Response(
        200,
        json={"events_received": 1},
        request=httpx.Request("POST", "https://graph.facebook.com/events"),
    )
    with (
        patch("drt.destinations.meta_conversions.time.time", return_value=1_700_000_000.9),
        patch("drt.destinations.meta_conversions.httpx.Client") as client_class,
    ):
        client = client_class.return_value.__enter__.return_value
        client.post.return_value = response
        MetaConversionsDestination().load([{}], _config(), SyncOptions())

    assert client.post.call_args.kwargs["json"]["data"][0]["event_time"] == 1_700_000_000


def test_batches_at_most_1000_events_per_request() -> None:
    responses = [
        httpx.Response(
            200,
            json={"events_received": count},
            request=httpx.Request("POST", "https://graph.facebook.com/events"),
        )
        for count in (1000, 1000, 1)
    ]
    with patch("drt.destinations.meta_conversions.httpx.Client") as client_class:
        client = client_class.return_value.__enter__.return_value
        client.post.side_effect = responses
        result = MetaConversionsDestination().load(
            [{} for _ in range(2001)],
            _config(),
            SyncOptions(rate_limit=RateLimitConfig(requests_per_second=0)),
        )

    assert result.success == 2001
    assert [len(call.kwargs["json"]["data"]) for call in client.post.call_args_list] == [
        1000,
        1000,
        1,
    ]


def test_retry_acquires_rate_limiter_for_every_attempt() -> None:
    transient = httpx.Response(
        503,
        request=httpx.Request("POST", "https://graph.facebook.com/events"),
    )
    success = httpx.Response(
        200,
        json={"events_received": 1},
        request=httpx.Request("POST", "https://graph.facebook.com/events"),
    )
    limiter = MagicMock()
    options = SyncOptions(
        retry=RetryConfig(max_attempts=2, initial_backoff=0, max_backoff=0),
        rate_limit=RateLimitConfig(requests_per_second=0),
    )

    with (
        patch("drt.destinations.meta_conversions.resolve_rate_limiter", return_value=limiter),
        patch("drt.destinations.meta_conversions.httpx.Client") as client_class,
    ):
        client = client_class.return_value.__enter__.return_value
        client.post.side_effect = [transient, success]
        result = MetaConversionsDestination().load([{}], _config(), options)

    assert result.success == 1
    assert client.post.call_count == 2
    assert limiter.acquire.call_count == 2


@pytest.mark.parametrize(
    "response_data",
    [
        {},
        {"events_received": None},
        {"events_received": "0"},
        {"events_received": True},
        {"events_received": 1, "messages": ["one event rejected"]},
    ],
    ids=["missing", "null", "string", "bool", "wrong-integer-count"],
)
def test_invalid_events_received_fails_the_whole_batch(
    response_data: dict[str, object],
) -> None:
    response = httpx.Response(
        200,
        json=response_data,
        request=httpx.Request("POST", "https://graph.facebook.com/events"),
    )
    with patch("drt.destinations.meta_conversions.httpx.Client") as client_class:
        client_class.return_value.__enter__.return_value.post.return_value = response
        result = MetaConversionsDestination().load([{}, {}], _config(), SyncOptions())

    assert result.success == 0
    assert result.failed == 2
    assert all(
        "does not identify individual failures" in err.error_message
        for err in result.row_errors
    )


def test_event_name_field_must_resolve_for_each_row() -> None:
    result = MetaConversionsDestination().load(
        [{}], _config(event_name=None, event_name_field="kind"), SyncOptions()
    )
    assert result.failed == 1
    assert "event name field 'kind'" in result.row_errors[0].error_message


@pytest.mark.parametrize(
    "values",
    [
        {"event_name": None, "event_name_field": None},
        {"event_name": "Purchase", "event_name_field": "kind"},
    ],
)
def test_exactly_one_event_name_source_is_required(values: dict[str, str | None]) -> None:
    with pytest.raises(ValidationError, match="exactly one of event_name or event_name_field"):
        _config(**values)


def test_event_id_field_is_required_for_retry_deduplication() -> None:
    with pytest.raises(ValidationError, match="event_id_field is required"):
        _config(event_id_field=None)
