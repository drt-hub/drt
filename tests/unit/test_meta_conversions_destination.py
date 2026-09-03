"""Tests for the Meta Conversions API destination (#1054)."""

from __future__ import annotations

import time
from unittest.mock import MagicMock, patch

import httpx
import pytest
from pydantic import ValidationError

from drt.config.models import (
    MetaConversionsDestinationConfig,
    RateLimitConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.meta_conversions import (
    MetaConversionsDestination,
    _event_time,
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
        "event_time_field": "occurred_at",
        "event_id_field": "event_id",
        "event_source_url_field": "page_url",
        "client_user_agent_field": "user_agent",
        "email_field": "email",
    }
    values.update(overrides)
    return MetaConversionsDestinationConfig.model_validate(values)


def _record(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "occurred_at": int(time.time()),
        "event_id": "conversion-1",
        "page_url": "https://example.com/products/1",
        "user_agent": "test browser",
        "email": "test@example.com",
    }
    values.update(overrides)
    return values


@pytest.mark.parametrize("email", ["test@example.com", "  Test@Example.com  "])
def test_email_hashing_normalizes_before_sha256(email: str) -> None:
    assert _hash_email(email) == EMAIL_HASH


@pytest.mark.parametrize("phone", ["14155551234", "+1 (415) 555-1234"])
def test_phone_hashing_normalizes_before_sha256(phone: str) -> None:
    assert _normalize_phone(phone) == "14155551234"
    assert _hash_phone(phone) == PHONE_HASH


def test_integer_event_id_is_coerced_to_string() -> None:
    response = httpx.Response(
        200,
        json={"events_received": 1},
        request=httpx.Request("POST", "https://graph.facebook.com/events"),
    )
    with patch("drt.destinations.meta_conversions.httpx.Client") as client_class:
        client = client_class.return_value.__enter__.return_value
        client.post.return_value = response
        result = MetaConversionsDestination().load(
            [_record(event_id=12345)], _config(), SyncOptions()
        )

    assert result.success == 1
    assert client.post.call_args.kwargs["json"]["data"][0]["event_id"] == "12345"


@pytest.mark.parametrize("event_id", [True, {"id": 12345}, [12345]])
def test_invalid_event_id_type_is_rejected_per_row(event_id: object) -> None:
    result = MetaConversionsDestination().load(
        [_record(event_id=event_id)], _config(), SyncOptions()
    )

    assert result.success == 0
    assert result.failed == 1
    assert "event id field 'event_id' must be a string or a plain number" in (
        result.row_errors[0].error_message
    )


@pytest.mark.parametrize("event_id", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_event_id_is_rejected_per_row(event_id: float) -> None:
    result = MetaConversionsDestination().load(
        [_record(event_id=event_id)], _config(), SyncOptions()
    )

    assert result.success == 0
    assert result.failed == 1
    assert "is NaN or infinite" in result.row_errors[0].error_message


@pytest.mark.parametrize(
    "email",
    [float("nan"), 12345, True, {"address": "test@example.com"}, ["test@example.com"]],
    ids=["nan", "integer", "bool", "dict", "list"],
)
def test_non_string_email_is_rejected_per_row(email: object) -> None:
    result = MetaConversionsDestination().load([_record(email=email)], _config(), SyncOptions())

    assert result.success == 0
    assert result.failed == 1
    assert "email must be a string" in result.row_errors[0].error_message


@pytest.mark.parametrize(
    "phone",
    [14155551234.0, True, {"number": "14155551234"}, ["14155551234"]],
    ids=["float", "bool", "dict", "list"],
)
def test_invalid_phone_type_is_rejected_per_row(phone: object) -> None:
    result = MetaConversionsDestination().load(
        [_record(phone=phone)],
        _config(email_field=None, phone_field="phone"),
        SyncOptions(),
    )

    assert result.success == 0
    assert result.failed == 1
    assert "phone must be a string or integer" in result.row_errors[0].error_message


def test_integer_phone_is_accepted_and_hashed() -> None:
    response = httpx.Response(
        200,
        json={"events_received": 1},
        request=httpx.Request("POST", "https://graph.facebook.com/events"),
    )
    with patch("drt.destinations.meta_conversions.httpx.Client") as client_class:
        client = client_class.return_value.__enter__.return_value
        client.post.return_value = response
        result = MetaConversionsDestination().load(
            [_record(phone=14155551234)],
            _config(email_field=None, phone_field="phone"),
            SyncOptions(),
        )

    assert result.success == 1
    user_data = client.post.call_args.kwargs["json"]["data"][0]["user_data"]
    assert user_data["ph"] == [PHONE_HASH]


def test_request_uses_bearer_auth_without_token_in_url_and_expected_payload() -> None:
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
    with (
        patch(
            "drt.destinations.meta_conversions.time.time",
            return_value=1_633_552_688 + 604_800,
        ),
        patch("drt.destinations.meta_conversions.httpx.Client") as client_class,
    ):
        client = client_class.return_value.__enter__.return_value
        client.post.return_value = response
        result = MetaConversionsDestination().load([record], config, SyncOptions())

    assert result.success == 1
    request = client.post.call_args
    assert request.args[0] == "https://graph.facebook.com/v25.0/123456789/events"
    assert "params" not in request.kwargs
    assert "access_token" not in request.args[0]
    assert request.kwargs["headers"]["Authorization"] == "Bearer meta-token"
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


def test_action_source_defaults_to_website() -> None:
    assert _config().action_source == "website"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("event_source_url_field", None),
        ("event_source_url_field", ""),
        ("event_source_url_field", "   "),
        ("client_user_agent_field", None),
        ("client_user_agent_field", ""),
        ("client_user_agent_field", "   "),
    ],
)
def test_website_action_source_requires_website_field_mappings(field: str, value: object) -> None:
    with pytest.raises(ValidationError) as exc_info:
        _config(**{field: value})

    message = str(exc_info.value)
    assert "event_source_url_field" in message
    assert "client_user_agent_field" in message
    assert "action_source: 'website'" in message


def test_non_website_action_source_does_not_require_website_field_mappings() -> None:
    config = _config(
        action_source="system_generated",
        event_source_url_field=None,
        client_user_agent_field=None,
    )

    assert config.action_source == "system_generated"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("page_url", None, "event source URL field 'page_url'"),
        ("page_url", "", "event source URL field 'page_url'"),
        ("page_url", "   ", "event source URL field 'page_url'"),
        ("user_agent", None, "client user agent field 'user_agent'"),
        ("user_agent", "", "client user agent field 'user_agent'"),
        ("user_agent", "   ", "client user agent field 'user_agent'"),
    ],
)
def test_website_fields_must_resolve_for_each_row(field: str, value: object, message: str) -> None:
    result = MetaConversionsDestination().load(
        [_record(**{field: value})], _config(), SyncOptions()
    )

    assert result.success == 0
    assert result.failed == 1
    assert message in result.row_errors[0].error_message


def test_rate_limit_key_is_shared_per_pixel_without_token_material() -> None:
    first = _config(access_token="first")
    same_pixel = _config(access_token="second")
    other_pixel = _config(pixel_id="987654321", access_token="first")

    assert first.rate_limit_key() == same_pixel.rate_limit_key()
    assert first.rate_limit_key() == "meta_conversions:123456789"
    assert other_pixel.rate_limit_key() != first.rate_limit_key()
    assert "first" not in first.rate_limit_key()


@pytest.mark.parametrize("value", [None, "", "   "], ids=["null", "empty", "blank"])
def test_event_time_field_rejects_null_empty_and_blank(value: str | None) -> None:
    # Without an explicit mapping, every row would silently be stamped with
    # the current sync time instead of its real transaction time — corrupting
    # Meta's attribution/optimization data on any backfill, delayed batch, or
    # replay (#1077). There is no compatibility default.
    with pytest.raises(ValidationError, match="event_time_field is required"):
        _config(event_time_field=value)


def test_event_time_field_is_required_when_omitted() -> None:
    values = {
        "type": "meta_conversions",
        "pixel_id": "123456789",
        "access_token": "meta-token",
        "event_name": "Purchase",
        "event_id_field": "event_id",
        "event_source_url_field": "page_url",
        "client_user_agent_field": "user_agent",
        "email_field": "email",
    }
    with pytest.raises(ValidationError, match="event_time_field"):
        MetaConversionsDestinationConfig.model_validate(values)


def test_event_time_field_is_stripped() -> None:
    assert _config(event_time_field="  occurred_at  ").event_time_field == "occurred_at"


def test_event_time_field_is_required_in_generated_json_schema() -> None:
    schema = MetaConversionsDestinationConfig.model_json_schema()
    assert "event_time_field" in schema["required"]
    assert schema["properties"]["event_time_field"]["type"] == "string"


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
            [_record(event_id=f"conversion-{index}") for index in range(2001)],
            _config(),
            SyncOptions(rate_limit=RateLimitConfig(requests_per_second=0)),
        )

    assert result.success == 2001
    assert [len(call.kwargs["json"]["data"]) for call in client.post.call_args_list] == [
        1000,
        1000,
        1,
    ]


def test_non_finite_value_fails_only_its_row() -> None:
    response = httpx.Response(
        200,
        json={"events_received": 3},
        request=httpx.Request("POST", "https://graph.facebook.com/events"),
    )
    records = [
        _record(event_id="conversion-1", amount=10),
        _record(event_id="conversion-2", amount=20),
        _record(event_id="conversion-invalid", amount=float("nan")),
        _record(event_id="conversion-3", amount=30),
    ]

    with patch("drt.destinations.meta_conversions.httpx.Client") as client_class:
        client = client_class.return_value.__enter__.return_value
        client.post.return_value = response
        result = MetaConversionsDestination().load(
            records,
            _config(value_field="amount"),
            SyncOptions(on_error="skip"),
        )

    assert result.success == 3
    assert result.failed == 1
    assert len(result.row_errors) == 1
    assert result.row_errors[0].batch_index == 2
    assert "custom_data.value nan is not JSON-serializable" in (result.row_errors[0].error_message)
    assert len(client.post.call_args.kwargs["json"]["data"]) == 3


@pytest.mark.parametrize("on_error", ["fail", "skip"])
def test_non_serializable_user_agent_fails_only_its_row(on_error: str) -> None:
    response = httpx.Response(
        200,
        json={"events_received": 1},
        request=httpx.Request("POST", "https://graph.facebook.com/events"),
    )
    records = [
        _record(event_id="conversion-valid"),
        _record(event_id="conversion-invalid", user_agent=object()),
    ]

    with patch("drt.destinations.meta_conversions.httpx.Client") as client_class:
        client = client_class.return_value.__enter__.return_value
        client.post.return_value = response
        result = MetaConversionsDestination().load(
            records,
            _config(),
            SyncOptions(on_error=on_error),
        )

    assert result.success == 1
    assert result.failed == 1
    assert len(result.row_errors) == 1
    assert result.row_errors[0].batch_index == 1
    assert "event_id 'conversion-invalid'" in result.row_errors[0].error_message
    assert "cannot be sent to Meta" in result.row_errors[0].error_message
    assert client.post.call_count == 1
    assert client.post.call_args.kwargs["json"]["data"][0]["event_id"] == ("conversion-valid")


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
        result = MetaConversionsDestination().load([_record()], _config(), options)

    assert result.success == 1
    assert client.post.call_count == 2
    assert limiter.acquire.call_count == 2


def test_meta_declared_transient_400_retries_and_succeeds() -> None:
    transient = httpx.Response(
        400,
        json={"error": {"message": "try again", "is_transient": True}},
        request=httpx.Request("POST", "https://graph.facebook.com/events"),
    )
    success = httpx.Response(
        200,
        json={"events_received": 1},
        request=httpx.Request("POST", "https://graph.facebook.com/events"),
    )
    options = SyncOptions(
        retry=RetryConfig(max_attempts=2, initial_backoff=0, max_backoff=0),
        rate_limit=RateLimitConfig(requests_per_second=0),
    )

    with patch("drt.destinations.meta_conversions.httpx.Client") as client_class:
        client = client_class.return_value.__enter__.return_value
        client.post.side_effect = [transient, success]
        result = MetaConversionsDestination().load([_record()], _config(), options)

    assert result.success == 1
    assert result.failed == 0
    assert client.post.call_count == 2


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(
            400,
            json={"error": {"message": "permanent", "is_transient": False}},
            request=httpx.Request("POST", "https://graph.facebook.com/events"),
        ),
        httpx.Response(
            400,
            json={"error": {"message": "classification missing"}},
            request=httpx.Request("POST", "https://graph.facebook.com/events"),
        ),
        httpx.Response(
            400,
            text="not JSON",
            request=httpx.Request("POST", "https://graph.facebook.com/events"),
        ),
    ],
    ids=["false", "missing", "non-json"],
)
def test_other_400_responses_do_not_retry(response: httpx.Response) -> None:
    options = SyncOptions(
        retry=RetryConfig(max_attempts=3, initial_backoff=0, max_backoff=0),
        rate_limit=RateLimitConfig(requests_per_second=0),
    )

    with patch("drt.destinations.meta_conversions.httpx.Client") as client_class:
        client = client_class.return_value.__enter__.return_value
        client.post.return_value = response
        result = MetaConversionsDestination().load([_record()], _config(), options)

    assert result.success == 0
    assert result.failed == 1
    assert client.post.call_count == 1


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
        result = MetaConversionsDestination().load(
            [_record(event_id="conversion-1"), _record(event_id="conversion-2")],
            _config(),
            SyncOptions(),
        )

    assert result.success == 0
    assert result.failed == 2
    assert all(
        "does not identify individual failures" in err.error_message for err in result.row_errors
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


@pytest.mark.parametrize(
    "record",
    [
        {"email": "test@example.com"},
        {"event_id": None, "email": "test@example.com"},
        {"event_id": "", "email": "test@example.com"},
        {"event_id": "   ", "email": "test@example.com"},
    ],
    ids=["missing", "null", "empty", "blank"],
)
def test_event_id_field_must_resolve_for_each_row(record: dict[str, object]) -> None:
    record = {"occurred_at": int(time.time()), **record}
    result = MetaConversionsDestination().load([record], _config(), SyncOptions())

    assert result.success == 0
    assert result.failed == 1
    assert "event id field 'event_id'" in result.row_errors[0].error_message


def test_at_least_one_customer_information_field_is_required() -> None:
    with pytest.raises(ValidationError) as exc_info:
        _config(
            action_source="system_generated",
            event_source_url_field=None,
            client_user_agent_field=None,
            email_field=None,
        )

    message = str(exc_info.value)
    for field in (
        "email_field",
        "phone_field",
        "client_ip_address_field",
        "client_user_agent_field",
        "fbc_field",
        "fbp_field",
    ):
        assert field in message


@pytest.mark.parametrize(
    "field",
    [
        "email_field",
        "phone_field",
        "client_ip_address_field",
        "client_user_agent_field",
        "fbc_field",
        "fbp_field",
    ],
)
def test_each_customer_information_field_satisfies_config_requirement(field: str) -> None:
    values: dict[str, object] = {
        "action_source": "system_generated",
        "event_source_url_field": None,
        "email_field": None,
        "phone_field": None,
        "client_ip_address_field": None,
        "client_user_agent_field": None,
        "fbc_field": None,
        "fbp_field": None,
        field: "identifier",
    }

    assert getattr(_config(**values), field) == "identifier"


@pytest.mark.parametrize("email", [None, "", "   "], ids=["null", "empty", "blank"])
def test_customer_information_must_resolve_for_each_row(email: object) -> None:
    result = MetaConversionsDestination().load(
        [_record(email=email)],
        _config(
            action_source="system_generated",
            event_source_url_field=None,
            client_user_agent_field=None,
        ),
        SyncOptions(),
    )

    assert result.success == 0
    assert result.failed == 1
    assert "Row missing customer information" in result.row_errors[0].error_message


def test_event_time_exactly_at_seven_day_boundary_is_accepted() -> None:
    current_time = 2_000_000_000
    timestamp = current_time - 604_800
    config = _config(event_time_field="occurred_at")

    with patch("drt.destinations.meta_conversions.time.time", return_value=current_time):
        assert _event_time({"occurred_at": timestamp}, config) == timestamp


def test_event_time_just_past_seven_day_boundary_is_rejected() -> None:
    current_time = 2_000_000_000
    timestamp = current_time - 604_801
    cutoff = current_time - 604_800
    config = _config(event_time_field="occurred_at")

    with (
        patch("drt.destinations.meta_conversions.time.time", return_value=current_time),
        pytest.raises(ValueError) as exc_info,
    ):
        _event_time({"occurred_at": timestamp}, config)

    assert str(timestamp) in str(exc_info.value)
    assert str(cutoff) in str(exc_info.value)


def test_event_time_comfortably_within_seven_day_boundary_is_accepted() -> None:
    current_time = 2_000_000_000
    timestamp = current_time - 86_400
    config = _config(event_time_field="occurred_at")

    with patch("drt.destinations.meta_conversions.time.time", return_value=current_time):
        assert _event_time({"occurred_at": timestamp}, config) == timestamp
