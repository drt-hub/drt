"""Unit tests for the Klaviyo destination (httpx mocked — no real Klaviyo)."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from drt.config.models import (
    KlaviyoDestinationConfig,
    RateLimitConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.base import ConnectionTestable
from drt.destinations.klaviyo import KlaviyoDestination


def _config(**overrides: Any) -> KlaviyoDestinationConfig:
    data: dict[str, Any] = {"type": "klaviyo", "api_key": "pk_test"}
    data.update(overrides)
    return KlaviyoDestinationConfig(**data)


def _options(**overrides: Any) -> SyncOptions:
    data: dict[str, Any] = {
        "rate_limit": RateLimitConfig(requests_per_second=0),
        "retry": RetryConfig(max_attempts=1, initial_backoff=0.0, backoff_multiplier=1.0),
        "on_error": "skip",
    }
    data.update(overrides)
    return SyncOptions(**data)


def _resp(status: int = 200, body: dict[str, Any] | None = None) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = json.dumps(body or {})
    r.json.return_value = body or {}
    if status >= 400:
        req = httpx.Request("POST", "https://a.klaviyo.com/api/profiles/")
        resp = httpx.Response(status, text=r.text, request=req)
        r.raise_for_status.side_effect = httpx.HTTPStatusError(
            f"HTTP {status}", request=req, response=resp
        )
    else:
        r.raise_for_status.return_value = None
    return r


def _patch_client(client: MagicMock) -> Any:
    cm = MagicMock()
    cm.__enter__.return_value = client
    cm.__exit__.return_value = False
    return patch("drt.destinations.klaviyo.httpx.Client", return_value=cm)


class TestKlaviyoConfig:
    def test_valid(self) -> None:
        assert _config().email_field == "email"
        assert _config().endpoint == "profile"

    def test_event_requires_metric_name_configuration(self) -> None:
        with pytest.raises(ValueError, match="metric_name or metric_name_field"):
            _config(endpoint="event", unique_id_field="event_id")

    def test_event_requires_unique_id_field(self) -> None:
        with pytest.raises(ValueError, match="unique_id_field"):
            _config(endpoint="event", metric_name="Upgraded Plan")

    @pytest.mark.parametrize("unique_id_field", ["", "   "])
    def test_event_rejects_blank_unique_id_field(
        self, unique_id_field: str
    ) -> None:
        with pytest.raises(ValueError, match="unique_id_field"):
            _config(
                endpoint="event",
                metric_name="Upgraded Plan",
                unique_id_field=unique_id_field,
            )

    def test_describe(self) -> None:
        assert _config().describe() == "klaviyo (profiles)"
        assert _config(
            endpoint="event",
            metric_name="Upgraded Plan",
            unique_id_field="event_id",
        ).describe() == "klaviyo (events)"

    def test_missing_api_key_raises(self) -> None:
        with pytest.raises(ValueError, match="api_key"):
            KlaviyoDestinationConfig(type="klaviyo", api_key_env=None)


class TestKlaviyoLoad:
    def test_empty_records_short_circuits(self) -> None:
        assert KlaviyoDestination().load([], _config(), _options()).success == 0

    def test_create_success(self) -> None:
        client = MagicMock()
        client.post.return_value = _resp(201, {"data": {"id": "P1"}})
        with _patch_client(client):
            result = KlaviyoDestination().load(
                [{"email": "a@x.com", "plan": "pro"}], _config(), _options()
            )
        assert result.success == 1
        client.patch.assert_not_called()
        body = client.post.call_args.kwargs["json"]
        assert body["data"]["attributes"]["email"] == "a@x.com"
        assert body["data"]["attributes"]["properties"] == {"plan": "pro"}

    @pytest.mark.parametrize("endpoint", [None, "profile"])
    def test_profile_payload_is_unchanged_for_default_and_explicit_endpoint(
        self, endpoint: str | None
    ) -> None:
        client = MagicMock()
        client.post.return_value = _resp(201, {"data": {"id": "P1"}})
        config = _config() if endpoint is None else _config(endpoint=endpoint)

        with _patch_client(client):
            result = KlaviyoDestination().load(
                [{"email": "a@x.com", "plan": "pro"}], config, _options()
            )

        assert result.success == 1
        assert client.post.call_args.args == ("https://a.klaviyo.com/api/profiles/",)
        assert client.post.call_args.kwargs == {
            "headers": {
                "Authorization": "Klaviyo-API-Key pk_test",
                "revision": "2024-10-15",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            "json": {
                "data": {
                    "type": "profile",
                    "attributes": {
                        "email": "a@x.com",
                        "properties": {"plan": "pro"},
                    },
                }
            },
        }

    def test_upsert_409_patches_existing(self) -> None:
        client = MagicMock()
        client.post.return_value = _resp(
            409, {"errors": [{"meta": {"duplicate_profile_id": "P9"}}]}
        )
        client.patch.return_value = _resp(200, {"data": {"id": "P9"}})
        with _patch_client(client):
            result = KlaviyoDestination().load(
                [{"email": "a@x.com"}], _config(), _options()
            )
        assert result.success == 1
        assert "/profiles/P9/" in client.patch.call_args.args[0]
        assert client.patch.call_args.kwargs["json"]["data"]["id"] == "P9"

    def test_409_without_duplicate_id_fails(self) -> None:
        client = MagicMock()
        client.post.return_value = _resp(409, {"errors": [{"meta": {}}]})
        with _patch_client(client):
            result = KlaviyoDestination().load(
                [{"email": "a@x.com"}], _config(), _options(on_error="skip")
            )
        assert result.failed == 1
        client.patch.assert_not_called()

    def test_list_membership_added(self) -> None:
        client = MagicMock()

        def _post(url: str, **kw: Any) -> MagicMock:
            if "relationships/profiles" in url:
                return _resp(204)
            return _resp(201, {"data": {"id": "P1"}})

        client.post.side_effect = _post
        with _patch_client(client):
            result = KlaviyoDestination().load(
                [{"email": "a@x.com"}], _config(list_id="LIST1"), _options()
            )
        assert result.success == 1
        list_calls = [
            c for c in client.post.call_args_list
            if "relationships/profiles" in c.args[0]
        ]
        assert len(list_calls) == 1
        assert "/lists/LIST1/" in list_calls[0].args[0]
        assert list_calls[0].kwargs["json"] == {"data": [{"type": "profile", "id": "P1"}]}

    def test_missing_email_recorded(self) -> None:
        client = MagicMock()
        with _patch_client(client):
            result = KlaviyoDestination().load(
                [{"name": "no email"}], _config(), _options(on_error="skip")
            )
        assert result.failed == 1
        assert "email" in result.row_errors[0].error_message

    def test_http_error_on_error_skip(self) -> None:
        client = MagicMock()
        client.post.return_value = _resp(400, {"errors": [{"detail": "bad"}]})
        with _patch_client(client):
            result = KlaviyoDestination().load(
                [{"email": "a@x.com"}], _config(), _options(on_error="skip")
            )
        assert result.failed == 1
        assert result.row_errors[0].http_status == 400

    def test_on_error_fail_stops(self) -> None:
        client = MagicMock()
        client.post.return_value = _resp(500, {})
        with _patch_client(client):
            result = KlaviyoDestination().load(
                [{"email": "a@x.com"}, {"email": "b@x.com"}],
                _config(),
                _options(on_error="fail"),
            )
        assert result.failed == 1  # stopped after the first
        assert client.post.call_count == 1

    def test_properties_template(self) -> None:
        client = MagicMock()
        client.post.return_value = _resp(201, {"data": {"id": "P1"}})
        config = _config(properties_template='{"tier": "{{ row.tier }}"}')
        with _patch_client(client):
            KlaviyoDestination().load([{"email": "a@x.com", "tier": "gold"}], config, _options())
        body = client.post.call_args.kwargs["json"]
        assert body["data"]["attributes"]["properties"] == {"tier": "gold"}

    def test_non_http_error_on_error_fail_stops(self) -> None:
        # Missing email raises inside _upsert (a non-HTTP error) → generic break.
        client = MagicMock()
        with _patch_client(client):
            result = KlaviyoDestination().load(
                [{"name": "x"}, {"name": "y"}], _config(), _options(on_error="fail")
            )
        assert result.failed == 1  # stopped after the first
        client.post.assert_not_called()

    def test_create_response_without_id(self) -> None:
        # 201 with no data.id → _created_id falls back to None (no list add).
        client = MagicMock()
        client.post.return_value = _resp(201, {})
        with _patch_client(client):
            result = KlaviyoDestination().load(
                [{"email": "a@x.com"}], _config(list_id="L1"), _options()
            )
        assert result.success == 1
        assert client.post.call_count == 1  # no list-membership call (no id)

    def test_409_json_unparseable_fails(self) -> None:
        client = MagicMock()
        bad = MagicMock()
        bad.status_code = 409
        bad.text = "not json"
        bad.json.side_effect = ValueError("no json")
        req = httpx.Request("POST", "https://a.klaviyo.com/api/profiles/")
        bad.raise_for_status.side_effect = httpx.HTTPStatusError(
            "HTTP 409", request=req, response=httpx.Response(409, request=req)
        )
        client.post.return_value = bad
        with _patch_client(client):
            result = KlaviyoDestination().load(
                [{"email": "a@x.com"}], _config(), _options(on_error="skip")
            )
        assert result.failed == 1
        client.patch.assert_not_called()

    def test_missing_api_key_at_load(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        monkeypatch.delenv("KLAVIYO_NOPE", raising=False)
        monkeypatch.chdir(tmp_path)
        config = _config(api_key=None, api_key_env="KLAVIYO_NOPE")
        with pytest.raises(ValueError, match="api_key"):
            KlaviyoDestination().load([{"email": "a@x.com"}], config, _options())


class TestKlaviyoEventLoad:
    def test_event_payload_with_all_optional_fields(self) -> None:
        client = MagicMock()
        client.post.return_value = _resp(202)
        config = _config(
            endpoint="event",
            metric_name_field="event_name",
            time_field="occurred_at",
            value_field="amount",
            unique_id_field="event_id",
            properties_template='{"cart_id": "{{ row.cart_id }}"}',
        )
        record = {
            "email": "a@x.com",
            "event_name": "Abandoned Cart",
            "occurred_at": "2022-11-08T00:00:00+00:00",
            "amount": 9.99,
            "event_id": "evt-123",
            "cart_id": "cart-456",
        }

        with _patch_client(client):
            result = KlaviyoDestination().load([record], config, _options())

        assert result.success == 1
        assert client.post.call_args.args == ("https://a.klaviyo.com/api/events/",)
        assert client.post.call_args.kwargs["headers"] == {
            "Authorization": "Klaviyo-API-Key pk_test",
            "revision": "2024-10-15",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        assert client.post.call_args.kwargs["json"] == {
            "data": {
                "type": "event",
                "attributes": {
                    "properties": {"cart_id": "cart-456"},
                    "metric": {
                        "data": {
                            "type": "metric",
                            "attributes": {"name": "Abandoned Cart"},
                        }
                    },
                    "profile": {
                        "data": {
                            "type": "profile",
                            "attributes": {"email": "a@x.com"},
                        }
                    },
                    "time": "2022-11-08T00:00:00+00:00",
                    "value": 9.99,
                    "unique_id": "evt-123",
                },
            }
        }

    @pytest.mark.parametrize(
        "occurred_at",
        [datetime(2022, 11, 8, tzinfo=timezone.utc), date(2022, 11, 8)],
    )
    def test_event_normalizes_warehouse_types_and_excludes_control_fields(
        self, occurred_at: datetime | date
    ) -> None:
        client = MagicMock()
        client.post.return_value = _resp(202)
        config = _config(
            endpoint="event",
            metric_name_field="event_name",
            time_field="occurred_at",
            value_field="amount",
            unique_id_field="event_id",
        )
        record = {
            "email": "a@x.com",
            "event_name": "Placed Order",
            "occurred_at": occurred_at,
            "amount": Decimal("12.34"),
            "event_id": Decimal("123"),
            "plan": "pro",
        }

        with _patch_client(client):
            result = KlaviyoDestination().load([record], config, _options())

        assert result.success == 1
        body = client.post.call_args.kwargs["json"]
        attributes = body["data"]["attributes"]
        assert attributes["time"] == occurred_at.isoformat()
        assert attributes["value"] == 12.34
        assert attributes["unique_id"] == "123"
        assert attributes["properties"] == {"plan": "pro"}
        json.dumps(body)  # httpx's json= encoding must accept the final payload.

    @pytest.mark.parametrize(
        ("config_field", "payload_key"),
        [
            ("time_field", "time"),
            ("value_field", "value"),
        ],
    )
    def test_event_optional_field_is_independently_omitted_when_unconfigured(
        self, config_field: str, payload_key: str
    ) -> None:
        client = MagicMock()
        client.post.return_value = _resp(202)
        config_values: dict[str, Any] = {
            "endpoint": "event",
            "metric_name": "Upgraded Plan",
            "time_field": "occurred_at",
            "value_field": "amount",
            "unique_id_field": "event_id",
        }
        config_values[config_field] = None
        config = _config(**config_values)
        record = {
            "email": "a@x.com",
            "occurred_at": "2022-11-08T00:00:00+00:00",
            "amount": 9.99,
            "event_id": "evt-123",
        }

        with _patch_client(client):
            result = KlaviyoDestination().load([record], config, _options())

        assert result.success == 1
        attributes = client.post.call_args.kwargs["json"]["data"]["attributes"]
        assert payload_key not in attributes

    def test_event_optional_fields_with_null_values_are_omitted(self) -> None:
        client = MagicMock()
        client.post.return_value = _resp(202)
        config = _config(
            endpoint="event",
            metric_name="Upgraded Plan",
            time_field="occurred_at",
            value_field="amount",
            unique_id_field="event_id",
        )

        with _patch_client(client):
            result = KlaviyoDestination().load(
                [
                    {
                        "email": "a@x.com",
                        "occurred_at": None,
                        "amount": None,
                        "event_id": "evt-123",
                    }
                ],
                config,
                _options(),
            )

        assert result.success == 1
        attributes = client.post.call_args.kwargs["json"]["data"]["attributes"]
        assert "time" not in attributes
        assert "value" not in attributes
        assert attributes["unique_id"] == "evt-123"

    @pytest.mark.parametrize(
        "record",
        [
            {"email": "a@x.com"},
            {"email": "a@x.com", "event_id": None},
            {"email": "a@x.com", "event_id": ""},
            {"email": "a@x.com", "event_id": "  "},
        ],
    )
    def test_event_missing_or_blank_unique_id_is_recorded(
        self, record: dict[str, Any]
    ) -> None:
        client = MagicMock()
        config = _config(
            endpoint="event",
            metric_name="Upgraded Plan",
            unique_id_field="event_id",
        )

        with _patch_client(client):
            result = KlaviyoDestination().load([record], config, _options())

        assert result.success == 0
        assert result.failed == 1
        assert "unique_id field 'event_id'" in result.row_errors[0].error_message
        client.post.assert_not_called()
        client.request.assert_not_called()

    def test_event_sends_empty_properties_when_row_has_no_properties(self) -> None:
        client = MagicMock()
        client.post.return_value = _resp(202)
        config = _config(
            endpoint="event",
            metric_name="Upgraded Plan",
            unique_id_field="event_id",
        )

        with _patch_client(client):
            result = KlaviyoDestination().load(
                [{"email": "a@x.com", "event_id": "evt-123"}], config, _options()
            )

        assert result.success == 1
        attributes = client.post.call_args.kwargs["json"]["data"]["attributes"]
        assert attributes["properties"] == {}

    def test_event_retry_acquires_rate_limit_for_each_attempt(self) -> None:
        client = MagicMock()
        client.post.side_effect = [_resp(500), _resp(202)]
        limiter = MagicMock()
        config = _config(
            endpoint="event",
            metric_name="Upgraded Plan",
            unique_id_field="event_id",
        )
        options = _options(
            retry=RetryConfig(
                max_attempts=2,
                initial_backoff=0.0,
                backoff_multiplier=1.0,
            )
        )

        with (
            patch(
                "drt.destinations.klaviyo.resolve_rate_limiter",
                return_value=limiter,
            ),
            _patch_client(client),
        ):
            result = KlaviyoDestination().load(
                [{"email": "a@x.com", "event_id": "evt-123"}], config, options
            )

        assert result.success == 1
        assert client.post.call_count == 2
        assert limiter.acquire.call_count == 2

    def test_event_missing_metric_name_is_recorded(self) -> None:
        client = MagicMock()
        config = _config(
            endpoint="event",
            metric_name_field="event_name",
            unique_id_field="event_id",
        )

        with _patch_client(client):
            result = KlaviyoDestination().load(
                [{"email": "a@x.com"}], config, _options(on_error="skip")
            )

        assert result.failed == 1
        assert "metric name" in result.row_errors[0].error_message
        client.post.assert_not_called()


class TestKlaviyoConnection:
    def test_declares_connection_testable(self) -> None:
        """The destination exposes its least-privilege connectivity probe."""
        assert isinstance(KlaviyoDestination(), ConnectionTestable)

    def test_test_connection_succeeds_on_2xx(self) -> None:
        client = MagicMock()
        client.get.return_value = _resp(200, {"data": []})
        with _patch_client(client):
            KlaviyoDestination().test_connection(_config())
        assert "/accounts/" in client.get.call_args.args[0]

    def test_test_connection_accepts_permission_denied_403(self) -> None:
        client = MagicMock()
        response = _resp(403, {"errors": [{"code": "permission_denied"}]})
        client.get.return_value = response
        with _patch_client(client):
            KlaviyoDestination().test_connection(_config())
        response.raise_for_status.assert_not_called()

    @pytest.mark.parametrize(
        ("status", "body"),
        [
            (403, {"errors": [{"code": "not_found"}]}),
            (403, {"unexpected": "body"}),
            (401, {"errors": [{"code": "not_authenticated"}]}),
        ],
    )
    def test_test_connection_rejects_other_http_errors(
        self, status: int, body: dict[str, Any]
    ) -> None:
        client = MagicMock()
        client.get.return_value = _resp(status, body)
        with _patch_client(client), pytest.raises(httpx.HTTPStatusError):
            KlaviyoDestination().test_connection(_config())

    def test_test_connection_rejects_403_with_unparseable_body(self) -> None:
        """_permission_denied()'s except-branch (a 403 whose body isn't even
        valid JSON) must fall through to a genuine failure, not silently
        swallow the parse error as if it were a benign permission_denied."""
        client = MagicMock()
        req = httpx.Request("GET", "https://a.klaviyo.com/api/accounts/")
        http_resp = httpx.Response(403, content=b"not json", request=req)
        response = MagicMock()
        response.status_code = 403
        response.json.side_effect = ValueError("not valid json")
        response.raise_for_status.side_effect = httpx.HTTPStatusError(
            "HTTP 403", request=req, response=http_resp
        )
        client.get.return_value = response
        with _patch_client(client), pytest.raises(httpx.HTTPStatusError):
            KlaviyoDestination().test_connection(_config())

    def test_test_connection_propagates_network_error(self) -> None:
        client = MagicMock()
        client.get.side_effect = httpx.ConnectError("no route")
        with _patch_client(client), pytest.raises(httpx.ConnectError, match="no route"):
            KlaviyoDestination().test_connection(_config())

    def test_test_connection_missing_key(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        monkeypatch.delenv("KLAVIYO_NOPE", raising=False)
        monkeypatch.chdir(tmp_path)
        config = _config(api_key=None, api_key_env="KLAVIYO_NOPE")
        with pytest.raises(ValueError, match="missing api_key"):
            KlaviyoDestination().test_connection(config)
