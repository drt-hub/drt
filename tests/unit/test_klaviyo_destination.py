"""Unit tests for the Klaviyo destination (httpx mocked — no real Klaviyo)."""

from __future__ import annotations

import json
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

    def test_describe(self) -> None:
        assert _config().describe() == "klaviyo (profiles)"

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


class TestKlaviyoConnection:
    def test_test_connection(self) -> None:
        client = MagicMock()
        client.get.return_value = _resp(200, {"data": []})
        with _patch_client(client):
            KlaviyoDestination().test_connection(_config())
        assert "/accounts/" in client.get.call_args.args[0]

    def test_test_connection_missing_key(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        monkeypatch.delenv("KLAVIYO_NOPE", raising=False)
        monkeypatch.chdir(tmp_path)
        config = _config(api_key=None, api_key_env="KLAVIYO_NOPE")
        with pytest.raises(ValueError, match="missing api_key"):
            KlaviyoDestination().test_connection(config)
