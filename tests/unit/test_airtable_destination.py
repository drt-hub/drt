"""Unit tests for the Airtable destination (httpx mocked — no real Airtable)."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import httpx
import pytest

from drt.config.models import (
    AirtableDestinationConfig,
    RateLimitConfig,
    RetryConfig,
    SyncOptions,
)
from drt.destinations.airtable import AirtableDestination


def _config(**overrides: Any) -> AirtableDestinationConfig:
    data: dict[str, Any] = {
        "type": "airtable",
        "access_token": "test-token",
        "base_id": "appABC",
        "table_name": "Customers",
    }
    data.update(overrides)
    return AirtableDestinationConfig(**data)


def _options(**overrides: Any) -> SyncOptions:
    data: dict[str, Any] = {
        "rate_limit": RateLimitConfig(requests_per_second=0),
        "retry": RetryConfig(max_attempts=1, initial_backoff=0.0, backoff_multiplier=1.0),
        "on_error": "skip",
    }
    data.update(overrides)
    return SyncOptions(**data)


def _ok() -> MagicMock:
    r = MagicMock()
    r.status_code = 200
    r.text = "{}"
    r.raise_for_status.return_value = None
    return r


def _http_error(status: int, text: str) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.text = text
    req = httpx.Request("POST", "https://api.airtable.com/v0/appABC/Customers")
    resp = httpx.Response(status_code=status, text=text, request=req)
    r.raise_for_status.side_effect = httpx.HTTPStatusError(
        f"HTTP {status}", request=req, response=resp
    )
    return r


def _patch_client(client: MagicMock) -> Any:
    cm = MagicMock()
    cm.__enter__.return_value = client
    cm.__exit__.return_value = False
    return patch("drt.destinations.airtable.httpx.Client", return_value=cm)


class TestAirtableDestinationConfig:
    def test_valid(self) -> None:
        c = _config()
        assert c.base_id == "appABC"
        assert c.primary_key is None

    def test_describe(self) -> None:
        assert _config().describe() == "airtable (appABC/Customers)"

    def test_missing_token_raises(self) -> None:
        with pytest.raises(ValueError, match="access_token"):
            AirtableDestinationConfig(
                type="airtable",
                access_token_env=None,
                base_id="x",
                table_name="t",
            )


class TestAirtableLoad:
    def test_empty_records_short_circuits(self) -> None:
        result = AirtableDestination().load([], _config(), _options())
        assert result.success == 0 and result.failed == 0

    def test_append_posts_records(self) -> None:
        client = MagicMock()
        client.request.return_value = _ok()
        with _patch_client(client):
            result = AirtableDestination().load(
                [{"name": "A"}, {"name": "B"}], _config(), _options()
            )
        assert result.success == 2
        method, url = client.request.call_args.args[0], client.request.call_args.args[1]
        assert method == "POST"
        assert url == "https://api.airtable.com/v0/appABC/Customers"
        body = client.request.call_args.kwargs["json"]
        assert body["records"] == [{"fields": {"name": "A"}}, {"fields": {"name": "B"}}]
        assert "performUpsert" not in body

    def test_upsert_patches_with_merge_field(self) -> None:
        client = MagicMock()
        client.request.return_value = _ok()
        with _patch_client(client):
            result = AirtableDestination().load(
                [{"record_id": "1", "name": "A"}],
                _config(primary_key="record_id"),
                _options(),
            )
        assert result.success == 1
        assert client.request.call_args.args[0] == "PATCH"
        body = client.request.call_args.kwargs["json"]
        assert body["performUpsert"] == {"fieldsToMergeOn": ["record_id"]}

    def test_batches_at_10(self) -> None:
        client = MagicMock()
        client.request.return_value = _ok()
        records = [{"i": i} for i in range(23)]
        with _patch_client(client):
            result = AirtableDestination().load(records, _config(), _options())
        assert result.success == 23
        assert client.request.call_count == 3  # 10 + 10 + 3

    def test_http_error_on_error_skip(self) -> None:
        client = MagicMock()
        client.request.return_value = _http_error(422, "bad field")
        with _patch_client(client):
            result = AirtableDestination().load(
                [{"name": "A"}], _config(), _options(on_error="skip")
            )
        assert result.failed == 1
        assert result.row_errors[0].http_status == 422

    def test_http_error_on_error_fail_stops(self) -> None:
        client = MagicMock()
        client.request.return_value = _http_error(500, "boom")
        records = [{"i": i} for i in range(15)]  # 2 chunks
        with _patch_client(client):
            result = AirtableDestination().load(
                records, _config(), _options(on_error="fail")
            )
        assert result.failed == 10  # first chunk failed
        assert client.request.call_count == 1  # stopped before the second chunk

    def test_non_http_error_recorded(self) -> None:
        client = MagicMock()
        client.request.side_effect = httpx.ConnectError("no route")
        with _patch_client(client):
            result = AirtableDestination().load(
                [{"name": "A"}], _config(), _options(on_error="skip")
            )
        assert result.failed == 1
        assert result.row_errors[0].http_status is None

    def test_non_http_error_on_error_fail_stops(self) -> None:
        client = MagicMock()
        client.request.side_effect = httpx.ConnectError("no route")
        records = [{"i": i} for i in range(15)]  # 2 chunks
        with _patch_client(client):
            result = AirtableDestination().load(
                records, _config(), _options(on_error="fail")
            )
        assert result.failed == 10
        assert client.request.call_count == 1  # stopped before the second chunk

    def test_load_missing_token_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        monkeypatch.delenv("AIRTABLE_NOPE", raising=False)
        monkeypatch.chdir(tmp_path)
        config = _config(access_token=None, access_token_env="AIRTABLE_NOPE")
        with pytest.raises(ValueError, match="access_token"):
            AirtableDestination().load([{"name": "A"}], config, _options())


class TestAirtableConnectionMissing:
    def test_test_connection_missing_token_raises(
        self, monkeypatch: pytest.MonkeyPatch, tmp_path: Any
    ) -> None:
        monkeypatch.delenv("AIRTABLE_NOPE", raising=False)
        monkeypatch.chdir(tmp_path)
        config = _config(access_token=None, access_token_env="AIRTABLE_NOPE")
        with pytest.raises(ValueError, match="missing access token"):
            AirtableDestination().test_connection(config)


class TestAirtableConnection:
    def test_test_connection_gets_one_record(self) -> None:
        client = MagicMock()
        client.get.return_value = _ok()
        with _patch_client(client):
            AirtableDestination().test_connection(_config())
        assert client.get.call_args.kwargs["params"] == {"maxRecords": 1}
