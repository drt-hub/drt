"""Tests for RestApiSource."""

import logging
from typing import Any

import httpx
import pytest

from drt.config.credentials import RestApiProfile
from drt.sources.rest_api import RestApiSource


def test_rest_api_source_extract_records() -> None:
    source = RestApiSource()

    # Default behavior without result_path
    assert source._extract_records([{"id": 1}], None) == [{"id": 1}]
    assert source._extract_records({"records": [{"id": 1}]}, None) == [{"id": 1}]
    assert source._extract_records({"data": [{"id": 1}]}, None) == [{"id": 1}]

    # With result_path
    data = {
        "response": {
            "items": [{"id": 1}, {"id": 2}]
        },
        "data": {
            "results": {
                "items": [{"id": 3}]
            }
        }
    }
    assert source._extract_records(data, "response.items") == [{"id": 1}, {"id": 2}]
    assert source._extract_records(data, "data.results.items") == [{"id": 3}]

    # With missing path
    assert source._extract_records(data, "response.missing") == []

    # With object at path instead of array
    assert source._extract_records(data, "response") == [{"items": [{"id": 1}, {"id": 2}]}]


def test_rest_api_source_extract_records_single_dict_fallback_logs_debug(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """When a response is a dict with neither 'records' nor 'data' arrays, the source
    wraps the dict as a single-record list (e.g. `{"error": "..."}` becomes one row).
    This is easy to miss, so the source emits a debug log noting the fallback shape.
    """
    source = RestApiSource()

    with caplog.at_level(logging.DEBUG, logger="drt"):
        result = source._extract_records({"error": "rate limit exceeded"}, None)

    assert result == [{"error": "rate limit exceeded"}]
    assert any(
        "wrapping single dict as one record" in record.message
        and "error" in record.message  # keys logged
        for record in caplog.records
    )


def test_rest_api_source_extract_single_page(monkeypatch: Any) -> None:
    source = RestApiSource()
    profile = RestApiProfile(
        type="rest_api",
        url="https://api.example.com/data",
        auth={"type": "bearer", "token": "test-token"},
        result_path="data"
    )

    class MockResponse:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}

        def json(self) -> dict[str, Any]:
            return {"data": [{"id": 1}, {"id": 2}]}

        def raise_for_status(self) -> None:
            pass

    class MockClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def __enter__(self) -> "MockClient":
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def request(
            self,
            method: str,
            url: str,
            headers: dict[str, str],
            params: dict[str, Any] | None,
            **kwargs: Any
        ) -> MockResponse:
            assert url == "https://api.example.com/data"
            assert headers["Authorization"] == "Bearer test-token"
            assert params is None
            return MockResponse()

    monkeypatch.setattr(httpx, "Client", MockClient)

    records = list(source.extract("", profile))
    assert len(records) == 2
    assert records[0]["id"] == 1
    assert records[1]["id"] == 2


def test_rest_api_source_extract_offset_pagination(monkeypatch: Any) -> None:
    source = RestApiSource()
    profile = RestApiProfile(
        type="rest_api",
        url="https://api.example.com/data",
        pagination={
            "type": "offset",
            "limit": 2,
            "offset_param": "skip",
            "limit_param": "take"
        }
    )

    class MockClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def __enter__(self) -> "MockClient":
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def request(
            self,
            method: str,
            url: str,
            headers: dict[str, str],
            params: dict[str, Any],
            **kwargs: Any
        ) -> Any:
            skip = int(params["skip"])
            take = int(params["take"])
            
            assert take == 2

            class MockResponse:
                def json(self) -> list[dict[str, Any]]:
                    if skip == 0:
                        return [{"id": 1}, {"id": 2}]
                    elif skip == 2:
                        return [{"id": 3}]
                    return []

                def raise_for_status(self) -> None:
                    pass
                    
                @property
                def headers(self) -> dict[str, str]:
                    return {}

            return MockResponse()

    monkeypatch.setattr(httpx, "Client", MockClient)

    records = list(source.extract("", profile))
    assert len(records) == 3
    assert records[0]["id"] == 1
    assert records[2]["id"] == 3


def test_rest_api_source_extract_cursor_pagination(monkeypatch: Any) -> None:
    source = RestApiSource()
    profile = RestApiProfile(
        type="rest_api",
        url="https://api.example.com/data",
        pagination={
            "type": "cursor",
            "limit": 2,
            "cursor_param": "after",
            "cursor_field": "next_cursor",
            "limit_param": "limit"
        }
    )

    class MockClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def __enter__(self) -> "MockClient":
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def request(
            self,
            method: str,
            url: str,
            headers: dict[str, str],
            params: dict[str, Any],
            **kwargs: Any
        ) -> Any:
            cursor = params.get("after")

            class MockResponse:
                def json(self) -> dict[str, Any]:
                    if not cursor:
                        return {"data": [{"id": 1}, {"id": 2}], "next_cursor": "cursor-2"}
                    elif cursor == "cursor-2":
                        return {"data": [{"id": 3}], "next_cursor": None}
                    return {"data": []}

                def raise_for_status(self) -> None:
                    pass

                @property
                def headers(self) -> dict[str, str]:
                    return {}

            return MockResponse()

    monkeypatch.setattr(httpx, "Client", MockClient)

    records = list(source.extract("", profile))
    assert len(records) == 3
    assert records[0]["id"] == 1
    assert records[2]["id"] == 3


def test_rest_api_source_extract_link_header_pagination(monkeypatch: Any) -> None:
    source = RestApiSource()
    profile = RestApiProfile(
        type="rest_api",
        url="https://api.example.com/data",
        pagination={
            "type": "link_header"
        }
    )

    class MockClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def __enter__(self) -> "MockClient":
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def request(
            self,
            method: str,
            url: str,
            headers: dict[str, str],
            params: dict[str, Any] | None,
            **kwargs: Any
        ) -> Any:
            class MockResponse:
                def json(self) -> list[dict[str, Any]]:
                    if url == "https://api.example.com/data":
                        return [{"id": 1}, {"id": 2}]
                    elif url == "https://api.example.com/data?page=2":
                        return [{"id": 3}]
                    return []

                def raise_for_status(self) -> None:
                    pass

                @property
                def headers(self) -> dict[str, str]:
                    if url == "https://api.example.com/data":
                        return {"link": '<https://api.example.com/data?page=2>; rel="next"'}
                    return {}

            return MockResponse()

    monkeypatch.setattr(httpx, "Client", MockClient)

    records = list(source.extract("", profile))
    assert len(records) == 3
    assert records[0]["id"] == 1
    assert records[2]["id"] == 3


# ---------------------------------------------------------------------------
# Incremental extraction (#767) — watermark pushed as a request param
# ---------------------------------------------------------------------------


def _recording_client(monkeypatch: Any, responses: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Patch httpx.Client with a recorder; returns the list of captured calls.

    ``responses[i]`` is ``{"json": ..., "headers": {...}}`` for the i-th
    request; the last entry repeats for any further requests.
    """
    calls: list[dict[str, Any]] = []

    class MockResponse:
        def __init__(self, spec: dict[str, Any]) -> None:
            self._spec = spec
            self.headers: dict[str, str] = spec.get("headers", {})

        def json(self) -> Any:
            return self._spec["json"]

        def raise_for_status(self) -> None:
            pass

    class MockClient:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            pass

        def __enter__(self) -> "MockClient":
            return self

        def __exit__(self, *args: Any) -> None:
            pass

        def request(
            self,
            method: str,
            url: str,
            headers: dict[str, str],
            params: dict[str, Any] | None,
            **kwargs: Any,
        ) -> MockResponse:
            calls.append({"url": url, "params": params})
            spec = responses[min(len(calls) - 1, len(responses) - 1)]
            return MockResponse(spec)

    monkeypatch.setattr(httpx, "Client", MockClient)
    return calls


def test_extract_incremental_injects_start_param(monkeypatch: Any) -> None:
    source = RestApiSource()
    profile = RestApiProfile(
        type="rest_api",
        url="https://api.example.com/users",
        incremental={"start_param": "updated_since"},
    )
    calls = _recording_client(monkeypatch, [{"json": [{"id": 1}]}])

    records = list(source.extract_incremental("", profile, "2026-01-01T00:00:00"))

    assert len(records) == 1
    assert calls[0]["params"] == {"updated_since": "2026-01-01T00:00:00"}


def test_extract_incremental_none_cursor_skips_injection(monkeypatch: Any) -> None:
    """First run with no stored watermark and no default: no param injected."""
    source = RestApiSource()
    profile = RestApiProfile(
        type="rest_api",
        url="https://api.example.com/users",
        incremental={"start_param": "updated_since"},
    )
    calls = _recording_client(monkeypatch, [{"json": [{"id": 1}]}])

    list(source.extract_incremental("", profile, None))

    assert calls[0]["params"] is None


def test_extract_incremental_offset_pagination_injects_every_page(monkeypatch: Any) -> None:
    source = RestApiSource()
    profile = RestApiProfile(
        type="rest_api",
        url="https://api.example.com/users",
        pagination={"type": "offset", "limit": 2},
        incremental={"start_param": "since"},
    )
    calls = _recording_client(
        monkeypatch,
        [
            {"json": [{"id": 1}, {"id": 2}]},  # full page -> fetch next
            {"json": [{"id": 3}]},  # short page -> stop
        ],
    )

    records = list(source.extract_incremental("", profile, "42"))

    assert len(records) == 3
    assert len(calls) == 2
    for call in calls:
        assert call["params"]["since"] == "42"


def test_extract_incremental_link_header_injects_first_request_only(monkeypatch: Any) -> None:
    source = RestApiSource()
    profile = RestApiProfile(
        type="rest_api",
        url="https://api.example.com/users",
        pagination={"type": "link_header"},
        incremental={"start_param": "since"},
    )
    calls = _recording_client(
        monkeypatch,
        [
            {
                "json": [{"id": 1}],
                "headers": {"link": '<https://api.example.com/users?page=2>; rel="next"'},
            },
            {"json": [{"id": 2}], "headers": {}},
        ],
    )

    records = list(source.extract_incremental("", profile, "2026-01-01"))

    assert len(records) == 2
    assert calls[0]["params"] == {"since": "2026-01-01"}
    # The server's next link is authoritative — no re-injection.
    assert calls[1]["params"] is None
    assert calls[1]["url"] == "https://api.example.com/users?page=2"


def test_extract_incremental_without_profile_config_warns_and_full_extracts(
    monkeypatch: Any, caplog: pytest.LogCaptureFixture
) -> None:
    source = RestApiSource()
    profile = RestApiProfile(type="rest_api", url="https://api.example.com/users")
    calls = _recording_client(monkeypatch, [{"json": [{"id": 1}]}])

    with caplog.at_level(logging.WARNING, logger="drt"):
        records = list(source.extract_incremental("", profile, "2026-01-01"))

    assert len(records) == 1
    assert calls[0]["params"] is None
    assert any("incremental.start_param" in r.message for r in caplog.records)


def test_plain_extract_never_injects_even_with_config(monkeypatch: Any) -> None:
    """Non-incremental syncs (mode: full) must not filter the endpoint."""
    source = RestApiSource()
    profile = RestApiProfile(
        type="rest_api",
        url="https://api.example.com/users",
        incremental={"start_param": "updated_since"},
    )
    calls = _recording_client(monkeypatch, [{"json": [{"id": 1}]}])

    list(source.extract("", profile))

    assert calls[0]["params"] is None
