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
