"""Tests for RestApiSource."""

import httpx

from drt.config.credentials import RestApiProfile
from drt.sources.rest_api import RestApiSource


def test_rest_api_source_extract_records():
    source = RestApiSource()

    # Default behavior without result_path
    assert source._extract_records([{"id": 1}], None) == [{"id": 1}]
    assert source._extract_records({"records": [{"id": 1}]}, None) == [{"id": 1}]
    assert source._extract_records({"data": [{"id": 1}]}, None) == [{"id": 1}]

    # With result_path
    data = {
        "response": {
            "items": [{"id": 1}, {"id": 2}]
        }
    }
    assert source._extract_records(data, "response.items") == [{"id": 1}, {"id": 2}]

    # With missing path
    assert source._extract_records(data, "response.missing") == []

    # With object at path instead of array
    assert source._extract_records(data, "response") == [{"items": [{"id": 1}, {"id": 2}]}]


def test_rest_api_source_extract_single_page(monkeypatch):
    source = RestApiSource()
    profile = RestApiProfile(
        type="rest_api",
        url="https://api.example.com/data",
        auth={"type": "bearer", "token": "test-token"},
        result_path="data"
    )

    class MockResponse:
        def __init__(self):
            self.headers = {}

        def json(self):
            return {"data": [{"id": 1}, {"id": 2}]}

        def raise_for_status(self):
            pass

    class MockClient:
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def request(self, method, url, headers, params, **kwargs):
            assert url == "https://api.example.com/data"
            assert headers["Authorization"] == "Bearer test-token"
            assert params is None
            return MockResponse()

    monkeypatch.setattr(httpx, "Client", MockClient)

    records = list(source.extract("", profile))
    assert len(records) == 2
    assert records[0]["id"] == 1
    assert records[1]["id"] == 2


def test_rest_api_source_extract_offset_pagination(monkeypatch):
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
        def __init__(self, *args, **kwargs):
            pass

        def __enter__(self):
            return self

        def __exit__(self, *args):
            pass

        def request(self, method, url, headers, params, **kwargs):
            skip = int(params["skip"])
            take = int(params["take"])
            
            assert take == 2

            class MockResponse:
                def json(self):
                    if skip == 0:
                        return [{"id": 1}, {"id": 2}]
                    elif skip == 2:
                        return [{"id": 3}]
                    return []

                def raise_for_status(self):
                    pass
                    
                @property
                def headers(self):
                    return {}

            return MockResponse()

    monkeypatch.setattr(httpx, "Client", MockClient)

    records = list(source.extract("", profile))
    assert len(records) == 3
    assert records[0]["id"] == 1
    assert records[2]["id"] == 3
