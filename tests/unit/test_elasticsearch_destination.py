"""Unit tests for the Elasticsearch / OpenSearch destination (#420).

Uses ``pytest_httpserver`` for real HTTP round-trips through the ``_bulk``
endpoint — this is what verifies the NDJSON request body shape and the
per-item bulk-response error parsing, which is the destination's main
correctness concern. No real cluster required (httpx talks to the local
test server). Auth / config edge cases are exercised directly.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from pytest_httpserver import HTTPServer

from drt.config.models import ElasticsearchDestinationConfig, SyncOptions
from drt.destinations.elasticsearch import ElasticsearchDestination


def _options(**kwargs: Any) -> SyncOptions:
    defaults: dict[str, Any] = {"on_error": "skip", "batch_size": 100}
    defaults.update(kwargs)
    return SyncOptions(**defaults)


def _config(url: str, **overrides: Any) -> ElasticsearchDestinationConfig:
    defaults: dict[str, Any] = {
        "type": "elasticsearch",
        "url": url,
        "index": "customers",
        "api_key": "test-key",
    }
    defaults.update(overrides)
    return ElasticsearchDestinationConfig.model_validate(defaults)


def _bulk_ok(n: int) -> dict[str, Any]:
    """A bulk response where all n docs succeeded."""
    return {"errors": False, "items": [{"index": {"status": 201}} for _ in range(n)]}


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestElasticsearchConfig:
    def test_minimal_valid(self) -> None:
        c = ElasticsearchDestinationConfig(
            type="elasticsearch", url="https://es:9200", index="idx", api_key="k"
        )
        assert c.index == "idx"
        assert c.op_type == "index"
        assert c.verify_tls is True
        assert c.id_field is None

    def test_describe(self) -> None:
        c = ElasticsearchDestinationConfig(
            type="elasticsearch", url="https://es:9200", index="customers", api_key="k"
        )
        assert c.describe() == "elasticsearch (customers)"


# ---------------------------------------------------------------------------
# Empty / auth
# ---------------------------------------------------------------------------


def test_empty_records_short_circuits() -> None:
    # No url needed — returns before any HTTP / auth resolution.
    result = ElasticsearchDestination().load([], _config("https://unused:9200"), _options())
    assert result.success == 0
    assert result.failed == 0


def test_api_key_auth_header(httpserver: HTTPServer) -> None:
    httpserver.expect_request("/_bulk", method="POST").respond_with_json(_bulk_ok(1))
    config = _config(httpserver.url_for(""), api_key="secret-key")

    ElasticsearchDestination().load([{"id": 1}], config, _options())

    req = httpserver.log[0][0]
    assert req.headers["Authorization"] == "ApiKey secret-key"
    assert req.headers["Content-Type"] == "application/x-ndjson"


def test_basic_auth_header(httpserver: HTTPServer, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ES_USER", "elastic")
    monkeypatch.setenv("ES_PASS", "pw")
    httpserver.expect_request("/_bulk", method="POST").respond_with_json(_bulk_ok(1))
    config = _config(
        httpserver.url_for(""),
        api_key=None,
        username_env="ES_USER",
        password_env="ES_PASS",
    )

    ElasticsearchDestination().load([{"id": 1}], config, _options())

    req = httpserver.log[0][0]
    # base64("elastic:pw") == "ZWxhc3RpYzpwdw=="
    assert req.headers["Authorization"] == "Basic ZWxhc3RpYzpwdw=="


def test_missing_auth_raises() -> None:
    config = _config("https://es:9200", api_key=None)
    with pytest.raises(ValueError, match="provide api_key"):
        ElasticsearchDestination().load([{"id": 1}], config, _options())


# ---------------------------------------------------------------------------
# Bulk body shape
# ---------------------------------------------------------------------------


class TestBulkBody:
    def test_ndjson_action_and_source_lines(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/_bulk", method="POST").respond_with_json(_bulk_ok(2))
        config = _config(httpserver.url_for(""), index="users", id_field="user_id")
        records = [
            {"user_id": "u1", "name": "Alice"},
            {"user_id": "u2", "name": "Bob"},
        ]

        result = ElasticsearchDestination().load(records, config, _options())
        assert result.success == 2

        body = httpserver.log[0][0].get_data(as_text=True)
        lines = [json.loads(line) for line in body.strip().split("\n")]
        # action line, source line, action line, source line
        assert lines[0] == {"index": {"_index": "users", "_id": "u1"}}
        assert lines[1] == {"user_id": "u1", "name": "Alice"}
        assert lines[2] == {"index": {"_index": "users", "_id": "u2"}}
        assert lines[3] == {"user_id": "u2", "name": "Bob"}
        # body must end with a trailing newline
        assert body.endswith("\n")

    def test_no_id_field_omits_id(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/_bulk", method="POST").respond_with_json(_bulk_ok(1))
        config = _config(httpserver.url_for(""), index="logs")  # no id_field
        ElasticsearchDestination().load([{"msg": "hi"}], config, _options())

        body = httpserver.log[0][0].get_data(as_text=True)
        action = json.loads(body.strip().split("\n")[0])
        assert action == {"index": {"_index": "logs"}}  # no _id

    def test_op_type_create(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/_bulk", method="POST").respond_with_json(_bulk_ok(1))
        config = _config(httpserver.url_for(""), op_type="create", id_field="id")
        ElasticsearchDestination().load([{"id": 7}], config, _options())

        body = httpserver.log[0][0].get_data(as_text=True)
        action = json.loads(body.strip().split("\n")[0])
        assert "create" in action
        assert action["create"]["_id"] == "7"


# ---------------------------------------------------------------------------
# Per-item + whole-batch errors
# ---------------------------------------------------------------------------


class TestErrorHandling:
    def test_per_item_error_recorded_others_succeed(self, httpserver: HTTPServer) -> None:
        """HTTP 200 but one document failed (e.g. 409 on op_type=create) →
        that row is a row error, the rest count as success."""
        bulk_response = {
            "errors": True,
            "items": [
                {"create": {"status": 201}},
                {
                    "create": {
                        "status": 409,
                        "error": {
                            "type": "version_conflict_engine_exception",
                            "reason": "document already exists",
                        },
                    }
                },
                {"create": {"status": 201}},
            ],
        }
        httpserver.expect_request("/_bulk", method="POST").respond_with_json(bulk_response)
        config = _config(httpserver.url_for(""), op_type="create", id_field="id")
        records = [{"id": 1}, {"id": 2}, {"id": 3}]

        result = ElasticsearchDestination().load(records, config, _options())

        assert result.success == 2
        assert result.failed == 1
        assert len(result.row_errors) == 1
        err = result.row_errors[0]
        assert err.batch_index == 1  # the second record
        assert err.http_status == 409
        assert "document already exists" in err.error_message

    def test_per_item_error_on_error_fail_stops(self, httpserver: HTTPServer) -> None:
        bulk_response = {
            "errors": True,
            "items": [
                {
                    "index": {
                        "status": 400,
                        "error": {"type": "mapper_parsing_exception", "reason": "bad"},
                    }
                },
                {"index": {"status": 201}},
            ],
        }
        httpserver.expect_request("/_bulk", method="POST").respond_with_json(bulk_response)
        config = _config(httpserver.url_for(""))

        result = ElasticsearchDestination().load(
            [{"id": 1}, {"id": 2}], config, _options(on_error="fail")
        )

        # First item failed → stop; second item in the same response is not
        # counted as success (we broke out).
        assert result.failed == 1
        assert result.success == 0

    def test_whole_batch_http_error_fails_all_rows(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/_bulk", method="POST").respond_with_data(
            "unauthorized", status=401
        )
        config = _config(httpserver.url_for(""))
        records = [{"id": 1}, {"id": 2}]

        result = ElasticsearchDestination().load(records, config, _options())

        assert result.success == 0
        assert result.failed == 2
        assert all(e.http_status == 401 for e in result.row_errors)

    def test_whole_batch_http_error_on_error_fail_stops(self, httpserver: HTTPServer) -> None:
        httpserver.expect_request("/_bulk", method="POST").respond_with_data(
            "forbidden", status=403
        )
        config = _config(httpserver.url_for(""))
        result = ElasticsearchDestination().load(
            [{"id": 1}, {"id": 2}], config, _options(on_error="fail", batch_size=1)
        )
        # First batch's 403 → stop; only one row recorded.
        assert result.failed == 1
        assert result.success == 0

    def test_transport_error_fails_all_rows(self) -> None:
        """A non-HTTP failure (connection refused / DNS) is captured per row
        with no http_status, rather than crashing the sync."""
        # Point at a port nothing is listening on → httpx.ConnectError.
        config = _config("http://127.0.0.1:1", index="idx")
        records = [{"id": 1}, {"id": 2}]

        result = ElasticsearchDestination().load(records, config, _options())

        assert result.success == 0
        assert result.failed == 2
        assert all(e.http_status is None for e in result.row_errors)

    def test_transport_error_on_error_fail_stops(self) -> None:
        config = _config("http://127.0.0.1:1", index="idx")
        result = ElasticsearchDestination().load(
            [{"id": 1}, {"id": 2}], config, _options(on_error="fail", batch_size=1)
        )
        # First batch errors and stops → only one row recorded.
        assert result.failed == 1
        assert result.success == 0

    def test_200_without_items_counts_chunk_success(self, httpserver: HTTPServer) -> None:
        """Defensive: a 200 from _bulk that lacks an ``items`` array (shouldn't
        happen in practice) treats the whole chunk as success rather than
        silently dropping rows."""
        httpserver.expect_request("/_bulk", method="POST").respond_with_json(
            {"took": 5, "errors": False}  # no "items"
        )
        config = _config(httpserver.url_for(""))
        result = ElasticsearchDestination().load([{"id": 1}, {"id": 2}], config, _options())

        assert result.success == 2
        assert result.failed == 0


# ---------------------------------------------------------------------------
# Batching
# ---------------------------------------------------------------------------


def test_batches_split_by_batch_size(httpserver: HTTPServer) -> None:
    # batch_size=2 over 5 records → 3 POSTs (2 + 2 + 1).
    from werkzeug.wrappers import Response as WerkzeugResponse

    def handler(req: Any) -> WerkzeugResponse:
        # Two NDJSON lines per document; reply with one OK item per doc.
        doc_count = len(req.get_data(as_text=True).strip().split("\n")) // 2
        return WerkzeugResponse(json.dumps(_bulk_ok(doc_count)), content_type="application/json")

    httpserver.expect_request("/_bulk", method="POST").respond_with_handler(handler)
    config = _config(httpserver.url_for(""), id_field="id")
    records = [{"id": i} for i in range(5)]

    result = ElasticsearchDestination().load(records, config, _options(batch_size=2))

    assert result.success == 5
    assert len(httpserver.log) == 3
