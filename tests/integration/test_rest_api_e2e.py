"""End-to-end integration tests for the REST API destination.

Uses pytest-httpserver to spin up a real local HTTP server — no mocking of
httpx internals. Tests verify the full extract → load flow including auth,
rate limiting, retry, and error handling.
"""

from __future__ import annotations

import json
import time

from drt.config.credentials import BigQueryProfile
from drt.config.models import (
    RateLimitConfig,
    RestApiDestinationConfig,
    RetryConfig,
    SyncConfig,
    SyncOptions,
)
from drt.destinations.rest_api import RestApiDestination
from drt.engine.sync import run_sync
from tests.integration.conftest import FakeSource

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _dest_config(httpserver, body_template: str | None = None, auth=None) -> RestApiDestinationConfig:  # noqa: E501
    return RestApiDestinationConfig(
        type="rest_api",
        url=httpserver.url_for("/webhook"),
        method="POST",
        headers={"Content-Type": "application/json"},
        body_template=body_template,
        auth=auth,
    )


def _sync(
    dest: RestApiDestinationConfig,
    rate_limit_rps: int = 100,
    on_error: str = "skip",
    retry: RetryConfig | None = None,
) -> SyncConfig:
    return SyncConfig(
        name="test_sync",
        model="ref('table')",
        destination=dest,
        sync=SyncOptions(
            batch_size=10,
            rate_limit=RateLimitConfig(requests_per_second=rate_limit_rps),
            on_error=on_error,
            retry=retry or RetryConfig(),
        ),
    )


def _profile() -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="p", dataset="d")


# ---------------------------------------------------------------------------
# Basic success
# ---------------------------------------------------------------------------

def test_full_sync_all_success(httpserver, fake_source, tmp_path):
    httpserver.expect_ordered_request("/webhook", method="POST").respond_with_data("OK", status=200)
    httpserver.expect_ordered_request("/webhook", method="POST").respond_with_data("OK", status=200)
    httpserver.expect_ordered_request("/webhook", method="POST").respond_with_data("OK", status=200)

    dest_cfg = _dest_config(httpserver)
    sync = _sync(dest_cfg)
    result = run_sync(sync, fake_source, RestApiDestination(), _profile(), tmp_path)

    assert result.success == 3
    assert result.failed == 0
    httpserver.check_assertions()


def test_body_template_rendered(httpserver, tmp_path):
    received = []

    def handler(request):
        received.append(json.loads(request.data))
        from werkzeug.wrappers import Response
        return Response("OK", status=200)

    httpserver.expect_request("/webhook", method="POST").respond_with_handler(handler)

    source = FakeSource([{"id": 42, "name": "Alice"}])
    dest_cfg = _dest_config(
        httpserver,
        body_template='{"user_id": "{{ row.id }}", "user_name": "{{ row.name }}"}',
    )
    sync = _sync(dest_cfg)
    run_sync(sync, source, RestApiDestination(), _profile(), tmp_path)

    assert len(received) == 1
    assert received[0]["user_id"] == "42"
    assert received[0]["user_name"] == "Alice"


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def test_bearer_auth_header_sent(httpserver, tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_TOKEN", "sk-secret123")
    received_headers = {}

    def handler(request):
        received_headers.update(dict(request.headers))
        from werkzeug.wrappers import Response
        return Response("OK", status=200)

    httpserver.expect_request("/webhook").respond_with_handler(handler)

    from drt.config.models import BearerAuth
    source = FakeSource([{"id": 1}])
    auth = BearerAuth(type="bearer", token_env="TEST_TOKEN")
    dest_cfg = _dest_config(httpserver, auth=auth)
    run_sync(_sync(dest_cfg), source, RestApiDestination(), _profile(), tmp_path)

    assert received_headers.get("Authorization") == "Bearer sk-secret123"


def test_api_key_auth_header_sent(httpserver, tmp_path, monkeypatch):
    monkeypatch.setenv("MY_KEY", "abc123")
    received_headers = {}

    def handler(request):
        received_headers.update(dict(request.headers))
        from werkzeug.wrappers import Response
        return Response("OK", status=200)

    httpserver.expect_request("/webhook").respond_with_handler(handler)

    from drt.config.models import ApiKeyAuth
    source = FakeSource([{"id": 1}])
    auth = ApiKeyAuth(type="api_key", header="X-API-Key", value_env="MY_KEY")
    dest_cfg = _dest_config(httpserver, auth=auth)
    run_sync(_sync(dest_cfg), source, RestApiDestination(), _profile(), tmp_path)

    assert received_headers.get("X-Api-Key") == "abc123"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------

def test_on_error_skip_continues(httpserver, tmp_path):
    # First request fails, rest succeed
    httpserver.expect_ordered_request("/webhook").respond_with_data("error", status=500)
    httpserver.expect_ordered_request("/webhook").respond_with_data("OK", status=200)
    httpserver.expect_ordered_request("/webhook").respond_with_data("OK", status=200)

    source = FakeSource([{"id": 1}, {"id": 2}, {"id": 3}])
    dest_cfg = _dest_config(httpserver)
    # max_attempts=1 so the 500 fails immediately without retry
    sync = _sync(dest_cfg, on_error="skip", retry=RetryConfig(max_attempts=1))
    result = run_sync(sync, source, RestApiDestination(), _profile(), tmp_path)

    assert result.failed == 1
    assert result.success == 2


# ---------------------------------------------------------------------------
# Retry
# ---------------------------------------------------------------------------

def test_retry_on_500_succeeds_on_third(httpserver, tmp_path):
    httpserver.expect_ordered_request("/webhook").respond_with_data("err", status=500)
    httpserver.expect_ordered_request("/webhook").respond_with_data("err", status=500)
    httpserver.expect_ordered_request("/webhook").respond_with_data("OK", status=200)

    source = FakeSource([{"id": 1}])
    dest_cfg = _dest_config(httpserver)
    sync = _sync(
        dest_cfg,
        retry=RetryConfig(max_attempts=3, initial_backoff=0.01, backoff_multiplier=1.0),
    )
    result = run_sync(sync, source, RestApiDestination(), _profile(), tmp_path)

    assert result.success == 1
    assert result.failed == 0
    httpserver.check_assertions()


# ---------------------------------------------------------------------------
# Rate limiting (timing-based — uses small rps for fast test)
# ---------------------------------------------------------------------------

def test_rate_limiting_enforces_delay(httpserver, tmp_path):
    for _ in range(3):
        httpserver.expect_request("/webhook").respond_with_data("OK", status=200)

    source = FakeSource([{"id": 1}, {"id": 2}, {"id": 3}])
    dest_cfg = _dest_config(httpserver)
    # 5 req/s → min 200ms between requests → 3 records should take ≥ 0.4s
    sync = _sync(dest_cfg, rate_limit_rps=5)

    t0 = time.monotonic()
    run_sync(sync, source, RestApiDestination(), _profile(), tmp_path)
    elapsed = time.monotonic() - t0

    assert elapsed >= 0.35  # generous lower bound for CI
