"""Tests for staged_upload destination."""

from __future__ import annotations

import json
from unittest.mock import ANY, MagicMock, patch

import httpx
import pytest
from pytest_httpserver import HTTPServer

from drt.config.models import (
    RateLimitConfig,
    RetryConfig,
    StagedUploadDestinationConfig,
    StagedUploadPhaseConfig,
    StagedUploadPollConfig,
    SyncOptions,
)
from drt.destinations.base import StagedDestination
from drt.destinations.staged_upload import StagedUploadDestination


def _options() -> SyncOptions:
    return SyncOptions()


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_implements_staged_destination_protocol() -> None:
    assert isinstance(StagedUploadDestination(), StagedDestination)


# ---------------------------------------------------------------------------
# Stage — record accumulation
# ---------------------------------------------------------------------------


def test_stage_accumulates_records() -> None:
    dest = StagedUploadDestination()
    config = StagedUploadDestinationConfig(
        type="staged_upload",
        stage=StagedUploadPhaseConfig(url="http://x"),
        trigger=StagedUploadPhaseConfig(url="http://x"),
    )
    dest.stage([{"a": 1}], config, _options())
    dest.stage([{"a": 2}, {"a": 3}], config, _options())
    assert len(dest._records) == 3


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def test_serialize_csv() -> None:
    dest = StagedUploadDestination()
    dest._records = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
    data = dest._serialize("csv").decode()
    assert "id,name" in data
    assert "Alice" in data
    assert "Bob" in data


def test_serialize_jsonl() -> None:
    dest = StagedUploadDestination()
    dest._records = [{"id": 1}, {"id": 2}]
    data = dest._serialize("jsonl").decode()
    lines = data.strip().split("\n")
    assert len(lines) == 2
    assert json.loads(lines[0]) == {"id": 1}


def test_serialize_json() -> None:
    dest = StagedUploadDestination()
    dest._records = [{"id": 1}, {"id": 2}]
    data = dest._serialize("json").decode()
    parsed = json.loads(data)
    assert len(parsed) == 2


# ---------------------------------------------------------------------------
# Full 3-phase flow
# ---------------------------------------------------------------------------


def test_finalize_stage_trigger_poll(httpserver: HTTPServer) -> None:
    """Full flow: stage upload → trigger job → poll success."""
    # Stage endpoint: accept file upload, return upload_id
    httpserver.expect_ordered_request("/upload", method="POST").respond_with_json(
        {"uploadId": "u-123"}
    )

    # Trigger endpoint: start job, return job_id
    httpserver.expect_ordered_request("/jobs", method="POST").respond_with_json({"jobId": "j-456"})

    # Poll endpoint: return success
    httpserver.expect_ordered_request("/jobs/j-456", method="GET").respond_with_json(
        {"status": "SUCCEEDED"}
    )

    config = StagedUploadDestinationConfig(
        type="staged_upload",
        format="csv",
        stage=StagedUploadPhaseConfig(
            url=httpserver.url_for("/upload"),
            method="POST",
            response_extract={"upload_id": "uploadId"},
        ),
        trigger=StagedUploadPhaseConfig(
            url=httpserver.url_for("/jobs"),
            method="POST",
            body_template='{"uploadId": "{{ upload_id }}"}',
            response_extract={"job_id": "jobId"},
        ),
        poll=StagedUploadPollConfig(
            url=httpserver.url_for("/jobs/{{ job_id }}"),
            method="GET",
            status_field="status",
            success_values=["SUCCEEDED"],
            failure_values=["FAILED"],
            interval_seconds=0,
            timeout_seconds=5,
        ),
    )

    dest = StagedUploadDestination()
    dest._records = [{"id": 1, "name": "test"}]
    result = dest.finalize(config, _options())

    assert result.success == 1
    assert result.failed == 0
    assert result.errors == []


def test_finalize_without_poll(httpserver: HTTPServer) -> None:
    """Stage + Trigger only (poll is optional)."""
    httpserver.expect_ordered_request("/upload", method="POST").respond_with_json(
        {"uploadId": "u-1"}
    )

    httpserver.expect_ordered_request("/jobs", method="POST").respond_with_json({"ok": True})

    config = StagedUploadDestinationConfig(
        type="staged_upload",
        format="jsonl",
        stage=StagedUploadPhaseConfig(
            url=httpserver.url_for("/upload"),
            response_extract={"upload_id": "uploadId"},
        ),
        trigger=StagedUploadPhaseConfig(
            url=httpserver.url_for("/jobs"),
            body_template='{"uploadId": "{{ upload_id }}"}',
        ),
        poll=None,
    )

    dest = StagedUploadDestination()
    dest._records = [{"x": 1}]
    result = dest.finalize(config, _options())

    assert result.success == 1
    assert result.failed == 0


def test_finalize_poll_failure(httpserver: HTTPServer) -> None:
    """Poll returns failure status."""
    httpserver.expect_ordered_request("/upload").respond_with_json({"uploadId": "u-1"})
    httpserver.expect_ordered_request("/jobs").respond_with_json({"jobId": "j-1"})
    httpserver.expect_ordered_request("/jobs/j-1").respond_with_json({"status": "FAILED"})

    config = StagedUploadDestinationConfig(
        type="staged_upload",
        stage=StagedUploadPhaseConfig(
            url=httpserver.url_for("/upload"),
            response_extract={"upload_id": "uploadId"},
        ),
        trigger=StagedUploadPhaseConfig(
            url=httpserver.url_for("/jobs"),
            response_extract={"job_id": "jobId"},
        ),
        poll=StagedUploadPollConfig(
            url=httpserver.url_for("/jobs/{{ job_id }}"),
            status_field="status",
            failure_values=["FAILED"],
            interval_seconds=0,
            timeout_seconds=5,
        ),
    )

    dest = StagedUploadDestination()
    dest._records = [{"x": 1}]
    result = dest.finalize(config, _options())

    assert result.success == 0
    assert result.failed == 1
    assert any("failed" in e.lower() for e in result.errors)


def test_finalize_poll_retries_transient_failure() -> None:
    """A transient status-check failure is retried without replaying earlier phases."""
    config = StagedUploadDestinationConfig(
        type="staged_upload",
        stage=StagedUploadPhaseConfig(
            url="https://upload.example.com",
            response_extract={"upload_id": "uploadId"},
        ),
        trigger=StagedUploadPhaseConfig(
            url="https://api.example.com/jobs",
            response_extract={"job_id": "jobId"},
        ),
        poll=StagedUploadPollConfig(
            url="https://api.example.com/jobs/{{ job_id }}",
            interval_seconds=0,
            timeout_seconds=5,
        ),
    )
    options = SyncOptions(
        retry=RetryConfig(max_attempts=2, initial_backoff=0.0, max_backoff=0.0),
        rate_limit=RateLimitConfig(requests_per_second=0),
    )
    dest = StagedUploadDestination()
    dest._records = [{"x": 1}]
    transient = httpx.Response(
        503,
        request=httpx.Request("GET", "https://api.example.com/jobs/j-1"),
    )
    complete = httpx.Response(
        200,
        json={"status": "SUCCEEDED"},
        request=httpx.Request("GET", "https://api.example.com/jobs/j-1"),
    )
    client = MagicMock()
    client.request.side_effect = [transient, complete]

    with (
        patch.object(
            dest,
            "_http_phase",
            side_effect=[{"uploadId": "u-1"}, {"jobId": "j-1"}],
        ) as http_phase,
        patch("drt.destinations.staged_upload.httpx.Client") as client_class,
    ):
        client_class.return_value.__enter__.return_value = client
        result = dest.finalize(config, options)

    assert result.success == 1
    assert result.failed == 0
    assert http_phase.call_count == 2
    assert client.request.call_count == 2


def test_post_poll_does_not_retry_transient_failure() -> None:
    poll_config = StagedUploadPollConfig(
        url="https://api.example.com/jobs/j-1",
        method="POST",
        interval_seconds=0,
        timeout_seconds=5,
    )
    retry_config = RetryConfig(max_attempts=3, initial_backoff=0.0, max_backoff=0.0)
    limiter = MagicMock()
    client = MagicMock()
    client.request.return_value = httpx.Response(
        503,
        request=httpx.Request("POST", poll_config.url),
    )

    with (
        patch("drt.destinations.staged_upload.httpx.Client") as client_class,
        patch("drt.destinations.staged_upload.with_retry") as retry_call,
    ):
        client_class.return_value.__enter__.return_value = client
        with pytest.raises(httpx.HTTPStatusError):
            StagedUploadDestination()._poll(
                poll_config, {}, retry_config, limiter, url=poll_config.url
            )

    retry_call.assert_not_called()
    client.request.assert_called_once_with("POST", poll_config.url, headers={}, timeout=ANY)
    limiter.acquire.assert_called_once_with()


def test_poll_retry_acquires_rate_limiter_per_attempt() -> None:
    poll_config = StagedUploadPollConfig(
        url="https://api.example.com/jobs/j-1",
        interval_seconds=0,
        timeout_seconds=5,
    )
    retry_config = RetryConfig(max_attempts=2, initial_backoff=0.0, max_backoff=0.0)
    limiter = MagicMock()
    client = MagicMock()
    client.request.side_effect = [
        httpx.Response(
            503,
            request=httpx.Request("GET", poll_config.url),
        ),
        httpx.Response(
            200,
            json={"status": "SUCCEEDED"},
            request=httpx.Request("GET", poll_config.url),
        ),
    ]

    with patch("drt.destinations.staged_upload.httpx.Client") as client_class:
        client_class.return_value.__enter__.return_value = client
        StagedUploadDestination()._poll(poll_config, {}, retry_config, limiter, url=poll_config.url)

    assert client.request.call_count == 2
    assert limiter.acquire.call_count == 2


def test_poll_retry_success_after_deadline_raises_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    poll_config = StagedUploadPollConfig(
        url="https://api.example.com/jobs/j-1",
        interval_seconds=0,
        timeout_seconds=1,
    )
    retry_config = RetryConfig(max_attempts=2, initial_backoff=60.0, max_backoff=60.0)
    limiter = MagicMock()
    client = MagicMock()
    client.request.side_effect = [
        httpx.Response(
            503,
            request=httpx.Request("GET", poll_config.url),
        ),
        httpx.Response(
            200,
            json={"status": "SUCCEEDED"},
            request=httpx.Request("GET", poll_config.url),
        ),
    ]
    clock = {"now": 0.0}
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["now"] += seconds + 0.01

    monkeypatch.setattr("drt.destinations.staged_upload.time.monotonic", lambda: clock["now"])
    monkeypatch.setattr("drt.destinations.retry.time.sleep", fake_sleep)

    with patch("drt.destinations.staged_upload.httpx.Client") as client_class:
        client_class.return_value.__enter__.return_value = client
        with pytest.raises(TimeoutError, match="Poll timed out after 1s"):
            StagedUploadDestination()._poll(
                poll_config, {}, retry_config, limiter, url=poll_config.url
            )

    assert sleeps == [1.0]
    assert client.request.call_count == 2


def test_poll_clamps_retry_backoff_to_remaining_timeout() -> None:
    poll_config = StagedUploadPollConfig(
        url="https://api.example.com/jobs/j-1",
        interval_seconds=0,
        timeout_seconds=5,
    )
    retry_config = RetryConfig(
        max_attempts=4,
        initial_backoff=2.0,
        backoff_multiplier=3.0,
        max_backoff=60.0,
        retryable_status_codes=(429, 503),
    )
    response = httpx.Response(
        200,
        json={"status": "SUCCEEDED"},
        request=httpx.Request("GET", poll_config.url),
    )

    with (
        patch("drt.destinations.staged_upload.httpx.Client"),
        patch(
            "drt.destinations.staged_upload.time.monotonic",
            side_effect=[100.0, 101.0, 101.0],
        ),
        patch("drt.destinations.staged_upload.with_retry", return_value=response) as retry_call,
    ):
        StagedUploadDestination()._poll(
            poll_config, {}, retry_config, MagicMock(), url=poll_config.url
        )

    clamped = retry_call.call_args.args[1]
    assert clamped is not retry_config
    assert clamped.max_backoff == 4.0
    assert clamped.max_attempts == retry_config.max_attempts
    assert clamped.initial_backoff == retry_config.initial_backoff
    assert clamped.backoff_multiplier == retry_config.backoff_multiplier
    assert clamped.retryable_status_codes == retry_config.retryable_status_codes
    assert retry_config.max_backoff == 60.0


def test_poll_clamps_request_timeout_to_fresh_remaining_budget() -> None:
    poll_config = StagedUploadPollConfig(
        url="https://api.example.com/jobs/j-1",
        interval_seconds=0,
        timeout_seconds=5,
    )
    response = httpx.Response(
        200,
        json={"status": "SUCCEEDED"},
        request=httpx.Request("GET", poll_config.url),
    )

    with (
        patch("drt.destinations.staged_upload.httpx.Client") as client_class,
        patch(
            "drt.destinations.staged_upload.time.monotonic",
            side_effect=[100.0, 101.0, 102.0, 102.0],
        ),
        patch(
            "drt.destinations.staged_upload.with_retry",
            side_effect=lambda operation, _config: operation(),
        ),
    ):
        client = client_class.return_value.__enter__.return_value
        client.request.return_value = response
        StagedUploadDestination()._poll(
            poll_config, {}, RetryConfig(), MagicMock(), url=poll_config.url
        )

    assert client.request.call_args.kwargs["timeout"] == 3.0


def test_poll_raises_timeout_at_loop_top_without_another_request() -> None:
    """If the deadline has already passed by the time the next iteration
    starts (e.g. the interval sleep after a non-terminal status consumed
    the remaining budget), the loop must raise immediately rather than
    attempt one more request."""
    poll_config = StagedUploadPollConfig(
        url="https://api.example.com/jobs/j-1",
        interval_seconds=0,
        timeout_seconds=5,
    )
    running = httpx.Response(
        200,
        json={"status": "RUNNING"},
        request=httpx.Request("GET", poll_config.url),
    )

    with (
        patch("drt.destinations.staged_upload.httpx.Client") as client_class,
        patch(
            "drt.destinations.staged_upload.time.monotonic",
            side_effect=[100.0, 101.0, 101.0, 101.0, 106.0],
        ),
        patch(
            "drt.destinations.staged_upload.with_retry",
            side_effect=lambda operation, _config: operation(),
        ),
    ):
        client = client_class.return_value.__enter__.return_value
        client.request.return_value = running
        with pytest.raises(TimeoutError, match="Poll timed out after 5s"):
            StagedUploadDestination()._poll(
                poll_config, {}, RetryConfig(), MagicMock(), url=poll_config.url
            )

    assert client.request.call_count == 1


def test_finalize_poll_acquires_rate_limiter_per_status_check() -> None:
    config = StagedUploadDestinationConfig(
        type="staged_upload",
        stage=StagedUploadPhaseConfig(
            url="https://upload.example.com",
            response_extract={"upload_id": "uploadId"},
        ),
        trigger=StagedUploadPhaseConfig(
            url="https://api.example.com/jobs",
            response_extract={"job_id": "jobId"},
        ),
        poll=StagedUploadPollConfig(
            url="https://api.example.com/jobs/{{ job_id }}",
            interval_seconds=0,
            timeout_seconds=5,
        ),
    )
    options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=7))
    limiter = MagicMock()
    dest = StagedUploadDestination()
    dest._records = [{"x": 1}]
    client = MagicMock()
    client.request.side_effect = [
        httpx.Response(
            200,
            json={"status": "RUNNING"},
            request=httpx.Request("GET", "https://api.example.com/jobs/j-1"),
        ),
        httpx.Response(
            200,
            json={"status": "SUCCEEDED"},
            request=httpx.Request("GET", "https://api.example.com/jobs/j-1"),
        ),
    ]

    with (
        patch.object(
            dest,
            "_http_phase",
            side_effect=[{"uploadId": "u-1"}, {"jobId": "j-1"}],
        ),
        patch("drt.destinations.staged_upload.httpx.Client") as client_class,
        patch(
            "drt.destinations.staged_upload.resolve_rate_limiter",
            return_value=limiter,
        ) as resolve_limiter,
    ):
        client_class.return_value.__enter__.return_value = client
        result = dest.finalize(config, options)

    assert result.success == 1
    resolve_limiter.assert_called_once()
    assert limiter.acquire.call_count == 2


def test_finalize_keys_rate_limiter_by_rendered_poll_host_not_trigger_host() -> None:
    """#1068: when the vendor's trigger response supplies the *entire* poll
    URL (not just a path segment appended to a static host), the rendered
    host — not the trigger's — must own the limiter identity. Two configs
    with different trigger hosts but the same rendered poll host must
    resolve to the same key; a config whose rendered poll host differs must
    resolve to a different one."""
    limiter = MagicMock()

    def _config(trigger_host: str) -> StagedUploadDestinationConfig:
        return StagedUploadDestinationConfig(
            type="staged_upload",
            stage=StagedUploadPhaseConfig(url="https://storage.example.com/upload"),
            trigger=StagedUploadPhaseConfig(
                url=f"https://{trigger_host}/jobs",
                response_extract={"status_url": "statusUrl"},
            ),
            poll=StagedUploadPollConfig(
                url="{{ status_url }}",
                interval_seconds=0,
                timeout_seconds=5,
            ),
        )

    def _run(config: StagedUploadDestinationConfig, poll_host: str) -> str:
        dest = StagedUploadDestination()
        dest._records = [{"x": 1}]
        with (
            patch.object(
                dest,
                "_http_phase",
                side_effect=[{}, {"statusUrl": f"https://{poll_host}/status/abc"}],
            ),
            patch("drt.destinations.staged_upload.httpx.Client"),
            patch(
                "drt.destinations.staged_upload.resolve_rate_limiter",
                return_value=limiter,
            ) as resolve_limiter,
            patch.object(dest, "_poll"),
        ):
            result = dest.finalize(config, _options())
        assert result.success == 1
        key_override: str = resolve_limiter.call_args.kwargs["key_override"]
        return key_override

    same_a = _run(_config("api-a.example.com"), poll_host="poll.example.com")
    same_b = _run(_config("api-b.example.com"), poll_host="poll.example.com")
    different = _run(_config("api-a.example.com"), poll_host="poll-other.example.com")

    assert same_a == "staged_upload:poll.example.com"
    assert same_a == same_b
    assert different == "staged_upload:poll-other.example.com"
    assert different != same_a


def test_finalize_stage_error(httpserver: HTTPServer) -> None:
    """Stage endpoint returns 500."""
    httpserver.expect_request("/upload").respond_with_data("error", status=500)

    config = StagedUploadDestinationConfig(
        type="staged_upload",
        stage=StagedUploadPhaseConfig(
            url=httpserver.url_for("/upload"),
        ),
        trigger=StagedUploadPhaseConfig(url="http://unused"),
    )

    dest = StagedUploadDestination()
    dest._records = [{"x": 1}]
    result = dest.finalize(config, _options())

    assert result.success == 0
    assert result.failed == 1
    assert len(result.errors) == 1


def test_records_cleared_after_finalize(httpserver: HTTPServer) -> None:
    """Records are cleared even on failure."""
    httpserver.expect_request("/upload").respond_with_data("error", status=500)

    config = StagedUploadDestinationConfig(
        type="staged_upload",
        stage=StagedUploadPhaseConfig(
            url=httpserver.url_for("/upload"),
        ),
        trigger=StagedUploadPhaseConfig(url="http://unused"),
    )

    dest = StagedUploadDestination()
    dest._records = [{"x": 1}]
    dest.finalize(config, _options())

    assert dest._records == []
