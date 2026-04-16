"""Tests for staged_upload destination."""

from __future__ import annotations

import json

from pytest_httpserver import HTTPServer

from drt.config.models import (
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
