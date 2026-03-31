"""Unit tests for RowError and DetailedSyncResult."""

from __future__ import annotations

import json

from drt.destinations.base import SyncResult
from drt.destinations.row_errors import RowError


class TestRowError:
    def test_basic_creation(self) -> None:
        err = RowError(
            batch_index=0,
            record_preview='{"id": 1}',
            http_status=422,
            error_message="invalid email format",
        )
        assert err.batch_index == 0
        assert err.record_preview == '{"id": 1}'
        assert err.http_status == 422
        assert err.error_message == "invalid email format"

    def test_timestamp_is_iso8601(self) -> None:
        err = RowError(
            batch_index=0,
            record_preview="{}",
            http_status=None,
            error_message="oops",
        )
        # ISO8601 timestamps contain "T" and "+"  or "Z"
        assert "T" in err.timestamp

    def test_http_status_can_be_none(self) -> None:
        err = RowError(
            batch_index=5,
            record_preview="{}",
            http_status=None,
            error_message="connection error",
        )
        assert err.http_status is None

    def test_record_preview_truncated_at_200_chars(self) -> None:
        record = {"key": "x" * 300}
        preview = json.dumps(record)[:200]
        err = RowError(
            batch_index=0,
            record_preview=preview,
            http_status=400,
            error_message="bad request",
        )
        assert len(err.record_preview) <= 200

    def test_record_preview_exactly_200_chars(self) -> None:
        # Build a record whose JSON representation is well over 200 chars
        record = {"key": "a" * 250}
        full_json = json.dumps(record)
        assert len(full_json) > 200
        preview = full_json[:200]
        err = RowError(
            batch_index=1,
            record_preview=preview,
            http_status=422,
            error_message="too long",
        )
        assert len(err.record_preview) == 200


class TestSyncResultRowErrors:
    def test_initial_state(self) -> None:
        result = SyncResult()
        assert result.success == 0
        assert result.failed == 0
        assert result.skipped == 0
        assert result.row_errors == []

    def test_total_property(self) -> None:
        result = SyncResult(success=3, failed=1, skipped=1)
        assert result.total == 5

    def test_append_row_errors(self) -> None:
        result = SyncResult()
        for i in range(3):
            result.row_errors.append(
                RowError(
                    batch_index=i,
                    record_preview=json.dumps({"id": i})[:200],
                    http_status=422,
                    error_message=f"error {i}",
                )
            )
        assert len(result.row_errors) == 3

    def test_row_errors_empty_on_success(self) -> None:
        result = SyncResult(success=5)
        assert result.row_errors == []
