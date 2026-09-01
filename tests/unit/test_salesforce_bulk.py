"""Unit tests for SalesforceBulkDestination (Bulk API 2.0)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import httpx
import pytest

from drt.config.models import (
    RateLimitConfig,
    RetryConfig,
    SalesforceBulkDestinationConfig,
    SyncOptions,
)
from drt.destinations.salesforce_bulk import SalesforceBulkDestination

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _config(instance_url: str = "https://test.salesforce.com") -> SalesforceBulkDestinationConfig:
    return SalesforceBulkDestinationConfig(
        type="salesforce_bulk",
        instance_url=instance_url,
        object_name="Contact",
        operation="upsert",
        external_id_field="External_Id__c",
        poll_timeout_seconds=60,
        poll_interval_seconds=0,  # no sleep in tests
        client_id_env="SF_CLIENT_ID",
        client_secret_env="SF_CLIENT_SECRET",
        username_env="SF_USERNAME",
        password_env="SF_PASSWORD",
    )


def _sync_options() -> SyncOptions:
    return SyncOptions(
        batch_size=100,
        rate_limit=RateLimitConfig(requests_per_second=1000),
        on_error="skip",
    )


def _make_response(status_code: int, json_body: dict | None = None, text: str = "") -> MagicMock:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = status_code
    resp.text = text
    resp.json.return_value = json_body or {}
    return resp


def _set_sf_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SF_CLIENT_ID", "test_client_id")
    monkeypatch.setenv("SF_CLIENT_SECRET", "test_client_secret")
    monkeypatch.setenv("SF_USERNAME", "test@example.com")
    monkeypatch.setenv("SF_PASSWORD", "testpassword")


# ---------------------------------------------------------------------------
# Test 1: stage() accumulates records without making HTTP calls
# ---------------------------------------------------------------------------


def test_stage_accumulates_records() -> None:
    dest = SalesforceBulkDestination()
    config = _config()
    options = _sync_options()

    dest.stage([{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}], config, options)
    dest.stage([{"id": "3", "name": "Carol"}, {"id": "4", "name": "Dave"}], config, options)

    assert len(dest._records) == 4


# ---------------------------------------------------------------------------
# Test 2: finalize() full lifecycle — all records succeed
# ---------------------------------------------------------------------------


def test_finalize_full_lifecycle_success(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_sf_env(monkeypatch)
    dest = SalesforceBulkDestination()
    config = _config()
    options = _sync_options()

    dest.stage([{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}], config, options)

    mock_client = MagicMock()
    # POST /oauth2/token
    mock_client.post.side_effect = [
        _make_response(200, {"access_token": "tok123"}),
        _make_response(200, {"id": "job001"}),
    ]
    # PUT /batches
    mock_client.put.return_value = _make_response(201, {})
    # PATCH /close
    mock_client.patch.return_value = _make_response(200, {})
    # GET /poll
    mock_client.get.return_value = _make_response(
        200,
        {
            "state": "JobComplete",
            "numberRecordsProcessed": 2,
            "numberRecordsFailed": 0,
        },
    )

    with patch("httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value = mock_client
        result = dest.finalize(config, options)

    assert result.success == 2
    assert result.failed == 0
    assert result.rows_extracted == 2
    assert result.row_errors == []


# ---------------------------------------------------------------------------
# Test 3: finalize() with failed records — row_errors populated
# ---------------------------------------------------------------------------


def test_finalize_with_failed_records(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_sf_env(monkeypatch)
    dest = SalesforceBulkDestination()
    config = _config()
    options = _sync_options()

    dest.stage([{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bad"}], config, options)

    failed_csv = "sf__Id,sf__Error,Name\n001,FIELD_ERROR,Bad\n"

    mock_client = MagicMock()
    mock_client.post.side_effect = [
        _make_response(200, {"access_token": "tok123"}),
        _make_response(200, {"id": "job001"}),
    ]
    mock_client.put.return_value = _make_response(201, {})
    mock_client.patch.return_value = _make_response(200, {})

    poll_resp = _make_response(
        200,
        {
            "state": "JobComplete",
            "numberRecordsProcessed": 2,
            "numberRecordsFailed": 1,
        },
    )
    failed_resp = MagicMock(spec=httpx.Response)
    failed_resp.status_code = 200
    failed_resp.text = failed_csv

    mock_client.get.side_effect = [poll_resp, failed_resp]

    with patch("httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value = mock_client
        result = dest.finalize(config, options)

    assert result.failed == 1
    assert len(result.row_errors) == 1
    assert result.row_errors[0].error_message == "FIELD_ERROR"


# ---------------------------------------------------------------------------
# Test 4: finalize() raises RuntimeError when job state is "Failed"
# ---------------------------------------------------------------------------


def test_finalize_job_failed_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_sf_env(monkeypatch)
    dest = SalesforceBulkDestination()
    config = _config()
    options = _sync_options()

    dest.stage([{"id": "1", "name": "Alice"}], config, options)

    mock_client = MagicMock()
    mock_client.post.side_effect = [
        _make_response(200, {"access_token": "tok123"}),
        _make_response(200, {"id": "job001"}),
    ]
    mock_client.put.return_value = _make_response(201, {})
    mock_client.patch.return_value = _make_response(200, {})
    mock_client.get.return_value = _make_response(200, {"state": "Failed"})

    with patch("httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value = mock_client
        with pytest.raises(RuntimeError, match="Failed"):
            dest.finalize(config, options)


# ---------------------------------------------------------------------------
# Test 5: finalize() with no staged records returns early — no HTTP calls
# ---------------------------------------------------------------------------


def test_empty_records_returns_early() -> None:
    dest = SalesforceBulkDestination()
    config = _config()
    options = _sync_options()

    # No stage() calls — _records is empty
    with patch("httpx.Client") as mock_client_cls:
        result = dest.finalize(config, options)
        mock_client_cls.assert_not_called()

    assert result.rows_extracted == 0


def test_status_poll_retries_transient_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_sf_env(monkeypatch)
    dest = SalesforceBulkDestination()
    config = _config()
    options = SyncOptions(
        retry=RetryConfig(max_attempts=2, initial_backoff=0.0, max_backoff=0.0),
        rate_limit=RateLimitConfig(requests_per_second=0),
    )
    dest.stage([{"id": "1", "name": "Alice"}], config, options)

    mock_client = MagicMock()
    mock_client.post.side_effect = [
        _make_response(200, {"access_token": "tok123"}),
        _make_response(200, {"id": "job001"}),
    ]
    mock_client.put.return_value = _make_response(201, {})
    mock_client.patch.return_value = _make_response(200, {})
    transient = httpx.Response(
        503,
        request=httpx.Request(
            "GET", "https://test.salesforce.com/services/data/v58.0/jobs/ingest/job001"
        ),
    )
    complete = _make_response(
        200,
        {
            "state": "JobComplete",
            "numberRecordsProcessed": 1,
            "numberRecordsFailed": 0,
        },
    )
    mock_client.get.side_effect = [transient, complete]

    with patch("httpx.Client") as mock_client_cls:
        mock_client_cls.return_value.__enter__.return_value = mock_client
        result = dest.finalize(config, options)

    assert result.success == 1
    assert mock_client.get.call_count == 2


def test_status_poll_retry_acquires_rate_limiter_per_attempt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_sf_env(monkeypatch)
    dest = SalesforceBulkDestination()
    config = _config()
    options = SyncOptions(
        retry=RetryConfig(max_attempts=2, initial_backoff=0.0, max_backoff=0.0),
        rate_limit=RateLimitConfig(requests_per_second=0),
    )
    dest.stage([{"id": "1", "name": "Alice"}], config, options)

    mock_client = MagicMock()
    mock_client.post.side_effect = [
        _make_response(200, {"access_token": "tok123"}),
        _make_response(200, {"id": "job001"}),
    ]
    mock_client.put.return_value = _make_response(201, {})
    mock_client.patch.return_value = _make_response(200, {})
    mock_client.get.side_effect = [
        httpx.Response(
            503,
            request=httpx.Request(
                "GET", "https://test.salesforce.com/services/data/v58.0/jobs/ingest/job001"
            ),
        ),
        _make_response(
            200,
            {
                "state": "JobComplete",
                "numberRecordsProcessed": 1,
                "numberRecordsFailed": 0,
            },
        ),
    ]
    limiter = MagicMock()

    with (
        patch("httpx.Client") as mock_client_cls,
        patch(
            "drt.destinations.salesforce_bulk.resolve_rate_limiter",
            return_value=limiter,
        ),
    ):
        mock_client_cls.return_value.__enter__.return_value = mock_client
        result = dest.finalize(config, options)

    assert result.success == 1
    assert mock_client.get.call_count == 2
    assert limiter.acquire.call_count == 2


def test_status_poll_retry_success_after_deadline_raises_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_sf_env(monkeypatch)
    dest = SalesforceBulkDestination()
    config = _config().model_copy(update={"poll_timeout_seconds": 1})
    options = SyncOptions(
        retry=RetryConfig(max_attempts=2, initial_backoff=60.0, max_backoff=60.0),
        rate_limit=RateLimitConfig(requests_per_second=0),
    )
    dest.stage([{"id": "1", "name": "Alice"}], config, options)

    mock_client = MagicMock()
    mock_client.post.side_effect = [
        _make_response(200, {"access_token": "tok123"}),
        _make_response(200, {"id": "job001"}),
    ]
    mock_client.put.return_value = _make_response(201, {})
    mock_client.patch.return_value = _make_response(200, {})
    mock_client.get.side_effect = [
        httpx.Response(
            503,
            request=httpx.Request(
                "GET", "https://test.salesforce.com/services/data/v58.0/jobs/ingest/job001"
            ),
        ),
        _make_response(200, {"state": "JobComplete"}),
    ]
    clock = {"now": 0.0}
    sleeps: list[float] = []

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["now"] += seconds + 0.01

    monkeypatch.setattr(
        "drt.destinations.salesforce_bulk.time.monotonic", lambda: clock["now"]
    )
    monkeypatch.setattr("drt.destinations.retry.time.sleep", fake_sleep)

    with (
        patch("httpx.Client") as mock_client_cls,
        patch(
            "drt.destinations.salesforce_bulk.resolve_rate_limiter",
            return_value=MagicMock(),
        ),
    ):
        mock_client_cls.return_value.__enter__.return_value = mock_client
        with pytest.raises(TimeoutError, match="did not complete within 1s"):
            dest.finalize(config, options)

    assert sleeps == [1.0]
    assert mock_client.get.call_count == 2


def test_status_poll_clamps_retry_backoff_to_remaining_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_sf_env(monkeypatch)
    dest = SalesforceBulkDestination()
    config = _config().model_copy(update={"poll_timeout_seconds": 5})
    retry_config = RetryConfig(
        max_attempts=4,
        initial_backoff=2.0,
        backoff_multiplier=3.0,
        max_backoff=60.0,
        retryable_status_codes=(429, 503),
    )
    options = SyncOptions(retry=retry_config)
    dest.stage([{"id": "1", "name": "Alice"}], config, options)

    mock_client = MagicMock()
    mock_client.post.side_effect = [
        _make_response(200, {"access_token": "tok123"}),
        _make_response(200, {"id": "job001"}),
    ]
    mock_client.put.return_value = _make_response(201, {})
    mock_client.patch.return_value = _make_response(200, {})
    complete = _make_response(
        200,
        {
            "state": "JobComplete",
            "numberRecordsProcessed": 1,
            "numberRecordsFailed": 0,
        },
    )

    with (
        patch("httpx.Client") as mock_client_cls,
        patch(
            "drt.destinations.salesforce_bulk.time.monotonic",
            side_effect=[100.0, 101.0, 101.0],
        ),
        patch(
            "drt.destinations.salesforce_bulk.with_retry", return_value=complete
        ) as retry_call,
        patch(
            "drt.destinations.salesforce_bulk.resolve_rate_limiter",
            return_value=MagicMock(),
        ),
    ):
        mock_client_cls.return_value.__enter__.return_value = mock_client
        result = dest.finalize(config, options)

    clamped = retry_call.call_args.args[1]
    assert result.success == 1
    assert clamped is not retry_config
    assert clamped.max_backoff == 4.0
    assert clamped.max_attempts == retry_config.max_attempts
    assert clamped.initial_backoff == retry_config.initial_backoff
    assert clamped.backoff_multiplier == retry_config.backoff_multiplier
    assert clamped.retryable_status_codes == retry_config.retryable_status_codes
    assert retry_config.max_backoff == 60.0


def test_status_poll_clamps_request_timeout_to_fresh_remaining_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_sf_env(monkeypatch)
    dest = SalesforceBulkDestination()
    config = _config().model_copy(update={"poll_timeout_seconds": 5})
    options = SyncOptions(retry=RetryConfig())
    dest.stage([{"id": "1", "name": "Alice"}], config, options)

    mock_client = MagicMock()
    mock_client.post.side_effect = [
        _make_response(200, {"access_token": "tok123"}),
        _make_response(200, {"id": "job001"}),
    ]
    mock_client.put.return_value = _make_response(201, {})
    mock_client.patch.return_value = _make_response(200, {})
    mock_client.get.return_value = _make_response(
        200,
        {
            "state": "JobComplete",
            "numberRecordsProcessed": 1,
            "numberRecordsFailed": 0,
        },
    )

    with (
        patch("httpx.Client") as mock_client_cls,
        patch(
            "drt.destinations.salesforce_bulk.time.monotonic",
            side_effect=[100.0, 101.0, 102.0, 102.0],
        ),
        patch(
            "drt.destinations.salesforce_bulk.with_retry",
            side_effect=lambda operation, _config: operation(),
        ),
        patch(
            "drt.destinations.salesforce_bulk.resolve_rate_limiter",
            return_value=MagicMock(),
        ),
    ):
        mock_client_cls.return_value.__enter__.return_value = mock_client
        result = dest.finalize(config, options)

    assert result.success == 1
    assert mock_client.get.call_args.kwargs["timeout"] == 3.0


def test_status_poll_acquires_rate_limiter_per_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_sf_env(monkeypatch)
    dest = SalesforceBulkDestination()
    config = _config()
    options = _sync_options()
    dest.stage([{"id": "1", "name": "Alice"}], config, options)

    mock_client = MagicMock()
    mock_client.post.side_effect = [
        _make_response(200, {"access_token": "tok123"}),
        _make_response(200, {"id": "job001"}),
    ]
    mock_client.put.return_value = _make_response(201, {})
    mock_client.patch.return_value = _make_response(200, {})
    mock_client.get.side_effect = [
        _make_response(200, {"state": "InProgress"}),
        _make_response(
            200,
            {
                "state": "JobComplete",
                "numberRecordsProcessed": 1,
                "numberRecordsFailed": 0,
            },
        ),
    ]
    limiter = MagicMock()

    with (
        patch("httpx.Client") as mock_client_cls,
        patch(
            "drt.destinations.salesforce_bulk.resolve_rate_limiter",
            return_value=limiter,
        ) as resolve_limiter,
    ):
        mock_client_cls.return_value.__enter__.return_value = mock_client
        result = dest.finalize(config, options)

    assert result.success == 1
    resolve_limiter.assert_called_once()
    assert limiter.acquire.call_count == 2
