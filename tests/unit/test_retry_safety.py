"""Operation replay classification from ADR 0023."""

from __future__ import annotations

from unittest.mock import patch

import httpx
import pytest

from drt.config.models import RetryConfig
from drt.destinations.retry import (
    DeliveryUnknownError,
    FailureSafety,
    OperationSafety,
    classify_failure_safety,
    with_retry,
)


def _status_error(status: int) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://api.example.com/objects")
    response = httpx.Response(status, request=request)
    return httpx.HTTPStatusError("failed", request=request, response=response)


@pytest.mark.parametrize(
    "error",
    [
        httpx.ConnectError("connect failed"),
        httpx.ConnectTimeout("connect timed out"),
        httpx.PoolTimeout("no connection acquired"),
        httpx.ProxyError("proxy connection failed"),
        httpx.UnsupportedProtocol("unsupported protocol"),
        _status_error(429),
    ],
)
def test_create_pre_send_and_429_failures_are_safe_to_replay(error: Exception) -> None:
    assert (
        classify_failure_safety(OperationSafety.BLIND_CREATE, error) is FailureSafety.SAFE_TO_REPLAY
    )


@pytest.mark.parametrize(
    "error",
    [
        httpx.ReadError("response dropped"),
        httpx.ReadTimeout("response timed out"),
        httpx.WriteError("request write failed"),
        httpx.WriteTimeout("request write timed out"),
        httpx.CloseError("connection close failed"),
        httpx.LocalProtocolError("client protocol error"),
        httpx.RemoteProtocolError("invalid response"),
        _status_error(500),
        _status_error(503),
    ],
)
def test_create_post_send_or_5xx_failures_are_delivery_unknown(error: Exception) -> None:
    assert (
        classify_failure_safety(OperationSafety.RECONCILIABLE_CREATE, error)
        is FailureSafety.DELIVERY_UNKNOWN
    )


@pytest.mark.parametrize("operation", [OperationSafety.IDEMPOTENT, OperationSafety.KEYED])
def test_proven_safe_operations_replay_after_any_retryable_failure(
    operation: OperationSafety,
) -> None:
    assert (
        classify_failure_safety(operation, httpx.ReadError("response dropped"))
        is FailureSafety.SAFE_TO_REPLAY
    )


def test_blind_create_is_not_replayed_after_delivery_unknown() -> None:
    attempts = 0

    def create() -> str:
        nonlocal attempts
        attempts += 1
        raise httpx.ReadError("response dropped")

    config = RetryConfig(max_attempts=3, initial_backoff=0.0)
    with patch("drt.destinations.retry.time.sleep") as sleep:
        with pytest.raises(DeliveryUnknownError, match="Delivery is unknown"):
            with_retry(create, config, operation=OperationSafety.BLIND_CREATE)

    assert attempts == 1
    sleep.assert_not_called()


def test_blind_create_still_retries_a_connect_error() -> None:
    attempts = 0

    def create() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("connect failed")
        return "ok"

    config = RetryConfig(max_attempts=2, initial_backoff=0.0)
    with patch("drt.destinations.retry.time.sleep"):
        assert with_retry(create, config, operation=OperationSafety.BLIND_CREATE) == "ok"
    assert attempts == 2
