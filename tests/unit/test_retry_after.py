"""Unit tests for ``Retry-After`` header handling in ``with_retry`` (#769).

On a retryable HTTP response (429 / 503) that carries a ``Retry-After``
header, ``with_retry`` should sleep at least as long as the server asked —
``max(retry_after, computed_backoff)`` — bounded by ``max_backoff`` — instead
of its blind exponential backoff. Network errors and header-less responses
keep the exponential path unchanged.

No real HTTP or wall-clock sleeping: ``time.sleep`` is patched and inspected.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import httpx
import pytest

from drt.config.models import RetryConfig
from drt.destinations.retry import parse_retry_after, with_retry

# ---------------------------------------------------------------------------
# parse_retry_after
# ---------------------------------------------------------------------------


def test_parse_delay_seconds() -> None:
    assert parse_retry_after("120") == 120.0
    assert parse_retry_after("0") == 0.0
    assert parse_retry_after("  30 ") == 30.0


def test_parse_absent_or_blank_returns_none() -> None:
    assert parse_retry_after(None) is None
    assert parse_retry_after("") is None
    assert parse_retry_after("   ") is None


def test_parse_garbage_returns_none() -> None:
    assert parse_retry_after("soon") is None
    assert parse_retry_after("-5") is None  # negative delay is invalid
    assert parse_retry_after("inf") is None  # non-finite rejected


def test_parse_http_date_future() -> None:
    # 60 seconds after the injected "now".
    now = 1_000_000.0
    # Wed, ... — build a date 60s ahead via a known epoch through email.utils.
    from email.utils import formatdate

    header = formatdate(now + 60, usegmt=True)
    got = parse_retry_after(header, now=now)
    assert got is not None
    assert abs(got - 60) < 1.0  # 1s tolerance for date-string second rounding


def test_parse_http_date_in_the_past_clamps_to_zero() -> None:
    from email.utils import formatdate

    now = 1_000_000.0
    header = formatdate(now - 300, usegmt=True)
    assert parse_retry_after(header, now=now) == 0.0


# ---------------------------------------------------------------------------
# with_retry honouring Retry-After
# ---------------------------------------------------------------------------


def _status_error(status: int, headers: dict[str, str] | None = None) -> httpx.HTTPStatusError:
    request = httpx.Request("POST", "https://api.example.com/v1/things")
    response = httpx.Response(status, headers=headers or {}, request=request)
    return httpx.HTTPStatusError("rate limited", request=request, response=response)


def _config(**overrides: Any) -> RetryConfig:
    defaults: dict[str, Any] = {
        "max_attempts": 3,
        "initial_backoff": 1.0,
        "backoff_multiplier": 2.0,
        "max_backoff": 60.0,
    }
    defaults.update(overrides)
    return RetryConfig(**defaults)


def test_retry_after_overrides_short_backoff() -> None:
    """429 with Retry-After: 30 sleeps 30s, not the 1s initial backoff."""
    calls = [
        lambda: (_ for _ in ()).throw(_status_error(429, {"Retry-After": "30"})),
        lambda: "ok",
    ]
    fn = lambda: calls.pop(0)()  # noqa: E731

    with patch("drt.destinations.retry.time.sleep") as sleep:
        result = with_retry(fn, _config())

    assert result == "ok"
    sleep.assert_called_once_with(30.0)


def test_retry_after_is_capped_by_max_backoff() -> None:
    """A large Retry-After can't stall the run past max_backoff."""
    calls = [
        lambda: (_ for _ in ()).throw(_status_error(503, {"Retry-After": "3600"})),
        lambda: "ok",
    ]
    fn = lambda: calls.pop(0)()  # noqa: E731

    with patch("drt.destinations.retry.time.sleep") as sleep:
        with_retry(fn, _config(max_backoff=60.0))

    sleep.assert_called_once_with(60.0)


def test_computed_backoff_wins_when_larger_than_retry_after() -> None:
    """max(retry_after, backoff): a tiny Retry-After doesn't shrink backoff."""
    calls = [
        lambda: (_ for _ in ()).throw(_status_error(429, {"Retry-After": "0"})),
        lambda: "ok",
    ]
    fn = lambda: calls.pop(0)()  # noqa: E731

    with patch("drt.destinations.retry.time.sleep") as sleep:
        with_retry(fn, _config(initial_backoff=5.0))

    sleep.assert_called_once_with(5.0)


def test_no_retry_after_header_uses_exponential_backoff() -> None:
    """Header-less 429 keeps the pre-existing exponential path."""
    calls = [
        lambda: (_ for _ in ()).throw(_status_error(429)),
        lambda: (_ for _ in ()).throw(_status_error(429)),
        lambda: "ok",
    ]
    fn = lambda: calls.pop(0)()  # noqa: E731

    with patch("drt.destinations.retry.time.sleep") as sleep:
        with_retry(fn, _config(initial_backoff=1.0, backoff_multiplier=2.0))

    # attempt 1 -> 1.0, attempt 2 -> 2.0 (unchanged behaviour)
    assert [c.args[0] for c in sleep.call_args_list] == [1.0, 2.0]


def test_transport_error_has_no_header_and_uses_backoff() -> None:
    """Network failures carry no response, so backoff applies as before."""
    calls = [
        lambda: (_ for _ in ()).throw(httpx.ConnectError("boom")),
        lambda: "ok",
    ]
    fn = lambda: calls.pop(0)()  # noqa: E731

    with patch("drt.destinations.retry.time.sleep") as sleep:
        result = with_retry(fn, _config(initial_backoff=1.0))

    assert result == "ok"
    sleep.assert_called_once_with(1.0)


def test_non_retryable_status_still_raises_immediately() -> None:
    """A 400 is not retryable — Retry-After handling must not swallow it."""
    fn = lambda: (_ for _ in ()).throw(  # noqa: E731
        _status_error(400, {"Retry-After": "10"})
    )
    with patch("drt.destinations.retry.time.sleep") as sleep:
        with pytest.raises(httpx.HTTPStatusError):
            with_retry(fn, _config())
    sleep.assert_not_called()
