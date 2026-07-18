"""Exponential backoff retry for transient HTTP failures.

Rust-migration note: pure logic, no I/O. Easily portable to Rust.
"""

from __future__ import annotations

import math
import time
from collections.abc import Callable
from datetime import timezone
from email.utils import parsedate_to_datetime
from typing import TypeVar

import httpx

from drt.config.models import RetryConfig, SyncOptions

T = TypeVar("T")


def resolve_retry(
    config_retry: RetryConfig | None,
    sync_options: SyncOptions,
) -> RetryConfig:
    """Pick the retry config for this destination invocation.

    Priority order: ``destination.retry`` > ``sync.retry`` > ``RetryConfig()``.
    ``sync_options.retry`` is always populated (default_factory=RetryConfig),
    so when no destination-level override is set the sync-level config wins.
    """
    return config_retry if config_retry is not None else sync_options.retry


def parse_retry_after(value: str | None, now: float | None = None) -> float | None:
    """Parse an HTTP ``Retry-After`` header into a wait in seconds (#769).

    Both RFC 7231 forms are supported:

    - **delay-seconds** — ``Retry-After: 120`` → ``120.0``
    - **HTTP-date** — ``Retry-After: Wed, 21 Oct 2026 07:28:00 GMT`` → the
      number of seconds until that instant (clamped at 0 for a past date)

    Returns ``None`` when the header is absent or unparseable, so the caller
    falls back to computed backoff. A past HTTP-date yields ``0.0`` (retry
    now) rather than ``None`` — the server did send an instruction, it has
    just already elapsed. ``now`` (epoch seconds) is injectable for tests.
    """
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None

    # delay-seconds form (the common case: 429/503 with an integer).
    try:
        seconds = float(text)
    except ValueError:
        pass
    else:
        if math.isfinite(seconds) and seconds >= 0:
            return seconds
        return None

    # HTTP-date form.
    try:
        dt = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        return None
    if dt is None:
        return None
    if dt.tzinfo is None:  # RFC dates are GMT; parsedate may return naive
        dt = dt.replace(tzinfo=timezone.utc)
    current = time.time() if now is None else now
    delta = dt.timestamp() - current
    return delta if delta > 0 else 0.0


def _retry_after_from_response(response: object) -> float | None:
    """Best-effort ``Retry-After`` extraction from an httpx-like response.

    Real ``httpx.Response`` objects always expose a ``.headers`` mapping, but
    ``with_retry`` wraps arbitrary callables whose exception may carry a
    response-like object without one — degrade to computed backoff rather than
    raise from inside the retry loop.
    """
    headers = getattr(response, "headers", None)
    getter = getattr(headers, "get", None)
    if not callable(getter):
        return None
    try:
        raw = getter("Retry-After")
    except Exception:
        return None
    return parse_retry_after(raw if isinstance(raw, str) else None)


def with_retry(fn: Callable[[], T], config: RetryConfig) -> T:
    """Execute ``fn`` with exponential backoff on transient failures.

    Retries on:
    - ``httpx.HTTPStatusError`` with a status code in ``config.retryable_status_codes``
    - ``httpx.TransportError`` (network-level failures)

    When a retryable HTTP response carries a ``Retry-After`` header (429 / 503
    from Slack, HubSpot, Intercom, Zendesk, …), the server's stated delay is
    honoured: the sleep becomes ``max(retry_after, computed_backoff)`` so we
    never retry *before* the API is ready, still bounded by ``max_backoff`` so
    a large or hostile header can't stall the run. Network failures and
    responses without the header keep pure exponential backoff.

    Raises the last exception if all attempts are exhausted.
    """
    backoff = config.initial_backoff
    last_exc: Exception | None = None

    for attempt in range(1, config.max_attempts + 1):
        retry_after: float | None = None
        try:
            return fn()
        except httpx.HTTPStatusError as e:
            if e.response.status_code not in config.retryable_status_codes:
                raise
            last_exc = e
            retry_after = _retry_after_from_response(e.response)
        except httpx.TransportError as e:
            last_exc = e

        if attempt < config.max_attempts:
            wait = backoff if retry_after is None else max(retry_after, backoff)
            time.sleep(min(wait, config.max_backoff))
            backoff *= config.backoff_multiplier

    assert last_exc is not None
    raise last_exc
