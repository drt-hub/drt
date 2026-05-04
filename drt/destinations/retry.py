"""Exponential backoff retry for transient HTTP failures.

Rust-migration note: pure logic, no I/O. Easily portable to Rust.
"""

from __future__ import annotations

import time
from collections.abc import Callable
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


def with_retry(fn: Callable[[], T], config: RetryConfig) -> T:
    """Execute ``fn`` with exponential backoff on transient failures.

    Retries on:
    - ``httpx.HTTPStatusError`` with a status code in ``config.retryable_status_codes``
    - ``httpx.TransportError`` (network-level failures)

    Raises the last exception if all attempts are exhausted.
    """
    backoff = config.initial_backoff
    last_exc: Exception | None = None

    for attempt in range(1, config.max_attempts + 1):
        try:
            return fn()
        except httpx.HTTPStatusError as e:
            if e.response.status_code not in config.retryable_status_codes:
                raise
            last_exc = e
        except httpx.TransportError as e:
            last_exc = e

        if attempt < config.max_attempts:
            time.sleep(min(backoff, config.max_backoff))
            backoff *= config.backoff_multiplier

    assert last_exc is not None
    raise last_exc
