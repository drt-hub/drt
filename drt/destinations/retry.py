"""Exponential backoff retry for transient HTTP failures.

Rust-migration note: pure logic, no I/O. Easily portable to Rust.
"""

from __future__ import annotations

import logging
import math
import time
from collections.abc import Callable
from datetime import timezone
from email.utils import parsedate_to_datetime
from enum import Enum
from typing import TypeVar

import httpx

from drt.config.models import RetryConfig, SyncOptions

T = TypeVar("T")


class OperationSafety(Enum):
    """Replay contract for one concrete HTTP operation (ADR 0023)."""

    IDEMPOTENT = "idempotent"
    KEYED = "keyed"
    RECONCILIABLE_CREATE = "reconciliable_create"
    BLIND_CREATE = "blind_create"


class FailureSafety(Enum):
    """Whether replaying after one failure can duplicate a side effect."""

    SAFE_TO_REPLAY = "safe_to_replay"
    DELIVERY_UNKNOWN = "delivery_unknown"


class RetryDirective(Enum):
    """A delivery-unknown handler can explicitly authorize another attempt."""

    RETRY = "retry"


class DeliveryUnknownError(RuntimeError):
    """The remote side may have committed, so automatic replay was suppressed."""

    def __init__(self, message: str, *, cause: Exception, attempt: int) -> None:
        super().__init__(message)
        self.cause = cause
        self.attempt = attempt

    @property
    def http_status(self) -> int | None:
        """Preserve a proxy's ambiguous status for ``RowError`` diagnostics."""
        if isinstance(self.cause, httpx.HTTPStatusError):
            return self.cause.response.status_code
        return None


def handle_blind_delivery_unknown(
    error: Exception,
    attempt: int,
    *,
    retry_creates: bool,
    max_attempts: int,
    destination: str,
    operation: str,
    logger: logging.Logger,
) -> RetryDirective:
    """Apply the shared blind-create escape hatch and recovery message.

    The provider-specific call sites supply only safe labels: neither payloads,
    endpoint credentials nor idempotency keys are logged.  This is deliberately
    a decision helper rather than another retry loop, so ``with_retry`` remains
    the single owner of attempt counts and backoff (ADR 0023).
    """
    if retry_creates and attempt < max_attempts:
        logger.warning(
            "%s retry_creates=true: retrying delivery-unknown %s "
            "(attempt %d/%d); duplicate-message or side-effect risk.",
            destination,
            operation,
            attempt,
            max_attempts,
        )
        return RetryDirective.RETRY

    raise DeliveryUnknownError(
        f"{destination} {operation} delivery is unknown: the request may have "
        "committed remotely. Assume it may have been delivered; verify the "
        "destination before manual replay and do not auto-resend. "
        "destination.retry_creates: true restores blind retries with duplicate "
        "message or side-effect risk.",
        cause=error,
        attempt=attempt,
    ) from error


def classify_failure_safety(
    operation: OperationSafety,
    error: Exception,
) -> FailureSafety:
    """Classify a retryable failure at the request boundary (ADR 0023).

    Idempotent and keyed operations can always repeat. Creates may repeat only
    when the client proves no request bytes were sent. A 429 also remains
    retryable: the server explicitly refused the attempt before accepting the
    operation. Read/write/response transport failures and retryable 5xx
    responses are delivery-unknown because the remote side may have committed.
    """
    if operation in {OperationSafety.IDEMPOTENT, OperationSafety.KEYED}:
        return FailureSafety.SAFE_TO_REPLAY
    if isinstance(error, httpx.HTTPStatusError) and error.response.status_code == 429:
        return FailureSafety.SAFE_TO_REPLAY
    if isinstance(error, (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout)):
        return FailureSafety.SAFE_TO_REPLAY
    return FailureSafety.DELIVERY_UNKNOWN


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


def with_retry(
    fn: Callable[[], T],
    config: RetryConfig,
    retry_on: Callable[[Exception], bool] | None = None,
    *,
    operation: OperationSafety | None = None,
    on_delivery_unknown: Callable[[Exception, int], T | RetryDirective] | None = None,
) -> T:
    """Execute ``fn`` with exponential backoff on transient failures.

    Retries on:
    - ``httpx.HTTPStatusError`` with a status code in ``config.retryable_status_codes``
    - ``httpx.TransportError`` (network-level failures)
    - any exception ``retry_on`` approves, when that predicate is supplied

    When a retryable HTTP response carries a ``Retry-After`` header (429 / 503
    from Slack, HubSpot, Intercom, Zendesk, …), the server's stated delay is
    honoured: the sleep becomes ``max(retry_after, computed_backoff)`` so we
    never retry *before* the API is ready, still bounded by ``max_backoff`` so
    a large or hostile header can't stall the run. Network failures and
    responses without the header keep pure exponential backoff.

    ``retry_on`` (#766) opens the same backoff loop to caller-defined
    classifications, including non-HTTP exceptions and HTTP statuses outside
    ``retryable_status_codes``. The SQL sources classify their own driver's
    exceptions, since every driver is an optional dependency this module cannot
    import. It is a **predicate, not a tuple of exception types**, because
    transient-ness frequently depends on an attribute rather than the class:
    Snowflake's ``390114`` session expiry is an ``OperationalError``, and so are
    plenty of permanent errors.

    ``retry_on`` is purely **additive** — it widens what counts as retryable
    and never suppresses the built-in httpx handling. When it is ``None``,
    behaviour is exactly as it was before #766.

    ``operation`` is opt-in so existing call sites retain byte-for-behavior
    compatibility while connectors adopt ADR 0023. For a create, an ambiguous
    failure is never fed back into ``fn`` blindly. ``on_delivery_unknown`` may
    reconcile and return the operation result, explicitly return
    ``RetryDirective.RETRY`` (the ``retry_creates`` escape hatch), or raise a
    connector-specific ``DeliveryUnknownError``. Without a handler, the shared
    error is raised immediately.

    Raises the last exception if all attempts are exhausted.
    """
    backoff = config.initial_backoff
    last_exc: Exception | None = None

    for attempt in range(1, config.max_attempts + 1):
        retry_after: float | None = None
        try:
            return fn()
        except httpx.HTTPStatusError as e:
            default_retryable = e.response.status_code in config.retryable_status_codes
            if not default_retryable and (retry_on is None or not retry_on(e)):
                raise
            last_exc = e
            retry_after = _retry_after_from_response(e.response)
        except httpx.TransportError as e:
            last_exc = e
        except Exception as e:
            if retry_on is None or not retry_on(e):
                raise
            last_exc = e

        if (
            operation is not None
            and last_exc is not None
            and classify_failure_safety(operation, last_exc) is FailureSafety.DELIVERY_UNKNOWN
        ):
            if on_delivery_unknown is None:
                raise DeliveryUnknownError(
                    "Delivery is unknown; refusing to replay an operation that may have "
                    "committed remotely.",
                    cause=last_exc,
                    attempt=attempt,
                ) from last_exc
            resolution = on_delivery_unknown(last_exc, attempt)
            if resolution is not RetryDirective.RETRY:
                return resolution

        if attempt < config.max_attempts:
            wait = backoff if retry_after is None else max(retry_after, backoff)
            time.sleep(min(wait, config.max_backoff))
            backoff *= config.backoff_multiplier

    assert last_exc is not None
    raise last_exc
