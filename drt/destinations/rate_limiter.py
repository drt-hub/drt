"""Minimum-interval rate limiter with optional burst credit.

Despite its original name and docstring this has never been a token bucket:
it paces requests by sleeping out the remainder of ``1 / requests_per_second``
since the last call. ``burst`` (#769) adds genuine accumulation on top, opt-in
and off by default.

Rust-migration note: the pacing state is still a single float timestamp —
trivially portable to Rust / PyO3. The ``threading.Lock`` added in #769 is a
Python-concurrency artifact, not part of the algorithm: a Rust port would
express the same mutual exclusion with its own synchronization primitive
(``Mutex<f64>`` or an atomic) rather than porting this field.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field


@dataclass
class RateLimiter:
    """Minimum-interval limiter with optional burst credit.

    By default (``burst=None``) it ensures at least
    ``1 / requests_per_second`` seconds between successive ``acquire()``
    calls, and idle time earns nothing: the first call after an hour of
    silence is paced exactly like the first call after one interval.

    With ``burst=N`` an idle period accrues credit worth up to ``N``
    requests, which callers may spend back-to-back before pacing resumes.
    This is implemented by letting ``_last`` run *behind* the wall clock by
    at most ``N`` intervals, so it doubles as the "next free slot" clock.

    ``acquire()`` is thread-safe — the registry (#769) hands a single
    instance to every worker thread targeting one endpoint, and the
    read-then-write of ``_last`` would otherwise be a data race that lets
    two threads both skip the wait.
    """

    requests_per_second: float
    burst: int | None = None
    # _last is the "next free slot" clock. It stays 0.0 until the first
    # acquire() so a fresh limiter counts as having been idle forever — with
    # burst that means it starts with a full allowance, and without burst the
    # 0.0 default reproduces the historical first-call-never-sleeps behaviour.
    _last: float = field(default=0.0, init=False, repr=False)
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)

    def acquire(self) -> None:
        """Block until the next request slot is available."""
        if self.requests_per_second <= 0:
            return  # no rate limiting
        min_interval = 1.0 / self.requests_per_second
        with self._lock:
            now = time.monotonic()
            if self.burst is not None:
                # Clamp how far _last may lag behind now: that backlog *is*
                # the accumulated credit, capped at burst intervals so an
                # arbitrarily long idle period cannot bank unbounded calls.
                # A never-used limiter (_last == 0.0) starts fully credited,
                # matching "idle since the dawn of the process".
                earliest = now - min_interval * self.burst
                if self._last == 0.0 or self._last < earliest:
                    self._last = earliest
            elapsed = now - self._last
            wait = min_interval - elapsed
            if wait > 0:
                time.sleep(wait)
            # burst=None: _last is the wall clock after the wait, exactly as
            # before #769. With burst: advance by whole intervals so unspent
            # credit survives into the next call instead of being discarded.
            self._last = time.monotonic() if self.burst is None else self._last + min_interval
