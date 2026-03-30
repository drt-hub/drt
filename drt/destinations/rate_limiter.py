"""Token-bucket rate limiter for HTTP requests.

Rust-migration note: this is a pure-Python, no-I/O class.
State is a float timestamp — trivially portable to Rust / PyO3.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class RateLimiter:
    """Simple token-bucket rate limiter.

    Ensures at least ``1 / requests_per_second`` seconds between
    successive ``acquire()`` calls.
    """

    requests_per_second: float
    _last: float = field(default=0.0, init=False, repr=False)

    def acquire(self) -> None:
        """Block until the next request slot is available."""
        if self.requests_per_second <= 0:
            return  # no rate limiting
        min_interval = 1.0 / self.requests_per_second
        elapsed = time.monotonic() - self._last
        wait = min_interval - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last = time.monotonic()
