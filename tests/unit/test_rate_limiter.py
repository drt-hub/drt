"""Boundary-value tests for RateLimiter.

Covers edge cases: requests_per_second=0, =1, very large values,
and verifies the v0.3.3 ZeroDivisionError fix.

Also covers the #769 additions: opt-in ``burst`` credit, the regression
guard pinning ``burst=None`` to the historical minimum-interval arithmetic,
and thread-safe ``acquire()`` for the shared-instance case.

See: https://github.com/drt-hub/drt/issues/101
See: https://github.com/drt-hub/drt/issues/769
"""

from __future__ import annotations

import threading
from unittest.mock import patch

from drt.config.models import RateLimitConfig, SyncOptions
from drt.destinations.rate_limiter import RateLimiter, resolve_rate_limit


def _make_limiter(rps: float) -> RateLimiter:
    """Create a fresh RateLimiter with the given rate."""
    return RateLimiter(requests_per_second=rps)


class TestZeroAndNegativeRps:
    """requests_per_second <= 0 should disable rate-limiting entirely."""

    @patch("drt.destinations.rate_limiter.time.sleep")
    def test_zero_rps_does_not_block(self, mock_sleep) -> None:
        """rps=0 must not crash (regression for ZeroDivisionError)."""
        rl = _make_limiter(0)
        rl.acquire()
        rl.acquire()
        rl.acquire()
        mock_sleep.assert_not_called()

    @patch("drt.destinations.rate_limiter.time.sleep")
    def test_negative_rps_does_not_block(self, mock_sleep) -> None:
        """Negative rps should behave the same as zero."""
        rl = _make_limiter(-1)
        rl.acquire()
        rl.acquire()
        mock_sleep.assert_not_called()

    def test_zero_rps_does_not_update_last(self) -> None:
        """_last should stay at default when rate-limiting is disabled."""
        rl = _make_limiter(0)
        rl.acquire()
        assert rl._last == 0.0


class TestRpsOne:
    """requests_per_second=1 should enforce ~1 s between acquires."""

    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_rps_one_first_call_no_sleep(self, mock_mono, mock_sleep) -> None:
        mock_mono.return_value = 100.0
        rl = _make_limiter(1)
        rl.acquire()
        mock_sleep.assert_not_called()

    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_rps_one_second_call_sleeps(self, mock_mono, mock_sleep) -> None:
        mock_mono.return_value = 100.0
        rl = _make_limiter(1)
        rl.acquire()
        rl.acquire()
        mock_sleep.assert_called_once_with(1.0)

    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_rps_one_no_sleep_after_interval(self, mock_mono, mock_sleep) -> None:
        call_count = 0

        def advancing_clock():
            nonlocal call_count
            call_count += 1
            return 100.0 if call_count <= 2 else 101.5

        mock_mono.side_effect = advancing_clock
        rl = _make_limiter(1)
        rl.acquire()
        rl.acquire()
        mock_sleep.assert_not_called()


class TestVeryLargeRps:
    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_large_rps_minimal_interval(self, mock_mono, mock_sleep) -> None:
        mock_mono.return_value = 100.0
        rl = _make_limiter(1_000_000)
        rl.acquire()
        rl.acquire()
        mock_sleep.assert_called_once()
        wait_arg = mock_sleep.call_args[0][0]
        assert wait_arg < 0.001


class TestRapidSuccessiveCalls:
    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_three_rapid_calls_at_rps_two(self, mock_mono, mock_sleep) -> None:
        mock_mono.return_value = 100.0
        rl = _make_limiter(2)
        rl.acquire()
        rl.acquire()
        rl.acquire()
        assert mock_sleep.call_count == 2
        for call in mock_sleep.call_args_list:
            assert abs(call[0][0] - 0.5) < 1e-9


class TestFractionalRps:
    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_fractional_rps(self, mock_mono, mock_sleep) -> None:
        """Ensure fractional requests_per_second behaves correctly."""
        mock_mono.return_value = 100.0
        rl = _make_limiter(2.5)  # interval = 0.4 seconds
        rl.acquire()
        rl.acquire()
        mock_sleep.assert_called_once_with(0.4)


class TestStateManagement:
    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_last_updated_after_acquire(self, mock_mono, mock_sleep) -> None:
        mock_mono.return_value = 42.0
        rl = _make_limiter(10)
        assert rl._last == 0.0
        rl.acquire()
        assert rl._last == 42.0

    def test_default_last_is_zero(self) -> None:
        rl = _make_limiter(5)
        assert rl._last == 0.0

    def test_repr_excludes_last(self) -> None:
        rl = _make_limiter(10)
        assert "_last" not in repr(rl)


class TestBurst:
    """Opt-in burst credit (#769).

    Without ``burst`` the limiter grants no credit for idle time: the very
    next call after an hour of silence is still gated on ``_last``. With
    ``burst=N`` an idle period accumulates up to N requests' worth of credit
    that can be spent back-to-back.
    """

    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_burst_allows_n_immediate_calls_then_throttles(
        self, mock_mono, mock_sleep
    ) -> None:
        """rps=1, burst=3: three acquires at t=0 don't sleep, the fourth does."""
        mock_mono.return_value = 1000.0
        rl = RateLimiter(requests_per_second=1, burst=3)

        rl.acquire()
        rl.acquire()
        rl.acquire()
        mock_sleep.assert_not_called()

        rl.acquire()
        mock_sleep.assert_called_once_with(1.0)

    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_burst_one_matches_interval_only_pacing(self, mock_mono, mock_sleep) -> None:
        """burst=1 is the smallest legal burst: one free call, then pacing."""
        mock_mono.return_value = 500.0
        rl = RateLimiter(requests_per_second=2, burst=1)

        rl.acquire()
        mock_sleep.assert_not_called()
        rl.acquire()
        mock_sleep.assert_called_once_with(0.5)

    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_idle_period_refills_credit_up_to_burst_cap(self, mock_mono, mock_sleep) -> None:
        """Credit accrues while idle but is capped at ``burst``, never beyond."""
        clock = {"t": 0.0}
        mock_mono.side_effect = lambda: clock["t"]
        rl = RateLimiter(requests_per_second=1, burst=2)

        # Spend the initial credit: two free calls.
        rl.acquire()
        rl.acquire()
        assert mock_sleep.call_count == 0

        # Idle for an hour — far more than burst*interval of credit accrues,
        # but the cap means only 2 free calls are granted, not 3600.
        clock["t"] = 3600.0
        rl.acquire()
        rl.acquire()
        assert mock_sleep.call_count == 0
        rl.acquire()
        assert mock_sleep.call_count == 1

    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_burst_disabled_when_rps_non_positive(self, mock_mono, mock_sleep) -> None:
        """rps<=0 short-circuits before any burst arithmetic."""
        mock_mono.return_value = 10.0
        rl = RateLimiter(requests_per_second=0, burst=5)
        rl.acquire()
        rl.acquire()
        mock_sleep.assert_not_called()
        assert rl._last == 0.0

    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_burst_credit_is_not_refilled_when_last_passes_through_zero(
        self, mock_mono, mock_sleep
    ) -> None:
        """Burst credit must be spent once, not re-granted mid-flight.

        Regression guard. "Fresh limiter" was first implemented as the value
        sentinel ``_last == 0.0``. But with burst, ``_last`` runs *behind* the
        clock while credit is spent — at ``monotonic() == 0.0`` it walks
        ``-2.0 → -1.0 → 0.0`` and then hits the sentinel again, re-entering
        the fresh-limiter branch and refilling the allowance on every third
        call. The limiter never slept: an unbounded rate leak, exactly where
        a rate limiter must not have one.
        """
        mock_mono.return_value = 0.0  # the value that made the sentinel wrong
        rl = RateLimiter(requests_per_second=1, burst=3)
        for _ in range(6):
            rl.acquire()
        # 3 free (the burst), then every subsequent call pays.
        assert mock_sleep.call_count == 3


class TestBurstNoneRegression:
    """``burst=None`` must be byte-identical to the pre-#769 limiter.

    Each case below pairs the burst-less limiter against the exact arithmetic
    the old implementation performed (``wait = min_interval - elapsed``), so a
    future change to the burst branch cannot quietly alter the default path.
    """

    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_default_burst_is_none(self, mock_mono, mock_sleep) -> None:
        mock_mono.return_value = 0.0
        assert _make_limiter(10).burst is None

    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_no_credit_accrues_over_idle_time(self, mock_mono, mock_sleep) -> None:
        """The distinguishing property: idling grants no free calls."""
        clock = {"t": 100.0}
        mock_mono.side_effect = lambda: clock["t"]
        rl = _make_limiter(1)

        rl.acquire()  # first call, no sleep
        clock["t"] = 10_000.0  # idle for ages
        rl.acquire()  # one free call (interval long elapsed)
        assert mock_sleep.call_count == 0
        rl.acquire()  # immediately gated again — no accumulated credit
        mock_sleep.assert_called_once_with(1.0)

    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_wait_equals_min_interval_minus_elapsed(self, mock_mono, mock_sleep) -> None:
        """Replicates the old formula exactly across a spread of rps/elapsed."""
        for rps, elapsed in [
            (1.0, 0.0),
            (1.0, 0.25),
            (2.0, 0.1),
            (2.5, 0.0),
            (10.0, 0.05),
            (1_000_000.0, 0.0),
        ]:
            mock_sleep.reset_mock()
            clock = {"t": 100.0}
            mock_mono.side_effect = lambda: clock["t"]
            rl = _make_limiter(rps)
            rl.acquire()
            clock["t"] = 100.0 + elapsed
            rl.acquire()

            expected = (1.0 / rps) - elapsed
            if expected > 0:
                assert mock_sleep.call_count == 1
                assert abs(mock_sleep.call_args[0][0] - expected) < 1e-12
            else:
                mock_sleep.assert_not_called()

    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_last_still_tracks_wall_clock(self, mock_mono, mock_sleep) -> None:
        """``_last`` remains the raw post-acquire timestamp when burst is off."""
        mock_mono.return_value = 77.0
        rl = _make_limiter(4)
        rl.acquire()
        assert rl._last == 77.0


class TestThreadSafety:
    """A shared limiter must serialise ``acquire()`` (#769).

    The registry in a later task hands one instance to N worker threads, so
    the read-then-write of ``_last`` has to happen under a lock. Without one,
    two threads both read the same stale ``_last`` and both skip the wait,
    doubling the effective rate.
    """

    @patch("drt.destinations.rate_limiter.time.sleep")
    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_acquire_is_serialised_under_concurrency(self, mock_mono, mock_sleep) -> None:
        """Two threads entering acquire() together must not both skip the wait."""
        mock_mono.return_value = 200.0
        rl = _make_limiter(1)
        rl.acquire()  # consume the first slot so both threads must wait

        start = threading.Barrier(2)
        errors: list[BaseException] = []

        def worker() -> None:
            try:
                start.wait(timeout=5)
                rl.acquire()
            except BaseException as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert not errors
        assert not any(t.is_alive() for t in threads)
        # Both threads were gated: neither raced past a stale _last.
        assert mock_sleep.call_count == 2

    @patch("drt.destinations.rate_limiter.time.monotonic")
    def test_critical_section_is_mutually_exclusive(self, mock_mono) -> None:
        """No two threads may be inside the critical section at once.

        ``time.sleep`` is patched with a probe that runs *inside* the lock and
        asserts it is alone there, so overlap is detected without ever
        sleeping for real.
        """
        mock_mono.return_value = 0.0
        rl = _make_limiter(1)
        rl.acquire()

        inside = 0
        overlaps: list[int] = []
        probe_lock = threading.Lock()

        def fake_sleep(_seconds: float) -> None:
            nonlocal inside
            with probe_lock:
                inside += 1
                if inside > 1:
                    overlaps.append(inside)
            # Yield to give any concurrent thread a chance to overlap.
            for _ in range(50):
                pass
            with probe_lock:
                inside -= 1

        with patch("drt.destinations.rate_limiter.time.sleep", side_effect=fake_sleep):
            threads = [threading.Thread(target=rl.acquire) for _ in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=5)

        assert not overlaps, f"acquire() overlapped: {overlaps}"


class TestResolveRateLimit:
    """Precedence for ``rate_limit`` (#769), mirroring ``resolve_retry``.

    Order: ``destination.rate_limit`` > ``sync.rate_limit`` > ``RateLimitConfig()``.
    ``resolve_retry`` has no tests anywhere in the repo, so these spell the
    contract out explicitly rather than leaning on the sibling's coverage.
    """

    def test_resolve_rate_limit_prefers_destination_override(self) -> None:
        destination = RateLimitConfig(requests_per_second=3)
        sync_options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=50))

        resolved = resolve_rate_limit(destination, sync_options)

        assert resolved is destination
        assert resolved.requests_per_second == 3

    def test_resolve_rate_limit_falls_back_to_sync_level(self) -> None:
        sync_level = RateLimitConfig(requests_per_second=50)
        sync_options = SyncOptions(rate_limit=sync_level)

        resolved = resolve_rate_limit(None, sync_options)

        assert resolved is sync_level
        assert resolved.requests_per_second == 50

    def test_resolve_rate_limit_uses_default_when_neither_set(self) -> None:
        """sync_options.rate_limit is default_factory-populated, so this is
        the untouched ``RateLimitConfig()`` default rather than None."""
        resolved = resolve_rate_limit(None, SyncOptions())

        assert resolved.requests_per_second == RateLimitConfig().requests_per_second
        assert resolved.burst is None

    def test_destination_override_carries_burst(self) -> None:
        """burst rides along with the override, not merged field-by-field."""
        destination = RateLimitConfig(requests_per_second=1, burst=5)
        sync_options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=50))

        resolved = resolve_rate_limit(destination, sync_options)

        assert resolved.burst == 5
        assert resolved.requests_per_second == 1

    def test_resolution_does_not_mutate_either_config(self) -> None:
        destination = RateLimitConfig(requests_per_second=3, burst=2)
        sync_level = RateLimitConfig(requests_per_second=50)
        sync_options = SyncOptions(rate_limit=sync_level)

        resolve_rate_limit(destination, sync_options)

        assert destination.requests_per_second == 3
        assert destination.burst == 2
        assert sync_level.requests_per_second == 50
        assert sync_level.burst is None
