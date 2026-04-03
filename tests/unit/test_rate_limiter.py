"""Boundary-value tests for RateLimiter.

Covers edge cases: requests_per_second=0, =1, very large values,
and verifies the v0.3.3 ZeroDivisionError fix.

See: https://github.com/drt-hub/drt/issues/101
"""

from __future__ import annotations

from unittest.mock import patch

from drt.destinations.rate_limiter import RateLimiter


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
