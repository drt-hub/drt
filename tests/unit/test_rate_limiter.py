"""Boundary-value tests for RateLimiter.

Covers edge cases: requests_per_second=0, =1, very large values,
and verifies the v0.3.3 ZeroDivisionError fix.

Also covers the #769 additions: opt-in ``burst`` credit, the regression
guard pinning ``burst=None`` to the historical minimum-interval arithmetic,
thread-safe ``acquire()`` for the shared-instance case, and the process-wide
limiter registry that makes sharing happen.

See: https://github.com/drt-hub/drt/issues/101
See: https://github.com/drt-hub/drt/issues/769
"""

from __future__ import annotations

import threading
from unittest.mock import patch

from drt.config.base import BearerAuth
from drt.config.destinations_saas import HubSpotDestinationConfig, SlackDestinationConfig
from drt.config.models import RateLimitConfig, SyncOptions
from drt.destinations.rate_limiter import (
    RateLimiter,
    _reset_limiter_registry,
    resolve_rate_limit,
    resolve_rate_limiter,
)


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
    def test_burst_allows_n_immediate_calls_then_throttles(self, mock_mono, mock_sleep) -> None:
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


class TestResolveRateLimiter:
    """The process-wide registry (#769).

    ``--threads N`` used to build N ``RateLimiter`` instances for one
    destination endpoint, so a configured 10 req/s became 10N req/s against a
    single vendor quota. The registry gives every worker targeting one endpoint
    the same limiter, and ``RateLimiter.acquire()`` is locked so sharing is safe.
    """

    def setup_method(self) -> None:
        _reset_limiter_registry()

    def teardown_method(self) -> None:
        # Module-level state: leaving entries behind would leak between tests.
        _reset_limiter_registry()

    def _slack(self, hook_env: str) -> SlackDestinationConfig:
        return SlackDestinationConfig(type="slack", webhook_url_env=hook_env)

    def test_same_key_returns_the_same_limiter_instance(self) -> None:
        """The whole point: one endpoint, one bucket, however many syncs."""
        config = self._slack("HOOK_A")
        options = SyncOptions()

        first = resolve_rate_limiter(config, options)
        second = resolve_rate_limiter(config, options)

        assert first is second

    def test_distinct_config_objects_with_one_key_share_a_limiter(self) -> None:
        """Identity is the *key*, not the config object — two syncs parse two
        config instances pointing at one endpoint."""
        options = SyncOptions()

        first = resolve_rate_limiter(self._slack("HOOK_A"), options)
        second = resolve_rate_limiter(self._slack("HOOK_A"), options)

        assert first is second

    def test_different_keys_return_different_limiters(self) -> None:
        options = SyncOptions()

        first = resolve_rate_limiter(self._slack("HOOK_A"), options)
        second = resolve_rate_limiter(self._slack("HOOK_B"), options)

        assert first is not second

    def test_hubspot_object_types_share_one_bucket(self) -> None:
        """End-to-end on the headline bug: one portal, two object types, one
        limiter — the registry inherits this from rate_limit_key()."""
        auth = BearerAuth(type="bearer", token_env="HUBSPOT_TOKEN")
        contacts = HubSpotDestinationConfig(type="hubspot", object_type="contacts", auth=auth)
        deals = HubSpotDestinationConfig(type="hubspot", object_type="deals", auth=auth)
        options = SyncOptions()

        assert resolve_rate_limiter(contacts, options) is resolve_rate_limiter(deals, options)

    def test_limiter_uses_the_resolved_rate(self) -> None:
        config = self._slack("HOOK_A")
        options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=4, burst=2))

        limiter = resolve_rate_limiter(config, options)

        assert limiter.requests_per_second == 4
        assert limiter.burst == 2

    def test_destination_override_beats_sync_level(self) -> None:
        """resolve_rate_limit precedence still applies through the registry.

        The ``rate_limit`` field itself lands on the destination configs in a
        later task, so this uses a stand-in carrying the same duck-typed shape
        the registry reads: a ``rate_limit`` attribute plus ``rate_limit_key()``.
        """

        class _ConfigWithOverride:
            rate_limit = RateLimitConfig(requests_per_second=2)

            def rate_limit_key(self) -> str:
                return "slack:HOOK_OVERRIDE"

        options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=50))

        assert resolve_rate_limiter(_ConfigWithOverride(), options).requests_per_second == 2

    def test_missing_rate_limit_attribute_falls_back_to_sync_level(self) -> None:
        """Configs without the field yet (it arrives in a later task) must
        resolve to the sync-level rate rather than raising."""
        options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=7))

        assert resolve_rate_limiter(self._slack("HOOK_A"), options).requests_per_second == 7

    # -- same endpoint, different rps: min-wins ------------------------------

    def test_lower_rps_tightens_an_existing_limiter(self) -> None:
        """min-wins: the stricter limit is the one the endpoint actually
        requires, so a later, slower config tightens the shared bucket."""
        options_fast = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=10))
        options_slow = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=2))

        first = resolve_rate_limiter(self._slack("HOOK_A"), options_fast)
        second = resolve_rate_limiter(self._slack("HOOK_A"), options_slow)

        assert first is second, "must stay one bucket — tightening, not replacing"
        assert second.requests_per_second == 2

    def test_higher_rps_does_not_loosen_an_existing_limiter(self) -> None:
        """The converse, and the reason min-wins was chosen: silently adopting
        the looser rate is how a shared endpoint starts returning 429s."""
        options_slow = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=2))
        options_fast = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=10))

        first = resolve_rate_limiter(self._slack("HOOK_A"), options_slow)
        second = resolve_rate_limiter(self._slack("HOOK_A"), options_fast)

        assert first is second
        assert second.requests_per_second == 2

    def test_burst_is_also_tightened_to_the_minimum(self) -> None:
        """Burst is capacity above the steady rate, so the same argument holds:
        the smallest declared burst is the safe one."""
        options_big = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=5, burst=10))
        options_small = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=5, burst=3))

        resolve_rate_limiter(self._slack("HOOK_A"), options_big)
        limiter = resolve_rate_limiter(self._slack("HOOK_A"), options_small)

        assert limiter.burst == 3

    def test_absent_burst_tightens_a_bursting_limiter(self) -> None:
        """burst=None is interval-only — stricter than any burst, so it wins."""
        options_burst = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=5, burst=10))
        options_none = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=5))

        resolve_rate_limiter(self._slack("HOOK_A"), options_burst)
        limiter = resolve_rate_limiter(self._slack("HOOK_A"), options_none)

        assert limiter.burst is None

    def test_disabled_sentinel_does_not_disable_a_paced_endpoint(self) -> None:
        """rps=0 means "no pacing", not "the slowest rate" (@Muawiya-contact).

        Numerically 0 < every real rate, so a plain min() lets the sentinel win
        and switches pacing off for *every* sync sharing the endpoint —
        ``acquire()`` returns immediately at rps<=0. That inverts the point of
        the PR: one sync opting out would remove the shared quota's protection
        from all the others.
        """
        options_paced = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=10))
        options_off = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=0))

        first = resolve_rate_limiter(self._slack("HOOK_A"), options_paced)
        second = resolve_rate_limiter(self._slack("HOOK_A"), options_off)

        assert first is second
        assert second.requests_per_second == 10

    def test_a_real_rate_tightens_a_disabled_limiter(self) -> None:
        """The reverse order, which the report did not cover but fails the same
        way: registering the sentinel *first* left the bucket at 0 forever,
        because no positive rate is ever numerically smaller. Registration order
        is thread-scheduling order, so both directions have to hold.
        """
        options_off = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=0))
        options_paced = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=10))

        first = resolve_rate_limiter(self._slack("HOOK_A"), options_off)
        second = resolve_rate_limiter(self._slack("HOOK_A"), options_paced)

        assert first is second
        assert second.requests_per_second == 10

    def test_two_disabled_configs_stay_disabled(self) -> None:
        """0 + 0 must remain the sentinel — tightening must not invent a rate
        where the user asked for none."""
        options_off = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=0))

        resolve_rate_limiter(self._slack("HOOK_A"), options_off)
        limiter = resolve_rate_limiter(self._slack("HOOK_A"), options_off)

        assert limiter.requests_per_second == 0

    def test_tightening_preserves_pacing_state(self) -> None:
        """Tightening must mutate the shared instance, never swap it — an
        in-flight thread holds a reference, and a fresh object would reset
        ``_last`` and hand out a free slot."""
        options_fast = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=10))
        options_slow = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=1))

        limiter = resolve_rate_limiter(self._slack("HOOK_A"), options_fast)
        with patch("drt.destinations.rate_limiter.time.monotonic", return_value=500.0):
            with patch("drt.destinations.rate_limiter.time.sleep"):
                limiter.acquire()
        assert limiter._used is True

        again = resolve_rate_limiter(self._slack("HOOK_A"), options_slow)

        assert again is limiter
        assert again._used is True, "pacing state was reset — a slot was leaked"

    # -- concurrency ---------------------------------------------------------

    def test_registry_lookup_is_thread_safe(self) -> None:
        """N threads racing on a cold key must all get the *same* instance.

        Without a lock around check-then-create, several threads see the empty
        slot and each construct a limiter — restoring the very N-buckets-per-
        endpoint bug the registry exists to remove.
        """
        options = SyncOptions()
        thread_count = 16
        start = threading.Barrier(thread_count)
        results: list[RateLimiter] = []
        results_lock = threading.Lock()
        errors: list[BaseException] = []

        def worker() -> None:
            try:
                # Every thread builds its own config object, as separate syncs do.
                config = SlackDestinationConfig(type="slack", webhook_url_env="HOOK_RACE")
                start.wait(timeout=5)
                limiter = resolve_rate_limiter(config, options)
                with results_lock:
                    results.append(limiter)
            except BaseException as exc:  # pragma: no cover - surfaced below
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(thread_count)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert not errors
        assert not any(t.is_alive() for t in threads)
        assert len(results) == thread_count
        assert len({id(limiter) for limiter in results}) == 1, "cold-key race created extra buckets"

    def test_concurrent_distinct_keys_each_get_one_limiter(self) -> None:
        """Threads racing across several cold keys must produce exactly one
        limiter per key — no cross-talk, no duplicates."""
        options = SyncOptions()
        key_count = 4
        per_key = 4
        total = key_count * per_key
        start = threading.Barrier(total)
        results: dict[str, set[int]] = {f"HOOK_{i}": set() for i in range(key_count)}
        results_lock = threading.Lock()

        def worker(hook: str) -> None:
            config = SlackDestinationConfig(type="slack", webhook_url_env=hook)
            start.wait(timeout=5)
            limiter = resolve_rate_limiter(config, options)
            with results_lock:
                results[hook].add(id(limiter))

        threads = [
            threading.Thread(target=worker, args=(f"HOOK_{i}",))
            for i in range(key_count)
            for _ in range(per_key)
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=5)

        assert all(len(ids) == 1 for ids in results.values())
        # And the four keys really are four distinct limiters.
        assert len({next(iter(ids)) for ids in results.values()}) == key_count


class TestVendorCap:
    """``max_requests_per_second`` pins a published vendor ceiling (#769).

    HubSpot (9/s), Notion (3/s), GitHub Actions (5/s) and Zendesk (11/s)
    clamped with ``min(rps, N)`` before the registry existed. The clamp has to
    survive the switch, or a destination-level override would silently let a
    user exceed a documented API limit.

    Critically the cap is applied to the *resolved* rate **before** the
    registry lookup, never to the instance afterwards: the returned limiter is
    shared, so a post-hoc assignment would let each caller overwrite the rate
    the others depend on.
    """

    def setup_method(self) -> None:
        _reset_limiter_registry()

    def teardown_method(self) -> None:
        _reset_limiter_registry()

    def _slack(self, hook_env: str) -> SlackDestinationConfig:
        return SlackDestinationConfig(type="slack", webhook_url_env=hook_env)

    def test_cap_clamps_a_higher_sync_level_rate(self) -> None:
        options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=100))

        limiter = resolve_rate_limiter(self._slack("HOOK_A"), options, max_requests_per_second=9)

        assert limiter.requests_per_second == 9

    def test_cap_does_not_raise_a_lower_configured_rate(self) -> None:
        """A cap is a ceiling, not a target — a slower config stays slower."""
        options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=2))

        limiter = resolve_rate_limiter(self._slack("HOOK_A"), options, max_requests_per_second=9)

        assert limiter.requests_per_second == 2

    def test_destination_override_cannot_exceed_the_cap(self) -> None:
        """The headline risk of adding destination.rate_limit: a user raising
        their own limit past what the vendor publishes."""
        config = self._slack("HOOK_A")
        config.rate_limit = RateLimitConfig(requests_per_second=500)
        options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=1))

        limiter = resolve_rate_limiter(config, options, max_requests_per_second=9)

        assert limiter.requests_per_second == 9

    def test_cap_applies_to_an_already_registered_limiter(self) -> None:
        """Second caller on a hot key: the cap must clamp before tighten_to,
        not after the instance is handed back."""
        slow = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=100))
        first = resolve_rate_limiter(self._slack("HOOK_A"), slow, max_requests_per_second=9)
        assert first.requests_per_second == 9

        # A second, uncapped-looking resolve on the same key must not loosen it.
        second = resolve_rate_limiter(
            self._slack("HOOK_A"),
            SyncOptions(rate_limit=RateLimitConfig(requests_per_second=100)),
            max_requests_per_second=9,
        )

        assert second is first
        assert second.requests_per_second == 9

    def test_no_cap_leaves_the_resolved_rate_untouched(self) -> None:
        """The plain (uncapped) connectors must behave exactly as before."""
        options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=100))

        limiter = resolve_rate_limiter(self._slack("HOOK_A"), options)

        assert limiter.requests_per_second == 100

    def test_cap_preserves_the_zero_means_unlimited_sentinel(self) -> None:
        """rps=0 disables pacing entirely and many destination tests rely on
        it; min(0, 9) would keep 0, but pin it so a future clamp rewrite
        cannot turn the sentinel into a real 9/s rate."""
        options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=0))

        limiter = resolve_rate_limiter(self._slack("HOOK_A"), options, max_requests_per_second=9)

        assert limiter.requests_per_second == 0

    def test_burst_survives_capping(self) -> None:
        """The cap constrains the steady rate only; burst is carried through."""
        options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=100, burst=4))

        limiter = resolve_rate_limiter(self._slack("HOOK_A"), options, max_requests_per_second=9)

        assert limiter.requests_per_second == 9
        assert limiter.burst == 4


class TestLimiterFactory:
    """``limiter_factory`` keeps the construction seam in the caller's module.

    Several destination tests assert pacing happens by patching
    ``drt.destinations.<name>.RateLimiter`` and counting ``acquire()`` on the
    instance it returns. Once ``load()`` delegates construction to the registry
    that patch would stop intercepting anything, and the tests would silently
    assert nothing. Passing the destination module's own ``RateLimiter`` name
    as the factory keeps those patch points live and keeps the registry's
    construction point injectable.
    """

    def setup_method(self) -> None:
        _reset_limiter_registry()

    def teardown_method(self) -> None:
        _reset_limiter_registry()

    def _slack(self, hook_env: str) -> SlackDestinationConfig:
        return SlackDestinationConfig(type="slack", webhook_url_env=hook_env)

    def test_factory_is_used_to_build_a_cold_key(self) -> None:
        sentinel = RateLimiter(requests_per_second=1)
        calls: list[tuple[float, int | None]] = []

        def factory(requests_per_second: float, burst: int | None) -> RateLimiter:
            calls.append((requests_per_second, burst))
            return sentinel

        options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=4, burst=2))

        limiter = resolve_rate_limiter(self._slack("HOOK_A"), options, limiter_factory=factory)

        assert limiter is sentinel
        assert calls == [(4, 2)], "factory must receive the resolved rate and burst"

    def test_factory_receives_the_capped_rate(self) -> None:
        """The vendor cap is applied before construction, not after."""
        calls: list[float] = []

        def factory(requests_per_second: float, burst: int | None) -> RateLimiter:
            calls.append(requests_per_second)
            return RateLimiter(requests_per_second=requests_per_second, burst=burst)

        options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=100))

        resolve_rate_limiter(
            self._slack("HOOK_A"),
            options,
            max_requests_per_second=3,
            limiter_factory=factory,
        )

        assert calls == [3]

    def test_factory_is_not_called_for_a_hot_key(self) -> None:
        """A registered endpoint is reused, so nothing new is constructed."""
        options = SyncOptions()
        first = resolve_rate_limiter(self._slack("HOOK_A"), options)

        def factory(requests_per_second: float, burst: int | None) -> RateLimiter:
            raise AssertionError("must reuse the registered limiter")

        reused = resolve_rate_limiter(self._slack("HOOK_A"), options, limiter_factory=factory)
        assert reused is first

    def test_default_factory_builds_a_real_rate_limiter(self) -> None:
        options = SyncOptions(rate_limit=RateLimitConfig(requests_per_second=6))

        limiter = resolve_rate_limiter(self._slack("HOOK_A"), options)

        assert isinstance(limiter, RateLimiter)
        assert limiter.requests_per_second == 6
