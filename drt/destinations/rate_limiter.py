"""Minimum-interval rate limiter with an optional backend extension point.

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
from typing import Protocol, runtime_checkable

from drt.config.models import RateLimitConfig, SyncOptions


@runtime_checkable
class RateLimitKeyed(Protocol):
    """A destination config the registry can bucket (#769).

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).

    Structural rather than nominal because the destination configs share no
    common base: 26 members of the ``DestinationConfig`` union inherit
    ``DescribableConfig`` (and with it the default ``rate_limit_key``), while 8
    SaaS configs subclass ``BaseModel`` directly and define the method
    themselves. A ``Protocol`` describes exactly what the registry needs
    without forcing those two lineages together.
    """

    def rate_limit_key(self) -> str: ...


@runtime_checkable
class RateLimiterBackend(Protocol):
    """A rate limiter the process-wide registry can share (#921, ADR 0012).

    Stability: Stable (frozen at v1.0, see ADR 0007 for the breaking-change policy).

    Structural rather than nominal so a third-party cross-process backend can
    implement the existing limiter shape without subclassing drt-core's local
    :class:`RateLimiter` implementation.
    """

    def acquire(self) -> None: ...

    def tighten_to(self, requests_per_second: float, burst: int | None) -> None: ...


def resolve_rate_limit(
    config_rate_limit: RateLimitConfig | None,
    sync_options: SyncOptions,
) -> RateLimitConfig:
    """Pick the rate-limit config for this destination invocation (#769).

    Priority order: ``destination.rate_limit`` > ``sync.rate_limit`` >
    ``RateLimitConfig()``. ``sync_options.rate_limit`` is always populated
    (default_factory=RateLimitConfig), so when no destination-level override
    is set the sync-level config wins.

    The winning config is returned whole — fields are not merged one by one,
    so a destination override that sets only ``burst`` does not inherit the
    sync-level ``requests_per_second``. This mirrors ``resolve_retry``.
    """
    return config_rate_limit if config_rate_limit is not None else sync_options.rate_limit


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
    # Accepted-but-unused (#921, ADR 0012): exists only so `type[RateLimiter]`
    # structurally satisfies `LimiterFactory`, whose `key` param a distributed
    # backend needs to derive its shared bucket identity. The in-process
    # `_limiter_registry` dict already scopes instances by key, so a local
    # `RateLimiter` has nothing to do with it. Excluded from repr/eq so it
    # never affects existing equality/printing behaviour.
    key: str = field(default="", repr=False, compare=False)
    # _last is the "next free slot" clock. The 0.0 default reproduces the
    # historical first-call-never-sleeps behaviour when burst is off.
    _last: float = field(default=0.0, init=False, repr=False)
    # Whether acquire() has ever run. Tracked explicitly rather than inferred
    # from ``_last == 0.0``: with burst, _last legitimately passes through 0.0
    # while credit is being spent (it runs *behind* the clock), so a value
    # sentinel re-triggers the fresh-limiter branch mid-flight and refills the
    # allowance on every pass — an unbounded rate leak.
    _used: bool = field(default=False, init=False, repr=False)
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
                # A never-used limiter starts fully credited, matching "idle
                # since the dawn of the process".
                earliest = now - min_interval * self.burst
                if not self._used or self._last < earliest:
                    self._last = earliest
            self._used = True
            elapsed = now - self._last
            wait = min_interval - elapsed
            if wait > 0:
                time.sleep(wait)
            # burst=None: _last is the wall clock after the wait, exactly as
            # before #769. With burst: advance by whole intervals so unspent
            # credit survives into the next call instead of being discarded.
            self._last = time.monotonic() if self.burst is None else self._last + min_interval

    def tighten_to(self, requests_per_second: float, burst: int | None) -> None:
        """Adopt the stricter of the current and given limits (#769).

        Called by the registry when a second sync resolves to an endpoint that
        already has a limiter. Mutates in place rather than returning a new
        instance: threads already holding this object must observe the change,
        and a replacement would reset ``_last`` and hand out a free slot.

        ``burst=None`` (interval-only, no accumulation) is the strictest
        setting, so it wins over any numeric burst.

        ``requests_per_second=0`` is the "pacing disabled" **sentinel**, not a
        rate, so it is excluded from the numeric comparison (@Muawiya-contact
        on #858). A plain ``min()`` would let it win against every real rate —
        ``acquire()`` returns immediately at ``rps<=0``, so one sync opting out
        would switch pacing off for every other sync sharing the endpoint,
        inverting the purpose of the shared bucket. Any positive rate is
        therefore treated as stricter than the sentinel, in both directions
        (registration order is thread-scheduling order); 0 tightening 0 stays
        disabled, since there is no rate to adopt.
        """
        with self._lock:
            if requests_per_second > 0 and (
                self.requests_per_second <= 0 or requests_per_second < self.requests_per_second
            ):
                self.requests_per_second = requests_per_second
            if burst is None or self.burst is None:
                self.burst = None
            else:
                self.burst = min(self.burst, burst)


# Process-wide limiter registry (#769).
#
# Before this, every destination ``load()`` constructed its own RateLimiter, so
# ``--threads N`` against one endpoint created N independent buckets and
# multiplied the configured rate by N — a configured 10 req/s hitting one
# HubSpot portal at 40 req/s under ``--threads 4``. Keyed by
# ``config.rate_limit_key()``, so configs share a limiter exactly when they
# share a vendor quota.
#
# Deliberately module-level and never cleared during a run: the bucket must
# outlive individual ``load()`` calls to pace across them. Entries are tiny
# (one float and two locks) and bounded by the number of distinct endpoints in
# the project, so unbounded growth is not a concern.
_limiter_registry: dict[str, RateLimiterBackend] = {}
_registry_lock = threading.Lock()


def _reset_limiter_registry() -> None:
    """Drop every registered limiter. Test hook only.

    Registry state is process-global, so without this a test that registers a
    limiter would leak pacing state into unrelated tests. Not part of the
    public API and never called by production code — a real run wants the
    registry to persist for the whole process.
    """
    with _registry_lock:
        _limiter_registry.clear()


@runtime_checkable
class LimiterFactory(Protocol):
    """Constructs a :class:`RateLimiterBackend`. See ``resolve_rate_limiter``.

    Stability: Internal — not covered by semver, may change without notice.

    ``key`` is ``config.rate_limit_key()`` — the same string
    :data:`_limiter_registry` uses to cache the *instance* this call
    creates. A distributed backend needs it too: it is the only stable
    identity shared across processes for "which quota is this," so a
    cross-process implementation derives its own shared bucket name (a
    Redis key, for example) from it. The local :class:`RateLimiter` ignores
    it — in-process, the cache dict keyed by the same string already gives
    every caller for one endpoint the same instance.

    ``key`` carries the same "do not log or serialize" caveat
    ``resolve_rate_limiter()`` and ``rate_limit_key()`` already document —
    it may embed a hostname, an env-var name, or other config-derived text.
    A distributed backend that writes it into a shared store verbatim (a
    Redis key, say) leaks that text to anything with read access to the
    store; derive an opaque digest instead (a secret-keyed HMAC, not a bare
    hash, so the digest can't be brute-forced back to the original text).

    All three parameters are keyword-only: ``resolve_rate_limiter`` always
    calls by keyword, and this avoids constraining implementers (including
    ``RateLimiter`` itself, whose dataclass field order is
    ``requests_per_second, burst, key`` for backward-compatible direct
    construction, not the Protocol's ``key`` first) to one positional order.
    """

    def __call__(
        self, *, key: str, requests_per_second: float, burst: int | None
    ) -> RateLimiterBackend: ...


def _default_limiter_factory(
    key: str, requests_per_second: float, burst: int | None
) -> RateLimiter:
    # `key` is unused by the local backend (the in-process registry dict
    # already scopes instances by key) but threaded through to RateLimiter's
    # own inert `key` field for consistency rather than discarded.
    return RateLimiter(key=key, requests_per_second=requests_per_second, burst=burst)


_backend_lock = threading.Lock()
_limiter_factory: LimiterFactory = _default_limiter_factory


def register_rate_limiter_backend(factory: LimiterFactory) -> None:
    """Install `factory` as the active rate-limiter backend for this process.

    Unlike the endpoint-keyed limiter registry, there is exactly one active
    backend factory per process — a second call replaces the first rather
    than erroring, so a caller (e.g. a test fixture) can reset to the local
    default via ``register_rate_limiter_backend(_default_limiter_factory)``.
    """
    global _limiter_factory
    with _backend_lock:
        _limiter_factory = factory


def get_rate_limiter_backend() -> LimiterFactory:
    """Return the currently active rate-limiter backend factory (the local default,
    :func:`_default_limiter_factory`, unless a plugin registered its own via
    :func:`register_rate_limiter_backend`).

    Ensures plugin discovery has run first (#921), the same fix
    ``drt/connectors/registry.py``'s ``_ensure_plugins_loaded()`` already
    applies to source/destination lookups: registration used to happen only
    in the Typer CLI's root callback, so a ``drt.rate_limiter_backends``
    entry point installed alongside drt would silently stay unregistered
    under the MCP server, dagster-drt, and the Airflow/Prefect
    ``run_drt_sync()`` entry point — exactly the orchestrator-launched,
    cross-process scenario this registry exists for. :func:`drt.plugins.load_plugins`
    is cached per process, so this costs one lock-and-flag check after the
    first call. Imported lazily to keep ``drt.plugins`` off this module's
    import path, matching that function's own reasoning. Called before
    acquiring ``_backend_lock``: a plugin's registration callback calls
    :func:`register_rate_limiter_backend`, which takes that same lock, and
    a non-reentrant ``threading.Lock`` would deadlock the thread against
    itself if this ran while the lock was already held.
    """
    from drt.plugins import load_plugins

    load_plugins()
    with _backend_lock:
        return _limiter_factory


def _reset_rate_limiter_backend() -> None:
    """Restore the local default. Test hook only — not called by production
    code, which registers at most once per process."""
    register_rate_limiter_backend(_default_limiter_factory)


def resolve_rate_limiter(
    config: RateLimitKeyed,
    sync_options: SyncOptions,
    max_requests_per_second: float | None = None,
    limiter_factory: LimiterFactory | None = None,
) -> RateLimiterBackend:
    """Return the shared limiter for this destination's endpoint (#769).

    Resolves the effective config (``destination.rate_limit`` > ``sync.rate_limit``
    > default, via :func:`resolve_rate_limit`), computes the endpoint identity
    with ``config.rate_limit_key()``, then returns the registered limiter for
    that key — creating it on first use. Every sync and every worker thread
    targeting one endpoint therefore paces against one bucket.

    ``max_requests_per_second`` is a **vendor ceiling** — HubSpot 9/s, Notion
    3/s, GitHub Actions 5/s, Zendesk 11/s — published by the API owner and not
    negotiable by config. It clamps the resolved rate *before* the registry is
    touched, so the cap is baked into the instance the registry creates or
    tightens. Applying it to the returned limiter instead would be a bug: the
    instance is shared, so each caller's assignment would clobber the rate the
    other callers rely on, and the last writer would win non-deterministically.
    A cap is a ceiling only — it never raises a lower configured rate, and it
    leaves the ``requests_per_second=0`` "no pacing" sentinel alone.

    **Same endpoint, different rates: min-wins.** When a key already has a
    limiter and the caller resolves a different rate, the *stricter* of the two
    is applied to the existing instance. Rationale: the tighter limit is the one
    the endpoint actually requires — running at the looser rate is precisely how
    a shared quota starts returning 429s — and a limit expressed anywhere in the
    project is a statement about the endpoint, not about one sync. The
    alternatives were rejected: last-wins makes the effective rate depend on
    thread scheduling order, and first-wins silently ignores a deliberate
    tightening. ``burst=None`` counts as strictest, since interval-only pacing
    grants no accumulation at all.

    The consequence worth knowing: one sync configured conservatively slows
    every other sync sharing that endpoint. That is the intended trade —
    correctness against the vendor's limit over per-sync throughput.

    ``limiter_factory`` overrides how a *new* limiter is constructed (it is not
    consulted when the key is already registered). Every destination's
    ``load()`` passes its own module-level ``RateLimiter`` name here — not as a
    genuine per-call override, but so ``patch("drt.destinations.<name>.RateLimiter")``,
    the way several destination tests assert per-record pacing, keeps
    intercepting construction. Without the seam those patches would target a
    name ``load()`` no longer calls and the assertions would quietly pass
    against nothing.

    That means an *unpatched* call always supplies the real ``RateLimiter``
    class as ``limiter_factory`` too, which would permanently shadow a
    registered cross-process backend (#921, ADR 0012) for every destination —
    the whole point of the registry defeated by the seam that predates it. So
    the bare, unpatched ``RateLimiter`` class is treated as "no genuine
    override" and falls through to :func:`get_rate_limiter_backend` exactly
    like ``None`` does; only a *different* callable — a test's patched mock,
    or an explicit caller-supplied factory — wins outright. This distinguishes
    the two by identity (``is RateLimiter``), not by behavior, so it costs
    nothing to get right: a mock is never the same object as the real class.

    The returned key is process-local. Do not log or serialize it: it may embed
    an env-var name or a credential digest, and the #696 review rejected
    published digests as brute-forceable.
    """
    resolved = resolve_rate_limit(getattr(config, "rate_limit", None), sync_options)
    key = config.rate_limit_key()

    rps = resolved.requests_per_second
    if max_requests_per_second is not None and rps > max_requests_per_second:
        # ``>`` rather than min(): keeps rps=0 (pacing disabled) intact, since
        # 0 is a sentinel rather than a rate and must not become the ceiling.
        rps = max_requests_per_second

    factory = limiter_factory
    if factory is None or factory is RateLimiter:
        # None: no override at all. `is RateLimiter`: the bare class, which
        # every destination's unpatched call also supplies for the test-patch
        # seam above — not a genuine override, so it must not shadow a
        # registered backend. A test's `patch(...)` replaces this name with a
        # distinct mock object, which is never `is RateLimiter`, so patched
        # tests are unaffected.
        factory = get_rate_limiter_backend()

    with _registry_lock:
        existing = _limiter_registry.get(key)
        if existing is None:
            limiter = factory(key=key, requests_per_second=rps, burst=resolved.burst)
            _limiter_registry[key] = limiter
            return limiter

    # Tighten outside the registry lock: RateLimiter has its own lock, and
    # holding both here would nest them (registry -> limiter) while acquire()
    # holds only the limiter's — a needless second lock-ordering constraint.
    existing.tighten_to(rps, resolved.burst)
    return existing
