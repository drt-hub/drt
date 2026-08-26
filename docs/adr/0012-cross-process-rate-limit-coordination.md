# ADR 0012 — Cross-process rate-limit coordination: pluggable, not owned

- **Status:** Accepted 2026-08-27.
- **Issue:** [#921](https://github.com/drt-hub/drt/issues/921).
- **Relates to:** [ADR 0008](0008-enterprise-boundary-rbac-and-audit-hooks.md)
  (the `register_x`/`get_x`, single-active-instance-per-process pattern this
  ADR reuses verbatim); [ADR 0009](0009-plugin-config-union-blocker.md) / #997
  (the entry-point plugin system this ADR's extension point is wired through);
  [ADR 0011](0011-subtraction-positioning-vs-reverse-etl.md) (no hosted/managed
  runtime — the reason this ADR does not ship a Redis client); ADR 0004's
  2026-07-29 amendment and [ADR 0005](0005-state-location-and-write-grants.md)
  (the prior, incorrect home for this residual — see Context).

## Context

#769/#858 shipped in-process rate-limit coordination: `--threads N` against
one endpoint shares a single `RateLimiter` keyed by
`config.rate_limit_key()`, so N worker threads pace against one bucket
instead of N independent ones. That does not extend across processes. A
Dagster sensor that launches one `RunRequest` per changed sync starts each
as its own OS process — N changed syncs hitting one destination endpoint in
one sensor tick means N independent buckets, each pacing to the full
configured rate. The endpoint sees up to N× the intended load.

ADR 0004's 2026-07-29 amendment folded this into #756 (remote state
backend) on the reasoning that a cross-process bucket "needs shared state."
ADR 0005 found that right about the need and wrong about the kind: a token
bucket needs low-latency atomic increment on every `acquire()` call, and
neither of #756's backends (object storage, no cheap compare-and-swap; a
warehouse table, worse on both counts) can serve that without the pacing
overhead becoming the bottleneck it exists to prevent. #921 was filed to
hold the problem statement on its own, with three candidate directions,
none committed.

## Decision — default stays exactly as-is; cross-process coordination is a registered extension point, not a shipped backend

**No behavior changes for any user who does not opt in.** The existing
`RateLimiter` dataclass (`drt/destinations/rate_limiter.py`) is untouched:
same pacing algorithm, same in-process `_limiter_registry`, same default for
every user today. This ADR does not pick between #921's three candidates by
building one — it makes the *shape* of the answer "some users get real
cross-process coordination via infrastructure they already run; everyone
else keeps today's in-process pacing plus a documented fan-out caveat,"
which is not a fourth candidate but a way to stop treating the first two as
mutually exclusive:

1. **Formalize `RateLimiter`'s implicit shape as a Protocol.** It already
   has exactly the methods a cross-process implementation would need to
   provide (`acquire() -> None`, `tighten_to(requests_per_second, burst) ->
   None`) — no changes to the class, just naming the contract explicitly so
   a third-party backend has something concrete to implement against,
   matching `RateLimitKeyed`'s existing structural-Protocol pattern in the
   same file.
2. **A single-active-backend registry**, mirroring `drt/security/base.py`'s
   `register_permission_checker()`/`get_permission_checker()` exactly:
   `register_rate_limiter_backend(factory)` / `get_rate_limiter_backend()`,
   defaulting to today's `_default_limiter_factory` (constructs the local
   `RateLimiter`). One active backend per process, not a per-key registry —
   the backend decides how *any* limiter for *any* key gets built; the
   existing per-key `_limiter_registry` dict is untouched and still caches
   whatever the active backend constructs. `LimiterFactory.__call__` gains a
   required, keyword-only `key` parameter (`config.rate_limit_key()`) — the
   only stable identity shared across processes for "which quota is this."
   Without it a distributed backend has no way to derive its own shared
   bucket name (a Redis key, for example); the local `RateLimiter` ignores
   it, since the in-process cache dict already scopes by the same string.
   `RateLimiter` itself gains a matching, inert `key` field purely so
   `type[RateLimiter]` keeps structurally satisfying the widened Protocol.
3. **`resolve_rate_limiter()`'s existing `limiter_factory` parameter is the
   seam — with one correction found in review.** Every destination's
   `load()`, not just test code, already passes its own module-level
   `RateLimiter` name here (so `patch("drt.destinations.<name>.RateLimiter")`
   keeps intercepting construction in tests). Naively falling back to
   `get_rate_limiter_backend()` only when `limiter_factory is None` would
   therefore never engage a registered backend at all: the bare, unpatched
   `RateLimiter` class shadows it for every real destination. The fallback
   instead triggers on `limiter_factory is None or limiter_factory is
   RateLimiter` — the bare class is treated as "no genuine override," the
   same as omitting it, while a test's patched mock (never `is RateLimiter`)
   or a caller's real override still wins outright. Everything else about
   the function (key resolution, vendor-ceiling clamping, min-wins
   tightening) is unchanged.
4. **A sixth entry-point group, `drt.rate_limiter_backends`**, added to
   `drt/plugins.py`'s `PLUGIN_GROUPS`, wired exactly like
   `drt.permission_checkers`/`drt.audit_loggers`: a third-party package
   exposes a zero-argument callable that calls
   `register_rate_limiter_backend(...)` as a side effect.
5. **`get_rate_limiter_backend()` triggers plugin discovery itself — a
   second correction found in review.** Plugin registration used to happen
   only in the Typer CLI's root callback (`drt/cli/main.py`); an installed
   `drt.rate_limiter_backends` entry point would therefore stay silently
   unregistered under the MCP server, dagster-drt, and the Airflow/Prefect
   `run_drt_sync()` entry point — exactly the orchestrator-launched,
   cross-process scenario #921 is about, and confirmed empirically (parsing
   a built-in destination type does not itself trigger plugin loading, only
   a *third-party* one does, via #997's registry lookup). This mirrors the
   identical fix `drt/connectors/registry.py`'s `_ensure_plugins_loaded()`
   already applies to source/destination lookups: every reader of "what's
   currently active" funnels through a `load_plugins()` call (idempotent,
   cached per process) rather than assuming the CLI already ran it.
6. **`load_plugins()` itself gained a same-thread reentrancy guard — a
   third correction found in review.** A registration callback that reads
   the currently active backend (a plausible pattern: wrap the existing
   factory rather than fully replace it) calls `get_rate_limiter_backend()`
   from *inside* the entry-point loop `load_plugins()` is already running,
   on the same thread. `_lock` is a plain, non-reentrant `threading.Lock`,
   so that nested call trying to acquire it a second time deadlocked the
   thread against itself — reproduced directly (a real, indefinite hang,
   not a theoretical race). Fixed with a `threading.local()` flag checked
   *before* `_lock` is attempted: a nested call on the *same* thread
   returns immediately with whatever has been discovered so far, while a
   *different* thread genuinely loading concurrently still blocks on
   `_lock` normally (thread-local, not a shared module-level flag, so it
   cannot mistake real concurrent loading for reentrancy).
7. **The guide's example backend serialized `key` verbatim into a Redis
   key — a fourth correction.** `rate_limit_key()` and
   `resolve_rate_limiter()` already document `key` as "do not log or
   serialize" (it may embed a hostname, an env-var name, or other
   config-derived text); a distributed backend writing it unmodified into
   a shared, commonly-observable store contradicts that. `LimiterFactory`'s
   docstring and the guide's example now derive an opaque, secret-keyed
   HMAC digest instead of using `key` directly.

**drt-core ships no distributed implementation.** Per ADR 0011, drt-core
does not take on a Redis (or any other shared-infrastructure) dependency by
default — not even as an optional extra bundled in this repo. An operator
who already runs Redis (or has access to their orchestrator's own
low-latency atomic-op primitive) can write a `RateLimiterBackend`
implementation against the Protocol above and register it via the entry
point, entirely outside drt-core. `docs/guides/building-a-destination.md`
already sets the precedent that drt-core documents an extension contract
without shipping every implementation of it; this ADR follows the same
shape.

**#921's second and third candidates are not superseded, they compose with
this one:**

- *Coarse pacing (`configured_rate / N`)* remains available to any operator
  today, with zero new code: set a lower `requests_per_second` in the sync
  config. This ADR does not build first-class support for it (an
  auto-discovered `N` is exactly the fragility #921 itself flagged — stale
  the moment fan-out changes) but does not block a future config knob for it
  either, should real demand justify one.
- *Explicit workaround, documented*: this ADR's real "default" answer for
  every user who does not register a backend. `docs/guides/using-webhook-trigger.md`
  and a new short note in the rate-limiting guide should point at
  `state:modified` batching (#772) as the way to avoid launching N
  processes against one endpoint in the first place, which is cheaper than
  coordinating N buckets after the fact.

## Consequences

- **Zero risk to existing users.** No config default changes, no new
  dependency, no new required setup. `RateLimiter`'s own tests are
  unaffected; the Protocol formalization is additive.
- **A real extension point exists for users who need exact cross-process
  correctness and already have the infrastructure for it** — the "Redis"
  candidate from #921, without drt-core deciding for every user that they
  need it.
- **This does not close #921's underlying tension for users with neither
  Redis nor batching discipline.** Anyone fanning out N processes against
  one rate-limited endpoint, with no registered backend and no `state:modified`
  batching, still sees up to N× load. This ADR makes that a documented,
  intentional trade (subtraction: drt-core does not own the coordination
  infrastructure) rather than an accidental gap — the same shape ADR 0008
  drew around RBAC.
- **Follow-up, not blocking:** a reference `RateLimiterBackend` implementation
  (e.g. Redis-backed) could ship as a separate, optional community or
  drt-hub package once a real user needs one — analogous to how built-in
  destinations and third-party ones share one registry today. Not built
  speculatively here.

## Freeze-scope addition to ADR 0007

| Protocol | File | Freeze scope at v1.0 |
|---|---|---|
| `RateLimiterBackend` | `drt/destinations/rate_limiter.py` | Public, frozen — the cross-process rate-limit coordination extension point |
