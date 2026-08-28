# Rate limiting

drt paces destination requests before sending them. The effective
`rate_limit` block is chosen in this order:

1. `destination.rate_limit`
2. the sync-level `rate_limit`
3. the default (`10` requests per second, with no burst credit)

A destination override replaces the whole sync-level block; fields are not
merged individually. For example, this destination uses `2.5` requests per
second and may spend up to three requests of accumulated idle-time credit:

```yaml
name: contacts_to_crm
model: ref('contacts')
rate_limit:
  requests_per_second: 8
destination:
  type: rest_api
  url: https://api.example.com/v1/contacts
  rate_limit:
    requests_per_second: 2.5
    burst: 3
```

Within one drt process, limiters are shared by the destination's
`rate_limit_key()`. That key represents the real quota holder—for example, a
HubSpot portal or an API host—so syncs and `--threads` workers targeting the
same endpoint use one bucket. If those syncs request different rates, the
strictest positive rate and smallest burst win. A connector's vendor ceiling
still clamps a higher configured rate. `requests_per_second: 0` disables
pacing; it is a sentinel, not a rate that can loosen or disable another sync's
shared bucket.

With `burst` omitted, idle time earns no credit and calls stay at least one
interval apart. `burst: N` allows idle time to accumulate up to `N` immediate
calls before interval pacing resumes; it does not raise the sustained rate.

## The cross-process gap

The shared bucket is process-local by default. If a Dagster sensor launches
`N` separate OS processes and each process targets the same endpoint, every
process gets its own bucket and the endpoint can receive up to `N` times the
configured rate. `--threads N` inside one process is coordinated; `N`
independent `drt run` processes are not.

This is the gap recorded in [#921](https://github.com/drt-hub/drt/issues/921).
[ADR 0012](../adr/0012-cross-process-rate-limit-coordination.md) makes the
trade-off explicit: drt keeps its dependency-free in-process default and
offers a backend contract for operators who already have suitable shared
infrastructure.

## Zero-code mitigation: batch changed syncs into one process

Before adding distributed coordination, avoid unnecessary process fan-out.
Instead of emitting one orchestrator run per changed sync in a sensor tick,
launch one drt command that selects all changed syncs against a manifest
baseline:

```bash
drt run \
  --select state:modified \
  --state ci-baseline/manifest.json \
  --threads 4
```

All selected syncs then share the process-wide endpoint buckets. This does not
make separate processes coordinate, and it does not detect runtime-only
configuration changes; it prevents the common `N`-process shape in the first
place. See the [state-aware selection guide](state-modified-selector.md) for
generating, storing, and restoring the required baseline manifest.

## Opt-in backend extension point

A third-party package can register a factory for objects satisfying the
structural `RateLimiterBackend` Protocol:

```python
import hashlib
import hmac
import os

from drt.destinations.rate_limiter import (
    RateLimiterBackend,
    register_rate_limiter_backend,
)

from .backend import SharedRateLimiter

# `key` may embed a hostname, an env-var name, or other config-derived text
# drt's own docs treat as not-for-logging/serializing (see `rate_limit_key()`
# and `resolve_rate_limiter()`'s docstrings). Never write it into Redis (or
# any store your team can query/export) verbatim — derive an opaque digest
# instead. A secret-keyed HMAC, not a bare hash, so the bucket name can't be
# brute-forced back to the endpoint identity from the digest alone.
_DIGEST_SECRET = os.environb[b"DRT_RATE_LIMIT_KEY_SECRET"]


def _bucket_name(key: str) -> str:
    digest = hmac.new(_DIGEST_SECRET, key.encode(), hashlib.sha256).hexdigest()
    return f"drt:rate-limit:{digest}"


def build_limiter(
    *,
    key: str,
    requests_per_second: float,
    burst: int | None,
) -> RateLimiterBackend:
    # `key` is the stable per-endpoint identity (drt's own `rate_limit_key()`)
    # — two calls for the same endpoint always produce the same digest, so
    # separate processes still coordinate on the same bucket.
    return SharedRateLimiter(
        redis_key=_bucket_name(key),
        requests_per_second=requests_per_second,
        burst=burst,
    )


def register() -> None:
    register_rate_limiter_backend(build_limiter)
```

`SharedRateLimiter` must implement:

```python
def acquire(self) -> None: ...

def tighten_to(
    self,
    requests_per_second: float,
    burst: int | None,
) -> None: ...
```

Expose the zero-argument `register()` callable through the plugin group:

```toml
[project.entry-points."drt.rate_limiter_backends"]
shared_rate_limiter = "my_package:register"
```

There is one active backend factory per process. A later registration replaces
the earlier one; this is not a keyed plugin registry. The existing endpoint-keyed
limiter cache still stores and reuses whatever the active factory constructs.

drt-core ships no distributed backend, Redis client, dependency, or optional
extra. This entry point is a contract for an external implementation, not a
`drt_project.yml` feature to configure. See [Third-Party Plugins](plugins.md)
for discovery and failure-isolation behavior.

## See also

- [Issue #921](https://github.com/drt-hub/drt/issues/921)
- [ADR 0012 — Cross-process rate-limit coordination](../adr/0012-cross-process-rate-limit-coordination.md)
- [State-aware selection](state-modified-selector.md)
