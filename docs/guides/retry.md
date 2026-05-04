# Retry policy

drt automatically retries transient HTTP failures (5xx errors, network blips, rate-limit responses) with exponential backoff. Retry behavior is configurable in YAML at two levels:

1. **`sync.retry`** — applied to every destination call in the sync.
2. **`destination.retry`** — overrides the sync-level config for that destination only.

> **Priority**: `destination.retry` > `sync.retry` > built-in defaults (`RetryConfig()`).

## Configuration

```yaml
sync:
  retry:
    max_attempts: 3                          # default: 3 (1 attempt + 2 retries on failure)
    initial_backoff: 1.0                     # default: 1.0 seconds
    backoff_multiplier: 2.0                  # default: 2.0 — set to 1.0 for linear/constant backoff
    max_backoff: 60.0                        # default: 60.0 seconds — cap on the wait between attempts
    retryable_status_codes: [429, 500, 502, 503, 504]  # default as shown
```

### Field reference

| Field | Default | Notes |
|---|---|---|
| `max_attempts` | `3` | Total attempts including the first call. `1` disables retry. |
| `initial_backoff` | `1.0` | Seconds to wait before the first retry. |
| `backoff_multiplier` | `2.0` | Wait grows by this factor each attempt. `1.0` = linear (constant), `2.0` = exponential doubling. |
| `max_backoff` | `60.0` | Upper bound on the per-retry wait, regardless of multiplier. |
| `retryable_status_codes` | `[429, 500, 502, 503, 504]` | HTTP status codes that trigger a retry. Other 4xx codes (e.g. `400`, `401`) raise immediately. |

### Per-destination override

Some destinations have stricter rate limits or unusual failure modes. Override `sync.retry` per-destination:

```yaml
destination:
  type: notion
  database_id: "abc123"
  auth:
    type: bearer
    token_env: NOTION_TOKEN
  retry:                    # Notion override — 7 attempts, 5s backoff cap
    max_attempts: 7
    initial_backoff: 2.0
    max_backoff: 5.0

sync:
  retry:                    # Falls back here for any other destination in the sync
    max_attempts: 3
```

Supported on every HTTP destination: `discord`, `github_actions`, `google_ads`, `hubspot`, `intercom`, `jira`, `linear`, `notion`, `rest_api`, `sendgrid`, `slack`, `teams`, `twilio`.

## When retry happens

drt retries on:

- **HTTP responses** with a status code in `retryable_status_codes`. Default codes cover rate limits (`429`) and gateway/upstream issues (`500`, `502`, `503`, `504`).
- **Transport errors** (`httpx.TransportError`) — network unreachable, DNS failure, connection reset, read timeout.

drt does **not** retry on:

- 4xx client errors other than `429` (assumed permanent — invalid auth, malformed payload).
- Application errors raised by your sync config (template failures, missing fields).

If all `max_attempts` are exhausted, the original exception propagates and the row is recorded as a failure (subject to `sync.on_error`).

## Backoff timing

For `initial_backoff: 1.0`, `backoff_multiplier: 2.0`, `max_backoff: 60.0`:

| Attempt | Wait before this attempt |
|---|---|
| 1 | — (immediate) |
| 2 | 1.0s |
| 3 | 2.0s |
| 4 | 4.0s |
| 5 | 8.0s |
| ... | ... |
| n | `min(initial_backoff * multiplier^(n-2), max_backoff)` |

Setting `backoff_multiplier: 1.0` with `initial_backoff: 1.0` gives constant 1-second waits — useful for connectors with predictable rate limits.

## Examples

### Aggressive retry for flaky upstream

```yaml
sync:
  retry:
    max_attempts: 10
    initial_backoff: 0.5
    backoff_multiplier: 2.0
    max_backoff: 30.0
```

### Quick fail (no retry)

```yaml
sync:
  retry:
    max_attempts: 1
```

### Per-destination tuning

```yaml
destination:
  type: rest_api
  url: https://api.flaky-vendor.example.com/events
  retry:
    max_attempts: 8
    retryable_status_codes: [429, 500, 502, 503, 504, 408]  # also retry on request timeout

sync:
  retry:                    # other destinations in this sync use the lighter policy
    max_attempts: 3
```

## See also

- [API reference: `sync.retry`](../llm/API_REFERENCE.md#sync-options) — full schema reference
- [`drt/destinations/retry.py`](../../drt/destinations/retry.py) — the `with_retry()` helper and `resolve_retry()` priority logic
