# Sync Failure Alerts

Configure push notifications so operators learn about sync failures immediately, without polling `drt status`.

## Quick start

Add an `alerts:` block to a sync YAML. The block is optional — syncs without `alerts:` behave exactly as before.

```yaml
name: profile_sync
model: |
  SELECT id, email FROM profiles WHERE updated_at >= '{{ cursor_value }}'
destination:
  type: postgres
  host: db.example.com
  table: app.profiles
  upsert_key: [id]
sync:
  mode: upsert
alerts:
  on_failure:
    - type: slack
      webhook_url_env: SLACK_ALERT_WEBHOOK
    - type: webhook
      url_env: ALERT_WEBHOOK_URL
```

## When alerts fire

`alerts.on_failure` dispatches when **either**:

- The sync run ends with `total_result.failed > 0` (one or more records failed and the sync did not raise), OR
- The sync raised an exception (e.g. connection error, query failure).

Alerts do **not** fire on:

- Successful syncs (`failed == 0` and no exception).
- `--dry-run` invocations (the sync did not actually run).

## Degraded-sync conditions (`on_degraded`)

`on_failure` fires only on **hard failure**. Real reverse-ETL degradation is often *partial* — a creeping error rate, a duration SLA breach, an empty source that "succeeds", or a DLQ backlog growing across runs. `alerts.on_degraded` ([#784](https://github.com/drt-hub/drt/issues/784)) adds **thresholds on four post-sync metrics**, evaluated after each sync from data already in `SyncResult` + the DLQ store (no new collection):

```yaml
alerts:
  on_degraded:
    channels:                       # same target types as on_failure; empty ⇒ JSON-only (CI)
      - { type: slack, webhook_url_env: SLACK_WEBHOOK_URL }
    conditions:                     # mapping metric → threshold; exactly one of gt/lt/gte/lte/eq
      row_errors_pct: { gt: 1 }     # failed / rows_extracted, as % (0 when nothing extracted)
      duration_seconds: { gt: 300 } # whole-sync wall time SLA
      rows_extracted: { eq: 0 }     # empty-source guard ("succeeds" with no rows)
      dlq_depth: { gt: 500 }        # DLQ backlog for this sync, cumulative across runs
```

- **`row_errors_pct` is `failed / rows_extracted`** — `skipped` is excluded (a skipped row is a *normal* outcome via `match_policy` / `--limit` / `lookups.check_only`); when `rows_extracted == 0` the rate is `0%` (empty source is the `rows_extracted` condition's job, never a false `100%`).
- **Conditions are a mapping**, not a list — duplicate metrics are impossible, and each threshold takes exactly one operator (`gt` / `lt` / `gte` / `lte` / `eq`).
- **Multiple tripped conditions coalesce into one alert** per sync per run. `--output json` gains `conditions_tripped: [{metric, operator, threshold, actual}, …]` so CI can react without parsing Slack.
- **`channels` defaults to empty** — conditions can be JSON-only (surfaced in `--output json`) with no paging target, handy for CI gates.
- Evaluated at the CLI seam, so `drt build` inherits it, and — like `on_failure` — it's **best-effort**: a failure while reading the DLQ or dispatching degrades to a logged warning and never sinks the run it monitors. `on_failure` is unchanged and orthogonal.

## Targets

### `type: slack`

Posts a single message to a [Slack incoming webhook](https://api.slack.com/messaging/webhooks).

| Field | Required | Notes |
|---|---|---|
| `type` | yes | Must be `"slack"`. |
| `webhook_url` | one of two | Literal URL. |
| `webhook_url_env` | one of two | Name of an env var holding the URL. |
| `message` | no | Format string. Default: `` "drt sync `{sync_name}` failed: {error}" `` |

Example:

```yaml
alerts:
  on_failure:
    - type: slack
      webhook_url_env: SLACK_ALERT_WEBHOOK
      message: ":warning: `{sync_name}` failed at {started_at} — {error}"
```

### `type: webhook`

Sends a generic HTTP POST/PUT to any URL.

| Field | Required | Notes |
|---|---|---|
| `type` | yes | Must be `"webhook"`. |
| `url` | one of two | Literal URL. |
| `url_env` | one of two | Name of an env var holding the URL. |
| `method` | no | `POST` (default) or `PUT`. |
| `headers` | no | Map of header name → value. |
| `body_template` | no | Format string for the request body. If omitted, drt sends a JSON object containing all template variables. |

Example with PagerDuty Events API v2:

```yaml
alerts:
  on_failure:
    - type: webhook
      url: https://events.pagerduty.com/v2/enqueue
      method: POST
      headers:
        Content-Type: application/json
      body_template: |
        {{
          "routing_key": "${PD_ROUTING_KEY}",
          "event_action": "trigger",
          "payload": {{
            "summary": "drt sync {sync_name} failed: {error}",
            "severity": "error",
            "source": "drt"
          }}
        }}
```

> Note the doubled `{{` / `}}`: Python `str.format()` uses `{}` for variable interpolation, so literal braces in JSON must be escaped.

## Template variables

Available in both `slack.message` and `webhook.body_template`:

| Variable | Type | Meaning |
|---|---|---|
| `sync_name` | str | Name of the failing sync (from `name:` field). |
| `error` | str | First error message from the failed batch, or `<no error message>` if none captured. On exception path: `"<ExceptionClass>: <message>"`. |
| `rows_processed` | int | `success + failed` row counts at time of failure. |
| `duration_s` | float | Elapsed wall-clock seconds. |
| `started_at` | str | ISO-8601 UTC timestamp when the sync started. |

Use `str.format()` syntax: `{sync_name}`, `{error}`, etc. Unknown variables raise `KeyError` at dispatch time and are logged but do not crash the sync.

## Best-effort guarantee

Alert dispatch is **best-effort**:

- A failing alert (network error, 4xx/5xx response, malformed template) is **logged** at WARNING level but never re-raised.
- The original sync error and `SyncResult` are unaffected by alert failures.
- Each target dispatches independently — a failing Slack target does not block the webhook target.

**Caveat:** alert dispatch happens *before* the original exception re-propagates to the caller. Each target has a 10-second `urllib` timeout, so a slow webhook endpoint can delay error propagation by up to `10 × len(on_failure)` seconds in the worst case. Keep alert endpoints fast.

## Environment variables

drt expands `${VAR}` references in YAML at load time across all string fields (#385), so you can write:

```yaml
alerts:
  on_failure:
    - type: webhook
      url: https://api.example.com/${ENV}/alerts
      headers:
        Authorization: Bearer ${ALERT_TOKEN}
```

For URL-only secrets, prefer `webhook_url_env` / `url_env` to keep secrets out of the YAML and out of any logs that might echo it.

## Out of scope (v0.7)

- On-success notifications.
- `drt test` validator failure alerts.
- Per-target retry / dedup of alert dispatch.

These may land in a future release if there's user demand. PagerDuty / OpsGenie / Datadog Events / etc. are reachable today via the generic webhook target.
