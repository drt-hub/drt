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

## Staleness: a sync that stopped running (`on_stale`)

`on_failure` and `on_degraded` fire while a run is happening, so they say nothing
about a sync that **stopped running** (a deleted schedule, an expired credential on
the scheduler, a runner that is down). `alerts.on_stale` covers that. It measures the
sync, not the destination table, so it works for every destination, including SaaS
and file ones:

```yaml
alerts:
  on_stale:
    max_age: 6h              # 30m, 6h, 2d, 1w (or "6 hours"); age of the last *successful* run
    never_run: warn          # warn | error — a sync with no run on record at all
    channels:                # same Slack/webhook items as on_failure; used with --notify
      - { type: slack, webhook_url_env: SLACK_WEBHOOK_URL, message: "drt sync `{sync_name}` is stale: {error}" }
```

```bash
drt status --check                       # exit 1 if any sync is stale (or never_run: error)
drt status --check --select tag:daily    # only some syncs
drt status --check --output json         # {"ok": ..., "syncs": [{sync, state, severity, age_seconds, ...}]}
drt status --check --notify              # also send each stale sync's alert to its channels
```

Rules:

- **Read-only, evaluated by the caller.** drt has no daemon (ADR 0004): cron, CI or
  any scheduler calls `drt status --check` on whatever cadence you choose.
- **Only a `success` run resets the clock** (never a dry run). `partial` and `failed`
  runs do not, because the destination did not receive the full data. The newest
  success is picked by completion time, so overlapping runs are judged correctly.
- **A sync that ran but has no success in its recent runs is `stale`**, not `never_run`.
  `never_run` means no run is on record at all. A last success dated in the future
  (a runner with a fast clock) is a `clock_skew` error, never "fresh".
- **It fails closed.** If no selected sync configures `on_stale`, or there are no syncs,
  the check exits 1 ("Nothing was checked") instead of reporting a meaningless green.
  Selected syncs without `on_stale` are listed as *not monitored* (`unmonitored` in JSON).
  While rolling the watchdog out before the syncs are configured, add `--allow-empty`:
  "nothing monitored" then exits 0 with a warning (a stale sync still exits 1).
- It reads run history (local, GCS/S3 or warehouse-backed); if history holds no
  success it falls back to the sync's stored state, which records the run's *start*
  time, so keep `max_age` longer than the run's duration if you disable history.
- **Unreadable evidence fails the check.** The state and history stores normally skip a
  corrupt record with a warning, which a watchdog must not read as "never ran". They now
  also report it to a side channel that does not depend on logging configuration, and
  `--check` exits 1 with the count (`unreadable_evidence` in JSON; MCP `ok` is false;
  `--allow-empty` does not hide it). Covered: a corrupt or non-object `state.json`
  (local and GCS/S3), malformed history lines (local and object-store), a success whose
  completion or start timestamp cannot be parsed, and a missing GCS or S3 bucket (probed with
  a one-item listing, so no extra IAM permission is needed). Warehouse-backed stores raise on
  query or decode errors, so those fail the command outright. It cannot detect a record
  that is well-formed but wrong.
- Unknown keys under `on_stale` (for example `never_runs`) are rejected, so a typo cannot
  quietly weaken the policy.
- `--notify` is explicit so a local check never posts to Slack by surprise, and it posts
  on **every** invocation for every non-OK sync: the watchdog's schedule is your alert
  cadence (an hourly watchdog posts hourly throughout an outage). The default Slack
  message reads "is stale". Custom messages can use the same variables as other alerts
  (`{sync_name}`, `{error}`, `{started_at}`, ...).
- `--check` cannot be combined with `--history`, `--sync` or `--verbose`.
- MCP: `drt_get_status(check=True)` returns the same verdict (it never posts to a
  channel).
- **Known limit:** a run interrupted gracefully (SIGTERM) after part of the data is
  recorded as `success` and counts as fresh; history does not yet record interruption.

### A scheduled freshness watchdog (GitHub Actions)

```yaml
name: drt freshness watchdog
on:
  schedule:
    - cron: "17 * * * *"   # hourly
  workflow_dispatch:
jobs:
  watchdog:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v7
      - uses: actions/setup-python@v7
        with:
          python-version: "3.12"
      - run: pip install drt-core
      - name: Check freshness and alert
        env:
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
          # plus the credentials of your state backend (history must be readable here)
        run: drt status --check --notify
```

`drt status --check` already exits 1 when nothing is monitored, so an empty or mistyped
configuration fails the job instead of passing silently.

History must be readable from the runner: use a remote state backend
([remote state](remote-state.md), [warehouse-backed state](warehouse-state.md)); a fresh
checkout has no local `.drt/history`.

## Out of scope (v0.7)

- On-success notifications.
- `drt test` validator failure alerts.
- Per-target retry / dedup of alert dispatch.

These may land in a future release if there's user demand. PagerDuty / OpsGenie / Datadog Events / etc. are reachable today via the generic webhook target.
