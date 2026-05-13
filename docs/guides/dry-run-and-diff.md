# Dry Run and Diff Preview

Before running a sync against production, you usually want to know:

1. **How many rows** would be sent? — `drt run --dry-run` already gives you the count.
2. **Which records** would change? — `drt run --dry-run --diff` adds a record-level preview.

This guide covers `--diff` (added in v0.7.1, [#413](https://github.com/drt-hub/drt/issues/413)).

## Quick start

```bash
drt run --dry-run --diff
drt run --dry-run --diff --select customer_health --diff-limit 50
drt run --dry-run --diff --output json   # for CI scripting
```

The flag is **only valid alongside `--dry-run`**:

```bash
$ drt run --diff
Error: --diff requires --dry-run
```

## What you get

`--diff` behaves differently depending on the destination type. The flag is the same; the depth varies.

### Queryable destinations (Postgres / MySQL / ClickHouse)

For SQL destinations with an `upsert_key`, drt fetches the current destination state and computes a true diff keyed on the upsert columns. The output shows:

- **Added** — rows in the source that are not in the destination
- **Updated** — rows where the upsert key matches but at least one other column changed (with field-level `old → new` rendering)
- **Deleted** — rows in the destination that are not in the source (only shown for `mode: replace`, since other modes never delete)

```
Diff preview — customer_health
  source rows: 1247 · destination rows: 1230

  + Added (3):
    + id=42, score=0.95, name=Alice
    + id=43, score=0.82, name=Bob
    + id=44, score=0.91, name=Carol

  ~ Updated (5):
    ~ id=10 — score: 0.62 → 0.74
    ~ id=11 — score: 0.55 → 0.81, last_seen: 2026-04-30 → 2026-05-06
    ~ id=12 — score: 0.30 → 0.45
    ~ id=20 — name: Bob → Robert
    ~ id=21 — score: 0.71 → 0.78

  - Deleted: none
```

For `mode: replace`, "Deleted" lists rows that would disappear after the swap. For `mode: full` / `mode: incremental` (upsert), "Deleted" is suppressed because no rows are removed.

### Non-queryable destinations (REST API, Slack, HubSpot, Notion, file destinations, …)

For destinations without an `upsert_key`-based query API, drt falls back to **sample mode** — it shows the first N records that would be sent, without doing any comparison:

```
Diff preview — alert_failures
  True diff not available for destination type 'slack' — showing a sample of records that would be sent.
  → 12 record(s) would be sent. Sample (first 5 — 7 more not shown):
    sync_name=customer_health, error=DB conn timeout, rows_failed=3
    sync_name=metrics_update, error=API 429, rows_failed=1
    sync_name=user_segments, error=schema mismatch, rows_failed=8
    ...
```

This still lets you eyeball the payload shape before sending.

## `--diff-limit` (default 20)

Limits the number of records shown per category (added / updated / deleted / sample). When the actual number of rows exceeds the limit, drt notes that some records were omitted:

```
…some records omitted (limit reached). Use --diff-limit N to see more.
```

## JSON output (`--output json`)

When `--diff` is combined with `--output json`, the diff is embedded in each sync entry under a `diff` key:

```json
{
  "syncs": [
    {
      "name": "customer_health",
      "status": "success",
      "diff": {
        "supported": true,
        "total_source_rows": 1247,
        "total_destination_rows": 1230,
        "added": [...],
        "updated": [
          {
            "old": {"id": 10, "score": 0.62},
            "new": {"id": 10, "score": 0.74},
            "changed_fields": ["score"]
          }
        ],
        "deleted": [],
        "truncated": false
      }
    }
  ]
}
```

This makes `--diff` useful in CI scripts that gate deployments on previewed change counts.

## Limitations and follow-ups

The current implementation is intentionally simple for v0.7.1. Tracked follow-ups:

- **Snowflake destination** does not yet expose query support, so `--diff` falls back to sample mode for Snowflake. Tracked in [#468](https://github.com/drt-hub/drt/issues/468).
- The destination query is currently `SELECT * FROM <table>`, which fetches the full table into memory. For large tables this can be slow. A `WHERE id IN (...)` batched optimisation is parked at [#470](https://github.com/drt-hub/drt/issues/470) (benchmark-gated).
- A future `--diff-fields` flag will let you limit the displayed columns ([#471](https://github.com/drt-hub/drt/issues/471)).
- API-based diff for upsert-keyed SaaS destinations (HubSpot, Notion) is parked behind `--diff-saas` ([#472](https://github.com/drt-hub/drt/issues/472)).
- The hardcoded "queryable types" tuple will be replaced by a `Destination.fetch_existing()` Protocol method during the v0.9 freeze prep ([#469](https://github.com/drt-hub/drt/issues/469)).
