# Dead Letter Queue — Replay Failed Records

In production, partial failures are routine: a per-record `429`, a handful
of rows that fail downstream validation while the rest of the batch
succeeds. By default drt counts those in `result.failed` and shows a
truncated preview per row (`drt run --verbose`), but the **records
themselves are dropped** — recovering them means re-running the whole sync.

The **Dead Letter Queue (DLQ)** persists each failed record so you can
replay just the failures with `drt retry`, instead of re-extracting and
re-sending everything.

> This is different from [retry policy](retry.md), which retries *transient*
> HTTP failures with backoff **inside** a single run. The DLQ captures
> records that still failed after those retries were exhausted — the
> durable, cross-run safety net.

## Enabling it

The DLQ is **opt-in per sync**. It writes the **full** record to disk —
unlike the row-error preview, which is deliberately truncated to 200 chars
to avoid logging PII — so you turn it on explicitly:

```yaml
name: post_users
model: ref('users')

destination:
  type: rest_api
  url: https://api.example.com/users

sync:
  on_error: skip          # keep going past per-record failures (so the rest land)
  dlq:
    enabled: true         # default: false
    max_records: 10000    # default: 10000 — cap; oldest dropped past this (0 = unbounded)
```

Failed records are written to `.drt/dlq/<sync_name>.jsonl`, one JSON object
per line, alongside `state.json` and `history/` in your project's `.drt`
directory.

> **Pair it with `on_error: skip`.** With the default `on_error: fail`, the
> sync stops at the first failed batch, so only that batch's failures reach
> the queue. `skip` lets the whole sync run, routing every per-record
> failure to the DLQ.

## Inspecting the queue

`drt status` shows queue depth for any sync with pending failures:

```text
⚠ post_users: 12 record(s) in dead letter queue — run drt retry post_users to replay.
```

In `--output json`, each sync carries a `dlq_depth` field.

## Replaying

```bash
drt retry post_users               # replay every queued record
drt retry post_users --limit 100   # replay only the oldest 100
drt retry post_users --dry-run     # preview depth, send nothing
drt retry post_users --clear       # give up — empty the queue (unrecoverable)
```

`drt retry` re-sends the stored records to the destination in
`sync.batch_size` chunks, drops the ones that now succeed, and writes the
rest back with an incremented `attempts` count. Records are stored
**post-mapping** (after `field_mappings` and lookups), so they replay
verbatim — retry doesn't touch the source.

Each queue entry records:

| Field | Meaning |
|-------|---------|
| `record` | The full payload sent to the destination (post-mapping) |
| `error_message` | The per-record error from the destination |
| `http_status` | HTTP status code, where applicable |
| `timestamp` | When the record first failed (preserved across retries) |
| `attempts` | How many times it has been tried (starts at 1, bumps on each `drt retry`) |

If a record keeps failing, `attempts` climbs each run — a signal that the
record is likely unrecoverable (bad data, a permanent `4xx`) rather than a
transient blip. Use `--clear` to drop those once you've decided they can't
be salvaged.

## Make retries idempotent

`drt retry` re-sends the stored payload as-is. If a record actually reached
the destination but the response was *reported* as a failure (a timeout after
a successful write, an ambiguous `5xx`), replaying it will write the row a
second time — **unless the destination deduplicates on a key you've
configured.** So before relying on the DLQ, give the destination a way to
recognise a re-sent record:

- **SQL destinations** (Postgres / MySQL / ClickHouse / Snowflake / BigQuery /
  Databricks) — set `mode: merge` (or `upsert`) with an `upsert_key`; a replay
  updates the existing row instead of inserting a duplicate.
- **Elasticsearch / OpenSearch** — set `id_field` so each document gets a
  stable `_id`; a replay re-indexes the same document (`op_type: index`)
  rather than creating a new one with an auto-generated id.
- **REST API and other HTTP destinations** — prefer an endpoint that upserts on
  a natural key, or one that honours an idempotency key, so a duplicate POST is
  a no-op server-side.

Append-only sinks (a keyless INSERT, a file/Parquet append, a webhook that just
records what it receives) can't dedupe a replay — there, treat a re-send as
"at-least-once" and reconcile downstream.

## Scope and limits

- **Capture requires per-record errors.** The DLQ captures records that the
  destination reports as individual `row_errors`, which covers the REST API
  destination and the ~24 others that emit per-record errors (Slack,
  Discord, Teams, HubSpot, Notion, Intercom, Linear, Jira, the SQL
  destinations, …). A whole-batch failure with no per-record attribution
  (e.g. a connection drop) is a re-run-the-sync situation, not a DLQ one.
- **Staged destinations and lookup-filtered rows are not captured** in this
  version — their original records aren't available at the point of failure.
- **`max_records` is a FIFO cap.** Past the cap, the oldest entries are
  dropped so the newest failures are always retained.

## See also

- [Retry policy](retry.md) — in-run backoff for transient HTTP failures
- [Field mappings](field-mappings.md) — what "post-mapping record" means
- [Sync history](sync-history.md) — per-run success/fail history
