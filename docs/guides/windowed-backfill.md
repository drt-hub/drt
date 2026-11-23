# Windowed backfills

Windowed backfills split a large incremental history into fixed cursor ranges and
save progress after every completed range. If a run fails, the next run starts at
the last completed window instead of replaying the whole history.

## Configure a timestamp backfill

```yaml
sync:
  mode: incremental
  cursor_field: updated_at
  backfill:
    window: 1d
    start: "2025-01-01T00:00:00Z"
    # end: "2025-04-01T00:00:00Z"  # optional, exclusive fixed ceiling
```

Timestamp windows are elapsed UTC durations. At the start of each run, drt
captures UTC now once and uses it as the automatic exclusive ceiling. A final
partial window is clipped to that ceiling.

For an integral cursor, use an integer window and start:

```yaml
sync:
  mode: incremental
  cursor_field: event_id
  backfill:
    window: 100000
    start: 0
```

For SQL sources, drt snapshots `MAX(cursor_field) + 1` once per run as the
numeric exclusive ceiling. A numeric API source must set `backfill.end`. REST
profiles must configure distinct `incremental.start_param` and
`incremental.end_param` request keys.

## Execution and resume behavior

Each query covers one half-open range: `cursor >= low AND cursor < high`.
Windows run sequentially. The existing transforms, destination lookups,
idempotency ledger, batching, DLQ behavior, and destination finalizer apply to
every window.

After a window is fully consumed and finalized, drt writes a provisional
`partial` StateStore record with `cursor_boundary: exclusive`, then writes a
separate configured watermark store. A failure of either write stops the run.
Completed windows are never rolled back. Empty windows also checkpoint, which
allows sparse histories to make progress.

`on_error: fail` leaves a window uncheckpointed. `on_error: skip` checkpoints a
fully consumed window when the *destination* reports pinpointed failed/DLQ rows,
matching ordinary incremental behavior. Missing, null, or out-of-range source
cursors are source-contract violations and always fail the window. Before a
checkpoint, drt verifies destination outcome cardinality, flushes the DLQ, and
calls an async destination's optional `flush_pending()` acknowledgement hook.
Any failure leaves the current window resumable.

The pre-checkpoint DLQ flush never evicts older failures. Capacity is checked
against the prospective ID-merged queue: retrying an uncheckpointed window
replaces its deterministic dead-letter IDs and consumes no extra capacity. If
genuinely new IDs would exceed a finite `dlq.max_records`, drt leaves the queue
and checkpoint unchanged. Raise `dlq.max_records` or drain the queue with `drt
retry <sync>` before resuming.

`watermark.lag` applies when resuming a durable checkpoint. drt subtracts the
lag, clamps at `backfill.start`, replays ordinary-sized overlap windows, and
never moves durable progress backward.

The optional warehouse idempotency ledger retains entries for seven days. When
a timestamp lag exceeds that retention, drt warns at run start: overlap rows
whose keys were pruned may be delivered again.

A fixed `backfill.end` makes later runs a no-op after that exclusive boundary is
reached. To continue into live data, remove only `backfill.end`; keep the
`backfill` block. Removing the whole block while an exclusive checkpoint exists
is refused because legacy incremental `>` semantics would reinterpret the saved
boundary unsafely.

## Destination support matrix

Windowed backfill is fail-closed by destination. `drt validate` and the engine
both reject a destination that has not declared the additive
`WindowedBackfillCapable` safety capability; plugin destinations are therefore
unsupported until their implementation opts in and proves the same contract.

| Destination | v1 status | Required configuration or exclusion reason |
|---|---|---|
| PostgreSQL | Supported | Exact integer (never boolean) or string source key values only, plus a non-empty `destination.upsert_key`; use ordinary deterministic, type-native equality. `citext` and nondeterministic ICU collations are outside the key contract. |
| MySQL | Supported | Exact integer source key values only in v1: a full-column PRIMARY/UNIQUE NOT NULL key using `TINYINT` through `BIGINT` (signed or unsigned). Strings and binary types are refused regardless of PAD SPACE/NO PAD collation because padding and coercion can collapse values the engine digest keeps distinct. Use an integer surrogate key. |
| Snowflake | Supported | Exact integer (never boolean) or string source key values only, `destination.mode: merge`, a non-empty `upsert_key`, and uncollated key columns. Insert mode and collated key expressions are outside the contract. |
| Databricks | Supported | Exact integer (never boolean) or string source key values only, `destination.mode: merge`, a non-empty `upsert_key`, and `UTF8_BINARY`/uncollated key columns. Case-insensitive and other non-binary collations are outside the contract. |
| BigQuery | Supported | Exact integer (never boolean) or string source key values only, `destination.mode: merge`, a non-empty `upsert_key`, and uncollated/default-binary key columns. Streaming insert mode and `COLLATE 'und:ci'` keys are outside the contract. |
| ClickHouse | Excluded | `upsert_key` is informational; writes remain inserts, and deduplication depends on target-engine settings drt does not verify. |
| Elasticsearch | Excluded | Not admitted in v1; keyed API replay and row-error correlation have not completed the destination audit. |
| Salesforce Bulk | Excluded | Failed rows currently report `batch_index=0`, so row attribution is not trustworthy. |
| File, Parquet, Google Sheets | Excluded | Overwrite/first-write and append file semantics cannot safely replay an uncheckpointed window. |
| S3, GCS, Azure Blob, staged upload | Excluded | Staged/object uploads are outside the v1 durable-acceptance proof. |
| Airtable, Amplitude, Discord, email SMTP, GitHub Actions, Google Ads, HubSpot, Intercom, Jira, Klaviyo, Linear, Meta Conversions, Mixpanel, Notion, Pipedrive, REST API, SendGrid, Slack, SQS, Teams, Twilio, Zendesk | Excluded | API/message delivery has no v1 proof of keyed, synchronous, idempotent replay for every configured operation; blind creates and notifications can duplicate. |

Unknown built-ins and newly installed plugins are excluded by default. The
runtime refusal names the destination type, its reason, and the declared
support list.

Every extracted row must also produce a non-NULL value for every configured
`destination.upsert_key` column after field mappings, computed fields, and
masking. The engine checks this before the destination write; if a key is
missing or NULL, it reports the sync, column, and window and leaves that window
uncheckpointed so an operator can correct the source data and retry safely.

Upsert keys must be unique within each window as well. drt tracks a canonical,
type-tagged 16-byte BLAKE2b digest for every destination-facing composite key
across all streamed batches in the window. The shared engine admits only exact
Python integers (not booleans) and strings; MySQL admits only integers. It
rejects floats (including NaN, infinity, and signed zero), decimals, booleans,
dates/timestamps, UUIDs, bytes, subclasses, and every other value type before
the batch write. Date/time precision and timezone normalization, UUID textual
form and case, and binary padding are not exact across every supported
destination, so drt refuses those domains instead of canonicalizing their
representations. Cast the key to an integer or string in model SQL, or use an
integer surrogate key.

A repeat fails before its batch is written, reports only the sync, key columns,
window, and one-based destination-row ordinal, and leaves the window
uncheckpointed. Type failures similarly report the sync, column, window, and
type, never the value. The set resets for the next window. Budget approximately
16 bytes per row plus Python set/object overhead; for very large windows,
dedupe the model on the upsert key or shrink `sync.backfill.window`. This guard
is sound only for target-typed values under the destination-exact equality
contract in the table above; it does not emulate database coercion or
collation.

DLQ writes are also part of the window boundary. Before either checkpointing a
successful window or leaving a failed/interrupted window, drt strictly merges
its buffered dead letters without evicting older entries. If `dlq.max_records`
has no room, the queue and checkpoint stay unchanged; the original row,
extraction, interruption, or contract failure remains the primary failure and
the capacity error is reported with it. Raise the cap or drain the queue with
`drt retry NAME`. Backfill cleanup never uses the ordinary capped append.

## Recovery and state safety

Use `drt state reset`; hand-editing `.drt/state.json` or
`.drt/watermarks.json` is unsupported and cannot repair remote state. The cursor
and `cursor_boundary: exclusive` marker are one checkpoint:

- `drt state reset NAME --runs --yes` clears run metadata but retains that
  checkpoint pair, so removing `sync.backfill` remains refused.
- `drt state reset NAME --watermark --yes` clears the configured watermark and
  the fallback cursor/boundary pair. Use this only when deliberately restarting
  from configured `backfill.start`.

Local state writes are atomic. A missing file means first run; corrupt local
JSON fails a backfill closed instead of silently restarting from `start`.

Run only one process for a given backfill sync at a time. Never-regress storage
is not a distributed lease or atomic compare-and-swap; single-flight is the
operator/scheduler's responsibility (`drt serve` already coalesces same-sync
triggers). Do not downgrade and run an older drt against state written by a
backfill: older behavior is backend-dependent and may discard or reinterpret
the exclusive marker.

## Current limits

- Backfill requires `mode: incremental`, cursor strategy, and a top-level
  `cursor_field`. Replace, mirror, diff strategy, floating-point cursors, nested
  cursor paths, and REST link-header pagination are unsupported.
- Only the destination configurations listed as supported above may execute a
  windowed backfill. This is checked by `drt validate` and again before source
  extraction.
- `--limit`, `--cursor-value`, and `--full-refresh` cannot be combined with
  `sync.backfill`.
- Slice 3 executes backfills only through `drt run` and MCP `drt_run_sync`.
  Library-default calls, `drt build`, `drt serve`, Airflow, Prefect, Dagster
  resources/assets/sensor-triggered runs, `drt test`, `drt plan`, `drt apply`,
  exact replay, and DLQ retry refuse before extraction.
- Dry-run is not a backfill preview in slice 3: `drt run --dry-run`, MCP
  `dry_run`, `drt build --dry-run`, `--diff`, and plan/apply do not query any
  window. Dry-run/diff arrives with slice 4.
- CLI `--backfill-start` and `--backfill-end` range overrides arrive in the next
  operational-surfaces slice. For now, configure `start` and optional `end` in
  YAML.
- Windows are deliberately sequential. `--threads` can run different syncs in
  parallel but never parallelizes windows inside one sync.
- Calendar windows, adaptive sizing, split-on-timeout, and distributed leases
  are not v1 features. Keep runner clocks synchronized and use lag for late
  arrivals.
- `run_results.json` window arrays, window OTel spans, backfill-specific alert
  text, history summaries, and `--backfill-start`/`--backfill-end` remain slice
  4 work.
