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
every window. Ordinary resume runs use the ledger's already-delivered filter.
An explicit operator range replay bypasses that filter so it can restore rows
the destination lost. After acknowledged success, drt renews each existing
ledger key's `delivered_at` through an additive ledger capability (or inserts a
missing key) before retention pruning. This prevents an old replayed key from
being pruned immediately and then re-delivered by the next ordinary lagged
resume. A configured ledger without renewal support refuses the replay before
extraction. If renewal fails after the destination has accepted a row, drt still
attempts the compliance audit entry with that delivery timestamp, then propagates
the renewal error without checkpointing or pruning the replay window. This also
applies when delivery is acknowledged through `flush_pending()`.

After a window is fully consumed and finalized, drt writes a provisional
`partial` StateStore record with `cursor_boundary: exclusive`, then writes a
separate configured watermark store. A failure of either write stops the run.
Completed windows are never rolled back. Empty windows also checkpoint, which
allows sparse histories to make progress.

Before writing either checkpoint store, drt parses the proposed high and every
stored cursor in the same exact family (signed integer, or UTC instant), takes
one maximum, and writes that canonical maximum to both stores. This comparison
does not pass integers through floating point and does not compare timestamp
text lexicographically. An unparseable stored cursor fails closed before either
store is changed.

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

Keep every backfill key component stable across retries. A key cannot overlap
the destination targets of `metadata_columns.synced_at`,
`metadata_columns.run_id`, or a future `metadata_columns.sync_run_id`, and it
cannot be produced by `computed_fields`. The engine resolves computed-field
names through `field_mappings` and refuses either overlap before extraction.
Alias stable source identity in the model SQL when the destination needs a
different key name.

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

Backfill DLQ IDs are derived from the sync name, the row's own cursor value
canonicalized in the backfill family, and the validated destination upsert key,
not from either window boundary or the final payload. Thus an open-ended retry
or an explicit replay with shifted window alignment replaces the existing entry
for the same row. Keyless plugin capabilities use the same row-cursor scheme
with the record captured before metadata injection. The fallback is serialized
canonically with explicit type tags, so values such as `1`, `"1"`, `1.0`,
`true`, `null`, and a datetime never collapse through string coercion. If two
keyless rows in one window would still produce the same deterministic ID, drt
refuses before that batch's destination write and checkpoint; configure a
unique destination key or deduplicate the model. The same window-wide check runs
before missing, null, or out-of-range cursor failures reach the DLQ, so strict
DLQ storage never silently coalesces two source-contract failures into one
retryable entry. This ID scheme replaces only
IDs from earlier unreleased slice commits: mainline has not enabled backfill
execution, so no compatibility migration is needed.

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

## Range overrides: `--backfill-start` and `--backfill-end`

For a one-off replay or a bounded catch-up you can override the range for a single
run without editing YAML:

```bash
# Re-send one day that a destination lost, without touching the checkpoint
drt run --select events_to_crm \
  --backfill-start 2026-03-01T00:00:00Z --backfill-end 2026-03-02T00:00:00Z

# Numeric cursor: integers
drt run --select orders_to_crm --backfill-start 1000000 --backfill-end 2000000
```

- **Start precedence:** `--backfill-start`, then the durable checkpoint, then
  `sync.backfill.start`. `--backfill-end` replaces the configured or automatic
  (run-time) ceiling and is exclusive, like every window high.
- **Format follows the sync:** integers for a numeric backfill, ISO-8601 instants
  for a timestamp backfill (offsets are converted to UTC). A value of the wrong
  kind is refused instead of reinterpreted, and the start must be before the end.
- **A start below the checkpoint is a replay, not a rewind.** The windows run and
  write again, but the checkpoint stays where it was until a completed window's
  high exceeds it, so a later scheduled run still resumes from the real progress.
  A start above the checkpoint skips the gap and the checkpoint advances normally.
- **Either range flag makes this an operator replay.** drt does not apply the
  idempotency ledger's already-delivered filter, because the explicit purpose may
  be to restore rows missing from the destination. It renews successful keys'
  delivery timestamps before pruning; a ledger that lacks the additive renewal
  capability is refused before extraction. The run summary reports
  `ledger_bypassed: true`; ordinary resume runs report `false` and retain the
  normal duplicate filter.
- `watermark.lag` is not applied to a CLI start (it applies only when the start
  came from durable storage).
- **Exactly one selected sync, and it must declare `sync.backfill`.** The two flags
  carry one range, so selecting several syncs, or a sync without `sync.backfill`,
  is an error rather than applying the same text to unrelated syncs. Validation
  uses the final selection after `--exclude`, state selectors, and `--failed`; an
  empty healthy selector result is still an error when either range flag is set.
- They are mutually exclusive with `--cursor-value`, `--full-refresh` and `--limit`.
- MCP `drt_run_sync` takes the same range as `backfill_start` / `backfill_end`.

## What a run reports

A backfill run adds two keys to its entry in `target/drt/run_results.json`, to
`--output json`'s `syncs[]`, and to the MCP `drt_run_sync` response:

- `backfill`: the plan summary — `requested_start`, `effective_start`, `ceiling`,
  `window`, `windows_planned`, `windows_completed`, `failed_window` (index or
  `null`), `final_checkpoint`, and `ledger_bypassed`.
- `windows`: one object per attempted window — `index`, `low`, `high` (half-open),
  `rows_extracted`, `rows_synced`/`rows_failed`/`rows_skipped`, `status`
  (`success`/`failed`), `checkpoint` (`written`, `not_written` or `failed`) and
  `duration_seconds`.

Both carry counts and bounds only, never row data or error text. If a window raises,
the run is reported as a failed sync without these keys (the exception, not a result,
reaches the CLI); use the checkpoint and `drt status` to see how far it got.

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
- Range overrides are available as `--backfill-start` / `--backfill-end` (see
  above). Dry-run/diff previews, window OTel spans and aggregate alert/history
  wording remain later slices.
- Windows are deliberately sequential. `--threads` can run different syncs in
  parallel but never parallelizes windows inside one sync.
- Calendar windows, adaptive sizing, split-on-timeout, and distributed leases
  are not v1 features. Keep runner clocks synchronized and use lag for late
  arrivals.
- Window OTel spans, backfill-specific alert text and history summaries remain
  slice 4 work.
