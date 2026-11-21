# ADR 0021 — Windowed backfill for incremental syncs

- **Status:** Accepted 2026-11-15 (owner approved all eight recommendations below).
- **Issue:** [#758](https://github.com/drt-hub/drt/issues/758)
- **Relates to:** [ADR 0005](0005-state-location-and-write-grants.md) (durable state),
  [ADR 0007](0007-protocol-stability-policy.md) (additive capability Protocols),
  [ADR 0013](0013-plan-apply-lineage.md) (plan/apply lineage), and
  [ADR 0014](0014-exact-replay-apply.md) (exact replay).

## Context

### Current cursor execution

Today `mode: incremental` requires `cursor_field`; cursor-based extraction is the
default strategy, while `incremental_strategy: diff` is a separate snapshot path
(`drt/config/sync_options.py:398-406`, `drt/config/sync_options.py:519-546`). The
engine resolves one lower bound in this order: `--cursor-value`, configured
watermark storage, the `StateStore` fallback, then `watermark.default_value`
(`drt/engine/sync.py:721-756`). With no resolved value, the first run is unbounded:
the resolver adds no cursor predicate (`drt/engine/resolver.py:154-160`).

For SQL-shaped sources, predicate construction is centralized rather than
implemented per connector. `resolve_model_ref()` wraps the resolved model as
`_drt_base` and injects `cursor_field > '<value>'`; a model containing
`{{ cursor_value }}` or `{{ watermark }}` is rendered and returned without the
wrapper because the author supplied the predicate (`drt/engine/resolver.py:128-160`).
The SQL sources then execute that already-rendered query: BigQuery
(`drt/sources/bigquery.py:110-130`), ClickHouse
(`drt/sources/clickhouse.py:65-115`), Databricks
(`drt/sources/databricks.py:152-212`), DuckDB
(`drt/sources/duckdb.py:25-58`), MySQL (`drt/sources/mysql.py:72-145`),
PostgreSQL (`drt/sources/postgres.py:156-233`), Redshift
(`drt/sources/redshift.py:81-144`), Snowflake
(`drt/sources/snowflake.py:143-202`), SQLite
(`drt/sources/sqlite.py:26-51`), and SQL Server
(`drt/sources/sqlserver.py:71-124`). Delta Lake and Iceberg likewise execute the
rendered SQL through DuckDB (`drt/sources/deltalake.py:42-91`,
`drt/sources/iceberg.py:58-84`).

API-shaped sources cannot receive a SQL predicate. The frozen, optional
`IncrementalSource` capability therefore accepts the resolved lower bound directly
(`drt/sources/base.py:83-113`); the engine selects it for incremental extraction
(`drt/engine/sync.py:280-313`, `drt/engine/sync.py:943-955`). The only built-in
implementation is the REST API source, whose `start_param` receives that value on
requests (`drt/config/base.py:168-178`, `drt/sources/rest_api.py:57-87`,
`drt/sources/rest_api.py:148-159`). Neither the SQL wrapper nor
`IncrementalSource` has an upper-bound surface today.

Extraction is streamed and interleaved with destination batches
(`drt/engine/sync.py:1019-1040`). The engine tracks the maximum non-null cursor it
observes, using numeric comparison when both values parse as numbers and otherwise
lexical comparison (`drt/engine/sync.py:1042-1053`, `drt/engine/cursor.py:13-18`).
It does not inject `ORDER BY`; on a fatal batch or lookup failure it rolls the
cursor back to the run's durable starting value because an unread later batch may
contain a lower cursor (`drt/engine/sync.py:1335-1374`). Tz-aware Python datetimes
are normalized to naive UTC before persistence to avoid representing one instant
two ways, while other values use `str()` unchanged
(`drt/engine/sync.py:71-88`).

The current cursor is therefore a **maximum observed row value**, not a completed
range boundary. It advances once, after extraction, loading, staged/non-staged
finalization, diff work and result assembly (`drt/engine/sync.py:1391-1474`,
`drt/engine/sync.py:1476-1493`). An empty run cannot advance beyond its starting
cursor. That is correct for an open-ended incremental read but cannot express “the
whole interval `[low, high)` was inspected and committed.”

### Current durability and operational surfaces

State and watermark writes flow through `SyncObserver.on_sync_completed`, which is
documented as a once-per-run event; durable observers must ignore dry runs
(`drt/engine/observer.py:51-117`). `StatePersistingObserver` writes one `SyncState`
and, when configured, one watermark after the run. It preserves the old cursor for
exact replay, refuses cursor regression, and re-persists an unchanged value for
lagged empty runs (`drt/engine/observer.py:250-349`). The engine calls that event
only on its normal return path; `on_sync_ended` is the unconditional flush point,
including for buffered DLQ records (`drt/engine/sync.py:668-676`,
`drt/engine/observer.py:412-469`).

The configured `StateStore` can be local, GCS, S3 or warehouse-backed; the shared
factory supplies state, history and DLQ implementations for each
(`drt/state/factory.py:16-34`, `drt/state/factory.py:80-99`,
`drt/state/factory.py:123-248`). A sync-level `watermark:` is a separate, optional
store with local, GCS and BigQuery implementations
(`drt/config/sync_options.py:83-104`, `drt/cli/_helpers.py:105-129`,
`drt/state/watermark.py:35-67`). Without it, `SyncState.last_cursor_value` is the
authoritative fallback (`drt/state/manager.py:30-55`,
`drt/state/manager.py:149-164`). Local watermark mutations use a process and OS
lock, GCS uses generation-conditional retries, and BigQuery uses a `MERGE`
(`drt/state/watermark.py:70-117`, `drt/state/watermark.py:177-269`,
`drt/state/watermark.py:329-358`).

One `run_sync()` call currently produces one history entry in its outer `finally`,
including aggregate counts, the cursor used, correlation ids, guards and plan
lineage (`drt/engine/sync.py:559-605`). `HistoryEntry` is explicitly one execution
of one sync and is additive/omit-when-absent for optional fields
(`drt/state/history.py:37-80`). The CLI similarly builds one result object per sync
and writes one durable `target/drt/run_results.json` per invocation
(`drt/cli/commands/run.py:337-370`, `drt/cli/commands/run.py:574-654`,
`drt/cli/commands/run.py:1292-1307`). That artifact deliberately lacks a
watermark-before/after pair today because the post-run value is delivered only to
the persisting observer (`drt/cli/commands/run.py:629-634`).

Per-record idempotency checks happen before each destination batch and successful
keys are marked immediately after that batch, limiting the crash window to one
batch (`drt/engine/sync.py:1209-1237`, `drt/engine/sync.py:1268-1297`). Pinpointed
row failures are handed to the DLQ observer per batch
(`drt/engine/sync.py:1315-1333`). Failure alerts and history are emitted once from
the outer run, while degraded alerts are evaluated once by the CLI from the final
`SyncResult` and DLQ depth (`drt/engine/sync.py:541-605`,
`drt/cli/commands/run.py:413-456`).

`watermark.lag` currently shifts only a storage-sourced lower bound; it does not
apply to a CLI override or first-run default, and the unlagged watermark seeds
progress so an overlap read cannot regress it (`drt/config/sync_options.py:93-118`,
`drt/engine/sync.py:790-808`). Integer cursors take an integer lag and timestamp
cursors take a duration; malformed or mismatched values fail before extraction
(`drt/engine/sync.py:91-125`).

### Current CLI, plan/apply and mode constraints

`drt run --cursor-value` is one lower-bound override for all selected incremental
syncs; it is rejected when none are incremental and is mutually exclusive with
`--full-refresh` (`drt/cli/commands/run.py:829-840`,
`drt/cli/commands/run.py:1076-1104`). `--threads` parallelizes different syncs in
one invocation, not work inside a sync (`drt/cli/commands/run.py:803-804`,
`drt/cli/commands/run.py:1179-1237`). `--limit` freezes cursor progress and is
already refused for mirror/replace because finalization would act on a partial
snapshot (`drt/engine/sync.py:1042-1047`, `drt/cli/commands/run.py:1057-1074`).

`drt plan` executes the same `run_sync(..., dry_run=True, compute_diff=True)` path,
so it reads the resolved cursor but persists neither state nor watermark
(`drt/cli/commands/plan.py:1-6`, `drt/cli/_plan_runner.py:186-240`). The plan stores
a keyed hash of the cursor, and normal apply recomputes the plan, refuses a moved
cursor, then runs with the verified cursor value
(`drt/engine/plan.py:261-305`, `drt/cli/_apply_flow.py:488-520`,
`drt/cli/_apply_flow.py:640-677`). Exact replay is deliberately different: its
rows are already destination-facing, so the engine disables source cursor
resolution and the persisting observer carries the previous cursor through
unchanged (`drt/engine/sync.py:721-727`, `drt/engine/observer.py:272-280`).

`replace` and `mirror` finalize after the batch loop and may promote a shadow table
or delete rows absent from the run; finalization is deliberately skipped on a
raised exception (`drt/engine/sync.py:635-659`, `drt/engine/sync.py:1418-1431`).
Run-time guards at that seam cover delete counts; create/update guards are evaluated
from a plan's complete change set (`drt/config/sync_options.py:209-233`,
`drt/engine/guards.py:194-230`, `drt/engine/plan.py:365-376`). A window is only a
partial source range, so applying replace/mirror finalization to one would be
destructive rather than incremental.

## Decision

### 1. Opt-in configuration and eligibility

Windowing is opt-in under `sync.backfill`:

```yaml
sync:
  mode: incremental
  cursor_field: updated_at
  backfill:
    window: 1d
    start: "2025-01-01T00:00:00Z"
    # end: "2025-04-01T00:00:00Z"  # optional, exclusive
```

For an integral numeric cursor:

```yaml
sync:
  mode: incremental
  cursor_field: event_id
  backfill:
    window: 100000
    start: 0
```

`window` is either a positive compact/long duration (`1d`, `6 hours`) or a
positive integer. Its type selects timestamp or integral-numeric arithmetic;
booleans, zero/negative values, floating-point cursors and mixed start/end types
are validation errors. `start` is required config and is used only when no durable
checkpoint exists. Optional `end` is an exclusive, fixed ceiling for a deliberately
bounded backfill.

`backfill` is valid only with `mode: incremental` and the default
`incremental_strategy: cursor`. It is rejected for full/upsert/replace/mirror,
for `incremental_strategy: diff`, and with `watermark.default_value` (two
competing first-run starts). It is also rejected with `--limit`: a sampled window
cannot be declared complete. In particular, replace/mirror are not “made safe” by
windowing; their finalizers require a complete snapshot.

### 2. Fixed half-open windows

The engine captures one run ceiling before the first extraction and plans ascending,
non-overlapping windows:

```text
[start, start + window)
[start + window, start + 2*window)
...
[low, run_ceiling)
```

Every extraction predicate is `cursor_field >= low AND cursor_field < high`.
The lower bound is inclusive because the preceding window excluded its `high`;
the upper bound is exclusive so a row belongs to exactly one normal window.
For SQL sources the engine always applies this predicate to the resolved model
subquery. A model using the legacy `{{ cursor_value }}`/`{{ watermark }}` template
is rejected for windowed backfill in the first release: that template owns only a
lower predicate and could filter out the inclusive boundary before the outer
wrapper sees it. Authors remove the template and let the engine inject both bounds.
The engine validates every returned non-null cursor against the range before the
row is loaded, making an incorrectly implemented source capability fail closed.
Null cursors are a row error: they cannot be assigned to a window or safely skipped
past.

For timestamp cursors, the automatic ceiling is a single `now()` captured in UTC at
run start. The last window uses `min(low + window, captured_now)`, even when no row
has that cursor. We do **not** use `MAX(cursor_field)` for timestamp cursors: a max
is not an exclusive boundary, is undefined for an empty source, and makes the
backfill chase rows arriving during the run. A fixed clock boundary makes the work
finite and lets an empty trailing interval be certified. Rows committed later with
an older timestamp are the late-arrival problem handled by `watermark.lag`, not a
reason to keep one backfill open indefinitely.

For integral numeric cursors, there is no clock-derived ceiling. SQL-shaped sources
snapshot `MAX(cursor_field)` from the resolved model before loading and use
`max + 1` as the exclusive ceiling. The maximum is captured once, so rows appended
during the run wait for the next run. A non-SQL source must receive an explicit
`backfill.end`/CLI end or implement an additive upper-bound capability; drt will not
guess a numeric endpoint. An empty numeric source has no window to certify and
leaves the checkpoint at `start`.

Timestamp inputs accept ISO-8601 offsets. Aware values are converted to UTC; naive
values are interpreted as UTC for compatibility with the existing persisted form.
Window arithmetic is elapsed UTC time, not local calendar time, so a `1d` window is
always 24 hours across DST. Bounds are rendered in the existing naive-UTC canonical
form; numeric bounds use exact integer arithmetic, never `float`.

### 3. One window is the checkpoint transaction

Within one window the existing pipeline remains: resolve the bounded query, stream
batches, track/validate cursors, apply lookups and transforms, load, record
idempotency/DLQ outcomes, and run the appropriate staged or non-staged finalizer.
Only after all of that succeeds does the engine durably checkpoint `high`.

“Succeeds” preserves `on_error` semantics. With `on_error: fail`, any fatal lookup,
load/finalize failure, interruption or exception leaves the current window
uncheckpointed. With `on_error: skip`, pinpointed failed rows remain failed/DLQ
rows but the fully consumed window is checkpointed, matching today's deliberate
advance-past-skipped-row behavior (`drt/engine/sync.py:1335-1364`). A checkpoint
failure is itself fatal and aborts the remaining windows; continuing without a
durable resume point could turn a later crash into silent omission.

Completed windows stay committed. A failure aborts the current and all later
windows, but never rolls the watermark back across a completed window. The next run
resolves the saved `high` as its logical `low` and retries from there. Because a
window is checkpointed only after its complete stream and finalizer, no `ORDER BY`
is required inside the window. Windows themselves are always processed in ascending
order.

Empty timestamp windows still run their completion bookkeeping and checkpoint
`high`. This is essential for sparse histories: otherwise a year with no events can
never reach the next populated year. The destination sees no load call for an empty
window, but the normal non-destructive incremental finalization contract completes
before the checkpoint.

The outer `run_sync()` retains one `run_id`, one `sync_run_id`, one start timestamp
and one aggregate `SyncResult`. `metadata_columns.synced_at` therefore remains the
overall run start, not a different value per window. Destination write state is
reset once on overall exit; it is not reset between windows. Windows are sequential
inside a sync. `--threads N` may run other syncs concurrently but never parallelizes
one sync's windows.

### 4. Additive source and observer capabilities

The frozen `Source`, `IncrementalSource` and `SyncObserver` signatures do not
change. Two additive capabilities are introduced:

- `WindowedIncrementalSource.extract_window(query, profile, low, high, ...)` for
  non-SQL sources. SQL sources continue to execute a centrally wrapped model query.
  REST support adds an optional `end_param` alongside `start_param`; a REST profile
  without it is not window-capable.
- `WindowCheckpointObserver.checkpoint_window(...)` for acknowledged persistence,
  plus fire-and-forget window-started/completed/failed events. The CLI's composite
  observer implements it by delegating to `StatePersistingObserver`. Unlike the
  existing fire-and-forget event methods, the checkpoint method may raise and the
  engine must stop on that error.

For each successful window, the checkpoint observer first writes `StateStore`
bookkeeping, including the proposed `high` and its boundary marker. If a separate
`WatermarkStorage` is configured, it writes that authoritative cursor second; if
not, the state value is itself authoritative. Failure of either write aborts the
run. This order is conservative under a split failure: a failed external-watermark
write leaves its older value authoritative, so the next run replays work, while the
state marker prevents an unsafe switch to legacy semantics. A provisional state is
`partial` until the outer run completes, so a hard crash is eligible for
`drt run --failed` (which selects any recorded non-success status today;
`drt/cli/commands/run.py:1022-1032`); the final `on_sync_completed` overwrites it
with the aggregate outcome. Dry runs emit window events but never checkpoint. A
non-dry-run windowed sync requires a checkpoint-capable observer; a library call
using only `NullObserver` fails before extraction instead of promising resume
semantics it cannot provide.

The state bookkeeping also records `cursor_boundary: exclusive`. This metadata is
necessary because a window checkpoint means “everything below this value was
completed,” whereas a legacy watermark means “this exact maximum row value was
observed.” The marker lives in the always-present `StateStore` even when a separate
`WatermarkStorage` holds the value. Removing `sync.backfill` while that marker is
present is a run-time refusal with remediation, not a silent switch back to strict
`>` semantics. Returning to ordinary incremental execution requires an explicit
migration/reset command that acknowledges the boundary conversion.

### 5. Resume, lag and CLI range overrides

Normal start precedence is:

1. CLI `--backfill-start`;
2. durable watermark/state checkpoint;
3. `sync.backfill.start`.

`--backfill-end` overrides the automatic/configured exclusive ceiling. The two
flags are accepted only when exactly one selected sync has `sync.backfill`, and are
mutually exclusive with `--cursor-value`, `--full-refresh` and `--limit`. Requiring
one selected sync prevents one timestamp range from being broadcast into unrelated
numeric or differently zoned syncs. Existing `--cursor-value` retains its exact
legacy meaning for syncs without `backfill`.

`sync.backfill` is a persistent execution policy, not a one-run flag. After the
initial history reaches its ceiling, later scheduled runs split the new
`[saved_checkpoint, captured_now)` range the same way; a fixed `backfill.end` makes
later runs a no-op once reached. An operator who wants live continuation removes
the fixed `end`, not the whole `backfill` block.

A CLI start below the durable checkpoint is a historical replay, not a state
rewind. Windows are executed, but the never-regress rule keeps the durable
checkpoint unchanged until a completed `high` exceeds it. This preserves the
current override safety property while allowing an explicit bounded replay.

`watermark.lag` applies only when the logical start came from durable storage, as
today. The planner subtracts the lag, clamps at configured `backfill.start`, and
creates ordinary-sized overlap windows up to the durable checkpoint before new
windows. Checkpoints in that overlap cannot regress state; once the plan crosses
the old checkpoint, each new `high` becomes durable. Lag is not applied to config
start or CLI range overrides. This avoids one oversized first query when lag is
larger than `window`, while preserving the existing “re-send overlap, persist
unlagged progress” contract.

### 6. Bookkeeping, guards, idempotency, DLQ and finalization

There is one history entry per **overall sync execution**, not one per window.
Per-window history rows would make a long first backfill look like thousands of
independent scheduled runs, exhaust bounded history, distort freshness, and flood
the catalog. The history entry gains an optional `backfill` summary containing
requested/effective start, ceiling, window size, windows planned/completed, the
failed window (if any), and final checkpoint. Counts/status remain aggregate.

`SyncResult` and `run_results.json` gain an omit-when-absent `backfill` block and a
`windows[]` array with index, `[low, high)`, row counts, status, duration and
checkpoint outcome. This is the detailed per-window record for CI. Window events
also produce child OTel spans under the existing sync span; existing extract/load
spans remain inside them.

Idempotency remains per destination batch, but its effect is naturally scoped to
each window. A retry of an uncheckpointed window can skip ledger-confirmed rows;
destinations without the opt-in warehouse ledger retain their existing replay
requirements. DLQ records retain the overall `sync_run_id` and add window bounds;
the DLQ observer flushes at every window boundary and again on overall exit so a
completed checkpoint cannot outrun its failed-row record.

Ordinary `drt run` still cannot evaluate `max_creates` or `max_updates_pct` without
a destination diff. Plan/apply evaluates configured guards over the **entire
selected backfill range**, because the reviewed action is one backfill, not each
small query. Runtime delete guards do not apply: windowed backfill rejects the
replace/mirror modes that own destructive finalization. Staged `finalize()` and
non-staged `finalize_sync()` run per window before its checkpoint; a finalizer
failure leaves that window resumable.

Failure and degraded alerts fire once for the aggregate run and include the failed
window and the last completed checkpoint. They do not fire once per window. This
keeps existing alert volume and makes one operator action correspond to one CLI
execution.

### 7. Plan/apply and exact replay

`drt plan` plans the complete bounded range selected for that invocation while
remaining read-only. The authenticated plan document gains a canonical `backfill`
block with window type/size, effective start, captured exclusive ceiling, durable
checkpoint before planning, and the ordered window bounds. Apply recomputes using
those same bounds, refuses if the durable checkpoint or any window's change set
moved, and then executes the verified windows sequentially. Guards are evaluated
against the aggregate plan.

For `plan --include-values`, the encrypted payload groups destination-facing rows
by window rather than storing one flat list. Exact apply verifies the live
destination as it does today, replays groups in order, and checkpoints the sealed
`high` only after each group succeeds. This is a narrow exception to ordinary
exact replay's preserve-the-cursor rule: the authenticated plan supplies the source
range proof that a flat exact payload lacks. Ordinary non-backfill exact replay
continues to preserve its prior cursor unchanged.

Plan files therefore disclose canonical range bounds as authenticated operational
metadata, just as they disclose sync name, destination and changed keys. Row values
remain only in the age-encrypted sidecar (`drt/engine/plan.py:123-160`). A later
privacy requirement can add keyed range display, but apply must retain authenticated
plaintext bounds somewhere to reproduce a clock-bounded plan without asking the
operator to retype them.

## Out of scope

- Parallel execution of windows. Parallelism makes ordered checkpointing,
  destination rate limiting and failure cancellation a different design.
- `incremental_strategy: diff`. It snapshots a full model and maintains a baseline;
  slicing that snapshot changes its meaning.
- Calendar windows such as “one local month,” adaptive window sizing, and dynamic
  split-on-timeout.
- A distributed lease preventing two separate processes from running the same sync.
  Existing backend concurrency protection prevents lost object updates but is not
  a scheduler/lease.

## Alternatives considered

### dbt microbatch semantics

dbt microbatch supplies event-time batches, lookback and parallel execution, and
is the closest user-facing precedent. Reusing its vocabulary was attractive, but
drt owns destination delivery, DLQ, idempotency, guard and watermark semantics; a
dbt model invocation cannot atomically certify that an external API load completed.
The decision borrows half-open event-time windows and lookback behavior without
making dbt a runtime dependency or adopting parallel batches.

### Chunking inside the source cursor

Sources could stop after N rows or N database fetches and let the engine save the
maximum observed cursor. That bounds memory, which sources already do through
streaming (`drt/engine/sync.py:1019-1040`), but not time or replay scope. Duplicate
cursor values can straddle an arbitrary row chunk, an unordered query can yield a
lower value later (`drt/engine/sync.py:1335-1349`), and an empty chunk proves no
time interval complete. Explicit `[low, high)` predicates make the checkpoint
independent of row order and count.

### Keyset pagination

Keyset pagination (`cursor > last_seen ORDER BY cursor LIMIT N`) is efficient and
useful for a stable unique key. `cursor_field` is not required to be unique, the
config accepts only a field name (`drt/config/sync_options.py:398-406`), and the
engine deliberately injects no ordering (`drt/engine/sync.py:1335-1349`).
The public contract therefore permits tied values and has no tie-breaker. A
compound `(cursor, tie_breaker)` contract would be a different feature and would
still not provide a natural time ceiling or empty window progress.

### Persist the maximum observed cursor per window

This would reuse the current mechanism, but a sparse or empty window could never
advance and a failure after a tied maximum would retain the existing boundary
ambiguity. Persisting the planned exclusive `high` records what was completely
inspected, not merely the largest value that happened to exist.

## Consequences

- Large initial loads are bounded by cursor range and resume after the last fully
  finalized window rather than replaying the whole history.
- The feature intentionally changes the backfill cursor from “max row seen” to
  “exclusive interval completed.” Empty intervals therefore advance state.
- Enabling windowing on a sync with legacy state may replay rows equal to the old
  maximum once (safe against loss, but potentially duplicate-producing). Removing
  windowing later requires an explicit boundary migration; it never silently
  reinterprets an exclusive checkpoint as a legacy maximum.
- A long run performs more source queries, finalizations, state writes and observer
  events. Smaller windows improve retry granularity but increase that overhead.
- Timestamp correctness now depends on the runner's UTC clock and an adequate lag
  for late-arriving rows. Numeric auto-ceilings require integral, monotonic cursors.
- Numeric auto-ceiling adds one `MAX(cursor_field)` query over the resolved model;
  an explicit `end` avoids that read when it is too expensive.
- State persistence at a window boundary becomes correctness-critical instead of
  best-effort; a checkpoint outage fails the run even if destination writes for the
  window succeeded, so retry may duplicate that window but cannot skip it.
- Plan artifacts and exact payloads gain a schema version and window grouping.
- The new capability Protocols preserve the frozen interfaces, but custom REST/API
  sources must opt in before they can promise an upper-bounded extract.

## Compatibility

When `sync.backfill` is absent **and no prior exclusive-boundary marker exists**,
query rendering, strict `>` lower-bound semantics, cursor max tracking, watermark
lag, observer call counts, history, alerts, run-results schema and plan/apply
behavior are byte-for-behavior unchanged. Existing `--cursor-value` remains a
one-bound recovery override. No existing source or observer must implement either
new capability. A marker can exist only after the new feature has run; refusing an
unacknowledged transition protects that new state rather than changing an existing
sync.

The new result/history/plan fields are additive and omitted for ordinary runs.
Config/schema generation accepts the new block but does not reinterpret
`watermark.default_value`. Existing state values need no migration: the first
windowed run treats the stored value as its logical low, and subsequent values are
exclusive completed boundaries.

## Implementation plan

| PR | Slice | Main acceptance criteria |
|---:|---|---|
| 1 | Config and range arithmetic | Add strict `BackfillConfig`, timestamp/integer canonicalization, `[low, high)` planner, validation against modes/diff/default/limit, and generated schemas. Pure tests cover DST, empty/final partial windows and integer overflow. |
| 2 | Bounded extraction | Add the resolver's two-sided SQL wrapper, reject legacy lower-only cursor templates for windowed syncs, add row range validation, numeric max snapshot, `WindowedIncrementalSource`, and REST `end_param`. Connector query-shape tests cover every SQL source family. |
| 3 | Sequential engine loop and checkpoints | Refactor one-window execution around the existing batch/finalize path; add the acknowledged checkpoint capability and exclusive-boundary marker; implement authoritative watermark/state ordering across local, GCS, S3, BigQuery-watermark and warehouse state backends; test failure/resume, empty windows and refusal of a silent legacy transition. |
| 4 | Operational surfaces | Add CLI range flags, sequential-within-sync thread behavior, window OTel/observer events, aggregate history, per-window run results, DLQ boundary flush and single aggregate alerts. |
| 5 | Plan/apply | Version the plan document, seal window bounds and pre-plan checkpoint, aggregate guards, refuse drift/moved progress, and execute verified windows sequentially. |
| 6 | Exact replay and end-to-end coverage | Group encrypted payload rows by window, checkpoint sealed highs only after successful groups, preserve ordinary exact behavior, and add local-SQL plus one live cloud-warehouse resume smoke test. |

## Amendment 2026-11-17 (slice 2 review)

The maintainer delegated the following eight slice-2 review decisions. They
refine bounded extraction without enabling execution: the slice-1
`BACKFILL_EXECUTION_ENABLED = False` gate remains in force.

1. **Timestamp bounds are UTC instants, not session-local strings.** The
   contract is: **a timestamp cursor column is compared as a UTC instant; naive
   columns are assumed to hold UTC**. PostgreSQL/Redshift use `TIMESTAMPTZ`,
   Snowflake uses `TO_TIMESTAMP_TZ`, BigQuery and Databricks use their explicit
   `TIMESTAMP` instant literals, SQL Server uses `datetimeoffset`, ClickHouse
   uses UTC `toDateTime64`, and DuckDB/Delta Lake/Iceberg use a UTC
   `TIMESTAMPTZ` bound with epoch comparison so both aware and naive cursor
   values are session-independent. MySQL switches the windowed connection to
   UTC because its timestamp literals cannot carry an instant independently;
   SQLite compares canonical UTC text and has no session time zone.
2. **Timestamp precision stops at microseconds.** Bounds with more than six
   fractional digits are rejected instead of being silently truncated by the
   Python timestamp parser.
3. **REST range parameters own distinct request keys.** `start_param` and
   `end_param` must be non-empty, contain no surrounding whitespace, differ
   from each other, and not collide with pagination request parameters. Offset
   pagination reserves `offset_param` and `limit_param`; cursor pagination
   reserves `cursor_param` and `limit_param`; link-header pagination reserves
   no query parameter. Profile loading validates this, and `extract_window`
   repeats it defensively for directly constructed profiles.
4. **Dotted cursor fields are paths.** Every dot-separated segment is quoted
   independently in both the window predicate and numeric `MAX` query, matching
   the legacy incremental path-expression behavior. Row validation traverses
   the same nested path.
5. **Derived-table wrapping preserves statement boundaries.** The resolved
   model is placed on its own lines inside parentheses, trailing whitespace and
   one trailing statement semicolon are removed, and multiple statements are
   rejected. Windowed local-model resolution preserves the file's final newline
   until this wrapping step, so a trailing `--` comment cannot consume the outer
   predicate. SQL Server models whose first token after comments is `WITH` are
   rejected with guidance to move the CTE into a view or subquery. A top-level
   trailing `ORDER BY` is also a SQL Server derived-table limitation; authors
   must remove it or move the ordered query behind a view/subquery unless they
   rewrite it into a form SQL Server permits in a derived table.
6. **Numeric bounds are signed 64-bit integers.** Configured and runtime bounds
   outside `[-2^63, 2^63-1]` are rejected. If an automatic `MAX(cursor) + 1`
   ceiling would exceed that range, extraction fails with guidance to set an
   explicit `backfill.end`.
7. **Legacy template detection is syntax-aware.** Parsed Jinja undeclared-name
   analysis detects filtered expressions and control-flow references to
   `cursor_value` or `watermark` without treating plain words in SQL comments
   or string literals as templates. Invalid Jinja falls back to the previous
   exact-token regex and therefore fails closed; exact-token behavior for local
   `ref()` model files is retained.
8. **Runtime Protocol presence is insufficient.**
   `WindowedIncrementalSource` remains `runtime_checkable`, whose native check
   proves only that the attribute is present. Engine capability checks use
   `is_window_capable(source)`, which also verifies required positional
   `query`, `profile`, `low`, and `high` parameters and permits additional
   parameters only when they have defaults.

### Round 2

The maintainer delegated these follow-up decisions after the second review;
where they differ, they supersede the first-round entries above:

1. **Every windowed SQL connection with session time-zone state is pinned to
   UTC.** The timestamp contract is now explicit: the cursor is a UTC instant,
   naive timestamp columns contain UTC, and the connection used for windowed
   extraction runs in a UTC session. PostgreSQL/Redshift run `SET TIME ZONE
   'UTC'`; Snowflake runs `ALTER SESSION SET TIMEZONE='UTC'`; Databricks runs
   `SET TIME ZONE 'UTC'`; DuckDB and its Delta Lake/Iceberg adapters run `SET
   TimeZone='UTC'`; ClickHouse sends `session_timezone='UTC'`; MySQL retains
   its existing UTC session setup. Legacy incremental connections are not
   changed. SQL Server has no equivalent session setting and BigQuery
   timestamps are always UTC. BigQuery `DATETIME` cursor columns are not
   supported for v1 backfills because the resolver cannot know the column type
   safely; cast the cursor to `TIMESTAMP` in the model. SQLite bounds use
   lexically sortable `YYYY-MM-DD HH:MM:SS[.ffffff]` UTC text, and stored text
   must use that same format (no `datetime(column)` normalization, preserving
   index use). SQL Server binds use `datetime2(7)` constants so indexed
   `datetime`/`datetime2` columns are not implicitly converted; a
   `datetimeoffset` column converts the constant.
2. **Nested cursor paths are unsupported in v1 windowed backfill.** Config
   validation and the resolver reject dotted `cursor_field` values with
   guidance to expose a top-level model column. Window predicates and numeric
   ceiling queries quote one identifier. Legacy incremental behavior is
   unchanged. To remain consistent with that legacy unquoted expression,
   PostgreSQL/Redshift still fold configured names to lowercase and Snowflake
   to uppercase before quoting; other dialects preserve configured case.
3. **REST link-header pagination is rejected for windowed extraction.** A
   followed link can silently omit both bounds, so v1 accepts only no
   pagination, offset pagination, or cursor pagination for bounded REST reads.
4. **Pagination parameter names are internally distinct.** Offset
   `offset_param`/`limit_param` and cursor `cursor_param`/`limit_param` are
   validated for every use, not only backfill. Comparisons are case-sensitive,
   matching HTTP query parameter names.
5. **The derived-model normalizer no longer parses statement semicolons.** It
   removes trailing whitespace, trailing line comments and at most one final
   semicolon. Other semicolons remain inside the derived table and therefore
   fail closed as database syntax errors. Empty/comment-only models are
   rejected. SQL Server's common leading `;WITH` form receives the same CTE
   guidance error as leading `WITH`.
6. **Malformed Jinja fails closed.** When parsing fails, drt scans `{{ ... }}`
   and `{% ... %}` block bodies for the names `cursor_value` or `watermark` and
   rejects the model rather than wrapping unresolved legacy cursor logic.
7. **Window capabilities must be synchronous.** `is_window_capable` rejects
   coroutine functions and async generators in addition to validating the
   required synchronous signature.

## Decisions (owner-approved 2026-11-15)

The maintainer accepted the recommended answer to every question below. They are
normative; the implementation PRs follow them.

1. **Should numeric cursors be integral only in the first release?** Recommended:
   yes. `max + 1` gives an exact exclusive ceiling and matches the existing integer
   lag contract (`drt/config/sync_options.py:95-118`); decimal/float successors and
   precision need a separate design.
2. **Should the automatic timestamp ceiling use runner UTC or source-database
   time?** Recommended: runner UTC captured once. It is connector-neutral and adds
   no dialect query; document clock sync and use lag for late arrivals.
3. **Should range bounds be plaintext in authenticated plan metadata?**
   Recommended: yes. Apply needs reproducible bounds, timestamps/ids are operational
   metadata, and row values remain encrypted; add redaction only on demonstrated
   sensitivity.
4. **Should history contain one aggregate entry or one entry per window?**
   Recommended: one aggregate entry with a backfill summary; keep detailed windows
   in `run_results.json` and spans so history/freshness retain “one invocation, one
   run” meaning.
5. **Should `on_error: skip` checkpoint a fully consumed window containing DLQ
   rows?** Recommended: yes, preserving current semantics. Operators who require
   all-row success already have `on_error: fail`.
6. **Should exact replay advance a window checkpoint?** Recommended: yes, only for
   grouped payloads whose bounds are authenticated by the plan. Ordinary exact
   replay must continue preserving its cursor.
7. **Should CLI override flags be `--backfill-start/--backfill-end` and require one
   selected sync?** Recommended: yes. Explicit names avoid changing
   `--cursor-value`, and single-sync scope prevents accidental cross-type ranges.
8. **Should `sync.backfill` be a persistent policy or silently become legacy
   incremental after the first catch-up?** Recommended: persistent, with an
   exclusive-boundary marker and an explicit transition command. A silent change
   to legacy `cursor > watermark` can skip a row exactly equal to the saved high.

## Amendment 2026-11-18 (slice 3 review)

The maintainer delegated and approved the following adversarial-review
decisions for slice 3. They supersede earlier text where the staged rollout is
narrower than the final ADR design.

1. **Execution is caller-opt-in and fail-closed.** `run_sync()` defaults
   `allow_backfill_execution` to false. Only `drt run` and MCP
   `drt_run_sync` opt in. Library calls, `drt build`, `drt serve`,
   Airflow/Prefect, Dagster resources/assets/sensor-triggered runs, `drt test`,
   plan/apply, retry, and exact replay refuse before extraction.
2. **Dry-run is refused in slice 3.** CLI `--dry-run`, MCP `dry_run`, and
   `build --dry-run` never query windows. Window preview/diff support remains
   slice 4 work.
3. **Source range violations are fatal to the window.** Out-of-range,
   missing, and null cursors leave the window uncheckpointed regardless of
   `on_error`; `on_error: skip` applies only to destination row failures.
4. **One observer acknowledges checkpoints.** A composite designates exactly
   one validated synchronous checkpoint observer. Its return value is
   authoritative. Other and registered observers receive window events only;
   they never receive checkpoint calls, avoiding cross-thread checkpoint
   interleaving through shared extras.
5. **Resume reads and writes are coupled.** The checkpoint capability reads
   the resume cursor and boundary from the same StateStore/watermark pair it
   writes. Supplying `StatePersistingObserver(store, None)` is sufficient even
   when `state_manager=None`; a capability unable to read its own store is
   refused.
6. **Warehouse legacy fallback is narrow.** BigQuery falls back only for its
   400 `invalidQuery` undefined-name error, Snowflake only for ProgrammingError
   904/SQLSTATE 42000 invalid identifier, and Databricks only for SQLSTATE
   42703/`UNRESOLVED_COLUMN`, all specifically naming `cursor_boundary`.
   Authentication, permission, network, and transient failures propagate.
7. **Outcome cardinality is a checkpoint invariant.** A window fails if a
   load result does not report exactly one success/failed/skipped outcome per
   submitted row, or staged finalization does not account for every staged
   row. Under-reporting rows are never marked delivered in the idempotency
   ledger.
8. **DLQ persistence precedes checkpointing.** The DLQ buffer is flushed at
   every window boundary. An append failure is fatal and the checkpoint is
   not written; the unconditional end-of-run flush remains best-effort cleanup.
9. **Staged partial success preserves skip semantics.** When finalization
   reports pinpointed failures, `on_error: skip` records/DLQs them and may
   checkpoint a fully consumed window; `on_error: fail` leaves it resumable.
10. **Destination success means durable target acceptance.** For backfill,
    `SyncResult.success` cannot mean merely queued locally. The additive
    `PendingFlushCapable.flush_pending()` hook lets async destinations wait for
    acknowledgement before every checkpoint; failure aborts the window.
    `reset_write_state` remains best-effort cleanup, never acknowledgement.
11. **Interrupted unfinished work is partial.** Cooperative cancellation with
    remaining windows records `partial`, is selected by `drt run --failed`,
    and triggers failure alerts without moving the safe checkpoint.
12. **The cursor and exclusive marker are one checkpoint.** `drt state reset
    --runs` clears run metadata but retains `last_cursor_value` and
    `cursor_boundary` together. `--watermark` clears both. Hand-editing state
    files is unsupported; use the command.
13. **Local rewrites are atomic and corruption fails closed.** Local
    `state.json` and `watermarks.json` writes use a same-directory temporary
    file, file fsync, `os.replace`, and directory fsync. Missing files remain
    normal first-run state. Corrupt JSON cannot be interpreted as an empty
    checkpoint by a backfill.
14. **Same-sync concurrency is unsupported.** Never-regress remains a
    read/compare/write guard, not a distributed CAS. Operators and schedulers
    must provide single-flight execution for a backfill sync; `drt serve`
    already coalesces same-sync triggers. Do not run an older drt version
    against state written by backfill; older behavior is backend-dependent and
    may discard or reinterpret the additive marker. A local best-effort lease
    is deferred rather than pretending to solve remote/concurrent execution.
15. **Lag versus ledger retention is visible.** A timestamp
    `watermark.lag` greater than the enabled ledger's seven-day retention emits
    a run-start warning because pruned overlap keys may be delivered again.
16. **Documentation names the rollout boundary.** Connector, MCP, LLM,
    changelog, guide, and skill text must state the supported entry points,
    recovery steps, and unsupported dry-run/diff/plan/limit/build/serve/
    orchestrator combinations.
17. **Tests are entry-point and failure-injection based.** Coverage inventories
    every production `run_sync` call and exercises each invariant above,
    including the executable SQLite three-window resume path.

Slice 4 still owns `run_results.json` `windows[]`, window OTel spans, alert
wording, aggregate history backfill summaries, and CLI
`--backfill-start`/`--backfill-end`. This amendment does not pull them forward.

## Amendment 2026-11-19 (slice 3 round 3)

The maintainer delegated the following round-3 decisions after two further
adversarial reviews. They refine and supersede the slice-3 amendment where
necessary.

1. **Pending acknowledgement precedes delivery bookkeeping.** For a
   `PendingFlushCapable` destination, successful batch keys and compliance
   audit entries remain in memory for the current window. The engine calls
   `flush_pending()` first, and writes the idempotency ledger and compliance
   delivery log only after that acknowledgement succeeds. A failed flush
   therefore leaves no false delivered key; retry re-sends the row. A
   destination without this capability retains the established per-batch
   ledger/audit behavior.
2. **Destination support is an explicit, conservative allowlist.** The frozen
   `Destination` Protocol is unchanged. An additive
   `WindowedBackfillCapable.backfill_unsupported_reason(config)` declaration
   admits a destination configuration; absence means unsupported for both
   built-ins and plugins. `drt validate` and the engine before extraction both
   refuse with the destination type, reason, and support list. The v1 proven
   set is PostgreSQL and MySQL with a non-empty `upsert_key`, plus Snowflake,
   Databricks, and BigQuery in keyed `merge` mode. PostgreSQL/MySQL writes are
   synchronous keyed upserts; the three warehouse MERGE paths are synchronous
   and keyed. Their append/insert variants are not admitted.
3. **Every other built-in is excluded until proven.** ClickHouse remains an
   insert path: its `upsert_key` is informational and server-side insert
   deduplication depends on table settings drt neither verifies nor configures.
   Salesforce Bulk is excluded because its current failure correlation always
   uses `batch_index=0`. File, JSON/JSONL/CSV, Parquet, Google Sheets, object
   storage, and staged-upload destinations are excluded because overwrite,
   first-write, append, or staged-promotion behavior is unsafe to replay as a
   window transaction. Elasticsearch and all remaining API/message
   destinations are conservatively excluded because keyed synchronous replay
   has not been proven for every operation; blind POST/create and notification
   delivery can duplicate. The complete built-in matrix is maintained in
   `docs/guides/windowed-backfill.md`; a registry inventory test makes a new
   destination default to unsupported.
4. **MCP matches CLI safety composition.** `drt_run_sync` installs a
   `DlqObserver` when `sync.dlq.enabled` so `on_error: skip` cannot checkpoint
   past an unpersisted failed row. MCP returns structured `{"error": ...}`
   refusals for `dry_run`, `limit`, `cursor_value`, and `full_refresh` on a
   backfill before credentials, connectors, or state are instantiated.
5. **The downgrade marker is read through the owning capability.** When the
   engine has no direct `state_manager` but the designated checkpoint observer
   owns state, a non-backfill run reads `cursor_boundary` through
   `read_window_checkpoint()`. An `exclusive` marker still refuses legacy `>`
   execution before extraction.
6. **Backfill DLQ identity is deterministic and append is idempotent.** A
   backfill dead letter id hashes the sync name, canonical window bounds, and
   destination-facing record content. Local, object-store, and warehouse DLQ
   appends replace/upsert an existing id rather than append a duplicate. A
   checkpoint failure followed by a window retry therefore leaves one retryable
   entry. Non-backfill dead letters keep their legacy random UUID identity.
7. **A committed atomic replacement never reports failure afterward.** Local
   atomic JSON writes still fsync the file, replace it, and attempt the POSIX
   directory fsync. Directory open/fsync/close errors after `os.replace` are
   tolerated because Windows and some filesystems do not support that operation
   and the replacement has already committed.
8. **`state reset --runs` preserves only exclusive checkpoints.** The command
   retains `(last_cursor_value, cursor_boundary)` only when the boundary is
   exactly `exclusive`. A legacy state whose boundary is absent follows the
   pre-backfill behavior: its run entry and fallback watermark are removed.
9. **Only the DLQ can block a boundary flush.** Registered and other composite
   observers receive window lifecycle events but are never called through
   `flush_window`; a same-named method cannot veto checkpoint progress. The
   designated `DlqObserver` alone performs the required strict pre-checkpoint
   flush.
10. **Warehouse legacy-column fallback remains error-specific.** PostgreSQL
    falls back only for `psycopg2.errors.UndefinedColumn` (SQLSTATE 42703) whose
    diagnostic names `cursor_boundary`. BigQuery, Snowflake, and Databricks
    retain the slice-3 review's equivalent code/classification and marker-name
    checks. An undefined different column, permission error, auth failure, or
    transient failure propagates.
11. **`--cursor-value` is not the windowed-backfill interface.** README, CLI,
    LLM context, and generated skills call it a legacy/ad-hoc one-bound recovery
    override and state that a sync declaring `sync.backfill` refuses it.

## Amendment 2026-11-21 (slice 3 round 6)

**A strict backfill DLQ flush never evicts.** Before mutating any local,
object-store, or warehouse DLQ, drt compares the existing depth plus all
current-window failures with a finite `dlq.max_records`. An overflow fails the
window before its checkpoint and leaves the queue unchanged; the operator must
raise the cap or drain the queue with `drt retry`. The ordinary non-backfill
append contract remains FIFO-capped.

## Amendment 2026-11-21 (slice 3 round 7)

**Every destination-facing upsert-key component must be present and non-NULL.**
The shared backfill engine validates this after payload transforms and before a
window batch reaches any `WindowedBackfillCapable` destination. A violation
names the sync, key column, and half-open window, aborts the write, and leaves
the window uncheckpointed; this is required because SQL uniqueness and `MERGE`
equality generally treat NULL keys as distinct, making replay duplicate rows.

## Amendment 2026-11-22 (slice 3 round 8)

**Destination-facing upsert keys must also be unique within each window.** The
shared engine incrementally tracks keys across all streamed batches in the
current window and rejects the first repeat before its batch is written. The
error names the sync, key columns, half-open window, and the repeat's one-based
destination-row ordinal, but never includes raw key values. The window remains
uncheckpointed, so any earlier keyed batches are safe to replay.

Tracking retains only a 16-byte BLAKE2b digest per row, plus Python set/object
overhead; records and raw keys are not buffered. The canonical digest input is
type-tagged: integers, strings, and floats are deliberately distinct (`1`,
`"1"`, and `1.0` do not collide by coercion), composite components retain their
order, and common warehouse scalar types have stable encodings. A digest
collision fails closed as a duplicate. The set resets after every window.
Operators whose window cardinality makes this memory cost too high must dedupe
the model on `destination.upsert_key` or shrink `sync.backfill.window`.
