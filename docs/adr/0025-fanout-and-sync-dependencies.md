# ADR 0025 — Multi-destination fan-out and sync dependencies

- **Status:** Proposed.
- **Date:** 2026-11-22.
- **Issues:** [#425](https://github.com/drt-hub/drt/issues/425)
  (multi-destination fan-out) and
  [#426](https://github.com/drt-hub/drt/issues/426) (sync dependency graph).
- **Relates to:** [ADR 0004](0004-streaming-and-event-triggered-syncs.md)
  (orchestrator boundary and no drt-owned watcher),
  [ADR 0011](0011-subtraction-positioning-vs-reverse-etl.md) (CLI-first OSS
  scope), [ADR 0021](0021-windowed-backfill.md) (window checkpoints), and
  [ADR 0022](0022-sync-hooks.md) (sync and invocation lifecycle hooks).

## Verification note

`gh issue view 425 --repo drt-hub/drt --comments` and the equivalent command
for #426 were attempted while drafting this ADR, but the environment could not
connect to `api.github.com`. The checked-out roadmap identifies #425 as
“Multi-destination fan-out” and #426 as “sync dependency graph”
(`ROADMAP.md:310`), and earlier roadmap entries place both in the older
orchestration backlog (`ROADMAP.md:182`, `ROADMAP.md:208`, `ROADMAP.md:236`).

**Unverified offline — issue text/comments unavailable:** the issue bodies,
acceptance criteria, examples, and all comments on #425/#426 could not be read.
Their contents must be checked by the maintainer before this ADR is accepted.
This ADR therefore treats only the two issue titles and the scope explicitly
requested for this draft as inputs. It does not attribute any further
requirement or opinion to an issue author.

## Context

### One sync currently means one source read and one destination

`SyncConfig` has one required `model`, one singular `destination`, and one
`sync` policy; there is no destination list, group, or dependency field
(`drt/config/sync_options.py:1266-1312`). `run_sync()` likewise accepts one
`SyncConfig`, one `Source`, and one `Destination | StagedDestination`, and its
contract is explicitly “Run a single sync: extract from source, load to
destination” (`drt/engine/sync.py:310-350`). The batch loop calls that one
destination's `load()` once per batch, or its `stage()`/`finalize()` path for a
staged destination (`drt/engine/sync.py:1221-1248`,
`drt/engine/sync.py:1373-1413`).

The CLI loads sync files in sorted path order and appends each parsed config to
one list (`drt/config/parser.py:279-304`). Selection preserves that definition
order, unions repeated selectors, subtracts exclusions, and deduplicates
repeated selector hits by sync name (`drt/cli/_selection.py:1-21`,
`drt/cli/_selection.py:93-126`). With no `--select`, selection starts from every
loaded sync, then `--exclude` still subtracts matching syncs
(`drt/cli/_selection.py:121-126`). `--failed` then intersects the
selection with syncs whose stored status is not success
(`drt/cli/commands/run.py:998-1055`).

One source object is constructed for the invocation and shared through the run
context, while `_run_one()` constructs a separate destination for each sync
(`drt/cli/commands/run.py:1106-1177`, `drt/cli/commands/run.py:205-277`). Thus
two ordinary syncs may point at the same `model` and different destinations
today, but they execute two independent source iterations. They are not a
single source snapshot and no atomicity is promised between them.

Sequential execution iterates the selected list in definition order. With
`--threads N`, however, the CLI submits every selected sync to one
`ThreadPoolExecutor` immediately and consumes completions in completion order
(`drt/cli/commands/run.py:1179-1237`). `--fail-fast` can stop or cancel work not
yet running, but already-running work drains. Therefore sorted YAML order is a
current sequential implementation detail, not a dependency contract that also
holds under `--threads`.

### Recovery and observability are per sync

The current durability boundary is the sync name:

- `StateStore.get_last_sync()` and `reset()` take `sync_name`, and
  `save_sync()` replaces the entry keyed by `SyncState.sync_name`
  (`drt/state/manager.py:30-70`, `drt/state/manager.py:129-173`).
- Every `WatermarkStorage` operation takes `sync_name`; the local store is a
  mapping from that name to one cursor (`drt/state/watermark.py:35-67`,
  `drt/state/watermark.py:98-117`). Cursor resolution reads the external
  watermark or state fallback for `sync.name`
  (`drt/engine/sync.py:703-738`).
- The DLQ is one queue per sync, and `drt retry <sync>` replays that queue to
  the sync's one configured destination (`drt/state/dlq.py:1-17`,
  `drt/state/dlq.py:178-210`, `drt/cli/commands/retry.py:39-86`,
  `drt/cli/commands/retry.py:89-157`,
  `drt/cli/commands/retry.py:286-299`,
  `drt/cli/commands/retry.py:546-560`).
- `HistoryEntry` is “One execution of one sync,” carries one `sync_name`, and
  local history uses one JSONL file per sync (`drt/state/history.py:53-84`,
  `drt/state/history.py:129-169`).
- The optional #1099 idempotency ledger keys every operation by `sync_name`;
  the engine checks and marks keys around the one destination load
  (`drt/state/idempotency.py:116-159`, `drt/engine/sync.py:1200-1219`,
  `drt/engine/sync.py:1250-1279`).
- On a non-dry run, `run_sync()` attempts a failure alert only when it raised or
  accumulated failed rows; alert dispatch is best-effort. It attempts one
  history row only when a history manager was supplied, also best-effort.
  Degraded alerts are separately evaluated, best-effort and only for non-dry
  CLI `_run_one()` results with configured degraded conditions, using that
  sync's result and DLQ depth (`drt/engine/sync.py:506-587`,
  `drt/cli/commands/run.py:413-456`).

On the normal return path, `StatePersistingObserver` writes one status and one
cursor for the sync, and optionally one external watermark
(`drt/engine/observer.py:250-349`). If `on_error: skip` permits a result with
row failures to return, its status may be partial and its cursor follows the
existing single-destination policy. If `run_sync()` raises before
`on_sync_completed`, a non-dry run still attempts its configured best-effort
failure alert, optional history append, and observer/DLQ final flush, but the
completion observer does not advance state (`drt/engine/sync.py:448-587`,
`drt/engine/sync.py:650-658`, `drt/engine/sync.py:1466-1475`).

After project, profile, variables, and sync discovery succeed, one CLI `drt run`
invocation allocates one `run_id` and gives it to each sync it actually runs.
Its `finally` block makes one best-effort write of
`target/drt/run_results.json`; the artifact contains entries for attempted and
fail-fast-skipped syncs, and may contain an empty result list for a post-discovery
no-op or rejected invocation. Preflight failures before sync discovery do not
write the artifact, and library or MCP engine calls do not acquire this CLI
`run_id` or artifact (`drt/cli/commands/run.py:574-654`,
`drt/cli/commands/run.py:968-990`, `drt/cli/commands/run.py:1292-1307`). The
artifact deliberately has no per-sync watermark-before/after pair because the
post-run value is owned by the persistence observer, not returned to the CLI
(`drt/cli/commands/run.py:629-634`).

These are not incidental storage choices. They are the units operators can
inspect, reset, retry, alert on, and correlate. A proposal that puts several
destinations behind one sync name must either redefine every one of these
surfaces or accept ambiguous recovery.

### Entry points do not share a multi-sync scheduler

The CLI owns selection and the thread pool. The MCP `drt_run_sync` tool accepts
one `sync_name`, resolves one destination, and calls engine `run_sync()`
directly without the CLI scheduler, CLI observer stack, or invocation `run_id`;
it also does not produce the CLI's `run_results.json`
(`drt/mcp/tools/run_sync.py:19-38`,
`drt/mcp/tools/run_sync.py:65-131`, `drt/engine/sync.py:316-330`,
`drt/engine/sync.py:416-427`). ADR 0022 already identifies this split and
requires a shared invocation runner before run-level hooks can behave the same
through CLI, MCP, `serve`, Airflow, and Prefect. **Verified current-code fact:**
that shared runner and ADR 0022's hook config are not present in this checkout;
searching the runtime/config code finds no sync/run hook fields. ADR 0022 is an
accepted design and interaction constraint, not a feature this ADR may assume
has shipped.

### Product boundaries

ADR 0011 keeps the OSS core CLI-first and excludes a hosted runtime and web UI.
ADR 0004 rejects a drt-owned watcher and explicitly says that ordering between
syncs is an orchestrator's job; its Tier 2 path delegates durable cursors,
backfill, and task ordering to Dagster (`docs/adr/0004-streaming-and-event-triggered-syncs.md:162-191`,
`docs/adr/0004-streaming-and-event-triggered-syncs.md:234-240`). A dependency
graph that orders a bounded `drt run` invocation is smaller than a watcher, but
it is still an orchestration feature and must justify reversing that explicit
boundary rather than arriving as an assumed convenience.

## Goals and evaluation criteria

Any accepted design must answer all of the following without weakening current
single-sync behavior:

1. What is the durable progress unit when destinations disagree?
2. Which destination owns a DLQ row and a ledger key?
3. What does retry resend after partial success?
4. Which result, history, alert, plan, and status objects represent the work?
5. What does selection mean: selected nodes only, dependency closure, or an
   error for an incomplete graph?
6. What happens under `--threads`, `--fail-fast`, interruption, dry-run,
   plan/apply, MCP, and direct engine calls?
7. Can the feature remain a process that starts, performs bounded work, and
   exits, preserving ADRs 0004 and 0011?

Distributed transactions across unrelated destinations, a hosted scheduler,
and a web DAG editor are not goals.

## Design space: multi-destination fan-out (#425)

### Option F1 — N ordinary syncs share the same source model

Authors define one independently named sync per destination, repeat the same
`model`, and optionally give them a common tag:

```yaml
# syncs/customers_to_hubspot.yml
name: customers_to_hubspot
tags: [customers_fanout]
model: ref('customers')
destination:
  type: hubspot
  object_type: contacts
sync:
  mode: upsert
```

```yaml
# syncs/customers_to_salesforce.yml
name: customers_to_salesforce
tags: [customers_fanout]
model: ref('customers')
destination:
  type: salesforce_bulk
  instance_url_env: SF_INSTANCE_URL
  object_name: Contact
  operation: upsert
  external_id_field: External_Id__c
  client_id_env: SF_CLIENT_ID
  client_secret_env: SF_CLIENT_SECRET
  username_env: SF_USERNAME
  password_env: SF_PASSWORD
sync:
  mode: upsert
```

`drt run --select tag:customers_fanout` selects both; `--threads 2` may run
them concurrently. Each destination keeps its own mappings, mode, guard,
watermark, state, history, DLQ, retry command, ledger namespace, alert policy,
and result entry.

This option is already expressible and needs no config or engine change. Its
cost is intentional duplication: the source query runs N times, the reads may
observe different snapshots, and a shared source may receive N concurrent
queries with `--threads`. A materialized dbt/warehouse model can make those
reads cheaper and snapshot-stable at the data layer, but drt does not cache or
broadcast the extracted rows.

### Option F2 — `destinations:` on one sync

One YAML object could replace singular `destination` with a list and extract
each batch once before sending it to every destination. This appears concise
but creates a new composite execution unit:

- one slow destination backpressures every other destination unless batches
  are buffered independently;
- staged and ordinary destinations finish at different times and have
  incompatible finalization/rollback boundaries;
- mappings, lookups, match/write policies, guards, contracts, rate limits, and
  destination-specific config may differ, so the supposedly shared record is
  not necessarily the same destination-facing row;
- one shared ledger namespace would let a key confirmed at destination A look
  delivered to destination B unless the storage key changes from
  `(sync_name, key)` to include a destination identity;
- one sync DLQ cannot tell `drt retry` which destination to reconstruct; and
- one history/status/alert/result object cannot say both “A succeeded and may
  advance” and “B failed and must replay” without a new nested schema.

Most importantly, one watermark has no safe meaning. Advancing it when any
destination succeeds loses rows for failed destinations. Advancing only when
all succeed preserves failed destinations but replays already committed writes
to successful ones. Keeping a watermark per destination is correct, but then
the implementation has recreated N independent syncs under one display name
and still needs either N source replays or a durable row cache to resume only
the failed leg.

### Option F3 — a thin group that references ordinary syncs

A project-level construct could name existing syncs without changing their
execution units:

```yaml
groups:
  customers_fanout:
    - customers_to_hubspot
    - customers_to_salesforce
```

Running a group would select those syncs and preserve all per-sync recovery
semantics. It would not share extraction or provide cross-destination
atomicity. It is therefore a selection alias, not engine fan-out.

The current tag selector already supplies the important behavior, including
union with other selectors and compatibility with `--threads`
(`drt/cli/_selection.py:48-78`, `drt/cli/_selection.py:113-126`). A second
group namespace adds project schema, validation, completion, docs, and MCP
surface without establishing a new capability.

### Fan-out recommendation

Recommend **F1 and decline first-class engine fan-out for #425**. Document N
ordinary syncs sharing a model and tag as the supported composition. Do not add
`destinations:` or a group construct in v1.4. The repeated query is a visible,
bounded cost; the alternatives hide N recovery units behind one name or require
a durable broadcast/cache subsystem far beyond the issue's apparent scope.

This is a recommendation to close or re-scope #425, not a claim that the live
issue author agreed: the issue discussion is unverified in this draft.

## Design space: sync dependencies (#426)

### Option D1 — explicit `depends_on` in each sync YAML

A sync could list upstream sync names:

```yaml
name: activate_accounts
depends_on: [activate_users]
model: ref('accounts')
destination:
  type: hubspot
  object_type: companies
```

The parser would need project-wide validation after all files load: unique
names, references that exist, no self-edge, and no cycle. The runner would need
a topological scheduler rather than submitting every selected sync at once.
With `--threads N`, only ready nodes could enter the pool; a failed parent would
block its descendants while independent branches continued.

The shape is reviewable and keeps ownership beside the dependent sync. It also
changes selection semantics. Silently adding ancestors to `--select child`
would perform writes the operator did not explicitly select; running the child
without its parent violates the declaration; and `--exclude parent` conflicts
with either choice. A bounded first version would need to reject an incomplete
selected graph and require the operator to select all required nodes explicitly.
Graph-expansion selector syntax could be a later, separately reviewed feature.

### Option D2 — a project-level DAG file

A central file could hold only edges:

```yaml
dependencies:
  activate_accounts: [activate_users]
```

This makes the whole graph easy to scan and keeps orchestration policy separate
from destination config. It also creates two files that must agree on names,
lets a sync's execution contract change without touching its own reviewed YAML,
and introduces a new project-level merge hotspot. It does not remove any
scheduler, selection, failure, parity, or artifact work required by D1.

### Option D3 — rely on external orchestrators

Dagster, Airflow, Prefect, warehouse-native jobs, and CI workflows already own
task graphs, retries, schedules, concurrency, and durable orchestration state.
Each node invokes one ordinary drt sync. drt remains responsible for the
source-to-destination transaction and its per-sync recovery surfaces; the
orchestrator decides whether a failed parent blocks, retries, or permits a
child.

This is the boundary ADR 0004 already chose. It does mean a plain multi-sync
`drt run` has no dependency guarantee, and teams needing a graph must operate
an orchestrator or write an explicit shell/CI sequence. That limitation is
honest and substantially smaller than maintaining a second graph scheduler
inside the CLI.

### Dependency recommendation

Recommend **D3 and decline #426 in drt-core**, delegating dependency graphs to
orchestrators. Do not infer dependencies from filename order, model lineage,
destination lookups, or shared tags. Continue to describe sequential definition
order only as current behavior when `--threads 1`, not as an execution contract.

If the maintainer chooses to reverse ADR 0004's boundary, D1 is the least broad
fallback: an additive `depends_on: list[str]` on `SyncConfig`, enforced only
within one bounded invocation. D2 is not recommended. That fallback must still
make the failure, selection, artifact, and parity decisions below before code
starts.

## Failure and progress semantics

### Recommended composition

Under F1, there is deliberately no group watermark. If destination A's sync
succeeds and destination B's sync fails fatally, A's existing completion path
may persist A's state/watermark and B does not advance through a completion
event. History (when enabled) and `run_results.json` record B's raised failure,
while `drt retry B` can replay only B's persisted DLQ rows. **Current-code caveat:**
`--failed` reads `StateStore`, and the raised path does not call the state
completion observer, so it is not a guaranteed selector for B after an
exception (`drt/cli/commands/run.py:1022-1032`,
`drt/engine/sync.py:448-505`). An ordinary later `drt run` without `--select`
starts from all loaded syncs and therefore reruns A unless another filter or
`--exclude` removes it. Only `drt retry B` and `drt run --select B` are B-only;
A's already-persisted success is not rolled back. If a destination returns a
partial result under the existing `on_error: skip` contract, that sync retains
today's cursor and DLQ behavior; fan-out composition adds no stronger all-record
guarantee.

This is partial success, not atomic fan-out. No design in scope can atomically
commit a CRM API, a queue, and a SQL database. Operators who require downstream
idempotence must use destination-native upsert/idempotency behavior and the
existing per-sync ledger where available.

### Why a shared fan-out watermark is rejected

For F2, “advance only when every destination succeeds” is conservative for the
failed leg but turns the next run into at-least-once replay for every successful
leg. “Advance when any succeeds” is data loss. A two-phase commit is unavailable
for most destinations, and persisting extracted payloads until all legs
acknowledge them would make drt own a durable queue. Therefore any future
single-extraction design must expose per-destination checkpoints and replay
identities; at that point it should be proposed in a new ADR with a storage and
retention design, not added as a `destinations:` shorthand.

### If core dependencies are accepted despite the recommendation

The minimum coherent semantics would be:

- a node becomes ready only after every selected dependency returns success;
  partial and failed both block descendants;
- a blocked node performs no source/destination I/O, writes no state,
  watermark, history, DLQ, ledger, audit trail, or alerts, and appears in
  `run_results.json` as `status: skipped`, `reason: dependency_failed`, plus
  the blocking sync names;
- the invocation exits nonzero because an actual ancestor failed, not because
  descendants were skipped;
- independent ready branches may run concurrently up to `--threads`; no two
  nodes are serialized merely because they share a source or destination;
- `--fail-fast` stops scheduling all new nodes after the first failure, while
  ordinary dependency blocking affects descendants only; and
- a signal/interruption prevents new nodes from becoming ready and preserves
  the current rule that in-flight batches finish cooperatively.

These semantics are proposed contingencies, not current behavior.

## Interaction with accepted ADRs

### ADR 0021 — windowed backfill

**Accepted future design, not current behavior:** ADR 0021 is accepted, but
backfill execution is still disabled by `BACKFILL_EXECUTION_ENABLED = False`;
the current engine rejects configured backfills before extraction
(`drt/config/sync_options.py:84-91`, `drt/config/sync_options.py:884-887`,
`drt/engine/sync.py:416-418`). Under that future design, F1 gives each
destination sync its own backfill checkpoint and independent window processing.
Two fan-out legs may be at different windows and may capture different run
ceilings. That is the expected consequence of independent recovery. A failed
leg would resume from its own checkpoint; a successful leg would not be rolled
back.

F2 would force the same unsafe watermark choice at every window: replay
successful destinations when one leg fails, or maintain per-destination window
checkpoints and a durable payload. That reinforces the F1 recommendation.

If D1 ever ships, a dependency is satisfied only after the parent's entire
backfill invocation completes. Pipelining a child after each parent window is
out of scope because it couples two checkpoint state machines and changes ADR
0021's “one window is the checkpoint transaction” boundary. A fixed-end parent
that has already reached its end still runs to a successful no-op before the
child becomes ready unless a future cached-success policy is designed.

### ADR 0022 — hooks

ADR 0022 defines `pre_extract` and `post_load` once per sync, and run hooks once
per invocation around all submitted syncs. Under F1, each destination sync gets
its own sync hooks; the eventual shared invocation runner would execute run
hooks once around the selected set. There is no group hook.

F2 would need a separate `post_load` outcome per destination. A successful
destination hook followed by another destination's failure again leaves the
shared watermark undefined. One aggregate post hook cannot accurately represent
several independently committed destinations.

If D1 ever ships, a parent's `post_load`, completion observer, history/alert
cleanup, and `on_sync_ended` must finish before the node is successful and a
child becomes ready. `on_run_start` remains before graph dispatch and
`on_run_end` after every started or blocked node settles. As ADR 0022 already
requires, CLI, MCP, `serve`, Airflow, and Prefect must use one shared invocation
runner before those semantics can be claimed.

## Interaction with `--threads`

F1 uses current concurrency unchanged. `--threads N` can execute N source
queries at once, so operators must size warehouse concurrency and destination
rate limits accordingly. The process-wide rate-limit registry may coordinate
destinations sharing a real quota key, but it does not make the source reads a
single query. Running with one thread avoids overlap but still performs one
query per sync.

F2 would add concurrency inside one sync as well as across syncs. It would need
bounded per-destination queues, cancellation, memory limits, and clarity on
whether `--threads` counts sync workers or destination workers. None exists
today; this is another reason not to treat a destination list as a config-only
change.

D1 would require replacing eager submission with a ready-queue scheduler.
`--threads` would cap running nodes, not graph width. That is implementable, but
it is real orchestration code and should not be hidden inside a parser PR.

## Compatibility and migration

The recommended F1/D3 outcome changes no YAML, Protocol, state layout, CLI,
MCP, artifact, or JSON Schema. Existing projects continue unchanged. A guide or
issue resolution may show the shared-model/shared-tag pattern and point graph
users to orchestration integrations.

If either rejected feature is later accepted:

- `destinations:` cannot silently replace `destination:`. Supporting both
  requires an explicit exclusivity validator, generated `sync.schema.json`
  updates, editor examples, config fingerprint coverage, and a migration story
  for every command that currently accesses `sync.destination` directly.
- `depends_on` is additive at the single-file model level, but graph validity
  is project-wide and must be checked by both strict and safe loaders. JSON
  Schema can type the list but cannot prove referenced names or acyclicity
  (`drt/config/schema.py:58-74`, `drt/config/parser.py:279-359`).
- Selection, `--failed`, `--exclude`, `--fail-fast`, state selectors, shell
  completion, `run`, `build`, `test`, `plan`, `apply`, `docs`, `serve`, and
  deploy scaffolds must state whether they operate on the selected node only or
  a graph closure. No command may silently choose a different rule.
- MCP/CLI parity is mandatory. The current MCP tool calls the engine directly
  for exactly one sync, without the CLI scheduler, CLI observer stack, or
  `run_id`. A core dependency feature cannot be advertised
  through CLI while MCP ignores it; both must call the shared invocation runner
  anticipated by ADR 0022. Direct `engine.run_sync()` should remain the
  single-sync primitive and should not recursively schedule dependencies.
- `run_results.json` changes should be additive and omit absent graph/fan-out
  fields. A dependency-blocked node needs a stable reason and blocker list.
  A future composite fan-out result needs one child result per destination and
  a schema-version decision; flattening it into today's one status/count is not
  sufficient.
- State/watermark/DLQ/history/ledger keys must not be migrated implicitly. Any
  move from existing N sync names into a composite name risks orphaning or,
  worse, combining recovery state. A migration command would need explicit
  source and destination identities and must refuse ambiguous merges.

No frozen `Source`, `Destination`, `StateStore`, `WatermarkStorage`,
`HistoryStore`, or `DlqBackend` signature needs to change for F1/D3. A future
F2 implementation must prefer additive capabilities or new storage Protocols;
it must not widen frozen interfaces casually.

## Test strategy

No production tests are required to accept the recommended no-code direction;
the only deliverable is documentation. If examples are added later, parser and
CLI documentation tests should ensure they remain valid.

If F2 is reconsidered, tests must precede implementation claims and include:

- a two-destination fault matrix: first/second destination fails before send,
  reports row failures, commits then loses the response, fails finalization,
  and fails a post-load hook;
- assertions for state, external watermark, DLQ ownership, retry target,
  ledger key namespace, history, alerts, OTel correlation, and
  `run_results.json` after every fault;
- ordinary plus staged destinations in both orders, slow-consumer backpressure,
  bounded memory, interruption, `--limit`, dry-run/diff, plan/apply, exact
  replay, guards, and windowed backfill; and
- proof that retrying one failed leg neither skips it because another leg's
  ledger entry exists nor resends successful legs without an explicit
  at-least-once contract.

If D1 is accepted, tests must cover:

- missing names, duplicate sync names, self-edges, simple and multi-node cycles,
  deterministic topological order, and stable diagnostics;
- incomplete selection, explicit exclusion of a dependency, `--failed`, state
  selectors, dry-run, plan/apply, `build`, and MCP parity;
- linear, diamond, and disconnected graphs at `--threads 1` and greater than
  one, proving children never start early and independent branches do;
- parent success/partial/failure/exception/interruption, descendant blocked
  artifacts, no blocked-node durability side effects, and `--fail-fast`; and
- ADR 0021 whole-backfill gating plus ADR 0022 sync/run hook order.

At least one integration test should use two real local SQL syncs so a mocked
future cannot accidentally “complete” before the parent's destination commit.
No test should claim cross-destination atomicity.

## Consequences of the recommendation

Positive consequences:

- progress, retry, history, alerts, and artifacts retain one clear recovery
  unit;
- no new scheduler or durable row-broadcast subsystem enters the core;
- CLI, MCP, and orchestrator entry points keep their current single-sync
  primitive; and
- the result is consistent with ADR 0011's small OSS core and ADR 0004's
  orchestrator boundary.

Negative consequences:

- fan-out repeats source extraction and can observe different snapshots;
- YAML repeats the model and any shared sync policy;
- plain `drt run` cannot express or enforce a dependency graph; and
- users needing ordered multi-step activation must use an orchestrator or an
  explicit serial CI/shell sequence.

Those limitations are visible. The rejected alternatives make the YAML shorter
but move ambiguity into recovery, where it is more expensive and dangerous.

## Decisions needed from the maintainer

The ADR remains Proposed until the maintainer answers these questions after
checking the live issue threads.

1. **Should #425 add a first-class destination list?** Recommendation: **no**.
   Close or re-scope it to documentation of N independently named syncs sharing
   one model and tag. Preserve one state/watermark/DLQ/history/ledger identity
   per destination.
2. **Is avoiding repeated source queries a v1.4 requirement?** Recommendation:
   **no**. If measured warehouse cost or snapshot inconsistency makes it a real
   requirement, open a separate ADR for a durable broadcast/cache design with
   per-destination acknowledgements; do not hide it in `destinations:`.
3. **Should drt add a thin fan-out group construct?** Recommendation: **no**.
   Tags plus `--select tag:<name>` already provide the bounded selection alias.
4. **Should #426 add core dependency scheduling?** Recommendation: **no**.
   Decline it in drt-core and delegate graphs to Dagster/Airflow/Prefect,
   warehouse-native jobs, or CI, reaffirming ADR 0004.
5. **If #426 is accepted despite that boundary, where should edges live?**
   Recommendation: use per-sync `depends_on: list[str]` (D1), never a separate
   project DAG file; limit it to one bounded invocation and require a shared
   CLI/MCP invocation runner.
6. **If core dependencies are accepted, should selecting a child silently run
   unselected parents?** Recommendation: **no** for the first version. Reject an
   incomplete selected graph with a precise error; an explicit graph-expansion
   selector can be designed later without surprising writes.
7. **If a dependency fails, what is the child outcome?** Recommendation: record
   the child only in the invocation artifact as skipped with
   `reason: dependency_failed`; do not write child state/history/DLQ/alerts, and
   let the ancestor failure make the invocation nonzero.
8. **What is the watermark when one fan-out destination fails?**
   Recommendation: there is no shared watermark. Each ordinary sync advances or
   retains only its own progress under existing semantics. Reject both
   “advance on any” (data loss) and “advance on all” (implicit replay of
   successes) as a composite default.
9. **Do the unavailable issue bodies/comments contain requirements that change
   these recommendations?** **Unverified offline.** The maintainer must check
   the live #425/#426 thread contents before acceptance.
