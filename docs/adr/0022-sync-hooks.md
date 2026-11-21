# ADR 0022 — Pre/post-sync hooks and run-level hooks

- **Status:** Accepted 2026-11-20 (owner approved every recommendation in the final section).
- **Issue:** [#764](https://github.com/drt-hub/drt/issues/764).
- **Relates to:** [ADR 0007](0007-protocol-stability-policy.md) (Protocol stability),
  [ADR 0013](0013-plan-apply-lineage.md) (plan/apply lineage),
  [ADR 0014](0014-exact-replay-apply.md) (exact replay), and
  [ADR 0021](0021-windowed-backfill.md) (the current extraction/finalization lifecycle).

## Context

### Current sync lifecycle

`run_sync()` represents one execution of one sync. It allocates `sync_run_id`,
stamps the caller's invocation-level `run_id` on the mutable aggregate
`SyncResult`, and calls `on_sync_started` before entering `_run_sync_body()`
(`drt/engine/sync.py:416-448`). The body first performs pure destination
capability checks, then resolves incremental state and the effective watermark
(`drt/engine/sync.py:691-790`). It resolves model SQL and attaches query tags
before any source operation (`drt/engine/sync.py:792-823`). A `pre_extract` hook
therefore has one unambiguous seam: after ids, capability checks, watermark,
template context and query tags exist, but before either snapshot-diff setup or
ordinary source iteration begins (`drt/engine/sync.py:825-937`).

Extraction is not a phase that completes before loading. The source iterator is
consumed in batches and each batch is transformed and sent before the next one
is read (`drt/engine/sync.py:1001-1023`, `drt/engine/sync.py:1062-1189`,
`drt/engine/sync.py:1221-1248`). Successful per-row idempotency and compliance
writes happen immediately after each successful destination batch, and DLQ
records are handed to the observer in that same loop
(`drt/engine/sync.py:1250-1315`). A hook between “extract” and “load” would
therefore not exist without first buffering the whole source, which this ADR
does not do.

After the loop, staged destinations finalize first. Non-staged destinations may
then run the duck-typed `finalize_sync()` used by swap, mirror and other
end-of-sync work (`drt/engine/sync.py:1373-1413`). Runtime delete guards execute
inside those destination finalizers; the engine carries any forced outcome
after they return (`drt/engine/sync.py:1400-1419`). Only then does a diff source
promote its snapshot baseline, and only a normal return reaches
`on_sync_completed`, which is where state and watermark persistence is delegated
(`drt/engine/sync.py:1423-1443`, `drt/engine/sync.py:1458-1475`). A `post_load`
hook belongs after every staged/non-staged finalizer and guard has succeeded but
before snapshot promotion and `on_sync_completed`. That ordering lets a hook
failure remain retryable without claiming that the source checkpoint or diff
baseline completed.

The outer `finally` is also part of the contract. In a non-dry run it sends failure
alerts and appends one history entry (best-effort, and only when a history manager
is configured) even when the body raises, then prunes ledgers, resets destination
write state (only when `not dry_run` or the destination is
`ManagedDestinationTableCapable`) and calls `on_sync_ended` (non-dry runs only)
(`drt/engine/sync.py:506-587`, `drt/engine/sync.py:589-658`). The last event is
the DLQ flush point on exceptional exits (`drt/engine/observer.py:119-130`,
`drt/engine/observer.py:444-456`). Hook errors must flow through this path; they
must not add a second persistence or cleanup path around it.

Today a raised exception always produces history status `failed`, while a
returned result with both successes and failures is `partial`
(`drt/engine/sync.py:541-583`). The CLI similarly loses partial counts when
`run_sync()` raises: its exception entry hard-codes zero synced/failed rows,
whereas a returned result carries the actual counts and `sync_run_id`
(`drt/cli/commands/run.py:278-310`, `drt/cli/commands/run.py:337-353`). That is
not accurate for a `post_load` failure: destination batches and even a
swap/mirror finalizer may already have committed. Hook outcomes therefore need
structured exception metadata rather than being flattened into today's generic
exception branch.

### Current dry-run, plan/apply and concurrency behavior

A dry run still extracts, transforms and can compute a destination diff, but it
does not call destination `load()`, `stage()` or either finalizer
(`drt/engine/sync.py:1185-1189`, `drt/engine/sync.py:1373-1408`). Persisting
observers explicitly ignore dry-run results (`drt/engine/observer.py:250-263`).
`drt plan` is this same `run_sync(..., dry_run=True, compute_diff=True)` path,
with planning flags and complete diff capture (`drt/cli/_plan_runner.py:186-240`).
Running arbitrary SQL hooks during either path would make the documented preview
path mutating even though destination writes and state persistence are disabled.

Normal `drt apply` first recomputes the plan read-only and compares config,
environment, cursor, DDL and diff fingerprints before approval and an atomic
single-use claim (`drt/cli/_apply_flow.py:455-548`,
`drt/cli/_apply_flow.py:605-628`). It then uses the ordinary `_run_one()` write
path. Exact replay substitutes an in-memory source containing already
destination-facing rows and deliberately disables source cursor semantics
(`drt/cli/_apply_flow.py:630-677`, `drt/engine/sync.py:703-709`). Source-side
hooks cannot be silently run through that fake source without violating exact
replay's “no source read” boundary.

`drt run --threads N` parallelizes syncs, not batches within one sync. Every
future shares one invocation `run_id`; the main thread waits for every running
future, including the in-flight remainder after `--fail-fast`, before producing
the summary (`drt/cli/commands/run.py:1153-1189`,
`drt/cli/commands/run.py:1190-1237`). That gives project-level hooks a natural
once-per-invocation boundary outside the pool.

That boundary is not currently shared by every entry point. MCP constructs the
connectors and calls engine `run_sync()` directly (`drt/mcp/tools/run_sync.py:65-128`).
Airflow and Prefect use `integrations._runner.run_drt_sync()`, which also calls
the engine directly, and `drt serve` invokes that runner in a background thread
(`drt/integrations/_runner.py:77-124`, `drt/cli/server.py:493-521`,
`drt/cli/server.py:738-746`). A shell wrapper around `drt run` therefore cannot
provide consistent lifecycle hooks for MCP, serve or orchestrator calls. The
invocation boundary must be a shared Python service, not Typer-only code.

### Connector capabilities and connection ownership

The frozen `Source` Protocol exposes extraction and a connectivity check, not
arbitrary statement execution (`drt/sources/base.py:22-71`). The registry's
built-in SQL-shaped sources are BigQuery, ClickHouse, Databricks, DuckDB, MySQL,
PostgreSQL, Redshift, Snowflake, SQLite and SQL Server; REST API is explicitly a
different source shape (`drt/connectors/registry.py:306-319`,
`drt/connectors/registry.py:365-377`). Delta Lake and Iceberg accept SQL only
after constructing an ephemeral DuckDB connection and registering an Arrow
object inside that connection (`drt/sources/deltalake.py:78-96`,
`drt/sources/iceberg.py:76-89`). A separately opened pre-hook session would not
see that registration, so those two are not hook-capable in the first release.

SQL sources own their connections inside `extract()`. For example, PostgreSQL
opens a connection and server-side cursor in the retried setup, keeps them alive
while rows stream, and closes the connection when iteration ends
(`drt/sources/postgres.py:209-245`). DuckDB similarly opens and closes an
extract-local connection (`drt/sources/duckdb.py:42-63`). BigQuery uses a client
and job rather than a reusable DB-API session (`drt/sources/bigquery.py:110-148`).
Retrofitting hooks into `Source.extract()` would change a frozen signature and
would couple the engine to connector-specific session ownership.

On the destination side, `QueryableDestination` is a frozen, optional
capability specifically for a scalar validation query, not general hook SQL
(`drt/destinations/base.py:372-414`). The built-in implementations are
PostgreSQL, MySQL, ClickHouse, Snowflake and Databricks; each opens a fresh
connection, executes a query, reads one integer and closes it
(`drt/destinations/postgres.py:258-277`, `drt/destinations/mysql.py:265-285`,
`drt/destinations/clickhouse.py:1109-1127`,
`drt/destinations/snowflake.py:778-802`,
`drt/destinations/databricks.py:1492-1507`). BigQuery is SQL-backed and
`ConnectionTestable`, but its test only calls `get_table()` and it does not
implement `QueryableDestination` (`drt/destinations/bigquery.py:1015-1026`).
`ConnectionTestable` merely promises that connectivity validation raises on
failure (`drt/destinations/base.py:181-190`); it is not evidence that arbitrary
SQL can run.

Query tagging is already constructed once in the engine and carried both as a
universal SQL comment and as native connector metadata where supported
(`drt/engine/sync.py:803-823`). Source retry deliberately covers connection and
initial execution only; it does not retry after rows have been yielded because
that could duplicate delivered data (`drt/sources/postgres.py:167-178`,
`drt/sources/postgres.py:228-245`). Hooks need an equally explicit, but stricter,
retry contract because arbitrary DDL/DML cannot be assumed idempotent.

### Configuration, templating and artifact safety today

`SyncOptions` owns operational behavior such as mode, watermark, batch size,
retry and `on_error`, making `sync.hooks` its natural home
(`drt/config/sync_options.py:548-590`). `ProjectConfig` currently owns the one
project profile, state/history, project `vars` and query tagging; it has no
invocation lifecycle fields (`drt/config/base.py:459-471`). Project vars resolve
in the order project file, `DRT_VAR_*`, invocation override, and `var()` is a
Jinja global (`drt/config/vars.py:12-21`, `drt/config/vars.py:95-103`,
`drt/config/vars.py:139-177`). Sync YAML strings other than `model` are rendered
during config loading, while the resolver owns the model's combined
`cursor_value`/`watermark`/`var()` context (`drt/config/parser.py:244-258`,
`drt/engine/resolver.py:140-179`, `drt/engine/resolver.py:237-242`). Hook SQL
needs a dedicated rendering path; treating it as an ordinary YAML string would
render it before `run_id`, `sync_run_id`, status and row counts exist.

Artifact safety is structural in some places and heuristic in others. The docs
manifest intentionally omits raw model SQL (`drt/docs/builder.py:404-413`), and
`run_results.json` drops raw exception text rather than pretending free-text
redaction is complete (`drt/cli/commands/run.py:533-571`). The docs UI also reads
raw sync YAML for a display tab; its current line redactor deliberately leaves
block scalars unchanged, which is exactly the likely representation of hook SQL
(`drt/docs/builder.py:630-687`, `drt/docs/builder.py:690-750`). History stores
exception strings and the docs manifest later applies best-effort redaction
(`drt/state/history.py:37-80`, `drt/docs/builder.py:281-318`). Hook SQL and a
driver error that echoes it must therefore be excluded by construction, not
passed through these heuristics.

## Decision

### 1. Add explicit, ordered SQL hook configuration

Per-sync hooks are nested under `sync.hooks`:

```yaml
sync:
  mode: incremental
  cursor_field: updated_at
  hooks:
    pre_extract:
      - sql: |
          CALL refresh_activation_stage('{{ sync_name }}', '{{ run_id }}')
        timeout: 5m
    post_load:
      - sql: |
          CALL refresh_serving_view('{{ sync_name }}', {{ rows_synced }})
        timeout: 2m
```

Project-level hooks are top-level fields in `drt_project.yml` and execute
against the project's source profile:

```yaml
on_run_start:
  - sql: "CALL begin_activation_run('{{ run_id }}')"
    timeout: 2m
on_run_end:
  - sql: "CALL finish_activation_run('{{ run_id }}', '{{ status }}')"
    timeout: 2m
```

Each phase is an ordered, non-empty list of strict `HookStatement` objects with
required non-blank `sql` and optional positive `timeout` (default `5m`, maximum
`1h`, parsed by the existing duration vocabulary). There is no scalar shorthand
in v1: the object form leaves room for timeout and later additive policy without
creating two equivalent schemas. Empty phases are rejected rather than treated
as absent.

`pre_extract` executes on the configured source. `post_load` executes on the
configured destination. `on_run_start` and `on_run_end` execute on the one
project source profile, because a selected invocation may contain many unrelated
destinations but has one authoritative source profile
(`drt/config/base.py:459-471`, `drt/cli/commands/run.py:924-945`). Run-level
destination hooks are out of scope; use a sync-level `post_load` for a
destination-specific action.

The first release supports source hooks for BigQuery, ClickHouse, Databricks,
DuckDB, MySQL, PostgreSQL, Redshift, Snowflake, SQLite and SQL Server. It rejects
REST API, Delta Lake and Iceberg source/run hooks. Destination hooks require a
`QueryableDestination` and a new hook executor capability; the built-in first
release is therefore PostgreSQL, MySQL, ClickHouse, Snowflake and Databricks.
BigQuery and every SaaS/file/queue destination are rejected. Validation names
the phase and unsupported connector before an invocation starts; direct library
calls repeat the capability check defensively.

### 2. Add capabilities; do not widen frozen Protocols

Introduce two new `@runtime_checkable` capabilities:

- `SqlHookSource.execute_hook(statements, profile, context, query_tags)`;
- `SqlHookDestination.execute_hook(statements, destination_config, context,
  query_tags)`.

Their concrete result is a list of statement durations/outcomes; it never
contains SQL text or returned rows. Implementations own connection setup,
transaction handling, timeout configuration, query tagging, cancellation and
cleanup. The engine checks the source capability for `pre_extract`; it checks
both `QueryableDestination` and the destination hook capability for
`post_load`. A plugin may opt in without a core type allowlist.

This follows ADR 0007: adding any method or changing any signature on the
already-frozen `Source`, `Destination`, `QueryableDestination`,
`ConnectionTestable` or `SyncObserver` Protocol is breaking, while a separate
optional capability is the sanctioned extension path
(`docs/adr/0007-protocol-stability-policy.md:34-76`,
`docs/adr/0007-protocol-stability-policy.md:79-97`). In particular,
`execute_test_query()` is not reused: it requires a scalar integer result and
documents a validation-query scope, neither of which is true for arbitrary
DDL/DML hooks (`drt/destinations/base.py:388-414`).

Each hook phase opens one fresh, dedicated source or destination connection,
runs its statements in order, and closes it before the lifecycle continues. It
uses the same resolved credentials, session defaults and native query tags as
the connector's ordinary path, but it is not the streaming extract connection
or any per-batch load connection. This is explicit: temporary tables and session
settings created by a hook are not promised to survive into extraction/loading.
Hooks must create durable database objects or call durable procedures when the
next phase needs their effects.

Where the backend supports transactions, the capability executes one phase in
one transaction and commits only after every statement succeeds; it rolls back
on failure. Warehouses that auto-commit DDL or have job-based execution document
that limitation. drt cannot promise cross-connection or cross-system atomicity
between a hook and the sync.

### 3. Fix the execution order

For a real invocation, the order is:

```text
validate + select syncs; allocate run_id
on_run_start (source, once)
  for each sync (parallel across syncs when --threads > 1):
    allocate sync_run_id; observer.on_sync_started
    resolve watermark/model/query tags
    pre_extract (source)
    stream extraction and destination batches
    staged finalize OR destination.finalize_sync (including runtime guards)
    post_load (destination)
    snapshot-diff baseline promotion
    observer.on_sync_completed (state/watermark persistence)
    outer history/alerts/reset/on_sync_ended
wait for every started sync
on_run_end (source, once, including failed/partial/interrupted runs)
write invocation result / return
```

`pre_extract` runs once per sync, including an empty eventual extract. A failure
aborts before the source iterator is created; no batch, finalizer, snapshot
promotion or `on_sync_completed` occurs. The existing outer failure alert,
history and `on_sync_ended` paths still run.

`post_load` runs once when the source was fully consumed and all applicable
finalizers returned, including a successful zero-row run. It runs after a
partial result only when `on_error: skip` deliberately allowed row failures to
continue. It is skipped after interruption, contract failure, a fatal lookup or
load under `on_error: fail`, a finalizer exception, or an unforced guard trip.
`on_error` remains a row-processing policy; it never swallows a hook failure.

`on_run_start` runs only after configuration, connector eligibility, selection
and CLI/MCP preflight succeed. If it fails, no sync is submitted. `on_run_end`
runs if and only if `on_run_start` completed, and it runs after all submitted
syncs settle even under `--fail-fast` or interruption. A failed `on_run_end`
changes the invocation exit to failure but does not rewrite the already durable
per-sync state or history outcomes.

### 4. Use a small, phase-aware template context

Hook SQL is rendered immediately before execution with strict undefined values.
All phases receive `run_id`, `rows_synced` and `status`. Sync phases additionally
receive `sync_name` and `sync_run_id`. Their values are:

| Phase | `rows_synced` | `status` |
|---|---:|---|
| `on_run_start` | `0` | `running` |
| `pre_extract` | `0` | `running` |
| `post_load` | successful destination rows so far | `success` or `partial` |
| `on_run_end` | aggregate successful rows | `success`, `partial`, `failed`, or `interrupted` |

`sync_name` and `sync_run_id` are undefined in run-level hooks; using them is a
validation/render error rather than an empty string. Hook-bearing direct library
calls that supply no invocation id allocate one so `run_id` is always defined.
Values interpolate as authored SQL, with no portable auto-quoting claim; the
project author supplies quotes/casts appropriate to the dialect.

The existing `var('name', default)` function is available, but hook rendering
uses only `drt_project.yml` vars plus process-owned `DRT_VAR_*` values. It does
not consume invocation overrides (`drt run --vars` or MCP's `vars` argument).
The parser must preserve hook SQL until this renderer, just as it already leaves
`model` for the resolver (`drt/config/parser.py:244-258`). This prevents an MCP
client from changing locally authored executable hook SQL through a request
parameter even though MCP legitimately permits per-call model vars today
(`drt/mcp/tools/run_sync.py:68-72`). `${ENV_VAR}` expansion remains supported;
the process environment is part of the trusted deployment configuration.

### 5. Make failures, timeouts and retries explicit

No hook statement is automatically retried. Existing destination HTTP retry
configuration and source extraction retry do not apply. A timeout or ambiguous
connection loss may mean the database committed even though drt saw an error;
authors must make hooks idempotent, preferably by using database-native
`CREATE ... IF NOT EXISTS`, idempotent refresh operations, or a ledger keyed by
`run_id`/`sync_run_id`. An orchestrator retry is a new invocation and may execute
the hook again.

`timeout` applies per statement, not to the whole phase. A connector is
hook-capable only if it can set a server/job/driver timeout and close or cancel
the operation on expiry; a Python worker thread that merely stops waiting is not
sufficient because the SQL would continue remotely. Timeout is reported as the
same phase failure with a stable `timeout` error code.

A `pre_extract` failure marks the sync failed with zero rows and does not advance
state. A `post_load` failure always marks it **partial**, even on a zero-row run:
the load/finalize phase completed, while the promised post phase did not, and a
backend that auto-commits hook SQL may not reveal whether work committed. It does
not call `on_sync_completed`, promote a diff snapshot, or advance a
cursor/watermark. Retrying may therefore resend already committed rows; this is
the conservative choice because advancing state would make a failed post hook
permanently non-retryable. The hook error carries the partial `SyncResult` to the
outer engine/CLI so history, alerts and
`run_results.json` report actual counts instead of today's exception-path zeros.

### 6. Keep dry-run and plan read-only; bind apply carefully

No hook runs during `drt run --dry-run`, MCP `dry_run`, `drt plan`, plan
recomputation inside apply, config validation, docs generation, connection
testing or unit tests. Skipping applies to all four phases. The result may report
that configured hooks were skipped for `dry_run`/`plan`, but there is no SQL
execution and no hook connection.

Normal apply executes hooks only in the approved write phase. After approval and
the single-use claim, it runs `on_run_start` and `pre_extract`, then recomputes
the plan once more **without hooks** and compares it to the approved artifact
before the first destination write. This post-hook verification is necessary
because a pre hook may intentionally materialize or refresh the model input. If
it differs, apply records the claim as failed and refuses destination writes;
the source-side hook effect itself cannot be rolled back by drt. The subsequent
write execution must receive `pre_hook_already_run=True` so it does not execute
the hook twice. `post_load` and `on_run_end` then follow the normal write
lifecycle.

Exact replay runs no source-side hook: it skips `on_run_start`, `pre_extract`
and source-side `on_run_end`, preserving the existing no-source contract. It
does run destination `post_load` after replay finalization, using the exact
apply's new `run_id` and `sync_run_id`. Changing any sync hook or project
run-hook configuration invalidates plan verification through a keyed digest;
the plan contains only the digest and configured phase names, never hook SQL or
rendered values. Normal and exact apply therefore cannot use an artifact
approved for a different post hook.

### 7. Define concurrency and every entry point once

Add a shared invocation runner that owns selection, one `run_id`, run hooks,
sync dispatch and aggregate outcome. The CLI, MCP real-run path, `drt serve`,
Airflow and Prefect must call it. Direct `engine.run_sync()` remains the
single-sync primitive and owns only `pre_extract`/`post_load`; it cannot infer a
multi-sync run boundary.

Under `--threads N`, `on_run_start` completes before the first future is
submitted and `on_run_end` starts only after all running futures have joined.
Per-sync hooks can execute concurrently on separate dedicated connections.
There is no global serialization across two independent drt processes, two MCP
requests or two orchestrator tasks; database locking and idempotence remain the
project's responsibility. `drt serve` continues to serialize overlap only for
the same sync while allowing different syncs concurrently
(`drt/cli/server.py:448-471`). Each accepted serve trigger is its own one-sync
invocation and therefore gets its own run-level hooks.

### 8. Report outcomes without publishing SQL

Each statement records only phase, one-based statement index, status
(`success`, `failed`, `timeout`, `skipped`), duration and a stable error code.
The raw template, rendered SQL, parameter values, query results and driver error
text are never fields on `SyncResult`, history, alerts, OTel attributes,
telemetry, plans, the docs manifest or `run_results.json`.

Sync hook summaries are additive, omit-when-absent fields on `SyncResult`,
`HistoryEntry` and the per-sync run-result entry. A hook failure participates in
the existing `alerts.on_failure` dispatch with `hook_phase` and safe error code;
the alert's free-text error is a fixed message that does not include SQL or the
driver cause. OTel emits child spans named `drt.sync.hook` with phase, statement
index, status and duration only. Existing `SyncObserver` methods remain
unchanged; an optional `HookObserver` capability may receive the same safe
outcome, while ordinary observers continue to see final sync lifecycle events.

Run hook summaries live under an invocation-level `hooks` object in
`run_results.json`. There is no synthetic per-sync history row and no existing
project-level alert configuration to dispatch. Logs and the invocation OTel
span report the safe outcome; adding project alerts is a separate design.

The docs manifest may expose booleans such as `pre_extract_configured`, never
SQL. The raw-YAML docs tab must replace every hook `sql` value—including block
scalars and even under `--full-labels`—with `« hook SQL omitted »` before the
text reaches a renderer. Hook exceptions stored in history use the stable safe
message, so manifest generation never has to recover secrets from free text.
The hardcoded-secret scanner also examines raw hook SQL for known credential
patterns and points authors to `${ENV_VAR}`; this is defense in depth, not a
claim that arbitrary SQL can be perfectly scanned.

### 9. Treat hooks as trusted arbitrary SQL

Hooks are code committed by the project owner, with the same trust boundary as
raw model SQL and `${ENV_VAR}`/`var()` interpolation. They are not row data and
not an MCP/request payload. MCP tools accept a sync name and run options, then
load the sync from the server's local project (`drt/mcp/tools/run_sync.py:19-38`,
`drt/mcp/tools/run_sync.py:65-88`); no tool gains a `hooks` or raw-SQL argument.
Permission checks for running a sync still happen before connector setup on the
CLI path (`drt/cli/commands/run.py:221-238`) and must wrap the shared invocation
runner as well.

There is no SQL verb allowlist or denylist. A read-only allowlist would defeat
refresh/materialization hooks, while a keyword denylist is not a security
boundary across stored procedures, comments and dialects. Each list item is
submitted as one driver call, but drt does not claim that every driver rejects
multi-statement text. Security comes from repository review, protected branches,
MCP's inability to supply hook text, and least-privilege database identities.
Operators should use a hook-specific role where possible; at minimum, source
credentials should be limited to the schemas/procedures hooks require and
destination credentials to the target database. A hook can exfiltrate data or
destroy objects within those grants, so enabling it is equivalent to approving
arbitrary SQL under that identity.

Query tags apply to hooks with the same `run_id`, plus a `hook_phase` tag. This
makes warehouse audit history attributable without placing SQL in drt artifacts.
Secrets interpolated into SQL may still appear in the warehouse's own query
history; authors should pass secret references to stored procedures or use
backend-native secret facilities rather than embedding credentials in SQL.

## Non-goals

- Shell, Python, HTTP or executable hooks; row-by-row callbacks; returned-row
  capture; a general workflow DAG; dependencies between syncs.
- Session-scoped setup shared with the streaming extract or per-batch load
  connection; cross-source/destination transactions.
- Destination-level run hooks across heterogeneous destinations; hooks on SaaS,
  file, queue, Delta Lake or Iceberg connectors in the first release.
- A scheduler or replacement for dbt/orchestrator task graphs.
- Proving arbitrary SQL safe, idempotent or secret-free.

## Alternatives considered

### dbt pre/post-hooks

dbt hooks are appropriate when dbt owns model materialization, but drt can run a
hand-written model or a previously built relation without invoking dbt. drt also
needs destination-side timing after its own `finalize_sync`, guards and replay
logic, which a dbt model hook cannot observe. Requiring dbt would contradict the
resolver's existing ability to use local SQL, a manifest relation or raw SQL
without invoking dbt (`drt/engine/resolver.py:93-139`).

### Shell wrappers around `drt run`

Wrappers are simple for one CLI command, but they do not wrap MCP's direct engine
call or the shared runner used by serve/Airflow/Prefect
(`drt/mcp/tools/run_sync.py:109-128`, `drt/integrations/_runner.py:108-124`).
They also cannot reliably place a post hook after destination finalization but
before state persistence. They remain useful for organization-wide process
setup outside drt's lifecycle.

### Orchestrator-level tasks

Airflow, Prefect and Dagster tasks provide richer dependencies, retries and
credentials, and remain preferable for multi-system workflows. They do not
cover CLI, MCP or `drt serve` uniformly, and repeating the same source refresh
around every integration is the portability gap #764 addresses. The hooks in
this ADR deliberately stay smaller than an orchestrator.

### Expose only a `refresh` primitive

A connector-specific `refresh` flag would be safer to document but cannot cover
stored procedures, audit markers, grants, temporary staging promotion or custom
warehouse operations without growing a new primitive for every use. Arbitrary
SQL is honest about the authority required. The cost is a narrower connector
set and a stronger project-owner/least-privilege threat model.

## Consequences

- Hooks become consistent across CLI, MCP, serve and built-in orchestrator
  integrations, rather than depending on an outer shell.
- Hook configuration is inert when absent. When present, it adds connections,
  SQL jobs and observable latency; `--threads` may run several hook connections
  concurrently.
- `post_load` is after the destination commit boundary. Its failure is partial,
  never an atomic rollback of the sync, and retry may duplicate rows.
- A plan remains read-only and therefore cannot preview a hook's side effects.
  Normal apply pays for one additional post-hook verification read.
- Exact replay stays source-independent but still performs destination cleanup/
  refresh through `post_load`.
- Fresh hook connections keep connector changes bounded but exclude
  session-temporary use cases and the ephemeral Delta/Iceberg DuckDB adapters.
- The supported destination set is intentionally smaller than “all SQL-looking
  destinations”: `ConnectionTestable` BigQuery is not `QueryableDestination`.
- Artifact schemas gain additive safe hook summaries. Docs rendering requires a
  structural hook-SQL omission rule because the current raw-YAML block-scalar
  path is not safe for this field.
- There is no new sandbox. A compromised repository author can exercise the
  database permissions of the runner, so protected review and least privilege
  are operational requirements.

## Compatibility

When all four hook fields are absent, engine ordering, connector calls,
connections, retries, query tags, observer events, history, alerts, plans,
run-results and exit codes are unchanged. Config additions are optional and
generated schemas are additive. Existing plugins remain valid and simply reject
a hook-bearing sync unless they opt into the new capability.

No frozen Protocol changes. `Source`, `Destination`, `QueryableDestination`,
`ConnectionTestable` and `SyncObserver` retain their exact method sets and
signatures. Safe reporting uses additive result/history fields and a separate
optional observer capability. Plan and manifest fields are additive and omitted
when hooks are absent; no existing artifact ever gains SQL text.

## Implementation plan

| PR | Slice | Main acceptance criteria |
|---:|---|---|
| 1 | Config, rendering and validation | Add strict hook models under `SyncOptions`/`ProjectConfig`, generated schemas, duration bounds, dedicated strict Jinja context, trusted hook vars, project-aware capability validation, fingerprint digests and secret-scanner coverage. No execution. |
| 2 | Source capability and lifecycle | Add `SqlHookSource`, implement the ten SQL sources, query tags, transaction/timeout/cancellation behavior, and place `pre_extract` before either extraction path. Tests prove no extraction/state/finalizer on failure and no hook on dry-run/plan/exact replay. |
| 3 | Destination capability and partial failure | Add `SqlHookDestination` for the five `QueryableDestination` implementations, execute after all finalizers/guards but before diff/state promotion, carry partial results through exceptions, and cover zero-row, staged, mirror, replace, guard, `on_error` and timeout cases. |
| 4 | Shared invocation runner | Move once-per-invocation orchestration behind one service used by CLI, MCP, serve, Airflow and Prefect; add run hooks, `--threads` barriers, `--fail-fast`/signal behavior and one-sync invocation semantics. |
| 5 | Apply and exact replay | Keep plan/replan hook-free; run source hooks only after claim, reverify after them, prevent double execution, skip source hooks for exact replay, run destination `post_load`, and bind hook digests without storing SQL. |
| 6 | Safe observability and docs | Add safe hook outcomes to results/history/run-results/alerts/spans, optional `HookObserver`, invocation summaries, and structural omission of hook SQL from docs raw YAML, manifest and every full-label path. Add leak-focused tests using secrets in templates, driver errors and block scalars. |
| 7 | Connector smoke coverage and documentation | Exercise one local transactional source/destination and representative BigQuery-source, Snowflake/Databricks/ClickHouse timeout/tag paths; document grants, idempotence, warehouse query-history exposure and unsupported connectors. |

## Amendment 2026-11-20 (fact-check)

A fact-check against the code and ADR 0021 found one design conflict, resolved here:

- **Windowed backfill and hooks (ADR 0021):** `post_load` is defined as once per sync,
  after all finalizers and before state/watermark advancement. A windowed backfill
  checkpoints each window, so earlier windows have already advanced state when a
  once-per-sync `post_load` runs; running it per window would break the
  once-per-sync definition. **Decision: `sync.hooks` and run-level hooks are rejected
  for syncs that set `sync.backfill` in v1** (validation error naming both features);
  the ordering for windows (hook per window before its checkpoint) can be designed in a
  separate ADR once backfill has real usage. This keeps both ADRs internally true.
- The description of the outer `finally` above was corrected: its effects are
  non-dry-run, conditional and best-effort, not unconditional.

## Decisions (owner-approved 2026-11-20)

The maintainer accepted the recommended answer to every question below. They are
normative for the implementation PRs.

1. **Should hooks be ordered objects rather than string-or-object shorthand?**
   Recommended: yes. One strict shape makes timeout behavior reviewable and
   avoids two public representations before the feature has real usage.
2. **Should run-level hooks execute on the project source?** Recommended: yes.
   There is one source profile per project invocation but potentially many
   destinations; destination-specific work already has `post_load`.
3. **Should a failed `post_load` be partial and leave cursor/diff state
   unadvanced?** Recommended: yes. Destination work may be committed, while
   advancing state would silently prevent the failed hook from being retried.
4. **Should dry-run and plan skip every hook, while normal apply reverifies after
   source hooks?** Recommended: yes. It preserves the read-only promise and
   prevents an apply-side refresh from bypassing the reviewed diff.
5. **Should exact replay skip every source/run hook but execute destination
   `post_load`?** Recommended: yes. Exact replay has no real source, while its
   destination still needs the same post-finalization contract as a normal load.
6. **Should hooks have no internal retry and use a 5-minute per-statement
   timeout?** Recommended: yes. Arbitrary DDL/DML is not safely retryable; the
   bounded default prevents a lost database call from hanging a worker forever.
7. **Should MCP invocation vars be excluded from hook `var()` resolution?**
   Recommended: yes. Hooks remain locally authored code and an MCP client cannot
   turn a reviewed template into request-supplied SQL.
8. **Should v1 use fresh connections, no SQL verb allow/deny list, and the narrow
   connector matrix above?** Recommended: yes. It preserves frozen streaming
   APIs and states the real trust boundary; broader session sharing or more
   connectors can be added only when their transaction/timeout semantics are
   designed and tested.
