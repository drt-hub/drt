# ADR 0004 — Streaming / event-triggered syncs

- **Status:** Proposed — the recommendation below is provisional until the
  per-warehouse trigger matrix ([#786](https://github.com/drt-hub/drt/issues/786),
  @Muawiya-contact) lands, and is written to be falsified by it. See
  [Falsification condition](#falsification-condition).
- **Issue:** [#786](https://github.com/drt-hub/drt/issues/786)
- **Implementation:** none — this ADR recommends **not** building a native
  watcher. The work it does sanction is listed under
  [Follow-up issues](#follow-up-issues).

## Context

drt runs when invoked. Census Live Syncs and Hightouch Streaming/CDC ship
"row lands in the warehouse → seconds later it is in the destination", and the
warehouses now feed them: Snowpipe Streaming, BigQuery's Storage Write API and
change history, Databricks DLT. The gap this opens is not throughput but
*staleness*: the operational use cases reverse ETL exists for — abandoned cart
to ads, fraud flag to the support tool, churn score to the CRM — are the ones
where a nightly or hourly sync is visibly wrong.

The question this ADR answers is what an OSS, CLI-first tool should do about
that. It is deliberately a build/no-build decision, not a design for a daemon.

Three facts about the code as it stands (v0.8.2) constrain the answer more than
the competitive framing does.

**`drt serve` is a trigger endpoint, not a trigger runtime.** It exists
(#218) and works for its designed cadence — a dbt job finishes, POST
`/sync/<name>`. Its module docstring says the quiet part outright: *"One sync
at a time — concurrent requests get 423 Locked"* (`drt/cli/server.py:15`). At
sub-minute cadence three properties stop being acceptable:

- A single global `_SyncLock` (`drt/cli/server.py:28`) serialises *every* sync,
  not merely concurrent runs of the same one, and a second trigger is answered
  `423` and **dropped** (`drt/cli/server.py:90`). There is no queue and no
  retry contract, so at streaming cadence events are lost silently rather than
  delayed. This is the load-bearing defect: silent event loss, not backpressure.
- The sync executes synchronously inside the request handler
  (`drt/cli/server.py:100`), so the HTTP response is held for the sync's full
  duration. Any sender with a timeout — Pub/Sub push, GitHub, EventBridge —
  records a failure for a sync that actually succeeded, and retries it.
- Authentication is an optional static bearer token (`drt/cli/server.py:61`).
  Real push sources sign their deliveries; without HMAC verification,
  `drt serve` cannot be exposed to one without a proxy in front.

**State is local.** `StateManager` reads and writes `.drt/state.json` on the
filesystem (`drt/state/manager.py:43`). A long-lived trigger consumer, a CI
run, and a developer's laptop cannot share watermarks, so any always-on
component would immediately own state that nothing else can see.

**`dagster-drt` has no sensors.** #786 supposes the integration "may already be
80% of the answer". It is not: the package ships `assets.py`, `resource.py`,
`specs.py`, `translator.py`, and exports seven symbols
(`integrations/dagster-drt/dagster_drt/__init__.py:6`), none of them a sensor.
The cheapest-looking path is unbuilt work, not a recipe waiting to be written
up. This changes the ADR's job from "document how to wire what exists" to
"specify what to build".

## Decision

### Do not build a native watcher

Recommend **no** on a drt-owned process that subscribes to warehouse change
feeds and runs syncs. Every trigger source needs a different long-lived
consumer — a Pub/Sub subscriber, a Snowflake `STREAM` poller, a Postgres
replication slot — and each carries its own credentials, backoff, ordering and
at-least-once semantics. That is a daemon fleet wearing one command's name, in
a tool whose stated posture is CLI-first (CLAUDE.md), and it would make drt
responsible for delivery guarantees the warehouses already provide.

The competitors' streaming products are hosted services. Reproducing the
service without the hosting is the worst of both: the operational burden lands
on the user, and drt inherits the support surface.

### Answer in three tiers, none of them new infrastructure

**Tier 1 — warehouse-native scheduling invoking `drt-action` or the CLI.**
The trigger lives where the data lands, and drt stays a process that starts,
syncs, and exits. This covers the majority of "fresh enough" requirements
(1–15 minutes) with zero drt-side runtime. *No gate — safe to document today.*

**Tier 2 — Dagster sensors, for teams already running an orchestrator.**
The recommended path for genuine event-driven activation, and the honest
version of #786's hypothesis: it requires *adding* sensors to `dagster-drt`,
where the asset and resource plumbing already exists to build on. A sensor
evaluating a cheap change signal and yielding a `RunRequest` per changed sync
is a small package addition, and Dagster supplies the durability, cursoring and
backfill semantics drt would otherwise have to invent.
*Gated on #756 and #769 — see [Gates](#gates-two-prerequisites-block-promotion-not-authorship).*

**Tier 3 — hardened `drt serve`, for push sources.**
Keep the endpoint for what it is good at, and fix the three defects above so it
can sit behind a real push subscription. Hardening is bounded work with a clear
finish line; it is not a step toward a daemon. *Gated on #769.*

### Gates: two prerequisites block promotion, not authorship

These are **gates, not footnotes**. The recommendation can be written and
merged now; the Tier 2 and Tier 3 paths must not be *promoted* — in docs, the
README, or a comparison table — until the gates clear. Event-driven guidance
published before then would document a configuration that breaks quietly
rather than loudly, which is the worst failure mode a docs deliverable has.

| Gate | Blocks | Why |
|---|---|---|
| **#756 remote state** | Tier 2 | `.drt/state.json` is local disk (`drt/state/manager.py:43`). A sensor in an orchestrator and a CI run genuinely cannot share a watermark today. A Tier 2 recommendation shipped before this tells users to build a topology whose two halves silently disagree about what has already synced. |
| **#769 rate limiting v2** | Tier 2, Tier 3 | Only the `Retry-After` half shipped (v0.8.1). The **per-destination `rate_limit` override** and the **shared bucket across threads** are still open. Frequent small runs multiply request bursts against SaaS destinations in a way a nightly batch never touches, and a per-process bucket that resets every run will trip limits nightly batches never saw. |

Non-blocking, for completeness:

- **Diff-based incremental (#755) — enabling, not blocking.** Valuable because
  it removes the cursor-column requirement, but a cursor works for micro-runs
  today.
- **`depends_on` (#426) — out of scope.** Ordering between syncs is an
  orchestrator's job, and Tier 2 gets it for free.

### Trigger matrix — provisional, pending #786's dedicated deliverable

> **Attribution.** The authoritative per-warehouse trigger matrix is
> @Muawiya-contact's piece of #786. The table below is *not* it: it is my
> reading of the shipped connector code, recorded here so this ADR is
> self-contained and handed over as raw material. **It is superseded by that
> matrix on landing**, and where the two disagree, the matrix wins.

| Source | Signal | Shape | Cost / caveat |
|---|---|---|---|
| Postgres | `LISTEN`/`NOTIFY`; logical decoding | push; push | NOTIFY needs a trigger the user installs; logical decoding needs a replication slot and is CDC, not reverse ETL's job |
| BigQuery | Table change notifications → Pub/Sub; `APPENDS` TVF | push; poll | Pub/Sub is the only true push here and lands in Tier 1 or 3 |
| Snowflake | `STREAM` + `SYSTEM$STREAM_HAS_DATA` | poll | Cheap to poll, and the closest thing to a purpose-built signal |
| Databricks | Table triggers / DLT; Delta commit version | push; poll | Delta version is a cheap monotonic cursor for a sensor |
| Delta Lake | Table version | poll | Cheapest in the matrix, and drt already calls `DeltaTable(...).version()` (`drt/sources/deltalake.py:67`) |
| Iceberg | Snapshot id | poll | Metadata-only read, reachable via the loaded pyiceberg table (`drt/sources/iceberg.py:52`), though drt does not read it today |
| ClickHouse, MySQL, Redshift, SQL Server | `max(updated_at)` or count probe | poll | No native change feed worth targeting; generic and slow |
| DuckDB / SQLite | file mtime | poll | Single-writer, local — sub-minute activation is not the use case |
| REST API | **none — stated non-option** | — | Polling the API *is* the extract (`drt/sources/rest_api.py:31`); a separate trigger adds a round trip and no information |

The structural pattern this reading suggests: **every cheap signal is a poll,
and every push signal is already a message bus the user runs.** That is the
strongest argument for sensors and against a native watcher — a sensor is a
scheduled cheap poll with durable cursors, which is exactly the shape of the
signals that actually exist.

### Falsification condition

The no-watcher recommendation rests on that structural pattern, not on a
general preference for less code. It should be revisited if the finished matrix
shows either:

1. **A cheap push signal that is not already a message bus the user runs** —
   i.e. a warehouse that will push to a consumer with no broker in between. A
   drt-owned consumer would then be adding capability, not duplicating a bus.
2. **A signal whose cost only makes sense amortised across a long-lived
   connection** — e.g. a change feed where per-poll setup dominates, making the
   scheduled-poll shape a sensor provides structurally wrong.

Absent both, the tiers stand. If the matrix contradicts this, the ADR should
change rather than be defended.

### Where the open-core line falls

Sub-minute activation is **core**. OPEN_CORE.md lists the sync engine —
explicitly including "rate limits, retry logic, cursor management" — under
*What's Always Free*, and commits that "if it ships in drt-core, it's free
forever". Freshness is a property of that engine, not a deployment feature, and
gating it would put drt on the wrong side of its own rule.

What is legitimately enterprise is **hosting the always-on component**: a
managed sensor/consumer with its own uptime, alerting and multi-tenant state.
That falls under the existing "Cloud hosting / drt Cloud — managed hosting,
zero-ops deployment" boundary item, so nothing about event-driven activation
moves the line. It is the same split Tier 1–3 already draws: the capability is
open, running it for you is not.

### Still out

- A native `drt watch` / daemon mode, per the decision above.
- CDC or log-based replication. drt activates warehouse data; it does not
  replicate into the warehouse.
- Exactly-once delivery. The tiers give at-least-once with idempotent upserts,
  which is what the destinations support and what the competitors deliver in
  practice.

## Consequences

drt's answer to "what about streaming?" becomes a documented posture instead of
a silence: warehouse-native scheduling for most, Dagster sensors for
event-driven teams, a hardened webhook for push sources. That is defensible in
a comparison table without shipping a daemon.

The cost is that Tier 2 is not free — `dagster-drt` needs sensors written, and
they are the only genuinely new surface this ADR sanctions. The benefit is that
they land in an integration package where Dagster owns durability, rather than
in the engine.

Two prerequisites (#756, #769) are promoted from "related" to blocking, with
the promotion scoped to *publishing* the guidance rather than writing it. The
practical effect is ordering: Tier 1 can be documented immediately, Tier 2 and
Tier 3 wait.

Deciding against the watcher now is what makes the v1.0 protocol freeze
cheaper: no trigger runtime means no trigger protocol to keep compatible.

## Follow-up issues

Sanctioned by this ADR, to be opened separately and tagged as related to #786:

1. **`drt serve` concurrency contract** — a *design* decision, not a bug fix.
   The open question is what a concurrent trigger should get: a bounded queue,
   `429` with `Retry-After`, or per-sync locks replacing the global one. Also
   in scope: return `202` with a run id instead of holding the request open,
   and add HMAC signature verification alongside the bearer token.
2. **`dagster-drt` sensors** — a generic cheap-signal sensor plus Delta/Iceberg
   version and Snowflake `STREAM` variants, yielding one `RunRequest` per
   changed sync. Blocked-by #756.
3. **Docs — "event-driven syncs" guide** covering all three tiers. Blocked-by
   #756 and #769, per the gates above.
