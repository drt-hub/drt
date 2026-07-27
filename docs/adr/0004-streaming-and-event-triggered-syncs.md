# ADR 0004 — Streaming / event-triggered syncs

- **Status:** Accepted. The recommendation was written to be falsified by the
  per-warehouse trigger matrix ([#786](https://github.com/drt-hub/drt/issues/786),
  @Muawiya-contact); that matrix has since landed as
  [docs/research/warehouse-trigger-matrix.md](../research/warehouse-trigger-matrix.md)
  and meets neither [falsification condition](#falsification-condition), so the
  recommendation stands unchanged. Accepted on that evidence rather than on
  having been asserted first — the matrix checked both conditions explicitly
  and found neither met.
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

Three facts about the code as it stands (v0.8.3) constrain the answer more than
the competitive framing does. Every citation below was re-verified against the
v0.8.3 tree; none of that release's changes touch `serve`, state locality, or
the Dagster integration.

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

### Trigger matrix

The authoritative per-warehouse trigger matrix — @Muawiya-contact's piece of
#786 — has landed as
[docs/research/warehouse-trigger-matrix.md](../research/warehouse-trigger-matrix.md).
It supersedes the provisional table that stood here, covering all thirteen
supported sources with a preferred mechanism, latency, infrastructure and cost
for each, cited to vendor documentation and connector code.

The structural pattern that reading suggested holds — **every cheap signal is a
poll** — and the cheapest signal is a poll for twelve of the thirteen sources.
That is the argument for sensors and against a native watcher: a sensor is a
scheduled cheap poll with durable cursors, which is exactly the shape of the
signals that actually exist. The two purpose-built signals (Snowflake `STREAM`,
SQL Server Change Tracking) are *designed* to be polled cheaply.

The matrix refines the second half of that claim, and the refinement matters
because the loose version is wrong in a way that would invite a fair objection.
Push is **not** rare — nine mechanisms turned up across the thirteen sources, so
"every push signal is already a message bus" understates what exists. The claim
holds instead because all nine fall into one of four categories, each of which
keeps drt out of the consumer business:

| Category | Why it isn't a drt-owned watcher |
|---|---|
| Already a broker the user provisions (BigQuery audit logs → Pub/Sub, Delta storage events, Iceberg via Glue → EventBridge) | The bus does the delivery; drt is a Tier 3 endpoint at most |
| Needs a long-lived consumer draining a queue (Postgres `LISTEN`/`NOTIFY` and logical decoding, MySQL binlog, SQL Server Query Notifications) | Exactly the daemon-per-source shape this ADR rejects |
| The platform's own scheduler invoking a job (Databricks table update trigger, Snowflake Alerts + webhook) | Managed polling that starts a process — Tier 1 by another name |
| A property of one SaaS API, not the connector (REST API webhooks) | Cannot be assumed by a connector configured against arbitrary endpoints |

Databricks' table update trigger is the closest counter-example — a
platform-managed push needing no broker and no drt-side consumer — and it
resolves as Databricks' own managed polling loop starting a job, which is Tier 1
working as designed rather than a broker-free event stream. See
[What this means for ADR 0004](../research/warehouse-trigger-matrix.md#what-this-means-for-adr-0004)
for the falsification check against both conditions below.

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

Sanctioned by this ADR and now tracked separately, so closing #786 does not drop
the work it authorised:

1. **[#854](https://github.com/drt-hub/drt/issues/854) — `drt serve` concurrency
   contract.** A *design* decision, not a bug fix. The open question is what a
   concurrent trigger should get: a bounded queue, `429` with `Retry-After`, or
   per-sync locks replacing the global one. Also in scope: return `202` with a
   run id instead of holding the request open, and add HMAC signature
   verification alongside the bearer token.
2. **[#855](https://github.com/drt-hub/drt/issues/855) — `dagster-drt` sensors**
   (the Tier 2 path). A generic cheap-signal sensor plus Delta/Iceberg version
   and Snowflake `STREAM` variants, yielding one `RunRequest` per changed sync.
   The two lakehouse signals are the cheapest first sensors to write: Delta's
   `version()` is already called in shipped code (`drt/sources/deltalake.py:67`)
   and Iceberg's snapshot id is reachable from a table drt already loads
   (`drt/sources/iceberg.py:51-52`). **Blocked by #756.**
3. **[#856](https://github.com/drt-hub/drt/issues/856) — "event-driven syncs"
   guide** covering all three tiers. Tier 1 is documentable now; Tiers 2 and 3
   are **gated on #756 and #769**, per the gates above.
