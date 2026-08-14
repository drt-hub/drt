# Event-driven syncs

drt runs when invoked — there is no drt-owned watcher process. Getting from
"a nightly sync" to "seconds after the source changes" is a choice of
*what invokes drt*, not a drt feature to turn on. [ADR 0004](../adr/0004-streaming-and-event-triggered-syncs.md)
answers that with three tiers, in the order to reach for them:

| Tier | What triggers drt | Best for | Status |
|---|---|---|---|
| **1. Warehouse-native scheduling** | The warehouse's own scheduler (a cron job, a `TASK`) calling `drt run` or [`drt-action`](https://github.com/drt-hub/drt-action) | 1–15 minute freshness, zero drt-side runtime | Ships today, every source |
| **2. Dagster sensors** | `dagster-drt`'s `build_drt_change_sensor()` polling a cheap change signal | Teams already running Dagster | Ships today — **Delta Lake and Iceberg only** |
| **3. Hardened `drt serve`** | A push source (webhook, Snowflake Alert, Pub/Sub) hitting drt's HTTP endpoint | Genuine push sources, sub-minute | Ships today |

None of these is a *drt-owned* watcher process — drt itself never runs as a
long-lived daemon polling for changes. Tiers 1 and 2 are exactly "drt starts,
syncs, and exits, invoked by something else." **Tier 3 is different**: `drt
serve` must stay running as a resident process to receive requests — it's
hardened precisely because it's the one path where something drt operates
needs to be up continuously. See ADR 0004's
[Decision](../adr/0004-streaming-and-event-triggered-syncs.md#decision)
section for why a drt-owned *watcher* (something that itself detects change,
as opposed to a listener that's told about it) was rejected, and the
[trigger matrix](../research/warehouse-trigger-matrix.md) for the
per-source research behind every recommendation below.

## Tier 1 — warehouse-native scheduling

The trigger lives where the data lands: a cron job, a cloud scheduler, or the
warehouse's own task scheduler invokes `drt run` (or the packaged
[`drt-action`](https://github.com/drt-hub/drt-action) GitHub Action) on an
interval. This is the default for every source drt supports and needs no new
infrastructure — see [`docs/guides/ci-cd-integration.md`](ci-cd-integration.md)
for the CI-runner shape of this pattern.

**This is also the recommended path for Snowflake and SQL Server today** —
with one requirement that's easy to get wrong. Both have a purpose-built
change signal — Snowflake `STREAM` + `SYSTEM$STREAM_HAS_DATA()`, SQL Server
Change Tracking — designed to be checked cheaply from *inside* the
warehouse's own scheduler: a Snowflake `TASK`'s `WHEN
SYSTEM$STREAM_HAS_DATA(...)` clause only runs the task body when the stream
actually has unconsumed rows.

**The task body must itself consume the stream via DML, or this doesn't
work at all.** A stream's offset only advances when it's read inside a DML
statement — plain querying never advances it (see the
[trigger matrix](../research/warehouse-trigger-matrix.md#snowflake)). If the
task body only invokes `drt run` via an external function or webhook and
never touches the stream with DML, nothing consumes it: `SYSTEM$STREAM_HAS_DATA()`
stays `TRUE` forever after the first real change, and the task keeps firing
— and its warehouse keeps spinning — indefinitely, whether or not anything
new has actually changed since the last run. This is the exact same trap
that blocks Tier 2 (see below), and it applies here just as much: give the
task body an explicit DML consumer, e.g. `INSERT INTO
<a_tracking_table> SELECT * FROM <stream>` alongside (or instead of) the
`drt run` invocation, so the stream's offset actually advances. A task that
only ever reads the stream through `WHEN`/`SYSTEM$STREAM_HAS_DATA()` without
a DML consumer is not a working trigger, regardless of tier.

## Tier 2 — Dagster sensors

For teams already running a Dagster orchestrator, `dagster-drt` ships
`build_drt_change_sensor()`: a sensor that polls a cheap, metadata-only
change signal and fires one `RunRequest` per detected change. Dagster
supplies the durability, cursoring, and backfill semantics drt itself
doesn't have.

```python
from dagster import Definitions
from dagster_drt import DagsterDrtResource, build_drt_change_sensor, drt_assets

@drt_assets(project_dir=".")
def my_syncs(context, drt: DagsterDrtResource):
    yield from drt.run(context=context)

change_sensor = build_drt_change_sensor(
    project_dir=".",
    asset_selection=[my_syncs],
    minimum_interval_seconds=60,
)

defs = Definitions(
    assets=[my_syncs],
    sensors=[change_sensor],
    resources={"drt": DagsterDrtResource(project_dir=".")},
)
```

### Supported sources: Delta Lake and Iceberg only

`build_drt_change_sensor()` supports **`deltalake`** (`DeltaTable.version()`,
a monotonically increasing integer) and **`iceberg`** (`current_snapshot().snapshot_id`)
profiles. The sensor only ever compares the two most recent values for
*equality*, never for ordering, so it works with either shape: Delta's
version number is genuinely monotonic, while Iceberg's `snapshot_id` is an
opaque, generated identifier — unique per snapshot but not ordered (Iceberg's
own monotonic field is `sequence_number`; `snapshot_id` can also move to an
*older* snapshot on a table rollback). Either way, "did the value I see now
differ from the value I saw last time" is the only property the sensor
relies on, with no side effects on the read.

**Snowflake and SQL Server are not supported here**, and this isn't a gap
waiting on a follow-up PR to close the same way — building it surfaced a
real mismatch. Snowflake's `SYSTEM$STREAM_HAS_DATA()` is a boolean that only
resets when the underlying stream is *consumed* via a DML transaction; plain
querying never advances it. drt's Snowflake extraction is read-only SELECT,
so nothing in a Tier 2 sensor's poll loop would ever consume the stream —
the boolean would flip `false → true` on the first real change and then
**latch permanently `true`**, so a cursor-diff sensor built the same way as
Delta/Iceberg would fire exactly once, ever, and then go silently quiet even
as real changes kept accumulating. Making the sensor consume the stream
itself (a throwaway DML statement purely to reset the flag) would mean
granting a normally read-only source connection write access — a decision
this ADR never scoped and one [ADR 0005](../adr/0005-state-location-and-write-grants.md)
would need to weigh in on first. SQL Server Change Tracking has its own
retention/versioning semantics that weren't verified against the same
cursor-diff shape either. Both are tracked in
[#975](https://github.com/drt-hub/drt/issues/975) rather than assumed away.
**Use Tier 1 or Tier 3 for Snowflake and SQL Server today.**

Calling `build_drt_change_sensor()` against any other profile type raises
`NotImplementedError` at evaluation time — a failed sensor tick in the
Dagster UI, not a silent permanent skip, so a misconfiguration is visible
rather than quietly inert.

### Deployment note: the sensor process needs the source profile

`_current_signal()` resolves the project's profile via
`drt.config.credentials.load_profile()` — the same profile lookup `drt run`
uses — which reads `~/.drt/profiles.yml` (or the equivalent secret-provider
URIs, see [`secret-provider-uris.md`](secret-provider-uris.md)) on whatever
host evaluates the sensor. A Dagster sensor runs inside the **Dagster
daemon**, not inside a job run's own container — so the daemon's host needs
the same source credentials available that a `drt run` invocation would,
**in addition to, not instead of**, the job run's own environment: when a
`RunRequest` fires, `DagsterDrtResource.run()` calls `load_profile()`
independently inside the job's own container. If your job runs execute in a
different container or host than the daemon (common with containerized
Dagster deployments), both need the source profile — the daemon to evaluate
the sensor, the job to actually run the sync. Missing it on the daemon side
only shows up as a sensor tick failure; missing it on the job side shows up
as a successful sensor tick followed by a failed sync run, which is easy to
misdiagnose as unrelated.

### State and remote backends

The sensor's own "has the source moved" signal is tracked entirely by
Dagster's sensor cursor (`context.update_cursor()`) — it is deliberately
**not** wired to drt's own state (`StateStore`/`WatermarkStorage`). Once a
`RunRequest` fires and Dagster launches the actual sync, that run reads and
writes drt's state exactly as it always has, through `DagsterDrtResource.run()`.
If you're running that resource against a remote `state.backend: gcs | s3`
project (see [`remote-state.md`](remote-state.md)), it now correctly routes
through the same `StatePersistingObserver` path the CLI uses.

## Tier 3 — hardened `drt serve`

For push sources — a webhook, a Snowflake Alert, a Pub/Sub push subscription
— `drt serve` is a hardened HTTP endpoint with a real delivery contract:
`202` + run id instead of holding the request open, same-sync coalescing
instead of dropping concurrent triggers, and pluggable `none`/`bearer`/`hmac`
auth. See [`using-webhook-trigger.md`](using-webhook-trigger.md) for the full
endpoint reference.

**Snowflake's recommended Tier 3 path**: a Snowflake Alert with a `STREAM`
condition, configured to hit `drt serve`'s `/sync/<name>` endpoint via
`WEBHOOK`. It's still Snowflake-side scheduled compute evaluating the
condition — the trigger matrix is explicit that this "wraps a poll" rather
than creating genuinely new push capability — and the **same DML-consumption
requirement from Tier 1 applies here too**: an Alert's condition check is
read-only, exactly like a `TASK`'s `WHEN` clause, so something still needs to
consume the stream via DML or the alert keeps firing on a stream that never
resets. What this path buys over Tier 1 is delivery shape (a webhook hit,
landing on drt serve's `202` + coalescing contract) — not an exemption from
the consumption requirement.

## Choosing a tier

- **Already fresh enough with a schedule?** Stay on Tier 1. It's zero
  drt-side runtime and covers most "fresh enough" requirements (1–15
  minutes).
- **Running Dagster, and the source is Delta Lake or Iceberg?** Tier 2 —
  `build_drt_change_sensor()` gives genuine event-driven activation for
  free, reusing plumbing you already have.
- **Running Dagster, but the source is Snowflake or SQL Server?** Tier 1
  (native `TASK`) or Tier 3 (Alert + webhook) — not Tier 2. See
  [#975](https://github.com/drt-hub/drt/issues/975) if this changes.
- **A push source with no orchestrator in the picture** (GitHub webhook, dbt
  Cloud job completion, a vendor's own webhook)? Tier 3.

None of these require a drt-owned *watcher*, and none of them lock you in.
Tier 1 needs no new infrastructure — the warehouse's own scheduler already
exists. Tier 2 is additive on top of an orchestrator you're already running.
Tier 3 does need something new to operate: `drt serve` as a resident process
that stays up to receive requests — lightweight and hardened, but not
zero-runtime the way Tiers 1 and 2 are.
