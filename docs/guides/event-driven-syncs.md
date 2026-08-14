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

None of these is a daemon drt runs for you — each is drt starting, syncing,
and exiting, invoked by something else. See ADR 0004's
[Decision](../adr/0004-streaming-and-event-triggered-syncs.md#decision)
section for why a drt-owned watcher was rejected, and the
[trigger matrix](../research/warehouse-trigger-matrix.md) for the
per-source research behind every recommendation below.

## Tier 1 — warehouse-native scheduling

The trigger lives where the data lands: a cron job, a cloud scheduler, or the
warehouse's own task scheduler invokes `drt run` (or the packaged
[`drt-action`](https://github.com/drt-hub/drt-action) GitHub Action) on an
interval. This is the default for every source drt supports and needs no new
infrastructure — see [`docs/guides/ci-cd-integration.md`](ci-cd-integration.md)
for the CI-runner shape of this pattern.

**This is also the recommended path for Snowflake and SQL Server today.**
Both have a purpose-built change signal — Snowflake `STREAM` +
`SYSTEM$STREAM_HAS_DATA()`, SQL Server Change Tracking — designed to be
checked cheaply from *inside* the warehouse's own scheduler: a Snowflake
`TASK`'s `WHEN SYSTEM$STREAM_HAS_DATA(...)` clause only runs the task body
(which can invoke `drt run` via an external function or orchestrator webhook)
when the stream actually has unconsumed rows, and the task's own execution is
what consumes the stream. That consumption is exactly what an
externally-polling Tier 2 sensor cannot get for free — see below.

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

`build_drt_change_sensor()` supports **`deltalake`** (`DeltaTable.version()`)
and **`iceberg`** (`current_snapshot().snapshot_id`) profiles. Both are
monotonic integers that a cursor-diff sensor can poll and compare against
Dagster's own sensor cursor with no side effects.

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
the same source credentials available that a `drt run` invocation would.
If your job runs execute in a different container or host than the daemon
(common with containerized Dagster deployments), the daemon host is the one
that needs the profile, not the job's execution environment.

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
`WEBHOOK`. This is a real push signal — the alert only fires when the stream
actually has data — without the polling-sensor consumption problem Tier 2
runs into, since the alert's own evaluation (inside Snowflake) is what checks
`SYSTEM$STREAM_HAS_DATA()`, not an external poller.

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

None of these require a drt daemon, and none of them lock you in — Tier 1
and Tier 3 need no new infrastructure at all, and Tier 2 is additive on top
of an orchestrator you're already running.
