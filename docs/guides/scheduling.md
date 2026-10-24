# Scheduling drt

drt starts, syncs, and exits when something invokes it. It does not run a
warehouse watcher or own a scheduling daemon. That boundary is deliberate:
[ADR 0004](../adr/0004-streaming-and-event-triggered-syncs.md) keeps trigger
delivery, retry, cursoring, and operations in systems already built to provide
them.

Choose the first option below that fits.

| Order | Trigger | Use it when | Trade-off |
|---|---|---|---|
| 1 | **Warehouse-native scheduling** | The warehouse can invoke a task, webhook, or external job close to where data lands. | Least new infrastructure and usually the best 1–15 minute path, but the exact integration is warehouse-specific. |
| 2 | **Dagster sensors** | The team already operates Dagster and wants change-aware runs for Delta Lake, Iceberg, Snowflake, or SQL Server. | Dagster supplies durable cursors, retries, and backfills; each poll still has a connection and operational cost. |
| 3 | **`drt serve`** | A real push source such as Pub/Sub, a webhook, or a Snowflake Alert can notify drt. | Sub-minute and event-driven, but the authenticated HTTP listener must stay available. It is a receiver, not a drt-owned change watcher. |
| 4 | **Deploy scaffolds** | A portable cron job in existing CI or container infrastructure is the practical fit. | Easy to review and own as code, but freshness is interval-based and the platform, credentials, state, logs, and retries remain yours to operate. |

For signal details and the per-source support matrix, see [Event-driven
syncs](event-driven-syncs.md). The rest of this page covers the fourth option.

## GitHub Actions

```bash
drt deploy github-actions --schedule "40 3 * * *" --select "tag:nightly"
```

This writes `.github/workflows/drt-sync.yml`, infers connector extras, and
wires each environment-variable reference to a GitHub Actions secret. Omit
`--schedule` for manual dispatch only. For a pull-request plan and apply loop,
use `drt deploy github-actions --with-plan`; see [Plan and
apply](plan.md#review-loop-in-ci-github-actions).

## Cloud Run Job

```bash
drt deploy cloud-run-job --schedule "40 3 * * *" --select "tag:nightly"
```

The command writes:

- `deploy/cloud-run-job/Dockerfile`, with inferred `drt-core` connector extras
  and `drt run` as its default command;
- `deploy/cloud-run-job/job.yaml`, a Cloud Run Job with the selector as an
  argument array and Secret Manager references for the project's environment
  variables; and
- `deploy/cloud-run-job/scheduler.sh`, an offline-generated `gcloud scheduler`
  command for the UTC cron schedule.

By default the image installs the latest `drt-core`, like
`deploy github-actions`. For reproducible builds pass
`--drt-version 1.2.1` and the Dockerfile installs `drt-core[...]==1.2.1`
instead (re-run the command with `--force` after you upgrade drt).

Replace the project, region, repository, image, and service-account
placeholders before applying it. Create the named Secret Manager secrets
separately and grant the Cloud Run Job service account access. The scaffold
never reads or emits their values. The scheduler service account also needs
permission to invoke the job.

Cloud Run tasks are ephemeral. Incremental syncs must use a durable
[remote state backend](remote-state.md); the image filesystem is not a state
store.

## Kubernetes CronJob

```bash
drt deploy k8s-cronjob \
  --schedule "40 3 * * *" \
  --select "destination:hubspot" \
  --image "registry.example.com/data/drt-sync:latest"
```

This writes `deploy/k8s-cronjob.yml`, a `batch/v1` CronJob using UTC with:

- `concurrencyPolicy: Forbid` and a one-hour `activeDeadlineSeconds`;
- CPU and memory requests and limits;
- non-root execution, the runtime-default seccomp profile, no privilege
  escalation, dropped capabilities, and a read-only root filesystem;
- writable `emptyDir` mounts only for drt's local state, run artifact, and
  temporary paths; and
- environment variables sourced by key from the named Kubernetes Secret
  (`drt-secrets` by default), never inline values.

Build the image with the project, `profiles.yml` containing only `*_env`
references, and the connector extras it needs. Create the Kubernetes Secret
out of band, review the manifest, then apply it. The `emptyDir` state vanishes
with the Job pod, so incremental or shared executions need a [remote state
backend](remote-state.md).

All three scaffold commands are deterministic and offline. They refuse to
overwrite an existing artifact unless `--force` is supplied; use `--dry-run`
to inspect generated content without writing it.
