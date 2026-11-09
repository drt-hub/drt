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
`deploy github-actions`. Pass `--drt-version 1.2.1` to pin the Dockerfile to
`drt-core[...]==1.2.1` (re-run the command with `--force` after you upgrade
drt). That pins drt only: the `python:3.12-slim` base tag and drt's own
dependencies still float, so it narrows rather than eliminates build drift.

The Dockerfile copies only `drt_project.yml`, `syncs/` and (if present)
`profiles.yml`; it never copies the whole project, so `.env`, `.drt/` and local
state do not end up in the image. `profiles.yml` is copied as-is, so it must hold
`*_env` references, not literal secrets: the command **refuses to scaffold** when
`syncs/`, `drt_project.yml` or `profiles.yml` contain literal credentials (a
`password`, `token`, `api_key`... that is not an `*_env` reference, `${VAR}` or a
`gcp-sm://` / `aws-sm://` / `vault://` URI), naming the location and never the value;
`--allow-inline-secrets` overrides that knowingly. The scan is a best-effort guard, not a guarantee (it recognizes credential-shaped keys such as `password`, `token`, `api_key`, `Authorization`, `webhook_url` and `auth.value`, and skips `sync.mask` and test data); keep literal secrets out of the YAML regardless. Project-level `*_env` references
(for example remote-state credentials) become runtime secrets, and provider URIs add
`gcp-secrets` / `aws-secrets` / `vault` to the extras. Build from the project root:
`docker build -f deploy/cloud-run-job/Dockerfile .`. The inferred extras include
the storage extra of a `state.backend: gcs|s3`.

Scaffolds accept a plain `--select` (name, glob, `tag:`, `destination:`). The
`state:modified` / `state:new` selectors need a baseline manifest through
`drt run --state`, which a scheduled job does not carry, so they are refused.
Output paths must stay inside the project and must not pass through a symlink; files are staged under temporary names first (a staging failure changes nothing) and then moved into place; an existing FIFO or hard-linked file is replaced rather than opened or modified, the final renames are not atomic across files (a failure there names the files already written; they are project files in Git, so `git restore` brings them back), and without `--force` a file that appears late is never clobbered. Cron expressions are range-checked, and `--image` / `--secret-name` are validated. A concurrent attacker who can swap a parent directory for a symlink between the check and the write is out of scope.

Replace the project, region, repository, image, and service-account
placeholders before applying it. Create the named Secret Manager secrets
separately and grant the Cloud Run Job service account access. The scaffold
never reads or emits their values. The scheduler service account also needs
permission to invoke the job.

Cloud Run tasks are ephemeral. Incremental syncs must use a durable
[remote state backend](remote-state.md); the image filesystem is not a state
store.

## Kubernetes CronJob

> The manifest sets `spec.timeZone` (UTC), which Kubernetes supports from 1.27 (beta in 1.25–1.26).
> On an older cluster, delete that line and express the schedule in the controller's time zone.

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
