# Remote state on GCS or S3

drt normally keeps run state, execution history, and the dead letter queue
(DLQ) under the project's local `.drt/` directory. That default is convenient
on a laptop, but the deployment model drt promotes—ephemeral CI runners, Cloud
Run Jobs, and Dagster or Airflow workers—destroys that disk after every run.
The production field report recorded in
[ADR 0005](../adr/0005-state-location-and-write-grants.md) describes the
consequences: `drt status` is blank in a fresh checkout, `drt retry` cannot see
yesterday's failures, and a laptop and a runner have separate histories.

Set the project-level `state.backend` to `gcs` or `s3` to put all three
surfaces in object storage that survives the runner and can be shared by every
operator using the project.

## Quick start

GCS:

```yaml
# drt_project.yml
state:
  backend: gcs
  bucket: my-drt-state
  prefix: production/customer-activation
```

S3:

```yaml
# drt_project.yml
state:
  backend: s3
  bucket: my-drt-state
  prefix: production/customer-activation
  region: ap-northeast-1
```

Install the matching extra before running drt:

```bash
pip install 'drt-core[gcs]'  # GCS
pip install 'drt-core[s3]'   # S3
```

## Configuration reference

The `state:` block belongs in `drt_project.yml`, alongside `history:`. Remote
fields default to `null`; fields that do not apply to the selected backend are
rejected rather than silently ignored.

| Field | Backend | Default | Required | Meaning |
|---|---|---|---|---|
| `backend` | all | `local` | no | `local`, `gcs`, or `s3`. Omitting the whole `state:` block preserves local behavior. |
| `bucket` | GCS, S3 | `null` | yes for GCS/S3 | Bucket containing the state objects. Rejected with `backend: local`. |
| `prefix` | GCS, S3 | `null` | no | Object-key prefix. Leading and trailing `/` characters are normalized away; an empty value writes at the bucket root. |
| `region` | S3 | `null` | no | AWS region passed to the boto3 session; otherwise boto3 resolves its default region. |
| `aws_profile` | S3 | `null` | no | Named boto3 profile, normally from `~/.aws/credentials`. |
| `aws_access_key_id_env` | S3 | `null` | no | Environment-variable name or [secret-provider URI](secret-provider-uris.md) resolving to the AWS access key ID. |
| `aws_secret_access_key_env` | S3 | `null` | no | Environment-variable name or secret-provider URI resolving to the AWS secret access key. |
| `aws_session_token_env` | S3 | `null` | no | Environment-variable name or secret-provider URI resolving to an optional AWS session token. |
| `endpoint_url` | S3 | `null` | no | Alternate S3-compatible endpoint, such as MinIO, LocalStack, R2, or Spaces. The endpoint must support conditional writes. |

`bucket` is required for both remote backends. The six S3-only fields
(`region`, `aws_profile`, the three `*_env` fields, and `endpoint_url`) are
rejected under `gcs`; every remote field is rejected under `local`.

## Google Cloud Storage

GCS state uses Application Default Credentials (ADC). The client is created
without an explicit credential argument, so use `gcloud auth
application-default login` for local development and an attached service
account in CI or production. No credential field belongs in the `state:`
block.

The backend reads object metadata and content, conditionally uploads objects,
and lists the history/DLQ prefixes. Install `drt-core[gcs]` and grant the
runner's principal the bucket-scoped Storage Object User role
(`roles/storage.objectUser`):

```bash
gcloud storage buckets add-iam-policy-binding gs://my-drt-state \
  --member="serviceAccount:drt-runner@my-project.iam.gserviceaccount.com" \
  --role="roles/storage.objectUser"
```

That built-in role includes the object create, read, update/delete, and list
permissions needed to replace existing objects safely. A custom role can be
narrower in unrelated permissions, but must still cover the equivalent object
operations used by the backend.

## Amazon S3

S3 state deliberately uses the same credential vocabulary and precedence as
the [S3 destination](../connectors/s3.md#authentication). The three `*_env`
fields are resolved through drt's `resolve_env()` chain; resolved values,
`aws_profile`, and `region` are passed to `boto3.session.Session`. If none is
set, boto3's standard credential chain applies (environment variables, shared
credentials/config files, and attached workload roles). `endpoint_url` is
passed when the S3 client is created.

Install `drt-core[s3]`. This minimal IAM policy permits the calls the backend
makes under one prefix:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ListDrtStatePrefix",
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::my-drt-state",
      "Condition": {
        "StringLike": {
          "s3:prefix": [
            "production/customer-activation",
            "production/customer-activation/*"
          ]
        }
      }
    },
    {
      "Sid": "ReadWriteDrtStateObjects",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject"
      ],
      "Resource": "arn:aws:s3:::my-drt-state/production/customer-activation/*"
    }
  ]
}
```

`HeadObject` and `GetObject` require `s3:GetObject`; prefix enumeration uses
`s3:ListBucket`; conditional object replacement uses `s3:PutObject`. If the
bucket uses a customer-managed KMS key, its key policy may require additional
KMS permissions.

## Object layout

The remote layout mirrors the local `.drt/` layout exactly. For a normalized
prefix `production/customer-activation`, drt uses:

```text
production/customer-activation/state.json
production/customer-activation/history/<sync_name>.jsonl
production/customer-activation/dlq/<sync_name>.jsonl
```

In general, the keys are `{prefix}/state.json`,
`{prefix}/history/<sync_name>.jsonl`, and
`{prefix}/dlq/<sync_name>.jsonl`; without a prefix they are rooted at
`state.json`, `history/`, and `dlq/`.

`state.json` is **one project-wide object**, containing every sync's latest
run state. History and DLQ data use one JSONL object per sync. See
[#948](https://github.com/drt-hub/drt/issues/948) for the settled layout's
rationale and trade-offs.

Remote history is pruned by both `history.retention_days` (default 30) and
`history.max_entries` (default 500 per sync). The count cap applies only to
remote backends and bounds the object downloaded and rewritten on each update;
local history keeps every entry inside the retention window.

## Concurrency and failure behavior

Object stores do not provide POSIX append, so each update is a
read-modify-write guarded by an object-version precondition. GCS pins reads to
a generation and writes with `if_generation_match`; S3 pins reads and writes
with the current ETag (`If-Match`), using `If-None-Match: *` to create an
object. A stale precondition triggers a fresh read and retry with jittered
backoff, up to eight attempts.

Exhausting those attempts while saving or resetting `state.json` raises
`drt.state.errors.StateContentionError`: silently losing current run state
would make recovery decisions incorrect. History and DLQ **appends** remain
best-effort: they log a warning and continue after contention retries or
another write error are exhausted, matching the local stores' philosophy that
telemetry persistence must not fail an otherwise-correct sync.

This whole-object update shape is a deliberate, measured trade-off. The
design rationale and reversibility constraint live in
[ADR 0005 — Where drt's state lives, and what it costs the operator](../adr/0005-state-location-and-write-grants.md),
not in this guide. In particular, its falsification condition calls out
write amplification and contention at roughly 50 syncs on a 15-minute cadence
as the threshold to measure rather than assume away.

## Migrating between local and remote state

Because remote keys mirror `.drt/`, migration is a byte-for-byte directory
sync with no schema conversion.

Adopt GCS, or return from GCS to local:

```bash
gsutil rsync -r .drt/ gs://my-drt-state/production/customer-activation/
gsutil rsync -r gs://my-drt-state/production/customer-activation/ .drt/
```

Adopt S3, or return from S3 to local:

```bash
aws s3 sync .drt/ s3://my-drt-state/production/customer-activation/
aws s3 sync s3://my-drt-state/production/customer-activation/ .drt/
```

Stop drt writers while copying so a concurrent run cannot change an object
mid-migration. After uploading, set the matching remote `state.backend`. To
return, download first if you want the accumulated remote state, history, and
DLQ locally, then set `backend: local` or remove the `state:` block. Dropping
back requires no code change and no data transformation. This is the
designed-in reversibility constraint in
[ADR 0005, Decision 4](../adr/0005-state-location-and-write-grants.md#decision),
not an incidental property of the implementation.

The sync commands do not delete objects that exist only on the destination,
so they are safe for normal adoption/return migrations. If you need a strict
mirror, review and remove stale objects separately.

## Project state and sync watermarks are independent

`state.backend` is project-level storage for run state, history, and the DLQ.
`sync.watermark.storage` is a per-sync choice for an incremental cursor. They
are different scopes, and setting either one does not relocate the other.

This is valid: shared project state on GCS with one sync's watermark still on
the runner's local disk.

```yaml
# drt_project.yml
state:
  backend: gcs
  bucket: my-drt-state
  prefix: production/customer-activation

# syncs/users.yml
sync:
  mode: incremental
  cursor_field: updated_at
  watermark:
    storage: local
```

The reverse is valid too: a project can keep the default local state backend
while an individual sync uses `watermark.storage: gcs` (or `bigquery`). Choose
and migrate each concern explicitly.

## What this isn't

- **Not a warehouse-queryable observability layer.** GCS/S3 make operational
  files durable and shared; they do not turn history into SQL tables. That
  separate tier is [#920](https://github.com/drt-hub/drt/issues/920), split
  from #756 by [ADR 0005](../adr/0005-state-location-and-write-grants.md).
- **Not object encryption managed by drt.** Use the bucket provider's
  server-side encryption, key-management, and IAM controls. drt does not
  encrypt the payload before upload.
- **Not automatic migration.** Changing `state.backend` does not copy existing
  `.drt/` data. Run the `gsutil rsync` or `aws s3 sync` command yourself.

## See also

- [ADR 0005 — Where drt's state lives, and what it costs the operator](../adr/0005-state-location-and-write-grants.md)
- [Sync execution history](sync-history.md)
- [Dead Letter Queue](dead-letter-queue.md)
- [Issue #948 — single-object `state.json` layout](https://github.com/drt-hub/drt/issues/948)
