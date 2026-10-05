# BigQuery

> Extract from BigQuery, or INSERT / MERGE / replace / mirror rows into BigQuery tables using `google-cloud-bigquery`.

## YAML Example

```yaml
destination:
  type: bigquery
  project: my-gcp-project
  dataset: analytics
  table: user_scores
  mode: merge                  # "insert" (default) | "merge"
  upsert_key: [user_id]        # required when mode: merge
  method: application_default  # "application_default" (default) | "keyfile"
  # keyfile: /path/to/sa.json  # required when method: keyfile
  # location: US               # optional dataset location

sync:
  mode: mirror                 # full | incremental | upsert | replace | mirror
  # replace_strategy: swap     # truncate (default) | swap; replace mode only
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"bigquery"` | — | Required |
| `project` | string | — | GCP project ID. **Required** |
| `dataset` | string | — | BigQuery dataset name. **Required** |
| `table` | string | — | Target table name. **Required** |
| `location` | string \| null | null | Dataset location (e.g. `US`, `EU`, `asia-northeast1`). |
| `mode` | `"insert"` \| `"merge"` | `"insert"` | Write strategy. `insert` = append; `merge` = upsert (requires `upsert_key`). Orthogonal to `sync.mode`. |
| `upsert_key` | list[str] \| null | null | Columns matched in the `MERGE … ON` clause. Required when `mode: merge`. |
| `method` | `"application_default"` \| `"keyfile"` | `"application_default"` | Authentication method (same convention as the BigQuery source). |
| `keyfile` | string \| null | null | Path to a service-account JSON keyfile. Required when `method: keyfile`. |

## Authentication

**Application Default Credentials** (default) — the standard chain: `GOOGLE_APPLICATION_CREDENTIALS` → `gcloud auth application-default login` → an attached service account on GCE / GKE / Cloud Run.

```yaml
destination:
  type: bigquery
  project: my-gcp-project
  dataset: analytics
  table: user_scores
  # method: application_default  # default — can omit
```

**Service-account keyfile** — for CI / cron where ADC isn't available:

```yaml
destination:
  type: bigquery
  project: my-gcp-project
  dataset: analytics
  table: user_scores
  method: keyfile
  keyfile: /secrets/bq-writer.json
```

The principal needs `bigquery.tables.updateData` on the target table and
`bigquery.jobs.create`. MERGE, mirror, and swap-replace also create and delete
scratch tables in the target dataset, so they need `bigquery.tables.create`,
`bigquery.tables.get`, `bigquery.tables.getData`, and
`bigquery.tables.delete` as applicable to load,
copy, query, and cleanup jobs. `roles/bigquery.dataEditor` on the dataset plus
`roles/bigquery.jobUser` on the project covers the normal setup.

## Write modes

### `mode: insert` (append)

```yaml
destination:
  type: bigquery
  mode: insert     # default
  ...
```

Rows are appended via the BigQuery **streaming insert API** (`insert_rows_json`), which reports errors per row — a row that fails is recorded in `result.row_errors` while the rest succeed (`on_error: skip`, default) or the batch raises (`on_error: fail`).

> **Note:** streaming inserts land in a write buffer and can take a short while to become available for `UPDATE` / `DELETE` / table copy. For "latest snapshot" semantics, prefer `mode: merge`.

### `mode: merge` (upsert)

```yaml
destination:
  type: bigquery
  mode: merge
  upsert_key: [user_id]
  ...
```

drt loads the batch into a temp table `<table>_drt_tmp` (`load_table_from_json`), runs a single

```sql
MERGE `project.dataset.table` T
USING `project.dataset.table_drt_tmp` S
ON T.user_id = S.user_id
WHEN MATCHED THEN UPDATE SET <non-key columns>
WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
```

then drops the temp table. When the target exists and contains every batch
column, drt supplies those target fields as the temp-table load schema instead
of relying on autodetect. This preserves types for all-`NULL` columns (which
BigQuery otherwise detects as `STRING`). A missing target or a batch column not
yet present in the target retains BigQuery's existing autodetect behavior.
Composite keys are supported (`upsert_key: [tenant_id, user_id]` → AND-joined
`ON`). When every column is in `upsert_key`, the `WHEN MATCHED` UPDATE is
skipped (effectively insert-if-not-exists). Because BigQuery load + MERGE are
**job-level** operations, merge error handling is **batch-level** (the whole
batch succeeds or fails) — coarser than the per-row staging used by the
Snowflake / Databricks destinations.

## Sync modes

| `sync.mode` | Behaviour on BigQuery |
|---|---|
| `full` | Re-extracts every run + writes via `config.mode` (insert / merge). |
| `incremental` | Watermark-based — extracts rows with `cursor_field > last_value`, writes via `config.mode`. |
| `upsert` | Same as `incremental` with `upsert_key` enforced. |
| `replace` | Rebuilds the table through load jobs. `replace_strategy: truncate` writes the first batch with `WRITE_TRUNCATE` and later batches with `WRITE_APPEND`; `swap` builds a shadow and atomically copies it over the target at end of sync. |
| `mirror` | Forces the MERGE path. The default strategy stages observed keys and deletes target rows not present in that table; `strategy: diff` stages and deletes only keys the source snapshot classified as removed. Requires `destination.upsert_key`. |

### Replace details

The default `replace_strategy: truncate` deliberately does **not** issue
`TRUNCATE TABLE`. The first batch is a load job with `WRITE_TRUNCATE`; later
batches are load jobs with `WRITE_APPEND`. BigQuery applies each completed load
job atomically, but a multi-batch run is still visible batch by batch and can
leave a partial replacement if a later batch fails. `WRITE_TRUNCATE` also uses
the load-job schema and can replace table metadata such as constraints and
column descriptions.

For a whole-sync atomic cutover, use:

```yaml
sync:
  mode: replace
  replace_strategy: swap
```

On the first batch, drt copies the target to
`<table>__drt_swap_<run-id>` in the same dataset, truncates that shadow (which has no
streaming buffer), and appends every batch via load jobs. At end of sync, one
BigQuery copy job with `WRITE_TRUNCATE` atomically overwrites the target, and
the shadow is dropped in a `finally` cleanup. BigQuery has no atomic table
rename, so this copy-job cutover is the atomic option. The target must already
exist; seeding the shadow from it preserves its schema, partitioning, and
clustering for the replacement. The per-run suffix prevents overlapping runs
against the same target from sharing a shadow table.

### Mirror details

```yaml
destination:
  type: bigquery
  mode: insert              # mirror overrides this and uses MERGE
  upsert_key: [user_id]
  ...

sync:
  mode: mirror
```

Each source batch is loaded to `<table>_drt_tmp`. On the first batch, drt creates
an empty `<table>__drt_mirror_keys_<run-id>` table by selecting the configured
key and scope columns from the target with `WHERE FALSE`, then loads each
batch's observed key/scope values into that typed table before the existing
MERGE runs. One target-schema lookup per batch supplies explicit types to both
the MERGE temp load and mirror-key load, avoiding BigQuery autodetect choosing
`STRING` when a nullable target column is all `NULL`. After all batches, drt
issues one anti-join delete:

```sql
DELETE FROM `project.dataset.table` AS T
WHERE NOT EXISTS (
  SELECT 1
  FROM `project.dataset.table__drt_mirror_keys_a1b2c3d4` AS K
  WHERE T.user_id = K.user_id
)
```

This avoids a giant value-interpolated `IN (...)` list: record values reach
BigQuery through the load job, while generated SQL contains only validated,
backtick-quoted identifiers. Composite keys and `sync.mirror.scope` are
supported by the default destination strategy; scope matching uses NULL-safe
`TO_JSON_STRING` equality. An empty source does not delete anything.
`mirror.strategy: tracked` is not yet supported. An interrupted run also skips
the anti-join delete because its staged keys cover only a processed prefix;
the unconditional write-state reset drops that run's key table, and an
interrupted diff run does not promote its source snapshot baseline. The
per-run suffix keeps overlapping invocations isolated.
Mirror mode rejects partition-decorated targets such as `events$20261005`;
target the base table instead.

For a BigQuery diff source, exact source-side removals can drive mirror deletes
without scanning the destination or staging every unchanged source key:

```yaml
sync:
  mode: mirror
  incremental_strategy: diff
  mirror:
    strategy: diff
```

Added and changed rows still use the normal MERGE path. At finalize time, drt
loads the removed keys into a target-typed, per-run
`<table>__drt_mirror_keys_diff_<run-id>` table and deletes matching target rows
with an `EXISTS` join. Removal-only runs therefore still perform the delete;
composite keys are AND-joined, and no value-interpolated `IN (...)` list is
generated. `strategy: diff` does not accept `mirror.scope` because its removed
key list is already the exact row-level deletion set.

Both MERGE and mirror use query-job DML. Rows previously written with the
legacy streaming insert API can remain in BigQuery's streaming buffer and be
temporarily unavailable to UPDATE/DELETE or table-copy operations. Avoid
switching a recently streamed target directly to mirror/swap, or wait until
its streaming buffer clears. Rows written by mirror and replace themselves use
load/query jobs rather than streaming inserts.

## As a source — diff-based incremental ([#1113](https://github.com/drt-hub/drt/issues/1113))

BigQuery supports `sync.incremental_strategy: diff` for models without a reliable cursor column.
Each run materializes the full model result in `_drt_snapshot_<sync_name>_<digest>` under the
source profile's `managed_schema`, then classifies added, changed, and removed rows with
server-side joins on `destination.upsert_key`. Added and changed rows follow the normal upsert
path; removed keys are exposed through `SyncResult.diff_removed_keys`, power
`mirror.strategy: diff`, and appear in `--dry-run --diff`
deletion previews.

```yaml
# ~/.drt/profiles.yml
bigquery_prod:
  type: bigquery
  project: my-gcp-project
  dataset: analytics
  method: application_default
  location: US
  managed_schema: _drt       # default; drt needs table create/update here
```

```yaml
destination:
  type: rest_api
  url: https://api.example.com/users
  upsert_key: [id]

sync:
  mode: upsert               # or mirror
  incremental_strategy: diff
  diff:
    hash_columns: all        # or an explicit non-empty column list
```

BigQuery output column matching is exact-case: an `upsert_key` or explicit `hash_columns` name
must match the materialized model column exactly, and a typo fails loudly. `hash_columns: all`
compares every non-key output column. The row hash uses `FARM_FINGERPRINT` over a deterministic
typed STRUCT serialization with an explicit NULL flag beside every value; it does not use
`CONCAT`, whose NULL propagation would otherwise make a `NULL` → `''` transition ambiguous. A
new model column re-sends existing rows once, while a removed model column is simply omitted from
the next shared-schema comparison, matching the Snowflake and Databricks legs.

The baseline advances only after a non-dry-run, unlimited sync finishes with zero row failures.
Scratch materialization uses a query destination with `WRITE_TRUNCATE`; promotion uses a table
copy job with the same disposition. BigQuery documents those job write actions as one atomic
update that occurs only after successful completion, so readers see the complete old or complete
new baseline. A skipped commit leaves the prior baseline unchanged, and the next extract
atomically overwrites abandoned scratch. Diff therefore needs `bigquery.jobs.create` plus
`bigquery.tables.create`, `get`, `getData`, `update`, and `updateData` in `managed_schema`; cursor
incremental remains the read-only-source alternative. An administrator can pre-create the dataset
and grant only its dataset-local permissions, using the same escape hatch as warehouse-backed
state.

Concurrent runs of the **same** diff sync are unsupported. A UUID is stored in a table label on
scratch; every classification stream checks it before and after reading, and commit checks it
around the atomic copy. If a replacement is detected after promotion, drt attempts to restore the
previous backup (or removes a first-run baseline) and fails loudly. This is best-effort overlap
detection, not a lock: table-label updates and copy jobs cannot share one BigQuery transaction, so
a narrow check-to-operation race remains. Use `drt serve` request coalescing
([#854](https://github.com/drt-hub/drt/issues/854)) or scheduler overlap protection.

## Notes

- Requires `pip install drt-core[bigquery]` (`google-cloud-bigquery`).
- **Query tagging** ([#768](https://github.com/drt-hub/drt/issues/768)): MERGE, mirror, and replace load/query/copy jobs get `labels` (BigQuery's native cost-attribution mechanism, queryable via `INFORMATION_SCHEMA.JOBS`) by default. `mode: insert`'s streaming insert (`insert_rows_json`) is a REST call, not a job, so it isn't labeled — labels are job-scoped. See `query_tagging` in `docs/llm/API_REFERENCE.md`.
- Tables are addressed fully-qualified as `<project>.<dataset>.<table>`.
- The target table must already exist with a compatible schema — drt writes into it, it does not create it.
- `--dry-run` is honoured — `destination.load()` is never called when dry_run is on.
- Same auth convention (`method` / `keyfile`) as the [BigQuery source](../../drt/sources/bigquery.py).
