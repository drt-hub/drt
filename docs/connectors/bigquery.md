# BigQuery Destination

> INSERT (append) or MERGE (upsert) rows into BigQuery tables using `google-cloud-bigquery`.

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

The principal needs `bigquery.tables.updateData` on the target table (plus `bigquery.tables.create` + `bigquery.jobs.create` on the dataset for the merge-path temp table).

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

then drops the temp table. Composite keys are supported (`upsert_key: [tenant_id, user_id]` → AND-joined `ON`). When every column is in `upsert_key`, the `WHEN MATCHED` UPDATE is skipped (effectively insert-if-not-exists). Because BigQuery load + MERGE are **job-level** operations, merge error handling is **batch-level** (the whole batch succeeds or fails) — coarser than the per-row staging used by the Snowflake / Databricks destinations.

## Sync modes

| `sync.mode` | Behaviour on BigQuery |
|---|---|
| `full` | Re-extracts every run + writes via `config.mode` (insert / merge). |
| `incremental` | Watermark-based — extracts rows with `cursor_field > last_value`, writes via `config.mode`. |
| `upsert` | Same as `incremental` with `upsert_key` enforced. |
| `mirror` / `replace` | Not yet supported on BigQuery — follow-ups (the temp-table + MERGE machinery is the natural basis for both). |

## Notes

- Requires `pip install drt-core[bigquery]` (`google-cloud-bigquery`).
- Tables are addressed fully-qualified as `<project>.<dataset>.<table>`.
- The target table must already exist with a compatible schema — drt writes into it, it does not create it.
- `--dry-run` is honoured — `destination.load()` is never called when dry_run is on.
- Same auth convention (`method` / `keyfile`) as the [BigQuery source](../../drt/sources/bigquery.py).
