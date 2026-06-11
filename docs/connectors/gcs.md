# GCS Destination

> Upload sync batches to a Google Cloud Storage bucket as **CSV / JSON / JSONL / Parquet**
> objects. Optional gzip compression for the text formats; native column
> compression for Parquet.

## YAML Example — CSV with gzip

```yaml
destination:
  type: gcs
  bucket: my-data-exports
  prefix: drt/users/
  format: csv
  compression: gzip
```

Default object name: `<prefix><UTC ISO8601 basic>.<ext>` →
`drt/users/20260605T123000Z.csv.gz`.

## YAML Example — Parquet (BigQuery-ready)

```yaml
destination:
  type: gcs
  bucket: my-data-lake
  prefix: events/
  format: parquet
  parquet_compression: snappy
  project_id: my-gcp-project
```

GCS + Parquet is the canonical staging path for **BigQuery external
tables** and **batch-load** ingestion.

## YAML Example — JSONL with service-account JSON keyfile

```yaml
destination:
  type: gcs
  bucket: events
  prefix: drt/
  format: jsonl
  credentials_path: /run/secrets/gcp-sa.json
  project_id: my-gcp-project
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"gcs"` | — | Required |
| `bucket` | string | — | GCS bucket name |
| `prefix` | string | `""` | Object-name prefix; for per-sync routing, set a unique prefix per sync |
| `format` | `csv` \| `json` \| `jsonl` \| `parquet` | `csv` | Output format |
| `compression` | `none` \| `gzip` | `none` | gzip compresses `csv` / `json` / `jsonl`. Ignored for `parquet` (see `parquet_compression`) |
| `parquet_compression` | `snappy` \| `gzip` \| `zstd` \| `none` | `snappy` | Parquet column compression (Parquet only) |
| `project_id` | string \| null | null | GCP project to scope the client to |
| `credentials_path` | string \| null | null | Service-account JSON keyfile (skips ADC) |
| `key_template` | string \| null | null | Override the object name; supports `{timestamp}` placeholder |

## Authentication

By default the destination uses **Application Default Credentials**
(ADC), which resolves in this order:

1. `GOOGLE_APPLICATION_CREDENTIALS` env var pointing at a JSON keyfile
2. `gcloud auth application-default login` (local development)
3. GCE / GKE / Cloud Run / Cloud Functions attached service account

This works without any YAML changes in the typical GCP-hosted
deployment. For local dev, `gcloud auth application-default login`
once is enough.

**Service-account JSON keyfile** (when ADC isn't available, e.g.
non-GCP CI / cron environments):

```yaml
credentials_path: /run/secrets/gcp-sa.json
```

The service account needs `roles/storage.objectCreator` (or
`roles/storage.objectAdmin` for overwriting via a fixed
`key_template`).

## File naming

Every sync writes one object. The default name is:

```
<prefix><UTC ISO8601 basic>.<ext>
```

— for example `drt/users/20260605T123000Z.csv`. Timestamping the name
(rather than overwriting a fixed name) matches the Census / Hightouch
convention and lets downstream consumers reliably detect "new objects
since last check".

### Custom naming via `key_template`

For per-sync routing, the recommended pattern is to use a sync-specific
**prefix**:

```yaml
destination:
  type: gcs
  bucket: data-lake
  prefix: drt/active_users/   # per-sync prefix
```

For more control, set `key_template`. The only supported placeholder is
`{timestamp}`. If the template includes its own extension, it is used
as-is; otherwise the format-derived extension is appended.

```yaml
# Produces: drt/users/snapshot-20260605T123000Z.csv
key_template: "snapshot-{timestamp}"
```

```yaml
# Produces: drt/users/latest.csv (no timestamp, no auto-extension)
key_template: "latest.csv"
```

## Format details

| Format | Encoding | Notes |
|---|---|---|
| `csv` | UTF-8 | Header row (column names) + `csv.DictWriter`, RFC 4180. Empty rows raise. |
| `json` | UTF-8 | JSON array of objects (`[{...}, {...}]`) |
| `jsonl` | UTF-8 | One JSON object per line — the de-facto data-lake standard |
| `parquet` | binary | Via `pandas` + `pyarrow`. Requires the separate `[parquet]` extra. |

`compression: gzip` is applied AFTER serialisation for the text formats
and sets the GCS object's `Content-Encoding: gzip` metadata. Downstream
consumers that respect Content-Encoding (most HTTP clients, BigQuery
external tables, Dataflow templates) decompress transparently.

## Sync modes

GCS destinations write **one object per sync run**. Because the default
key is timestamped, every run produces a fresh object — there is no
"replace existing data" semantic for a GCS destination. `sync.mode`
values (`full`, `incremental`, `upsert`, `replace`, `mirror`) don't
change GCS behaviour: every batch becomes a row inside the object, and
the object is uploaded once when the batch completes.

If you need "latest snapshot" semantics, point `key_template` at a
fixed name like `latest.csv` — downstream consumers will see an
overwrite each run. Note that this loses re-run replay since prior
exports are gone.

## Notes

- Requires `pip install drt-core[gcs]` (depends on `google-cloud-storage>=2.0`).
- For `format: parquet`, also requires `pip install drt-core[parquet]`.
- Empty batches short-circuit before any `google.cloud` import or GCS
  call — the same "no driver was imported" contract used by the SQL
  destinations. A run with zero source rows produces zero GCS objects.
- Errors during serialisation are recorded in `result.errors` and
  fail the batch. Errors during upload (GCS-side) are recorded as
  `result.failed = len(records)` so the sync's other batches keep
  going.
- IAM bindings must allow `storage.objects.create` on the bucket /
  prefix.

## References

- [GCS upload-from-string API](https://cloud.google.com/python/docs/reference/storage/latest/google.cloud.storage.blob.Blob#google_cloud_storage_blob_Blob_upload_from_string)
- [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
- [Parquet Apache spec](https://parquet.apache.org/)
