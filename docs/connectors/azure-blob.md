# Azure Blob Destination

> Upload sync batches to an Azure Blob Storage container as **CSV / JSON / JSONL / Parquet**
> blobs. Optional gzip compression for the text formats; native column
> compression for Parquet. Completes the cloud-storage trio alongside
> [S3](s3.md) and [GCS](gcs.md).

## YAML Example — CSV with gzip (connection string)

```yaml
destination:
  type: azure_blob
  container: data-exports
  prefix: drt/users/
  format: csv
  compression: gzip
  connection_string_env: AZURE_STORAGE_CONNECTION_STRING
```

Default blob name: `<prefix><UTC ISO8601 basic>.<ext>` →
`drt/users/20260605T123000Z.csv.gz`.

## YAML Example — Parquet via managed identity

```yaml
destination:
  type: azure_blob
  container: data-lake
  prefix: events/
  format: parquet
  parquet_compression: snappy
  account_url: https://mystorage.blob.core.windows.net
```

When `account_url` is set without `connection_string_env`, drt
authenticates via `DefaultAzureCredential` — the right shape for
apps running on Azure with a managed identity (App Service, AKS,
Container Apps, VMs, Functions).

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"azure_blob"` | — | Required |
| `container` | string | — | Container name |
| `prefix` | string | `""` | Blob-name prefix; for per-sync routing, set a unique prefix per sync |
| `format` | `csv` \| `json` \| `jsonl` \| `parquet` | `csv` | Output format |
| `compression` | `none` \| `gzip` | `none` | gzip compresses `csv` / `json` / `jsonl`. Ignored for `parquet` (see `parquet_compression`) |
| `parquet_compression` | `snappy` \| `gzip` \| `zstd` \| `none` | `snappy` | Parquet column compression (Parquet only) |
| `connection_string_env` | string \| null | null | Env var name holding the storage-account connection string |
| `account_url` | string \| null | null | Storage account blob endpoint (`https://<account>.blob.core.windows.net`) for `DefaultAzureCredential` |
| `key_template` | string \| null | null | Override the blob name; supports `{timestamp}` placeholder |

## Authentication

Two paths. Exactly one of `connection_string_env` or `account_url`
must be set.

### Connection string (most common)

Set `connection_string_env` to the name of an env var holding the
storage-account connection string. drt resolves the env var at
runtime and passes it to `BlobServiceClient.from_connection_string`.

```yaml
connection_string_env: AZURE_STORAGE_CONNECTION_STRING
```

```bash
export AZURE_STORAGE_CONNECTION_STRING="DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...;EndpointSuffix=core.windows.net"
```

This is the right shape for CI / cron / non-Azure-hosted deployments.

### DefaultAzureCredential chain (Azure-hosted apps)

Set `account_url` to the storage account's blob endpoint and leave
`connection_string_env` unset. drt uses `DefaultAzureCredential`,
which resolves in this order:

1. Environment variables (`AZURE_CLIENT_ID` / `AZURE_TENANT_ID` / `AZURE_CLIENT_SECRET`)
2. Workload identity (AKS)
3. Managed identity (App Service / Functions / Container Apps / VMs)
4. Azure CLI (`az login`) — local dev
5. Azure PowerShell, Azure Developer CLI, VS Code

```yaml
account_url: https://mystorage.blob.core.windows.net
```

The credential needs `Storage Blob Data Contributor` (or finer-grained
`Storage Blob Delegator` + per-container ACL) on the storage account.

## File naming

Every sync writes one blob. The default name is:

```
<prefix><UTC ISO8601 basic>.<ext>
```

— for example `drt/users/20260605T123000Z.csv`. Timestamping the name
(rather than overwriting a fixed name) matches the S3 / GCS convention
and lets downstream consumers reliably detect "new blobs since last
check".

> `upload_blob` is called with `overwrite=True`. With the default
> timestamped key, this never matters (a fresh blob name every run).
> If you set `key_template` to a fixed name like `latest.csv`, each
> run overwrites the previous blob — intentional, see "Sync modes"
> below.

### Custom naming via `key_template`

For per-sync routing, the recommended pattern is to use a sync-specific
**prefix**:

```yaml
destination:
  type: azure_blob
  container: data-lake
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
and sets the blob's `Content-Encoding: gzip` metadata (via Azure's
`ContentSettings`). Downstream consumers that respect Content-Encoding
(most HTTP clients, Azure Data Factory, Synapse copy activities,
Databricks `read.format("csv")`) decompress transparently.

## Sync modes

Azure Blob destinations write **one blob per sync run**. Because the
default key is timestamped, every run produces a fresh blob — there is
no "replace existing data" semantic. `sync.mode` values (`full`,
`incremental`, `upsert`, `replace`, `mirror`) don't change behaviour:
every batch becomes a row inside the blob, and the blob is uploaded
once when the batch completes.

If you need "latest snapshot" semantics, point `key_template` at a
fixed name like `latest.csv` — downstream consumers will see an
overwrite each run. Note that this loses re-run replay since prior
exports are gone.

## Notes

- Requires `pip install drt-core[azure]` (depends on `azure-storage-blob>=12.0` and `azure-identity>=1.15`).
- For `format: parquet`, also requires `pip install drt-core[parquet]`.
- Empty batches short-circuit before any `azure.storage.blob` import
  or Azure call — the same "no driver was imported" contract used by
  the SQL destinations. A run with zero source rows produces zero blobs.
- Errors during serialisation are recorded in `result.errors` and
  fail the batch. Errors during upload (Azure-side) are recorded as
  `result.failed = len(records)` so the sync's other batches keep
  going.
- Configuration errors (`connection_string_env` resolves empty,
  neither `connection_string_env` nor `account_url` set) raise
  immediately rather than silently producing a broken client.

## References

- [Azure Blob Storage upload_blob API](https://learn.microsoft.com/en-us/python/api/azure-storage-blob/azure.storage.blob.blobclient?view=azure-python#azure-storage-blob-blobclient-upload-blob)
- [DefaultAzureCredential](https://learn.microsoft.com/en-us/python/api/overview/azure/identity-readme?view=azure-python#defaultazurecredential)
- [Parquet Apache spec](https://parquet.apache.org/)
