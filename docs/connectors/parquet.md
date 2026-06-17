# Parquet Destination

> Write records to a Parquet file (columnar, compressed). Requires `pip install drt-core[parquet]`.

## YAML Example

```yaml
destination:
  type: parquet
  path: output/users.parquet
  compression: snappy            # "snappy" (default) | "gzip" | "zstd" | "none"
  # partition_by: [country]      # optional Hive-style partition columns
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"parquet"` | — | Required |
| `path` | string | — | Output file (or directory, when `partition_by` is set). Parent directories are created if missing. **Required** |
| `compression` | `"snappy"` \| `"gzip"` \| `"zstd"` \| `"none"` | `"snappy"` | Parquet column compression codec. |
| `partition_by` | list[str] \| null | null | Columns to partition by (Hive-style `col=value/` directories under `path`). |

## Notes

- Requires `pip install drt-core[parquet]` (pulls in `pandas` + `pyarrow`).
- `snappy` is the best default (fast, splittable); `zstd` gives a higher ratio; `gzip` maximises compatibility.
- With `partition_by`, `path` is treated as a directory and one Parquet file is written per partition value combination — convenient for downstream engines (BigQuery external tables, Athena, Spark) that prune by partition.
- For uploading Parquet to object storage, the [S3](s3.md) / [GCS](gcs.md) / [Azure Blob](azure-blob.md) destinations support `format: parquet` directly.
- `--dry-run` is honoured — nothing is written when dry_run is on.
