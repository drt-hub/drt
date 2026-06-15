# File Destination

> Write records to a local file as CSV, JSON, or JSONL. Part of core — no extra install.

## YAML Example

```yaml
destination:
  type: file
  path: output/users.csv
  format: csv          # "csv" (default) | "json" | "jsonl"
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"file"` | — | Required |
| `path` | string | — | Output file path (relative to the working directory or absolute). Parent directories are created if missing. **Required** |
| `format` | `"csv"` \| `"json"` \| `"jsonl"` | `"csv"` | Output format. `csv` = header + rows; `json` = a single JSON array; `jsonl` = one JSON object per line. |

## Formats

- **csv** — a header row from the first record's keys, then one row per record.
- **json** — the whole batch as a single JSON array (`[ {...}, {...} ]`).
- **jsonl** — newline-delimited JSON, one object per line (streaming-friendly, append-friendly downstream).

`datetime` / `Decimal` / `UUID` values are serialised with a `str` fallback so they never break the write.

## Notes

- Core connector — no `pip install` extras needed.
- The file is rewritten each run (the batch is written in full). For object-storage equivalents that timestamp each upload, see [s3.md](s3.md) / [gcs.md](gcs.md) / [azure-blob.md](azure-blob.md).
- For columnar output, use the [Parquet destination](parquet.md).
- `--dry-run` is honoured — nothing is written when dry_run is on.
