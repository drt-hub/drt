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

- **csv** — a header row from the first record's keys, then one row per record across every batch. All records must have the same columns; a mismatch fails the batch instead of producing a misaligned file.
- **json** — the whole sync as a single JSON array (`[ {...}, {...} ]`). Keeping that array valid requires buffering the sync's records in memory and rewriting the complete file after every batch. Memory use therefore grows with the sync's row count; prefer JSONL for large exports.
- **jsonl** — newline-delimited JSON, one object per line. Batches are appended after the first, so this is the streaming-friendly format for large exports and append-friendly downstream.

`datetime` / `Decimal` / `UUID` values are serialised with a `str` fallback so they never break the write.

## Notes

- Core connector — no `pip install` extras needed.
- The first batch of each run rewrites any existing file at `path`; subsequent batches from that run accumulate in the same file. For object-storage equivalents that timestamp each upload, see [s3.md](s3.md) / [gcs.md](gcs.md) / [azure-blob.md](azure-blob.md).
- For columnar output, use the [Parquet destination](parquet.md).
- `--dry-run` is honoured — nothing is written when dry_run is on.
