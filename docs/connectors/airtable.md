# Airtable Destination

> Write records into an Airtable base table — append or upsert. Core connector — no extra install (uses `httpx`).

## YAML Example

```yaml
destination:
  type: airtable
  access_token_env: AIRTABLE_TOKEN
  base_id: appXXXXXXXXXXXXXX
  table_name: Customers
  primary_key: record_id        # omit for append-only
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"airtable"` | — | Required |
| `access_token` / `access_token_env` | string \| null | `access_token_env: AIRTABLE_TOKEN` | Personal access token (Bearer). Prefer the `_env` form. One is required. |
| `base_id` | string | — | Airtable base ID (`app…`). **Required** |
| `table_name` | string | — | Target table name (URL-encoded automatically). **Required** |
| `primary_key` | string \| null | null | When set, records are **upserted** by matching this field (Airtable `performUpsert.fieldsToMergeOn`). Omit for **append-only**. |
| `retry` | RetryConfig \| null | null | Per-destination override of `sync.retry`. |

## Authentication

Create a [personal access token](https://airtable.com/create/tokens) with the `data.records:write` scope on your base, then:

```bash
export AIRTABLE_TOKEN="pat..."
```

## Write modes

- **Append** (no `primary_key`) — `POST /v0/{base_id}/{table_name}` with `{"records": [{"fields": {...}}, ...]}`.
- **Upsert** (`primary_key` set) — `PATCH` with `performUpsert.fieldsToMergeOn: [<primary_key>]`; rows matched on that field are updated, the rest inserted.

Airtable caps batch writes at **10 records per request**, so the sync batch is automatically chunked into groups of 10 (one HTTP request each). Use `sync.rate_limit` to stay within Airtable's 5 req/s per-base limit.

## Notes

- Core connector — no `pip install` extras needed.
- The row dict keys become **Airtable field names**. Airtable is schema-enforcing: a field that doesn't exist on the table, or a value that doesn't match the field type, surfaces as a row error (`on_error: skip` records it; `fail` stops the sync).
- The target table and its fields must already exist — drt writes into them, it does not create columns.
- For upsert, the `primary_key` field must be a real Airtable field that uniquely identifies a row.
- `--dry-run` is honoured — nothing is written when dry_run is on.
