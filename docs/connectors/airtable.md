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
| `retry_creates` | bool | `false` | Allow replay of an append (or of an upsert batch with a row lacking a usable `primary_key` value) after a delivery-unknown failure. This can create duplicate rows; irrelevant to fully keyed upserts. |
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

## Retry safety

An append request is a **blind create**. If Airtable may have accepted it but the
response is lost, or a retryable 5xx is returned after delivery, drt sends the
request once and records every row in that chunk as delivery-unknown. Automatic
replay could append the entire chunk twice. A 429 or a proven pre-send connection
failure remains retryable.

Setting `primary_key` changes the request to Airtable's keyed `PATCH`
`performUpsert` operation, so normal retries keep using the identical payload. This holds only when **every** row of the 10-row request has a non-null, non-blank `primary_key` value; a request containing a row without one can create a record, so it is treated like an append (sent once; `retry_creates: true` to replay).
Set `retry_creates: true` only to restore legacy ambiguous append replay with
duplicate-row risk. The optional warehouse ledger helps across later runs, but
cannot resolve whether an in-flight append committed.

## Rate limiting

**Vendor limit:** 5 requests/second **per base** (a 429 locks the base out for 30s). drt applies **no automatic cap** here — set one explicitly:

```yaml
destination:
  type: airtable
  base_id: appXXXXXXXXXXXXXX
  table_name: Contacts
  rate_limit:
    requests_per_second: 5
```

`destination.rate_limit` beats `sync.rate_limit`, which beats the default of 10/s.

The limiter is shared per **base** — `table_name` is deliberately excluded, because Airtable's 5 req/s cap covers every table in the base. Two syncs writing to different tables in one base under `drt run --threads 4` therefore share one bucket, which is what keeps them under the real limit. When they request different rates, the lowest wins for both.

## Notes

- Core connector — no `pip install` extras needed.
- The row dict keys become **Airtable field names**. Airtable is schema-enforcing: a field that doesn't exist on the table, or a value that doesn't match the field type, surfaces as a row error (`on_error: skip` records it; `fail` stops the sync).
- The target table and its fields must already exist — drt writes into them, it does not create columns.
- For upsert, the `primary_key` field must be a real Airtable field that uniquely identifies a row.
- `--dry-run` is honoured — nothing is written when dry_run is on.
