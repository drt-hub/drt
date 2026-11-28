# Elasticsearch / OpenSearch Destination

> Bulk-index DWH rows into Elasticsearch (or API-compatible OpenSearch)
> via the `_bulk` API — one HTTP round-trip per batch, powering search
> UIs and dashboards (Kibana / OpenSearch Dashboards). Uses only core
> `httpx`; no client library, no extra install.

## YAML Example

```yaml
destination:
  type: elasticsearch
  url: https://localhost:9200
  api_key_env: ES_API_KEY
  index: customers
  id_field: user_id        # row field → document _id (omit for auto-generated ids)
  op_type: index           # "index" (upsert) | "create" (insert-only)
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"elasticsearch"` | — | Required |
| `url` | string | — | Cluster base URL (e.g. `https://localhost:9200`) |
| `index` | string | — | Target index name |
| `id_field` | string \| null | null | Row field whose value becomes the document `_id`. Omit → the cluster auto-generates ids |
| `op_type` | `index` \| `create` | `index` | `index` upserts (replace-if-exists); `create` inserts only (409 if the `_id` exists) |
| `api_key` | string \| null | null | API key (direct value) |
| `api_key_env` | string \| null | null | Env var holding the API key |
| `username_env` | string \| null | null | Env var holding the HTTP Basic username |
| `password_env` | string \| null | null | Env var holding the HTTP Basic password |
| `verify_tls` | bool | `true` | Set `false` for self-signed dev clusters |
| `retry_creates` | bool | `false` | Allow replay of an auto-id bulk request after delivery becomes unknown. This can duplicate the entire batch; irrelevant when every item has an explicit `_id`. |
| `retry` | object \| null | null | Per-destination retry override (see `sync.retry`) |

## Authentication

Provide **one** of:

**API key** (recommended) — `Authorization: ApiKey <key>`:
```yaml
api_key_env: ES_API_KEY
```
```bash
export ES_API_KEY="VnVhQ2ZHY0JDZGJrU..."   # base64 id:api_key from the cluster
```

**HTTP Basic** — username + password:
```yaml
username_env: ES_USER
password_env: ES_PASS
```

Without either, the sync fails fast with a clear config error.

## Document shape

Each source row becomes one document. The **whole row** is the
document `_source`. If `id_field` is set, that field's value becomes
the `_id` (so re-runs update the same document under `op_type: index`);
without it, the cluster assigns a random id.

```yaml
# Row: {"user_id": "u1", "name": "Alice", "tier": "vip"}
# →  POST /_bulk
#    {"index": {"_index": "customers", "_id": "u1"}}
#    {"user_id": "u1", "name": "Alice", "tier": "vip"}
```

## Op types

| `op_type` | Behaviour | Use when |
|-----------|-----------|----------|
| `index` (default) | Upsert — replaces the document if `_id` already exists | Keeping an index in sync with a source table |
| `create` | Insert-only — a row whose `_id` already exists fails with 409 | Append-only event/log indices where duplicates are errors |

## Retry safety

Retry classification is per `_bulk` request. When every item has an explicit
`_id` from `id_field`, the request is **keyed** and normal retries reuse the exact
NDJSON body. This covers both `index` and `create`; an insert-only retry may return
the usual per-item 409 after the first attempt committed, and drt keeps recording
that 409 as a row error.

If `id_field` is omitted, or any row in the batch has no value for it, the whole
request uses auto-generated IDs and is a **blind create**. A lost response or
post-send retryable 5xx is sent once and every row in the request gets a
delivery-unknown error, because replay could create a second document for every
committed item. A 429 and proven pre-send connection failure remain retryable.
`retry_creates: true` restores legacy ambiguous replay with whole-batch duplicate
risk.

## Per-document error handling

The `_bulk` API returns **HTTP 200 even when individual documents
fail** — it sets `"errors": true` and a per-item `error`. drt parses
the `items` array and maps each failure back to its source row, so:

- A 409 on one `create` row → that row lands in `result.row_errors`
  (with the cluster's reason), and the rest of the batch still indexes.
- A mapping/parse error on one document → same — isolated to that row.
- A whole-batch HTTP failure (401 auth, 5xx after retries) → every row
  in the batch is recorded as failed.

`on_error: skip` (the typical choice here) keeps the sync going past
per-row failures. With `on_error: fail`, drt still accounts for every item in the
already-completed bulk response, then stops before sending the next batch.

## OpenSearch

OpenSearch shares the same `_bulk` API surface — point `url` at your
OpenSearch endpoint and it works unchanged. For local OpenSearch with
the bundled self-signed certificate, set `verify_tls: false`.

## Notes

- (core) — no extra install; uses `httpx`, already a core dependency.
- One `_bulk` POST per `sync.batch_size` rows. Tune `batch_size` to your
  cluster's `http.max_content_length` (default 100 MB) and document size.
- Index template / mapping management and ILM policies are out of scope
  — create the index (and any mappings) on the cluster side first.
- Use `drt run --dry-run` to preview the rows that would be indexed
  before a real run.

## References

- [Elasticsearch Bulk API](https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html)
- [OpenSearch Bulk](https://opensearch.org/docs/latest/api-reference/document-apis/bulk/)
- [Create an API key](https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-create-api-key.html)
