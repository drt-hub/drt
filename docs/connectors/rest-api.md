# REST API Destination

> Send records to any HTTP endpoint with Jinja2 body templates.

## YAML Example

```yaml
destination:
  type: rest_api
  url: "https://api.example.com/webhook"
  method: POST
  headers:
    Content-Type: "application/json"
  body_template: |
    {
      "user_id": {{ row.id }},
      "name": "{{ row.name }}",
      "email": "{{ row.email }}"
    }
  auth:
    type: bearer
    token_env: API_TOKEN
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"rest_api"` | — | Required |
| `url` | string | — | Target endpoint URL |
| `method` | `GET\|POST\|PUT\|PATCH\|DELETE` | `POST` | HTTP method |
| `headers` | dict | `{}` | Custom HTTP headers |
| `body_template` | string \| null | null | Jinja2 template for request body. Variables accessed as `{{ row.field }}` |
| `body_mode` | `record\|batch` | `record` | Send one request per record or one request per batch sub-chunk |
| `batch_template` | string \| null | null | Required in batch mode. Jinja2 template with the current sub-chunk in `rows` |
| `max_records_per_request` | int \| null | null | Batch-mode request cap; `null` sends the engine-provided chunk in one request |
| `error_path` | string \| null | null | Dotted path to an index-aligned per-record error list in a non-2xx JSON response |
| `auth` | AuthConfig \| null | null | Authentication config (see below) |

## Batch request bodies

Set `body_mode: batch` to send multiple records in each HTTP request. The
engine still applies `sync.batch_size` first; `max_records_per_request` can
split that engine chunk into smaller HTTP requests. When it is omitted, the
whole engine chunk is sent in one request. Rate limiting is applied once per
HTTP request rather than once per record.

```yaml
sync:
  batch_size: 1000

destination:
  type: rest_api
  url: https://api.example.com/v1/users/batch
  method: POST
  headers:
    Content-Type: "application/json"
  body_mode: batch
  max_records_per_request: 100
  batch_template: |
    {"records": {{ rows | tojson_safe }}}
  error_path: data.results
```

Batch mode sends the rendered body as raw bytes, not through an HTTP client
helper that infers a content type from the payload shape — unlike some
JSON-aware client libraries, nothing sets `Content-Type` for you. Set it
explicitly in `headers` (as in the record-mode example above) or a JSON API
will commonly reject the request outright.

`batch_template` uses `rows`, a list of the post-mapping records in the
current HTTP request. It is required in batch mode; `body_template` is only
valid in record mode. `max_records_per_request` must be at least 1.

By default, any non-2xx response fails every record in that request's
sub-chunk. `error_path` opts into per-record failure mapping and follows this
exact contract:

- The response must be JSON, and the dotted path (for example `results` or
  `data.results`) must resolve through object keys to a list.
- The list length must exactly equal the number of records in this HTTP
  request. Mapping is index-aligned within that sub-chunk: element `i`
  describes record `i` in the same request.
- A `null` element means that record succeeded. Any non-null element means it
  failed. For an object, drt takes the first present message key in this order:
  `error`, `message`, `error_message`; otherwise it uses the string form of
  the element.
- If JSON parsing, path traversal, list typing, or list length validation
  fails, drt logs a warning and fails the entire sub-chunk. It never partially
  applies a malformed mapping.

`error_path` is consulted only for non-2xx responses. A 2xx response that
embeds per-item failures (including HTTP 207-style payloads returned with a
2xx status) is currently treated as success; inspecting those responses is a
known limitation.

With `on_error: fail`, drt stops after the first sub-chunk containing any
failure, including one mapped through `error_path`. With `on_error: skip`, it
continues to later sub-chunks. An empty engine batch returns immediately and
does not render `batch_template`, acquire a rate-limit token, or open an HTTP
connection.

## Authentication

Supports four auth types via the `auth` field:

```yaml
# Bearer token
auth:
  type: bearer
  token_env: API_TOKEN

# API key
auth:
  type: api_key
  header: X-API-Key
  value_env: MY_API_KEY

# Basic auth
auth:
  type: basic
  username_env: API_USER
  password_env: API_PASS

# OAuth2 Client Credentials
auth:
  type: oauth2_client_credentials
  token_url: https://auth.example.com/oauth/token
  client_id_env: CLIENT_ID
  client_secret_env: CLIENT_SECRET
```

## Common Patterns

**Webhook with JSON payload:**
```yaml
body_template: '{"event": "new_user", "data": {"name": "{{ row.name }}"}}'
```

**Form-encoded POST (no template — sends record as JSON by default):**
```yaml
destination:
  type: rest_api
  url: "https://api.example.com/users"
  method: POST
```

**PUT upsert with ID in URL (use body_template for the path):**
```yaml
url: "https://api.example.com/users"
method: PUT
body_template: '{"id": {{ row.id }}, "name": "{{ row.name }}"}'
```

## Serializing datetime / Decimal / UUID columns

Jinja2's built-in `tojson` filter calls `json.dumps(value)` with no `default=`, so it raises `Object of type datetime is not JSON serializable` when a row contains a `datetime`, `date`, `Decimal`, or `UUID` (common for BigQuery `TIMESTAMP`, Postgres `numeric` / `uuid`, etc.).

Use the `tojson_safe` filter instead — it encodes the same types as ISO 8601 / string representations:

```yaml
body_template: |
  {
    "name":     {{ row.name | tojson_safe }},
    "metadata": {{ row | tojson_safe }}
  }
```

`tojson_safe` mirrors `tojson` for all JSON-native types (strings, numbers, bool, None, lists, dicts) and additionally handles:

| Python type | Encoded as |
|---|---|
| `datetime`, `date`, `time` | ISO 8601 string (`obj.isoformat()`) |
| `Decimal` | string (`str(obj)`) |
| `UUID` | string (`str(obj)`) |

Anything else still raises `TypeError`, matching `json.dumps`. The default `tojson` filter is unchanged.

## Notes

- In record mode, without `body_template`, each record is sent as-is as a JSON object
- Rate limiting and retry are configured in the `sync` section, not the destination
- The generic REST API destination covers any HTTP endpoint — use specific destinations (Slack, HubSpot, etc.) when available for better defaults

---

# REST API Source

> Pull records from any HTTP endpoint (`profiles.yml` profile, since v0.7).

```yaml
# ~/.drt/profiles.yml
api_users:
  type: rest_api
  url: https://api.example.com/users
  auth:                       # optional — same four auth types as the destination
    type: bearer
    token_env: USERS_API_TOKEN
  pagination:                 # optional — offset | cursor | link_header
    type: offset
    limit: 100
  result_path: data.items     # optional dot-path to the records array in the response
  incremental:                # optional — see below
    start_param: updated_since
```

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"rest_api"` | — | Required |
| `url` | string | — | Endpoint to GET records from |
| `auth` | AuthConfig \| null | null | Bearer / API key / Basic / OAuth2 client credentials |
| `pagination` | PaginationConfig \| null | null | `offset` (offset/limit params), `cursor` (token from the response), or `link_header` (RFC 5988 `Link: rel="next"`); `max_pages` caps all styles (default 100) |
| `result_path` | string \| null | null | Dot-notation path to the records array (defaults: top-level list, `records`, or `data`) |
| `incremental.start_param` | string \| null | null | Incremental extraction — see below |

## Incremental extraction

For `mode: incremental` syncs, set `incremental.start_param` to the query
parameter your API uses for "records changed since". drt injects the sync's
last watermark value into that parameter, so the API filters server-side
instead of re-sending the full endpoint every run:

```yaml
# syncs/users_from_api.yml
name: users_from_api
model: api_users            # decorative for REST sources — the profile defines the endpoint
sync:
  mode: incremental
  cursor_field: updated_at  # record field whose max value becomes the new watermark
  watermark:
    default_value: "2026-01-01T00:00:00Z"   # first-run fallback sent to start_param
```

How the pieces compose:

- **Engine-side cursor tracking is unchanged** — `cursor_field` names the
  record field whose max value is persisted after each run (local
  `.drt/watermarks.json`, or `gcs` / `bigquery` storage).
- On the next run drt requests `GET <url>?updated_since=<last watermark>`.
- The parameter is sent on every page for `offset` / `cursor` / no-pagination
  styles; for `link_header` only on the first request (the server's `next`
  links are authoritative full URLs).
- `--cursor-value` works as usual for bounded backfills.
- Without `incremental.start_param`, `mode: incremental` still tracks the
  watermark but re-extracts the full endpoint every run (drt logs a warning).

## Retry on transient extract failures ([#766](https://github.com/drt-hub/drt/issues/766))

Each page request is retried automatically (3 attempts, exponential backoff from 1s, capped at
60s). No configuration — it is always on. The destination side has had this since
[#277](https://github.com/drt-hub/drt/issues/277); before #766 the source side had none, so a
single 503 on page 40 of a paginated read failed the whole sync.

**Retried:** the status codes in `retryable_status_codes` (429, 500, 502, 503, 504) and
transport-level failures (connect timeouts, DNS, resets). A `Retry-After` header on a 429/503 is
honoured ([#769](https://github.com/drt-hub/drt/issues/769)) — drt waits
`max(retry_after, computed_backoff)`, capped by `max_backoff`.

**Not retried:** 4xx other than 429. A bad request or an unauthorized token won't succeed on
repeat, and retrying it burns your API quota.

⚠️ **Scope: one page request at a time.** A failure *between* pages is retried too, since each
page re-enters the retry loop — but **records already yielded from earlier pages are not
re-read**. They have been loaded into the destination and cannot be un-sent, so a mid-pagination
failure fails the sync with the earlier pages already delivered rather than silently duplicating
them. See [API_REFERENCE](../llm/API_REFERENCE.md#source-side-retry-766) for the full rationale.
