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
| `auth` | AuthConfig \| null | null | Authentication config (see below) |

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

- Without `body_template`, each record is sent as-is as a JSON object
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