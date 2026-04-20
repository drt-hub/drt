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

## Notes

- Without `body_template`, each record is sent as-is as a JSON object
- Rate limiting and retry are configured in the `sync` section, not the destination
- The generic REST API destination covers any HTTP endpoint — use specific destinations (Slack, HubSpot, etc.) when available for better defaults