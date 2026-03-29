# drt API Reference

Single-file reference for all configuration fields. Optimized for LLM use — use this to generate valid drt YAML without hallucinating field names.

---

## `drt_project.yml`

```yaml
name: my-project          # required: project identifier
version: "0.1"            # optional, default: "0.1"
profile: default          # optional, default: "default" — maps to ~/.drt/profiles.yml
```

---

## `~/.drt/profiles.yml`

```yaml
default:
  type: bigquery            # "bigquery" | "duckdb" | "postgres"
  project: my-gcp-project   # BigQuery: GCP project ID
  dataset: analytics        # BigQuery: dataset name
  method: application_default  # "application_default" | "keyfile"
  keyfile: ~/.drt/sa.json   # only when method=keyfile

# DuckDB example:
duckdb_local:
  type: duckdb
  database: ./data/local.duckdb
  dataset: main

# PostgreSQL example:
prod_pg:
  type: postgres
  connection_string_env: DATABASE_URL   # env var with postgres:// URL
  dataset: public
```

---

## `syncs/<name>.yml` — Full Schema

```yaml
name: notify_slack          # required: unique sync identifier (matches filename)
description: "..."          # optional: human-readable description
model: ref('new_users')     # required: ref('table') | raw SQL | path to .sql file

destination:                # required: see Destination Configs below
  type: rest_api
  # ... destination-specific fields

sync:                       # optional: all fields have defaults
  mode: full                # "full" (default) | "incremental"
  cursor_field: updated_at  # required when mode=incremental — column name for watermark
  batch_size: 100           # default: 100 — rows per destination call
  on_error: fail            # "fail" (default) | "skip"
  rate_limit:
    requests_per_second: 10 # default: 10
  retry:
    max_attempts: 3         # default: 3
    initial_backoff: 1.0    # default: 1.0 seconds
    backoff_multiplier: 2.0 # default: 2.0
    max_backoff: 60.0       # default: 60.0 seconds
    retryable_status_codes: [429, 500, 502, 503, 504]  # default as shown
```

---

## Destination Configs

### `type: rest_api`

```yaml
destination:
  type: rest_api
  url: "https://hooks.example.com/webhook"   # required
  method: POST                               # "GET"|"POST"|"PUT"|"PATCH"|"DELETE", default: POST
  headers:                                   # optional dict
    Content-Type: "application/json"
    X-Custom-Header: "value"
  body_template: |                           # optional Jinja2 template → request body
    {
      "user_id": "{{ row.id }}",
      "email": "{{ row.email }}"
    }
  auth:                                      # optional — see Auth Configs
    type: bearer
    token_env: MY_API_TOKEN
```

### `type: slack`

```yaml
destination:
  type: slack
  webhook_url: "https://hooks.slack.com/..."   # provide webhook_url OR webhook_url_env
  webhook_url_env: SLACK_WEBHOOK_URL           # env var name
  message_template: "New user: {{ row.name }} ({{ row.email }})"  # Jinja2, default: "{{ row }}"
  block_kit: false                             # true = message_template is Block Kit JSON
```

Block Kit example:
```yaml
  block_kit: true
  message_template: |
    {
      "blocks": [
        {
          "type": "section",
          "text": {"type": "mrkdwn", "text": "*New user:* {{ row.name }}"}
        }
      ]
    }
```

### `type: github_actions`

```yaml
destination:
  type: github_actions
  owner: myorg                    # required: GitHub org or user
  repo: myapp                     # required: repository name
  workflow_id: deploy.yml         # required: workflow filename or numeric ID
  ref: main                       # default: "main" — branch/tag to run on
  inputs_template: |              # optional Jinja2 template → JSON object for workflow inputs
    {
      "environment": "{{ row.env }}",
      "version": "{{ row.version }}"
    }
  auth:
    type: bearer
    token_env: GITHUB_TOKEN       # needs actions:write permission
```

### `type: hubspot`

```yaml
destination:
  type: hubspot
  object_type: contacts           # "contacts" | "deals" | "companies", default: "contacts"
  id_property: email              # default: "email" — upsert deduplication key
  properties_template: |          # optional Jinja2 template → JSON object of HubSpot properties
    {
      "email": "{{ row.email }}",
      "firstname": "{{ row.first_name }}",
      "lastname": "{{ row.last_name }}",
      "company": "{{ row.company }}"
    }
  auth:
    type: bearer
    token_env: HUBSPOT_TOKEN      # Private App token with CRM write scope
```

---

## Auth Configs

Auth configs are used inside destination configs under the `auth:` key.

### Bearer Token

```yaml
auth:
  type: bearer
  token_env: MY_TOKEN     # recommended: name of env var containing the token
  token: "sk-..."         # not recommended: hardcoded token (use token_env instead)
```

→ Sends `Authorization: Bearer <token>` header.

### API Key

```yaml
auth:
  type: api_key
  header: X-API-Key       # default: "X-API-Key" — header name
  value_env: MY_API_KEY   # recommended: env var name
  value: "abc123"         # not recommended: hardcoded value
```

→ Sends `<header>: <value>` header.

### Basic Auth

```yaml
auth:
  type: basic
  username_env: API_USERNAME   # required: env var name
  password_env: API_PASSWORD   # required: env var name
```

→ Sends `Authorization: Basic <base64(username:password)>` header.

---

## Complete Examples

### Slack notification — incremental

```yaml
name: new_user_slack
description: "Notify Slack when new users sign up"
model: ref('users')

destination:
  type: slack
  webhook_url_env: SLACK_WEBHOOK_URL
  message_template: ":wave: New user: *{{ row.name }}* ({{ row.email }})"

sync:
  mode: incremental
  cursor_field: created_at
  batch_size: 50
  on_error: skip
  rate_limit:
    requests_per_second: 5
```

### HubSpot contacts upsert — full

```yaml
name: sync_contacts_hubspot
description: "Keep HubSpot contacts in sync with DWH"
model: ref('active_customers')

destination:
  type: hubspot
  object_type: contacts
  id_property: email
  properties_template: |
    {
      "email": "{{ row.email }}",
      "firstname": "{{ row.first_name }}",
      "lastname": "{{ row.last_name }}",
      "company": "{{ row.company_name }}",
      "lifecyclestage": "customer"
    }
  auth:
    type: bearer
    token_env: HUBSPOT_TOKEN

sync:
  mode: full
  batch_size: 100
  on_error: skip
  retry:
    max_attempts: 5
    initial_backoff: 2.0
```

### GitHub Actions deploy trigger

```yaml
name: trigger_deploy
description: "Trigger deploy workflow for approved releases"
model: "SELECT env, version FROM releases WHERE approved = true AND deployed = false"

destination:
  type: github_actions
  owner: myorg
  repo: myapp
  workflow_id: deploy.yml
  ref: main
  inputs_template: |
    {
      "environment": "{{ row.env }}",
      "version": "{{ row.version }}"
    }
  auth:
    type: bearer
    token_env: GITHUB_TOKEN

sync:
  mode: incremental
  cursor_field: approved_at
  on_error: fail
```

### REST API with custom auth header

```yaml
name: push_to_webhook
model: ref('events')

destination:
  type: rest_api
  url: "https://api.example.com/events"
  method: POST
  headers:
    Content-Type: "application/json"
  body_template: |
    {
      "event_id": "{{ row.id }}",
      "type": "{{ row.event_type }}",
      "occurred_at": "{{ row.created_at }}"
    }
  auth:
    type: api_key
    header: X-API-Key
    value_env: EXAMPLE_API_KEY

sync:
  batch_size: 50
  rate_limit:
    requests_per_second: 20
  retry:
    max_attempts: 3
    retryable_status_codes: [429, 500, 502, 503, 504]
  on_error: skip
```
