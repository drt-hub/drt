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
  type: bigquery            # "bigquery" | "duckdb" | "sqlite" | "postgres" | "redshift" | "clickhouse"
  project: my-gcp-project   # BigQuery: GCP project ID
  dataset: analytics        # BigQuery: dataset name
  location: US              # optional: "US" (default), "EU", "asia-northeast1", etc.
  method: application_default  # "application_default" | "keyfile"
  keyfile: ~/.drt/sa.json   # only when method=keyfile

# DuckDB example:
duckdb_local:
  type: duckdb
  database: ./data/local.duckdb
  dataset: main

# SQLite example:
sqlite_local:
  type: sqlite
  database: ./data/local.db     # path to .sqlite/.db file, or ":memory:"

# PostgreSQL example:
prod_pg:
  type: postgres
  connection_string_env: DATABASE_URL   # env var with postgres:// URL
  dataset: public

# Redshift example:
redshift_prod:
  type: redshift
  host: my-cluster.xxx.us-east-1.redshift.amazonaws.com
  port: 5439              # default: 5439
  dbname: analytics
  user: analyst
  password_env: REDSHIFT_PASSWORD
  schema: public          # default: "public"

# ClickHouse example:
ch_prod:
  type: clickhouse
  host: localhost
  port: 8123              # default: 8123 (HTTP interface)
  database: default
  user: default
  password_env: CLICKHOUSE_PASSWORD
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
  mode: full                # "full" (default) | "incremental" | "upsert"  # "upsert" is a semantic alias for "full" when upsert_key is set
  cursor_field: updated_at  # required when mode=incremental — column name for watermark
  batch_size: 100           # default: 100 — rows per destination call
  on_error: fail            # "fail" (default) | "skip"
  rate_limit:
    requests_per_second: 10 # default: 10 — set to 0 to disable rate limiting
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

### `type: discord`

```yaml
destination:
  type: discord
  webhook_url: "https://discord.com/api/webhooks/..."  # provide webhook_url OR webhook_url_env
  webhook_url_env: DISCORD_WEBHOOK_URL                 # env var name
  message_template: "New user: {{ row.name }} ({{ row.email }})"  # Jinja2, default: "{{ row }}"
  embeds: false                                        # true = message_template is embeds JSON
```

Embeds example:
```yaml
  embeds: true
  message_template: |
    {
      "embeds": [
        {
          "title": "{{ row.title }}",
          "description": "{{ row.description }}",
          "color": 3447003
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

### `type: google_sheets`

```yaml
destination:
  type: google_sheets
  spreadsheet_id: "1BxiMVs0XRA5nFMd..."   # required: Google Sheets ID from URL
  sheet: "Sheet1"                           # default: "Sheet1"
  mode: overwrite                           # "overwrite" (default) | "append"
  credentials_path: /path/to/sa-key.json   # service account JSON keyfile
  credentials_env: GOOGLE_SA_KEY_PATH      # or: env var pointing to keyfile
```

> `overwrite` clears the sheet then writes header + data rows. `append` adds data rows only.

### `type: postgres` (destination)

```yaml
# Option A: connection string via env var
destination:
  type: postgres
  connection_string_env: DATABASE_URL  # env var with postgres://user:pass@host:5432/dbname
  table: public.analytics_scores       # required: target table
  upsert_key: [id]                     # required: columns for ON CONFLICT

# Option B: individual parameters
destination:
  type: postgres
  host_env: TARGET_PG_HOST           # env var for host (or use host:)
  port: 5432                         # default: 5432
  dbname_env: TARGET_PG_DBNAME       # env var for database name
  user_env: TARGET_PG_USER           # env var for user
  password_env: TARGET_PG_PASSWORD   # env var for password
  table: public.analytics_scores     # required: target table
  upsert_key: [id]                   # required: columns for ON CONFLICT
  ssl:                               # optional: SSL/TLS connection
    enabled: true
    ca_env: PG_SSL_CA                # env var for CA cert path
    cert_env: PG_SSL_CERT            # env var for client cert path
    key_env: PG_SSL_KEY              # env var for client key path
```

> Uses `INSERT ... ON CONFLICT (upsert_key) DO UPDATE SET ...` for idempotent writes.
> `connection_string_env` takes precedence over individual parameters when both are set.

### `type: mysql`

```yaml
# Option A: connection string via env var
destination:
  type: mysql
  connection_string_env: MYSQL_URL     # env var with mysql://user:pass@host:3306/dbname
  table: analytics.scores              # required: target table
  upsert_key: [id]                     # required: columns for ON DUPLICATE KEY

# Option B: individual parameters
destination:
  type: mysql
  host_env: TARGET_MYSQL_HOST        # env var for host
  port: 3306                         # default: 3306
  database_env: TARGET_MYSQL_DB      # env var for database
  user_env: TARGET_MYSQL_USER        # env var for user
  password_env: TARGET_MYSQL_PASS    # env var for password
  table: analytics.scores            # required: target table
  upsert_key: [id]                   # required: columns for ON DUPLICATE KEY
  ssl:                               # optional: SSL/TLS connection
    enabled: true
    ca_env: MYSQL_SSL_CA             # env var for CA cert path
    cert_env: MYSQL_SSL_CERT         # env var for client cert path
    key_env: MYSQL_SSL_KEY           # env var for client key path
```

> Uses `INSERT ... ON DUPLICATE KEY UPDATE ...` for idempotent writes.
> `connection_string_env` takes precedence over individual parameters when both are set.

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

### Discord notification — incremental

```yaml
name: new_order_discord
description: "Notify Discord when new orders arrive"
model: ref('orders')

destination:
  type: discord
  webhook_url_env: DISCORD_WEBHOOK_URL
  message_template: ":package: New order #{{ row.order_id }} from {{ row.customer_name }} (${{ row.total }})"

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

### Google Sheets export — overwrite

```yaml
name: export_to_sheets
description: "Export user data to Google Sheets"
model: ref('users')

destination:
  type: google_sheets
  spreadsheet_id: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms"
  sheet: "Sheet1"
  mode: overwrite
  credentials_path: /path/to/sa-key.json

sync:
  mode: full
  batch_size: 100
```

### PostgreSQL upsert

```yaml
name: sync_scores
description: "Upsert analytics scores to target Postgres"
model: ref('user_scores')

destination:
  type: postgres
  host_env: TARGET_PG_HOST
  dbname_env: TARGET_PG_DBNAME
  user_env: TARGET_PG_USER
  password_env: TARGET_PG_PASSWORD
  table: public.analytics_scores
  upsert_key: [user_id]

sync:
  mode: incremental
  cursor_field: updated_at
  on_error: skip
```

### MySQL upsert

```yaml
name: sync_leads_mysql
description: "Upsert lead scores to target MySQL"
model: ref('lead_scores')

destination:
  type: mysql
  host_env: TARGET_MYSQL_HOST
  database_env: TARGET_MYSQL_DB
  user_env: TARGET_MYSQL_USER
  password_env: TARGET_MYSQL_PASS
  table: marketing.lead_scores
  upsert_key: [lead_id]
  ssl:
    enabled: true
    ca_env: MYSQL_SSL_CA

sync:
  mode: upsert
  batch_size: 200
  on_error: skip
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
