# drt — LLM Context

This document is optimized for LLM consumption. It gives you the full context needed to help users configure, debug, and extend drt.

## What is drt?

**drt** (data reverse tool) is a CLI tool that syncs data from a data warehouse to external services, declaratively via YAML.

```
dlt (load into DWH) → dbt (transform) → drt (activate out of DWH)
```

- **Category:** Reverse ETL
- **Tagline:** "Reverse ETL for the code-first data stack"
- **Install:** `pip install drt-core` or `uv add drt-core`
- **Package name:** `drt-core` (PyPI) — CLI command is `drt`

## What drt is NOT

- Not a data loader (that's dlt)
- Not a transformer (that's dbt)
- Not a scheduler — it runs via CLI or cron, not a built-in scheduler
- Not a SaaS — fully self-hosted OSS (Apache 2.0)

## Architecture

```
drt_project.yml          # project config (source profile)
syncs/*.yml              # one file per sync definition

CLI (drt run)
  → Config Parser        # parse + validate YAML via Pydantic
  → Source               # extract rows from DWH
  → Engine (sync.py)     # batch, orchestrate, track cursor
  → Destination          # load rows to external service
  → State Manager        # persist last run result to .drt/state.json
```

## Project Structure

```
my-project/
├── drt_project.yml       # required: project name + source profile
├── syncs/
│   ├── notify_slack.yml  # one sync per file
│   └── update_hubspot.yml
└── syncs/models/
    └── active_users.sql  # optional: custom SQL (overrides ref())
```

## Sources (where data comes from)

| Source | Extra | Notes |
|--------|-------|-------|
| BigQuery | `drt-core[bigquery]` | Uses ADC or keyfile |
| DuckDB | (core) | Local `.duckdb` file |
| PostgreSQL | `drt-core[postgres]` | Connection string via env |

Source is configured in `~/.drt/profiles.yml` (dbt-style):

```yaml
default:
  type: bigquery
  project: my-gcp-project
  dataset: analytics
```

## Destinations (where data goes)

| Destination | `type` value | Notes |
|-------------|-------------|-------|
| REST API (generic) | `rest_api` | Any HTTP endpoint |
| Slack Webhook | `slack` | Incoming webhook |
| GitHub Actions | `github_actions` | workflow_dispatch trigger |
| HubSpot CRM | `hubspot` | Contacts / Deals / Companies upsert |

## CLI Commands

```bash
drt init                          # interactive project wizard
drt list                          # list sync definitions
drt validate                      # validate all sync YAMLs
drt run                           # run all syncs
drt run --select <sync-name>      # run one sync
drt run --dry-run                 # preview without writing data
drt status                        # show recent sync results
```

## Key Concepts

### Sync Modes

**Full sync** (default): Extract all rows and send to destination on every run.

**Incremental sync**: Extract only new/updated rows using a watermark column.
- Set `sync.mode: incremental` and `sync.cursor_field: <column>`
- drt saves `last_cursor_value` in `.drt/state.json` after each run
- Next run automatically injects `WHERE <cursor_field> > '<last_value>'`

### Model Reference

The `model` field in a sync can be:
- `ref('table_name')` — expands to `SELECT * FROM <dataset>.<table_name>`
- Raw SQL — `SELECT id, email FROM analytics.users WHERE active = true`
- drt checks `syncs/models/<name>.sql` first; if found, uses that file's content

### Jinja2 Templates

Destination configs support Jinja2 templating with `{{ row.<field> }}`:

```yaml
body_template: |
  {"text": "New user: {{ row.name }} ({{ row.email }})"}
```

The `row` variable contains all columns from the current record as a dict.

### Error Handling

- `on_error: fail` (default) — stop the entire sync on first failure
- `on_error: skip` — log the error, continue with remaining records

### Rate Limiting

```yaml
sync:
  rate_limit:
    requests_per_second: 10  # default: 10
```

### Retry

```yaml
sync:
  retry:
    max_attempts: 3           # default: 3
    initial_backoff: 1.0      # seconds
    backoff_multiplier: 2.0   # exponential: 1s, 2s, 4s...
    max_backoff: 60.0         # cap at 60s
    retryable_status_codes: [429, 500, 502, 503, 504]
```

## State File

`.drt/state.json` stores the result of the last run per sync:

```json
{
  "notify_slack": {
    "sync_name": "notify_slack",
    "last_run_at": "2026-03-30T12:00:00",
    "records_synced": 42,
    "status": "success",
    "last_cursor_value": "2026-03-30T11:59:00"
  }
}
```
