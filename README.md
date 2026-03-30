<picture>
  <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/drt-hub/.github/main/profile/assets/logo-dark.svg">
  <img src="https://raw.githubusercontent.com/drt-hub/.github/main/profile/assets/logo.svg" alt="drt logo" width="200">
</picture>

# drt — data reverse tool

> Reverse ETL for the code-first data stack.

[![CI](https://github.com/drt-hub/drt/actions/workflows/ci.yml/badge.svg)](https://github.com/drt-hub/drt/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/drt-core)](https://pypi.org/project/drt-core/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)
[![Python](https://img.shields.io/pypi/pyversions/drt-core)](https://pypi.org/project/drt-core/)

**drt** syncs data from your data warehouse to external services — declaratively, via YAML and CLI.
Think `dbt run` → `drt run`. Same developer experience, opposite data direction.

```bash
# pip
pip install drt-core          # core (DuckDB included)
pip install drt-core[bigquery]  # + BigQuery

# uv (recommended)
uv add drt-core
uv add drt-core[bigquery]

drt init
drt run --dry-run
drt run
```

---

## Why drt?

| Problem | drt's answer |
|---------|-------------|
| Census/Hightouch are expensive SaaS | Free, self-hosted OSS |
| GUI-first tools don't fit CI/CD | CLI + YAML, Git-native |
| dbt/dlt ecosystem has no reverse leg | Same philosophy, same DX |
| LLM/MCP era makes GUI SaaS overkill | LLM-native by design |

---

## Quickstart

### 1. Install

```bash
pip install drt-core[bigquery]
# or
uv add drt-core[bigquery]
```

### 2. Initialize a project

```bash
mkdir my-drt-project && cd my-drt-project
drt init
```

This creates:

```
my-drt-project/
├── drt_project.yml   # project config
└── syncs/            # put your sync definitions here
```

`drt init` prompts for source type: **bigquery**, **duckdb**, or **postgres**.

### 3. Create a sync

```yaml
# syncs/notify_slack.yml
name: notify_slack
description: "Notify Slack on new users"
model: ref('new_users')
destination:
  type: rest_api
  url: "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
  method: POST
  headers:
    Content-Type: "application/json"
  body_template: |
    { "text": "New user: {{ row.name }} ({{ row.email }})" }
sync:
  mode: full
  batch_size: 100
  rate_limit:
    requests_per_second: 5
  on_error: skip
```

### 4. Run

```bash
drt run --dry-run        # preview, no data written
drt run                  # run all syncs
drt run --select notify_slack  # run one sync
drt status               # check recent sync results
```

---

## CLI Reference

```bash
drt init                    # initialize project
drt list                    # list sync definitions
drt run                     # run all syncs
drt run --select <name>     # run a specific sync
drt run --dry-run           # dry run
drt validate                # validate sync YAML configs
drt status                  # show recent sync status
```

---

## Connectors

| Type | Name | Status | Install |
|------|------|--------|---------|
| **Source** | BigQuery | ✅ v0.1 | `pip install drt-core[bigquery]` |
| **Source** | DuckDB | ✅ v0.1 | (core) |
| **Source** | PostgreSQL | ✅ v0.1 | `pip install drt-core[postgres]` |
| **Source** | Snowflake | 🗓 planned | `pip install drt-core[snowflake]` |
| **Source** | Redshift | 🗓 planned | `pip install drt-core[redshift]` |
| **Source** | MySQL | 🗓 planned | `pip install drt-core[mysql]` |
| **Destination** | REST API | ✅ v0.1 | (core) |
| **Destination** | Slack Incoming Webhook | ✅ v0.1 | (core) |
| **Destination** | GitHub Actions (workflow_dispatch) | ✅ v0.1 | (core) |
| **Destination** | HubSpot (Contacts / Deals / Companies) | ✅ v0.1 | (core) |
| **Destination** | Google Sheets | 🗓 v0.4 | (core) |
| **Destination** | Salesforce | 🗓 planned | (core) |
| **Destination** | Notion | 🗓 planned | (core) |
| **Destination** | Linear | 🗓 planned | (core) |
| **Destination** | SendGrid | 🗓 planned | (core) |

---

## Roadmap

| Version | Focus |
|---------|-------|
| **v0.1** ✅ | BigQuery / DuckDB / Postgres sources · REST API / Slack / GitHub Actions / HubSpot destinations · CLI · dry-run |
| **v0.2** ✅ | Incremental sync (`cursor_field` watermark) · retry config per-sync · 53 tests |
| v0.3 | MCP Server (`uvx drt mcp run`) · AI Skills for Claude Code · LLM-readable docs · row-level error details |
| v0.4 | Dagster / Airflow integration · Google Sheets connector · Snowflake source |
| v1.x | Rust engine (PyO3) |

---

## Ecosystem

drt is designed to work alongside, not against, the modern data stack:

```
dlt (load) → dbt (transform) → drt (activate)
```

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Disclaimer

drt is an independent open-source project and is **not affiliated with,
endorsed by, or sponsored by** dbt Labs, dlt-hub, or any other company.

"dbt" is a registered trademark of dbt Labs, Inc.
"dlt" is a project maintained by dlt-hub.

drt is designed to complement these tools as part of the modern data stack,
but is a separate project with its own codebase and maintainers.

## License

Apache 2.0 — see [LICENSE](LICENSE).
