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
pip install drt-core          # core (DuckDB included)
pip install drt-core[bigquery]  # + BigQuery
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

| Type | Name | Install |
|------|------|---------|
| **Source** | BigQuery | `pip install drt-core[bigquery]` |
| **Source** | DuckDB | `pip install drt-core[duckdb]` |
| **Source** | PostgreSQL | `pip install drt-core[postgres]` |
| **Destination** | REST API | (core) |
| **Destination** | Slack Incoming Webhook | (core) |
| **Destination** | GitHub Actions (workflow_dispatch) | (core) |
| **Destination** | HubSpot (Contacts / Deals / Companies) | (core) |

---

## Roadmap

| Version | Focus |
|---------|-------|
| **v0.1** ✅ | BigQuery / DuckDB / Postgres sources · REST API / Slack / GitHub Actions / HubSpot destinations · CLI · dry-run |
| v0.2 | Incremental sync, state persistence improvements |
| v0.3 | Scheduling (cron-style), Google Sheets connector |
| v0.4 | MCP Server (`uvx drt mcp run`), AI Skills |
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

## License

Apache 2.0 — see [LICENSE](LICENSE).
