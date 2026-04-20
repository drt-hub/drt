# Choosing a Reverse ETL Tool

This page compares approaches to reverse ETL — activating data from your warehouse to external services. We focus on factual differences so you can make the right choice for your stack.

## The Modern Data Activation Stack

```
dlt (load) → dbt (transform) → drt (activate)
```

All three are open-source, CLI-first, YAML-configured, and MCP-enabled. Together they form a fully open data stack where every layer speaks the same language — including to your AI tools.

---

## Comparison

|  | **drt** | **Census** | **Hightouch** | **Polytomic** |
|---|---|---|---|---|
| **Type** | OSS (Apache 2.0) | SaaS (Fivetran) | SaaS | SaaS / self-hosted |
| **Pricing** | Free | Paid plans | Paid plans | Paid plans / free tier |
| **Deployment** | Self-hosted | Managed cloud | Managed cloud | Managed / self-hosted |
| **Configuration** | YAML + CLI | GUI | GUI | GUI |
| **Git-native** | Yes (YAML in repo) | Partial (API) | Partial (API) | Partial (API) |
| **CI/CD** | Native (exit codes, `--output json`) | Webhook/API | Webhook/API | API |
| **Sources** | 10 | 20+ | 30+ | 20+ |
| **Destinations** | 22 | 200+ | 200+ | 50+ |
| **MCP Server** | Yes (6 tools) | Partial (enrichment) | No | Yes |
| **LLM Skills** | Yes (Claude Code) | AI Columns | No | No |
| **Orchestration** | Dagster, Airflow, Prefect | Built-in | Built-in | Built-in |
| **Data validation** | `drt test` (5 validators) | Built-in | Built-in | Limited |
| **Incremental sync** | Cursor-based | Multiple strategies | Multiple strategies | Multiple strategies |
| **Managed infrastructure** | No (you host) | Yes | Yes | Yes / No |

---

## When to choose drt

**drt is a good fit when:**

- You want a **free, self-hosted** solution with no vendor lock-in
- Your team already uses **dbt and/or dlt** and wants the same developer experience for reverse ETL
- You value **Git-native configuration** — YAML files in your repo, reviewed in PRs, deployed via CI
- You work with **AI coding tools** (Claude, Cursor) and want your reverse ETL layer to be accessible via MCP
- You need **10-30 destinations** rather than 200+ — and the ones you need are covered
- You prefer to **own your data pipeline** end-to-end

**drt is not the right fit when:**

- You need **200+ pre-built connectors** out of the box — Census and Hightouch have much larger connector catalogs
- You need a **managed, no-ops solution** — drt requires you to host and maintain the pipeline
- Your team is **non-technical** and prefers a GUI over YAML/CLI
- You need **built-in scheduling and monitoring** — drt relies on external orchestrators (Dagster, Airflow, cron)

---

## The ecosystem advantage

drt is designed to complement, not compete with, the modern data stack:

| Layer | Tool | What it does |
|---|---|---|
| **Load** | [dlt](https://dlthub.com/) | Extract and load data into your warehouse |
| **Transform** | [dbt](https://www.getdbt.com/) | Transform data inside the warehouse |
| **Activate** | **drt** | Sync data from the warehouse to external services |

All three share the same philosophy:

- **Declarative configuration** (YAML/SQL)
- **CLI-first** (`dlt pipeline`, `dbt run`, `drt run`)
- **Git-native** (config lives in your repo)
- **MCP-enabled** (LLMs can operate all three)
- **Open source** (no vendor lock-in at any layer)

This means your entire data pipeline — from ingestion to activation — can be:
- Version-controlled in a single repo
- Tested in CI before deployment
- Operated by AI assistants via MCP
- Self-hosted with zero SaaS costs

---

## Connector coverage

drt currently supports:

**Sources (10):** BigQuery, DuckDB, PostgreSQL, Snowflake, SQLite, Redshift, ClickHouse, MySQL, Databricks, SQL Server

**Destinations (22):** REST API, Slack, Discord, Teams, GitHub Actions, HubSpot, Google Sheets, PostgreSQL, MySQL, ClickHouse, Parquet, CSV/JSON/JSONL, Jira, Linear, SendGrid, Notion, Twilio SMS, Intercom, Email SMTP, Salesforce Bulk API, Google Ads, Staged Upload

**Integrations:** Dagster (`dagster-drt`), Airflow (built-in), Prefect (built-in), dbt manifest reader

New connectors are added regularly by the community. The generic REST API destination covers any HTTP endpoint not yet supported natively. See [Good First Issues](https://github.com/drt-hub/drt/issues?q=is%3Aopen+label%3A%22good+first+issue%22) to contribute a connector.

---

## Further reading

- [Quickstart](https://github.com/drt-hub/drt#quickstart) — get running in 5 minutes
- [MCP Server](https://github.com/drt-hub/drt#mcp-server) — connect drt to Claude or Cursor
- [Contributing](https://github.com/drt-hub/drt/blob/main/CONTRIBUTING.md) — add a connector or feature