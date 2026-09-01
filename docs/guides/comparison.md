# Choosing a Reverse ETL Tool

This page explains how drt differs from commercial reverse-ETL tools — not
with a feature checklist, but with the one structural claim that actually
matters, plus the reasoning behind it. See
[ADR 0011](../adr/0011-subtraction-positioning-vs-reverse-etl.md) for the
full decision this page is based on.

## The Modern Data Activation Stack

```
dlt (load) → dbt (transform) → drt (activate)
```

All three are open-source, CLI-first, YAML-configured, and MCP-enabled. Together they form a fully open data stack where every layer speaks the same language — including to your AI tools.

---

## The core difference

**Your data goes straight from your warehouse to the destination, never
through a drt-hosted intermediary.** Commercial reverse-ETL tools —
Census, Hightouch, RudderStack Reverse ETL, Polytomic, and similar — route
it through their hosted service to sync it. drt runs wherever you already
run code (GitHub Actions, Dagster, cron, a container) and talks directly
to the destination API.

That one structural fact is also why three other things are true, on
purpose rather than by accident of what hasn't been built yet:

- **No per-row bill.** There's no metered infrastructure standing between
  your warehouse and the destination to bill against.
- **No web UI.** Config-as-code — YAML in your repo, reviewed in PRs,
  diffable — is the position, not a placeholder for a UI that hasn't been
  built yet.
- **No audience/segmentation builder.** Building the record set to sync is
  a SQL/dbt-modeling problem. drt reads whatever query or dbt model a
  sync's `model` field points at and syncs exactly that — it doesn't
  duplicate the modeling layer you already have.

drt does one thing — read a query, map fields, write to a destination API
— and leaves loading, transforming, hosting, and billing to the tools built
for those jobs. This is a deliberate design decision, not a temporary gap:
see the FAQ below for the reasoning and its honest tradeoff.

---

## Read-only source access, by design

Reverse ETL tools generally need read access to the warehouse. Some also
write into it — a staging table (or a whole managed schema) to track which
rows they've already synced, so re-runs can diff efficiently instead of
re-scanning everything. That write access is a real trust decision, and
it isn't always reversible: Hightouch's high-performance "Lightning" sync
engine requires warehouse write and, per Hightouch's own documentation,
cannot be switched back to their read-only "Basic" engine once enabled.
Segment Reverse ETL requires a managed `_segment_reverse_etl` schema with
read and write for the same reason. (Not every vendor documents this
publicly — we only state what we could verify.)

drt's cursor-based incremental sync (`mode: incremental` +
`cursor_field`) never requires write access to the source to *read* from
it — it's the permanent, first-class strategy, not a stripped-down
fallback waiting to be superseded. drt's design deliberately leaves room
for a warehouse-write-requiring strategy later, as an *opt-in,
separately-scoped* addition for teams that want it — not a replacement,
and not scheduled work today. Whenever it lands, switching between the
two will stay a config change, not a one-way door. See
[ADR 0005](../adr/0005-state-location-and-write-grants.md) for the full
reasoning.

**One existing, unrelated exception worth naming honestly:** the older,
separately-configured `sync.watermark.storage: bigquery` option (an
alternative to the default local/GCS/S3 watermark storage, unrelated to
this ADR) creates and writes a small `_drt_watermarks` tracking table.
That write only touches your *source* warehouse if you point
`watermark.project`/`watermark.dataset` at the same project/dataset your
source reads from — a choice you make, not drt's default. Use `local`,
`gcs`, or `s3` watermark storage (or a separate BigQuery
project/dataset) to keep the source warehouse completely untouched.

---

## FAQ: why not a UI, a hosted runtime, or an audience builder?

**Isn't a UI table stakes for a reverse-ETL tool?** For a commercial
product, yes — the UI usually *is* the product. drt's position is that
config-as-code is a better fit for a team that already reviews dbt models
and Airflow DAGs in pull requests: a sync is a YAML file, diffable and
git-blame-able like any other change to the pipeline.

**Why no hosted/managed version?** Because that's what makes "no per-row
bill" structural rather than a pricing tier that could change later.
There's no drt-operated service between your warehouse and your
destinations to meter in the first place — drt runs as a process inside
infrastructure you already operate.

**Why doesn't drt have an audience builder?** Because you likely already
have one: dbt. Segmenting "which rows should sync" is a modeling problem,
and duplicating dbt's job inside the sync tool is the same "one tool doing
four jobs" mistake this design avoids elsewhere. Point a sync's `model`
field at a dbt model or raw SQL, and drt syncs exactly what that query
returns.

**Does this mean drt is missing features?** Depends on what you're
comparing. drt covers 13 sources and 34 destinations today (see below) —
fewer than a 200+-connector commercial catalog, and if your workflow
needs one of those, a commercial tool is the better fit. What drt doesn't
have is a UI, a hosted runtime, or an audience builder to catch up on —
those are the position, not gaps waiting to be filled. New destinations
are also a plugin away, not a request to drt-hub: see
[Contributing](https://github.com/drt-hub/drt/blob/main/CONTRIBUTING.md).

**Is this permanent?** For `drt-core` (this repository), yes — a feature
request reintroducing a UI, hosted runtime, or audience builder here would
be declined by default, citing
[ADR 0011](../adr/0011-subtraction-positioning-vs-reverse-etl.md). That
ADR governs this OSS engine specifically, not every product drt-hub might
ever build.

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

**Sources (13):** BigQuery, DuckDB, PostgreSQL, Snowflake, SQLite, Redshift, ClickHouse, MySQL, Databricks, SQL Server, Delta Lake, Iceberg, REST API

**Destinations (34):** REST API, Slack, Discord, Teams, GitHub Actions, HubSpot, Zendesk, Google Sheets, PostgreSQL, MySQL, ClickHouse, Snowflake, Databricks, BigQuery, Parquet, File (CSV/JSON/JSONL), S3, GCS, Azure Blob, Jira, Linear, SendGrid, Notion, Twilio SMS, Intercom, Email SMTP, Salesforce Bulk API, Staged Upload, Google Ads, Amplitude, Mixpanel, Elasticsearch, Airtable, Klaviyo

**Integrations:** Dagster (`dagster-drt`), Airflow (built-in), Prefect (built-in), dbt manifest reader

New connectors are added regularly by the community, and third-party connectors can register as a plugin without drt-hub as a gatekeeper. The generic REST API destination covers any HTTP endpoint not yet supported natively. See [Good First Issues](https://github.com/drt-hub/drt/issues?q=is%3Aopen+label%3A%22good+first+issue%22) to contribute a connector.

---

## Further reading

- [Quickstart](https://github.com/drt-hub/drt#quickstart) — get running in 5 minutes
- [MCP Server](https://github.com/drt-hub/drt#mcp-server) — connect drt to Claude or Cursor
- [Contributing](https://github.com/drt-hub/drt/blob/main/CONTRIBUTING.md) — add a connector or feature
