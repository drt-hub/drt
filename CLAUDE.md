# CLAUDE.md — AI Agent Context for drt

This file gives AI agents (Claude Code, Cursor, etc.) the context needed to work effectively in this codebase.

## What is drt?

**drt** (data reverse tool) is a CLI tool that syncs data from a data warehouse (BigQuery) to external services via declarative YAML configuration. Think of it as the reverse of dlt: `dlt` loads data *into* a DWH; `drt` activates data *out of* a DWH.

**Tagline:** "Reverse ETL for the code-first data stack."

## Architecture

```
Config Parser → Source (BigQuery) → Sync Engine → Destination (REST API)
                                                         ↓
                                                   State Manager
```

Key design principle: **module boundaries are drawn for future Rust rewrite (PyO3)**. The `engine/sync.py` module is the primary Rust candidate — keep it pure (no I/O side effects beyond protocol calls). Logging, state persistence, OTel spans, and any other observability/persistence side effect MUST flow through `drt.engine.observer.SyncObserver`. Direct `logger.*`, `state_manager.save_sync(...)`, or `watermark_storage.save(...)` calls inside `engine/sync.py` are guarded by `tests/unit/test_engine_observer.py` boundary checks and will fail CI.

## Package Layout

```
drt/
├── cli/          # Typer CLI commands
├── config/       # Pydantic models + YAML parser
├── connectors/   # Connector registry — auto-discovery of sources/destinations
├── sources/      # Source Protocol + BigQuery impl
├── destinations/ # Destination Protocol + REST API impl
├── engine/       # Sync orchestration (future Rust core)
├── state/        # Local JSON state persistence
└── templates/    # Jinja2 renderer (future MiniJinja/Rust)
```

## Protocols (critical interfaces)

- `Source.extract(query: str, config: ProfileConfig) -> Iterator[dict]`
- `Destination.load(records: list[dict], config: DestinationConfig, sync_options: SyncOptions) -> SyncResult`
- `StateManager.get_last_sync / save_sync`

Connector dispatch uses a centralized registry (`drt/connectors/registry.py`) — adding a new connector requires registering it there, not editing `main.py`. Implementations use `assert isinstance(config, SpecificConfig)` for type narrowing. `type: ignore` is only allowed for external library issues.

## Development Commands

```bash
make dev      # install with dev + bigquery extras
make test     # pytest
make lint     # ruff + mypy
make fmt      # ruff format + fix
```

## Current Status

- **v0.7.6 released** — Small follow-up. Adds the **Amplitude destination** (#574, Identify API + HTTP V2 events API) and the **`tojson_safe` Jinja2 filter** (#580 / PR [#581](https://github.com/drt-hub/drt/pull/581)) that unblocks `datetime` / `Decimal` / `UUID` columns flowing into REST API `body_template` rendering without `CAST(... AS STRING)` workarounds in model SQL. Also lands a CLI `--log-format` typer 0.26.1 compatibility fix (#577 / PR [#578](https://github.com/drt-hub/drt/pull/578)), a retrofit of `ErrorFormatter` stage detection to an engine-emitted attribute (PR [#571](https://github.com/drt-hub/drt/pull/571), supersedes #544's traceback-walk heuristic), and Phase 2a of the `cli/main.py` split (PR [#572](https://github.com/drt-hub/drt/pull/572), continues #565's Phase 1). No breaking changes — drop-in upgrade from v0.7.5.
- **v0.7.5** — Production Ready follow-up #3 + Tech Foundation Hardening (Epic [#538](https://github.com/drt-hub/drt/issues/538) closed, 11 child issues). CI hardened (nightly + publish gate + CodeQL + pip-audit + SBOM); functional reverse-ETL E2E coverage established via DuckDB harness + boundary tests; CLI/UX polished (`ErrorFormatter`, `drt sources/destinations --detailed`, `drt init --template`); load-bearing refactors landed (`SyncObserver` engine I/O boundary, destinations serializer consolidation, `BaseSqlDestinationConfig`, `cli/main.py` split Phase 1). Also ships the accumulated work since v0.7.4 — REST API source polish, sync catalog (#499 P1+P2), `drt_run_test` MCP tool, OpenTelemetry Phase 1 config, hardcoded secret detection, lookup ambiguity warning, orphan shadow cleanup. No new connectors, no breaking changes — drop-in upgrade from v0.7.2 / v0.7.3 / v0.7.4.
- **v0.7.4** — Patch release for MySQL schema-qualified identifier handling (#511, PR #514). MySQL counterpart to the Postgres `Identifier()` fix that shipped in v0.7.3; the `_quote_ident` helper is now applied consistently across replace / insert / upsert / row-count paths so `mydb.scores` correctly quotes as `` `mydb`.`scores` ``. PR #514 actually landed on `main` two days after the v0.7.3 tag was cut, so the wheel published as `drt-core==0.7.3` did **not** contain it; v0.7.4 is the release that actually delivers it.
- **v0.7.3** — Patch release for Postgres schema-qualified identifier handling (#442, PR #498). Cherry-pick of the qualified `Identifier()` composition fix on top of the v0.7.2 line — `marketing.events` and similar `schema.table` configs no longer fail at SQL execution. No new features, no breaking changes.
- **v0.7.2** — Production Ready follow-up #2: opt-in anonymous telemetry (#263, PostHog Cloud EU), deprecation warnings in `drt validate` (#467), Postgres `psycopg2.sql` SQL composition hardening (#442). Telemetry is off by default + `DO_NOT_TRACK` honored; release-time API key injection workflow (#481) ships with the wheel.
- **v0.7.1** — Production Ready follow-up: `drt run --dry-run --diff` for record-level preview (#413), tz-aware cursor stringification fix (#475), `on_error=fail` alignment for Notion / REST API / Email SMTP (#463), `VERSIONING.md` policy doc (#457).
- **v0.7.0** — Production Ready theme: graceful shutdown on SIGTERM/SIGINT (#279), per-destination retry override (#277), sync execution history (#276), zero-downtime replace via staging table swap (#338), FK existence check via `lookups.check_only` (#354), `json_columns` config (#316), `drt doctor` (#264), `--quiet` flag (#265), Slack/webhook failure alerts (#414). Plus first DWH destination (Snowflake #353), Codespaces playground (#407), and `OPEN_CORE.md`.
- **v0.6.2** — `watermark.default_value` + `--cursor-value` CLI + watermark observability (#390, #391)
- **v0.6.1** — `${VAR}` env substitution in all sync YAML string fields (#385)
- **v0.6.0** — Notion/Twilio/Intercom/Email SMTP/Salesforce Bulk/Google Ads destinations, `--threads` parallel execution, `--log-format json`, `--select tag:`, JSON Schema validation, freshness/unique/accepted_values tests, `drt sources`/`drt destinations`, `--dry-run` row count diff, StagedDestination Protocol, destination_lookup, GOVERNANCE.md
- CLI fully wired: `init`, `run`, `list`, `validate`, `status`, `test`, `mcp run`, `serve`, `sources`, `destinations`, `doctor`, `cloud push` (stub)
- Sources: BigQuery, DuckDB, PostgreSQL, Redshift, SQLite, ClickHouse, Snowflake, MySQL, Databricks, SQL Server
- Destinations: REST API, Slack, Discord, Microsoft Teams, GitHub Actions, HubSpot, Google Sheets, PostgreSQL, MySQL, ClickHouse, Snowflake, Parquet, CSV/JSON/JSONL, Jira, Linear, SendGrid, Notion, Twilio, Intercom, Email SMTP, Salesforce Bulk, Google Ads, Staged Upload, Amplitude
- Integrations: MCP Server (`drt-core[mcp]`), dagster-drt, Airflow, Prefect, dbt manifest reader
- 833+ tests, integration tests use `pytest-httpserver`

## What NOT to do

- Do not add a GUI or web UI — this is a CLI-first tool
- Do not add RBAC or multi-tenancy — small team / personal use
- Do not add `type: ignore` — only allowed for external library issues (`no-untyped-call`, `import-untyped`)
- Do not add heavy dependencies to core — extras (`[bigquery]`, `[mcp]`) exist for a reason

## Roadmap Reference

**SSoT for upcoming releases: [ROADMAP.md](ROADMAP.md)** — each version has Theme / Scope / Out of scope / Target / Progress link.

- **Shipped releases:** see [CHANGELOG.md](CHANGELOG.md) or [GitHub Releases](https://github.com/drt-hub/drt/releases)
- **Issue-level tracking:** [GitHub Milestones](https://github.com/drt-hub/drt/milestones)
- **Good First Issues:** https://github.com/drt-hub/drt/issues?q=is%3Aopen+label%3A%22good+first+issue%22

When scope shifts between versions, update ROADMAP.md first, then re-label issues to match.
