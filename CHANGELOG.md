# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## dagster-drt

> `dagster-drt` is published as a separate PyPI package with its own version.

### [0.2.0] - 2026-04-04 (dagster-drt)

- **API alignment with dagster-dbt / dagster-dlt patterns** (#176, #177, #178, #179)
- **`@drt_assets` decorator** — wraps `@multi_asset(can_subset=True)`, replacing the old `drt_assets()` function
- **`build_drt_asset_specs()`** — pure spec generation, decoupled from execution. Enables Dagster Pipes remote execution (#175)
- **`DagsterDrtResource`** — `ConfigurableResource` with `.run()` yielding `MaterializeResult` per sync. Auto-resolves `project_dir` from `@drt_assets` metadata
- **`DagsterDrtTranslator.get_asset_spec()`** — new primary override point (per-attribute methods deprecated)
- **`partitions_def`, `backfill_policy`, `pool`** support on `@drt_assets`
- **Asset `kinds`** metadata — `{"drt", "<destination_type>"}` visible in Dagster UI
- **Group name conflict detection** — raises if translator and decorator both set group names
- **Subset execution** — `DagsterDrtResource.run()` respects `context.selected_asset_keys`
- Old `drt_assets()` function renamed to `drt_assets_legacy()` with deprecation warning
- Requires `drt-core>=0.4.1`, `dagster>=1.6`

### [0.1.0] - 2026-04-01 (dagster-drt)

- First PyPI release (`pip install dagster-drt`)
- **DagsterDrtTranslator** (#127): Customise asset keys, group names, deps, and metadata (follows dagster-dbt pattern)
- **DrtConfig with dry_run** (#126): RunConfig controllable from Dagster UI, plus `drt_assets(dry_run=True)` for build-time defaults
- **MaterializeResult** (#128): Assets return structured metadata (rows_synced, rows_failed, rows_skipped, dry_run, row_errors_count)
- Requires `drt-core>=0.4.1`

---

## drt-core

## [Unreleased]

- **REST API: lift `extract_next_link` into shared `drt._http_utils`** (#530, item 1): removes the cross-domain import where `drt/sources/rest_api.py` reached into `RestApiDestination` for a static helper. The RFC 5988 Link-header parser now lives in a new top-level `drt/_http_utils.py` module; source and destination both call it directly. No behaviour change — the five existing pagination unit tests are retargeted to the new symbol. Items 2-5 of #530 already landed in PR #534.
- **`drt --version` enriched with Python / install path / platform** (#515): the version flag now prints four diagnostic lines instead of one — `drt version X.Y.Z`, `Python X.Y.Z (CPython)`, `Install: <site-packages path>`, `Platform: <system> <release> (<machine>)`. First line is byte-identical to the old output so scripts grepping `drt version X.Y.Z` keep working. Saves a back-and-forth on bug reports where the relevant Python / OS / install info would otherwise need a follow-up. Seven unit tests in `tests/unit/test_cli_version.py` lock the format and the BC contract on the first line.
- **CLI help examples** (#516): Added `Examples:` sections to `drt run`, `drt list`, and `drt validate` help output so new users can copy common command patterns directly from `--help`.
- **Connection test in `drt validate`** (#367, PR #484): Added `--check-connection` flag to test connectivity to SQL destinations (PostgreSQL, MySQL, ClickHouse, Snowflake) before running syncs via `SELECT 1`. Reports pass/fail per sync in both text and `--output json`; non-SQL destinations are skipped gracefully. Snowflake destination internally refactored to share a `_connect()` helper between `load` and `test_connection`. Contributed by @GokulKashyap.
- **`drt cloud status` stub command** (#491, PR #488; Rich rendering polish in #494, PR #496): Companion stub to `drt cloud push`. Both commands now print a unified "drt Cloud coming soon" message via a shared `CLOUD_MESSAGE` constant, rendered through Rich `console.print` with a bold blue `drt Cloud` heading and a dim coming-soon footer. Lays groundwork for the future drt Cloud control plane. Contributed by @Photon101.
- **`drt docs generate --format mermaid`** (epic #499, sub-issue #501): First slice of the sync catalog & lineage primitive. Renders a Mermaid `graph LR` of `source → sync → destination`, including heuristic lookup edges between syncs whose destination table is referenced by another sync's `lookups`. Pinable in README via GitHub-flavored Mermaid fence. `--format html` and `--format json` raise `NotImplementedError` pending follow-up phases (P2/P3 of #499). New package `drt/docs/` lays the foundation for `drt docs generate --format html` (Phase 3, v0.8) and `drt docs serve` (Phase 4, v0.8.x).
- **`drt docs generate --format json` + manifest.json schema v1** (epic #499, sub-issue #507): Public, semver-relevant `manifest.json` artifact written to `target/docs/` (or `--output DIR`). Includes `schema_version: 1`, `drt_version`, `generated_at` (ISO-8601 UTC), `project`, full `syncs`/`sources`/`destinations`/`edges`, and optional per-sync `state` block (latest run snapshot from `.drt/state.json`). State field names are explicitly renamed from the internal `drt.state.SyncState` (`last_sync_at` / `rows_synced` / `last_status` / `last_error` / `last_cursor_value`) so the public schema stays stable even when the runtime layer evolves. `--no-state` opts out for docs committed to public repos. `Manifest.to_dict()` / `Manifest.from_dict()` enable round-trip for downstream tooling (Backstage / OpenMetadata / custom dashboards).
- **Hardcoded sync secret detection in `drt validate`** (#424, PR #503): New `drt/config/secrets.py` raw-YAML scanner walks `syncs/*.yml` for known token patterns (AWS, GitHub, Slack, Stripe, etc.) with a Shannon-entropy fallback for unknown formats. Findings surface in `drt validate` as ⚠️ warnings while the default exit code stays `0`, matching the deprecation-warning contract from #467. New `drt validate --strict` flag promotes warnings to validation errors (exit `1`); warning details are included under `--output json`. Contributed by @Photon101.
- **`ConnectionTestable` destination protocol** (#495, PR #497): New runtime-checkable `Protocol` in `drt/destinations/base.py` formalizing the `test_connection()` method introduced by #484. `drt validate --check-connection` now dispatches via `isinstance(dest, ConnectionTestable)` instead of dynamic `getattr`, giving destination implementers (including third-party ones) a typed contract. PostgreSQL / MySQL / ClickHouse / Snowflake destinations are declared `ConnectionTestable`; non-SQL destinations correctly fall through to the skip path. Contributed by @Photon101.
- **Postgres: qualified `schema.table` identifiers now safely composed** (#442, PR #498; also shipped in [0.7.3](#073---2026-05-17)): follow-up to PR #452 / PR #485. The row-count, replace, swap, finalize, insert, and upsert SQL paths previously passed `f"{schema}.{table}"` style strings through `psycopg2.sql.Identifier()`, which double-quoted the entire dotted name into a single identifier (`"marketing.email_events"`). The fix splits qualified names into separate schema and relation `Identifier()` components, while keeping swap shadow/old suffixes attached to the relation name only (so `marketing.email_events` becomes `marketing."email_events__drt_swap"`, not a single quoted identifier). Contributed by @Photon101.
- **`drt_run_test` MCP tool** (#368, PR #520): new tool on the `drt-core[mcp]` server that mirrors the `drt test` CLI — runs each sync's `tests:` block against the destination and reports per-test pass/fail. Optional `sync_name` argument restricts to one sync. Non-queryable destinations (REST API, Slack, etc.) are reported as `skipped` with a reason rather than failing. Lets LLMs (Claude Code, Cursor) verify data-quality assertions without dropping to a terminal. As part of this work, `_test_display_name` was extracted from `drt.cli.main` into a public `drt.engine.test_runner.test_display_name` so the CLI and MCP wrapper render identical test labels. CI now installs `[mcp]` extras so `tests/unit/test_mcp.py` actually runs (previously 0% patch coverage).
- **Ambiguous lookup ordering warning** (#453, PR #521): `apply_lookups` evaluates lookups in YAML insertion order and the first miss wins — so when multiple lookups share a source column with differing `on_miss` policies (e.g. one `fail`, one `skip`), reordering keys silently flips row fate. drt now scans for this pattern at sync startup via `detect_ambiguous_lookup_ordering()` in `drt/destinations/lookup.py` and emits a single warning per ambiguous source column via the existing `drt` logger. No auto-reorder — keeps existing configs working unchanged.
- **`/drt-changelog` repo skill** (#372, PR #519): new skill at `.claude/commands/drt-changelog.md` that drafts an `[Unreleased]` CHANGELOG entry from squash-merged PRs since the latest `v*` tag — parses conventional commit prefixes (`feat/fix/refactor/perf`), enriches with PR title + contributor login via `gh pr view`, and outputs in drt's `(#NNN, PR #MMM)` bold-headline style. Does not write to `CHANGELOG.md` automatically.
- **Orphan shadow table cleanup** (#447, PR #492): new `drt clean --orphans` command safely lists and drops orphan `__drt_swap` shadow tables left behind by interrupted swap-strategy replacements. Scoped to the current sync's base table so one sync never sees another sync's shadow tables. PostgreSQL implements `list_orphan_swap_tables()` and `drop_orphan_swap_tables()` behind a new opt-in `OrphanCleanup` `Protocol` in `drt/destinations/base.py` — kept separate from `Destination` so the existing destination connectors stay valid without implementing cleanup methods. Dry-run is the default; `--execute` is required to actually drop, and each drop is validated (`schema.table` shape + `__drt_swap` suffix) and committed per-table so one failure does not roll back the rest. Contributed by @Muawiya-contact.
- **`drt init` post-scaffold "Next steps:" block** (#517, PR #529): after `drt init` writes the scaffold files, the CLI now prints a numbered 5-step recipe (Edit `drt_project.yml` → Edit `syncs/example_sync.yml` → `drt validate` → `drt run --dry-run` → `drt run`) with parenthetical hints and a `Docs:` link. The leading two "Edit ..." lines explicitly point at the two concrete files to touch before any command runs, which was the main friction point for first-time users. Contributed by @cian-ps.
- **OpenTelemetry — Phase 1 config schema + extras** (#429, PR #527): scaffolds the `observability.otel` config block (endpoint, service_name, headers) as a single top-level entry in `profiles.yml` — not per-profile, since OTel is a run-level concern (one span per sync run). `drt-core[otel]` extras pull in `opentelemetry-sdk` + `opentelemetry-exporter-otlp`. Secrets in `headers` (e.g. `${OTEL_TOKEN}`) stay raw on disk; env-var expansion happens at exporter init time in Phase 2 ([#531](https://github.com/drt-hub/drt/issues/531)). Duplicate env-expander helpers consolidated to use `drt.config.parser.expand_env_vars`. No exporter wiring or engine instrumentation in this PR — that's Phase 2 + Phase 3 of the epic. Contributed by @Muawiya-contact.
- **Telemetry: PII-not-collected posture; retention cleanup workflow removed** (PR #536, closes #482): drt's opt-in `sync_completed` payload now includes three PostHog meta-properties — `$ip: ""`, `$geoip_disable: true`, `$process_person_profile: false` — that suppress server-side IP recording, GeoIP resolution, and per-`distinct_id` person-profile creation on the receiving end. These are defense-in-depth in front of the project-level "Discard client IP data" setting, so a future operator change at the project layer can't accidentally start collecting geo / IP data. Replaces the planned 90-day API-based retention cleanup (workflow + script introduced in PR #535 and reverted by this PR) — PostHog does not expose a public REST endpoint for bulk event deletion by name + timestamp, so PII-at-source removal is the cleaner and stronger guarantee. The 1-year retention number on the maintainer's PostHog Free tier is unchanged but now applies to PII-free data. See `docs/telemetry.md` for the full posture.
- **REST API source polish** (PR #534, addresses 4/5 items of #530): docstring on `RestApiSource.extract()` documents that the Source-Protocol `query` argument is intentionally ignored (the URL lives in `config`); four `# type: ignore[no-any-return]` in `_extract_records` replaced with `cast(list[dict[str, Any]], ...)` per CLAUDE.md's "no `type: ignore` outside external libs" rule; the previously-unreferenced module logger now wires a DEBUG log on the single-dict fallback path so silent shape drift (e.g. `{"error": "rate limit"}` arriving without a `records`/`data` wrapper) becomes visible under `--log-level debug`. Item 1 of #530 (lifting `_extract_next_link` into a shared HTTP util) remains open for a community contributor.

## [0.7.3] - 2026-05-17

**Theme: Patch release for Postgres schema-qualified identifier handling.**

Cherry-pick of PR #485 + PR #498 onto the v0.7.2 release line — so users on `drt-core~=0.7.2` get the fix without the v0.8.0 feature set. PR #485 completes the `psycopg2.sql` migration in the swap path (prerequisite); PR #498 then fixes the actual user-facing bug. No new features, no breaking changes.

### Fixed

- **Postgres: qualified `schema.table` identifiers now safely composed** (#442, PR #498): the row-count, replace, swap, finalize, insert, and upsert SQL paths previously passed `f"{schema}.{table}"` style strings through `psycopg2.sql.Identifier()`, which double-quoted the entire dotted name into a single identifier (`"marketing.email_events"`) — so any Postgres destination configured with a schema-qualified table name failed at SQL execution. The fix splits qualified names into separate schema and relation `Identifier()` components, while keeping swap shadow/old suffixes attached to the relation name only (so `marketing.email_events` becomes `marketing."email_events__drt_swap"`, not a single quoted identifier). Contributed by @Photon101.
- **Postgres: swap path fully migrated to `psycopg2.sql` composition** (PR #485, fixes #483): prerequisite for the qualified-identifier fix above. The swap-path SQL (DROP TABLE IF EXISTS, CREATE TABLE LIKE, INSERT, ALTER TABLE RENAME, cleanup DROP) was still using f-string formatting; this commit migrates it to `psycopg2.sql.SQL` + `Identifier` for safe composition. Originally queued for v0.7.2 but landed post-release (#485's CLA signature was in v0.7.2 but the commit itself was not); bundled into this patch because PR #498 depends on it. Contributed by @Photon101.

## [0.7.2] - 2026-05-11

**Theme: Production Ready follow-up #2.** Opt-in anonymous telemetry, deprecation warnings in `drt validate`, Postgres `psycopg2.sql` hardening — closing out the v0.7 cycle items that didn't make v0.7.1.

### Breaking Changes

None. Drop-in upgrade from v0.7.1.

### Added

- **Opt-in anonymous usage telemetry** (#263, PR #446): a new `drt/telemetry.py` module sends a single anonymous `sync_completed` event per `drt run` when the user explicitly opts in. Off by default. Honors `DO_NOT_TRACK=1`. Allow-list `properties` (`drt_version`, `python_version`, `os`, `source_type`, `destination_type`, `sync_mode`, `rows_synced`, `duration_seconds`, `status`) — never sends sync names, model SQL, destination URLs, credentials, or project paths. The wire envelope additionally carries `event`, `distinct_id`, `timestamp`, and `api_key`. Configure via `drt config set telemetry.enabled true` or `DRT_TELEMETRY=1`. Inspect what would be sent with `drt config show-telemetry`. Endpoint defaults to PostHog Cloud EU region; override with `DRT_TELEMETRY_ENDPOINT` and `DRT_TELEMETRY_API_KEY` for self-hosted PostHog. Privacy posture is documented in [docs/telemetry.md](docs/telemetry.md), including the GDPR disclosure for EU/EEA opt-ins. Contributed by @kiwamizamurai.
- **Deprecation warnings in `drt validate`** (#467, PR #478): the `validate` command now reads `drt/deprecations.py` and surfaces ⚠️ warnings for any deprecated config keys it finds in your sync YAMLs — both in text output and as a per-sync `deprecations` array under `--output json`. Exit code stays `0` (warnings are non-blocking). The registry is currently empty (no active deprecations); add entries to `DEPRECATED_SYNC_KEYS` when announcing a new deprecation per VERSIONING.md Step 1. Migration guides live under `docs/migration/`. Closes the Step 2 ("Add Tooling Support") TODO from #457. Contributed by @Muawiya-contact.
- **Release-time telemetry key injection workflow** (#481): `.github/workflows/publish-drt-core.yml` now sed-injects the `POSTHOG_WRITE_KEY` repo secret into `drt/telemetry.py:_DEFAULT_API_KEY` before `uv build`, with a smoke check that asserts the injection happened. Fail-safe when the secret is unset (community forks ship with telemetry physically disabled).

### Changed

- **Postgres destination: safe SQL composition via `psycopg2.sql`** (#442, PR #452): replaced f-string interpolation of table and column identifiers in `_load_replace`, `_load_upsert`, `_build_insert_sql`, and `_build_upsert_sql` with `psycopg2.sql.SQL` / `psycopg2.sql.Identifier`. Eliminates a class of identifier-injection bugs in environments where table/column names are derived from config rather than hard-coded. Swap-path methods (`_load_replace_swap` / `finalize_sync` from #435 / #448) are tracked for follow-up in #483. Contributed by @Khush-domadia.

### Documentation

- **GDPR disclosure section in `docs/telemetry.md`** (PR #446): documents data controller (K. Masuda as natural person, transferable to a future legal entity), retention (1 year on Free tier; #482 tracks reducing to 90 days via API-based cleanup), erasure contact (`drt.hub.dev@gmail.com`), and the Art. 6(1)(a) / Art. 46(2)(c) SCCs legal basis for the US-incorporated processor / EU-hosted storage split.

## [0.7.1] - 2026-05-07

**Theme: Production Ready follow-up.** Tail of the v0.7 cycle — record-level dry-run preview, watermark cursor correctness fix, `on_error=fail` alignment for the remaining HTTP destinations, and the new VERSIONING.md policy doc.

### Breaking Changes

None. Drop-in upgrade from v0.7.0.

### Added

- **`drt run --dry-run --diff`** (#413, PR #473): Record-level preview before deploying a sync. For queryable destinations (Postgres / MySQL / ClickHouse), compares extracted source records against the destination state keyed on `upsert_key` and shows added / updated (with field-level `old → new` diffs) / deleted records. For non-queryable destinations falls back to "sample mode" — first N records that would be sent. Output works in both text (rich tables) and `--output json` (embedded `diff` key per sync). New flag `--diff-limit N` (default 20). The `--diff` flag is only valid alongside `--dry-run`. Documentation: [`docs/guides/dry-run-and-diff.md`](docs/guides/dry-run-and-diff.md). Follow-ups: #468 (Snowflake support), #469 (Protocol method), #470 (perf), #471 (`--diff-fields`), #472 (API-based SaaS diff).
- **`VERSIONING.md` — semver and deprecation policy** (#431, PR #457, polished in PR #464): documents the project's versioning contract pre-1.0, what counts as a breaking change at each layer (CLI / config schema / Python API), and the deprecation cadence (announced one minor version, removable one minor after that). Cross-linked from the `CONTRIBUTING.md` PR checklist. Contributed by @Muawiya-contact.

### Fixed

- **Watermark advance for tz-aware cursor values** (#475, PR #476): `drt/engine/sync.py` was calling `str()` directly on cursor field values, which for tz-aware datetimes (e.g. BigQuery `TIMESTAMP` columns from the Python BQ client) produced strings with a `+00:00` suffix. When user SQL or `default_value` was written tz-naive, the next run compared a naive `WHERE col >= TIMESTAMP('YYYY-MM-DD HH:MM:SS')` against the tz-aware persisted form and the boundary row re-fired on every run. The engine now normalizes tz-aware datetimes to naive UTC before stringifying. Reported by @K-Masuda-SL after a prod incident where a single `recording_sessions` row triggered a downstream GHA `workflow_dispatch` three times in a row.
- **`on_error=fail` not respected by Notion / REST API / Email SMTP destinations** (#365, PR #463): three HTTP destinations continued processing the rest of the batch after the first failure even when `on_error: fail` was configured. Now all three short-circuit and `return result` on the first failure, matching the documented contract and the behavior of every other destination. New `on_error=fail` and per-destination retry override tests lock the semantic in across the webhook surface.

## [0.7.0] - 2026-05-06

**Theme: Production Ready.** Reliability, observability, and correctness for syncs that run in production environments — graceful shutdown on SIGTERM/SIGINT, retry knobs per destination, atomic zero-downtime table replace, sync execution history, FK existence filtering, opinionated JSON column handling. Plus the first DWH destination (Snowflake), the GitHub Codespaces playground for zero-setup onboarding, and `OPEN_CORE.md` documenting the open core boundary.

This release closes 9 v0.7 milestone issues plus several spillover items shipped early.

### Breaking Changes

None. Drop-in upgrade from v0.6.x.

### Added

- **Sync failure alerts** (#414): Configure `alerts.on_failure` in sync YAML to send Slack or generic HTTP webhook notifications when a sync ends with `failed > 0` or raises an exception. Two target types in v0.7: `slack` (Slack incoming webhook) and `webhook` (generic HTTP POST/PUT with optional `body_template`). Template variables: `sync_name`, `error`, `rows_processed`, `duration_s`, `started_at`. Dispatch is best-effort — alert failures are logged but never affect sync correctness or override the original exception.
- **Graceful shutdown on SIGTERM/SIGINT** (#279): `drt run` now handles `SIGTERM` (container stop, K8s pod eviction, Airflow cancellation) and `SIGINT` (Ctrl+C) cooperatively. Signal handler sets a `stop_event` checked between batches so the in-flight batch always completes; state and watermark are persisted before exit. POSIX-conventional exit codes: `130` for SIGINT, `143` for SIGTERM. A 30-second watchdog force-exits if the current batch hangs — pair with K8s `terminationGracePeriodSeconds: 60` for a bounded shutdown window. New `SyncResult.interrupted` flag lets integrations (Dagster / Airflow / Prefect) distinguish a clean cancellation from an error. See [docs/guides/graceful-shutdown.md](docs/guides/graceful-shutdown.md).
- **FK existence check via `lookups.check_only`** (#354): Filter source rows by whether a foreign key exists in the destination, without resolving a value. Set `check_only: true` on a lookup (and omit `select`); rows whose match key is missing in the destination table are filtered per `on_miss` (`skip` or `fail`). Common use case: BigQuery has prd-like data but the destination DB (e.g. staging) holds only a subset — silently drop rows pointing at non-existent FKs instead of failing the sync. Source columns are always preserved (no implicit drop) since the target name is just a label, not a destination column. Works alongside regular value-resolving lookups in the same sync.
- **Snowflake destination** (#353): Write rows back to Snowflake tables. Supports `mode: insert` (append) and `mode: merge` (upsert via temp staging table + `MERGE` statement using `upsert_key`). Auth via `account_env` / `user_env` / `password_env`. Install: `pip install drt-core[snowflake]`. Contributed by @PFCAaron12.
- **Zero-downtime replace via staging table swap** (#338): `sync.replace_strategy: swap` enables truly atomic table replacement — drt writes to a shadow table (`{table}__drt_swap`) per batch and atomically renames it to the original at the end of the sync. Supported on PostgreSQL (transactional `ALTER TABLE RENAME`), MySQL (atomic `RENAME TABLE`), and ClickHouse (atomic `EXCHANGE TABLES`, requires 21.8+). Default remains `truncate` (existing TRUNCATE → INSERT behavior). Follow-ups tracked in #433 (orphan auto-cleanup) and #434 (Snowflake support).
- **Per-destination retry override** (#277): Each HTTP destination can now declare its own `retry: RetryConfig` block in YAML to override the sync-level `sync.retry`. Useful when one destination has stricter rate limits or unusual failure modes (e.g. Notion 7 attempts while other destinations stay at the default 3). Priority: `destination.retry` > `sync.retry` > built-in defaults. Brings drt in line with the per-adapter retry knobs in dbt and dlt. Documentation: [`docs/guides/retry.md`](docs/guides/retry.md).
- **Sync execution history** (#276): Every `drt run` now appends a record to `.drt/history/<sync_name>.jsonl` with timestamp, status, rows synced/failed, duration, and errors. Inspect via `drt status --history [--sync NAME] [--limit N] [--output json]` or via the new `drt_get_history` MCP tool. Configurable retention (`history.retention_days`, default 30) prunes old entries lazily on each append. Best-effort: history persistence never affects sync correctness. The CLI/MCP counterpart to the run-history UI in Census/Hightouch — brought to a Git-native, scriptable workflow. Documentation: [`docs/guides/sync-history.md`](docs/guides/sync-history.md).
- **GitHub Codespaces playground** (#407, closes #283): Zero-setup "click and try" onboarding via the **Open in GitHub Codespaces** badge. The devcontainer installs `drt-core[duckdb]`, seeds a sample DuckDB warehouse, and ships two runnable examples — `examples/duckdb_to_file` (DuckDB → CSV) and `examples/duckdb_to_rest` (DuckDB → REST API). Contributed by @safridwirizky.
- **`json_columns` config for explicit JSON serialization** (#316): Declare which Postgres/MySQL destination columns hold JSON/JSONB data via `json_columns: [col1, col2]`. Listed columns are wrapped with the driver-native JSON adapter (`psycopg2.extras.Json` for Postgres, `json.dumps` for MySQL); unlisted columns receiving dict/list values raise an early `ValueError` pointing at the missing column instead of failing deep inside the driver. Backward-compatible — when omitted, all dict/list values are auto-wrapped as before. Contributed by @armorbreak001.
- **`drt doctor` command** (#264): Diagnostics for the user's environment — checks Python version, drt version, project file existence, profile resolution, sync file count, optional extras installation status, and common environment variables. Pinpoints setup issues for new users without forcing them to read traceback output. Contributed by @pureqin.
- **`--quiet` / `-q` flag for `drt run`** (#265): Suppresses banner / sync-result / summary / watermark output for CI and cron use cases where logs are noise. `--quiet` wins over `--verbose` when both are passed; `--output json` is unaffected so structured output still flows. Contributed by @Pawansingh3889.
- **`drt test --output json` and `drt test --dry-run`** (#366, #371): Brings `drt test` to feature parity with `drt run`. JSON output gives CI integrations structured pass/fail data; dry-run prints the test plan (test name, target, type) without hitting any database. Contributed by @wahajahmed010.
- **`drt cloud push` stub command** (#302): Placeholder Typer subcommand that prints an "enterprise cloud push" message and exits cleanly. Reserves the CLI surface so future enterprise integrations don't break user shell aliases / scripts. Re-landed under maintainer authorship after the original contributor (#308) didn't return to sign the CLA. See [OPEN_CORE.md](OPEN_CORE.md) for what's free vs. enterprise.
 

### Changed

- **Connector registry** (#381): Replaced hardcoded `isinstance` chains in `_get_destination()` / `_get_source()` with a centralized registry (`drt/connectors/registry.py`). Adding a new connector no longer requires editing `main.py`. Error messages now list available connectors on typo. Contributed by @Muawiya-contact.
- **REST API destination pagination** (#260): Fetch data from paginated APIs. 3 strategies: offset/limit, cursor-based, HTTP Link headers. Contributed by @Muawiya-contact.
- **Retry resolution helper** (#277): Each HTTP destination now uses a single `resolve_retry(config.retry, sync_options)` helper instead of duplicating its own `_DEFAULT_RETRY` fallback. Removes ~110 lines of dead code (the per-destination `_DEFAULT_RETRY` constants were unreachable because `SyncOptions.retry` is always populated by `default_factory`).

### Fixed

- **PostgreSQL destination**: crash on `dict` values bound for JSONB columns — wrapped with `psycopg2.extras.Json` (#315). Contributed by @armorbreak001.
- **Notion destination**: `sync_options.retry` override was silently ignored — Notion always used the hardcoded `_DEFAULT_RETRY` (3 attempts) regardless of user configuration. Now respects user-configured retry like every other HTTP destination (#438). Contributes to #365.
- **Snowflake destination**: Fixed missing row-level error tracking during merge staging, corrected SQL generation when all columns are upsert keys, and unified credential resolution to support `secrets.toml` fallback.
- **`BasicAuth` credentials from `secrets.toml`**: `BasicAuth` was the only auth type that couldn't resolve credentials from `.drt/secrets.toml` — it called `os.environ.get()` directly while every other auth type went through `resolve_env()`. Users storing BasicAuth credentials in `secrets.toml` got a misleading "env var not set" error. Now matches the other auth types (#386). Contributed by @armorbreak001.
- **`replace_strategy: swap` ignored `json_columns` config** (#448): When both `replace_strategy: swap` (#338) and `json_columns` (#316) were configured on a Postgres or MySQL destination, swap mode silently bypassed the explicit JSON column declarations because `_load_replace_swap` did not thread `config.json_columns` through to `_serialize_value`. Now both strategies honour the config consistently — swap-mode dict values in unlisted columns raise the same fail-fast `ValueError` as truncate mode. ClickHouse is unaffected (its connector handles JSON encoding driver-side and has no `json_columns` config). Discovered post-rebase of #435 onto #382 — neither feature was wrong in isolation; the gap was an interaction artifact of parallel development.


## [0.6.2] - 2026-04-20

### Added

- **`watermark.default_value`** (#390): Configure a fallback cursor value for first-run incremental syncs. Prevents broken SQL (e.g. `WHERE TIMESTAMP(...) >= TIMESTAMP('')`) when no watermark file exists yet. Without a default, drt now raises a clear error with actionable guidance instead of silently rendering an empty string.
- **`--cursor-value` CLI option** (#390): Override the cursor/watermark value at runtime for backfill and recovery scenarios. The override takes highest priority in the fallback chain and the resulting watermark is persisted on success.
- **Watermark source observability** (#391): Operators can now see _where_ the cursor value came from — `storage`, `default_value`, or `cli_override` — via structured INFO logs, `--output json` fields (`watermark_source`, `cursor_value_used`), and an end-of-run summary in text mode.

### Fixed

- **PostgreSQL destination**: crash on `dict` values bound for JSONB columns — dict values are now wrapped with `psycopg2.extras.Json` before binding (#315)

## [0.6.1] - 2026-04-20

### Fixed

- **`${VAR}` env substitution in sync YAML** (#385): Environment variable placeholders like `${PIPES_GCS_BUCKET}` are now expanded in **all string fields** of sync YAML config — not just the `model:` SQL. This enables multi-environment setups (DEV/PRD) without duplicating sync files. Common use cases: `sync.watermark.bucket`, `destination.url`, `destination.host`. Missing variables raise `ValueError` consistently with the existing SQL expansion behaviour.

## [0.6.0] - 2026-04-19

### Added

- **`drt sources` and `drt destinations` CLI commands** (#223): List available source and destination connectors with descriptions. Rich table formatting for clean terminal output. Foundation for future auto-discovery features.
- **SQL Server source connector** (#91): Extract data from Microsoft SQL Server using pure-Python `pymssql`. Supports host, port, database, user, password_env, schema. Install: `pip install drt-core[sqlserver]`.
- **Databricks source connector** (#88): Extract data from Databricks SQL Warehouse using `databricks-sql-connector`. Supports Unity Catalog, access token auth. Install: `pip install drt-core[databricks]`.
- **Webhook trigger endpoint** (#218): New `drt serve` command starts a lightweight HTTP server (stdlib `http.server`) so you can trigger syncs via `POST /sync/<name>`. Includes health check, bearer token auth, and single-sync concurrency control (423 on parallel requests). Guide: `docs/guides/using-webhook-trigger.md`.
- **Prefect integration** (#213): Built-in `run_drt_sync()` helper and `drt_sync_task` for Prefect 2.x/3.x. No extra package needed — included in drt-core. Shares the runner with Airflow integration via `drt.integrations._runner`.
- **Airflow integration** (#70): Built-in `run_drt_sync()` helper and `DrtRunOperator` for Apache Airflow. No extra package needed — included in drt-core.
- **Google Ads destination** (#217): Upload offline click conversions. Supports partial failure handling and OAuth2 auth.
- **Staged Upload destination** (#258): Async bulk-upload APIs (e.g. Amazon Marketing Cloud, Salesforce Bulk API). Declarative 3-phase YAML config: Stage (file upload) → Trigger (job kick) → Poll (completion wait). Supports CSV, JSON, JSONL. New `StagedDestination` Protocol.
- **OAuth2 Client Credentials auth** (#259): Token exchange with caching for REST API destination.
- **REST API destination pagination** (#260): Fetch data from paginated APIs before processing. Supports 3 strategies: offset/limit, cursor-based, and HTTP Link headers. Extractable via `fetch_paginated()` for read-before-write upsert patterns.
- **`drt init --from-dbt`** (#215): Generate sync YAML scaffolds from dbt `manifest.json`.
- **`--output json` for validate/list** (#230): Structured JSON output for `drt validate` and `drt list`.
- **MCP Server: `drt_list_connectors`** (#262): New tool listing all available sources and destinations.
- **MCP Server: improved `drt_validate`** (#262): Per-file error reporting via `load_syncs_safe()`.
- **`drt test` — freshness, unique, accepted_values validators** (#233): New test types for post-sync validation. `freshness` validates data recency (e.g., `max_age: "7 days"`), `unique` prevents duplicates, `accepted_values` enforces column whitelists. Supports human-readable time formats ("7 days", "24 hours", etc). Follow-up to #141 (row_count, not_null tests).
- **BigQuery → Discord example** (#266): Alert pipeline that queries BigQuery for recent error rows and posts a Discord notification per row via Incoming Webhook using incremental sync. Includes `examples/bigquery_to_discord/`.
- **Notion destination** (#38): Append rows to a Notion database via the Notion API. Supports `properties_template` (Jinja2 → JSON for page properties), `rich_text` fallback for records without a template, retry with backoff, rate limiting (3 req/s). Contributed by @armorbreak001, with credit to @PFCAaron12 and @pureqin.
- **Twilio SMS destination** (#159): Send SMS per row via Twilio Messages API. Basic auth (Account SID + Auth Token), Jinja2 templates for message body and per-row phone number (`to_template`), E.164 format validation. Contributed by @PFCAaron12.
- **Intercom destination** (#163): Create/update contacts via Intercom REST API v2. Bearer token auth, Jinja2 `properties_template` for flexible contact attribute mapping. Contributed by @PFCAaron12.
- **`--log-format json` flag** (#275): Structured JSON logging for `drt run`. Emits one JSON object per sync event (start, complete, error) to stderr. Separate from `--output json` (final result). Contributed by @Khush-domadia.
- **GOVERNANCE.md** (#310): Formalized contributor ladder (Contributor → Triage Collaborator → Owner), soft-assignment policy (first-PR-wins), lazy consensus decision model, and 14-day stale rule.

### Fixed

- **MySQL destination**: auto-serialize `dict`/`list` values to JSON strings before passing to pymysql (#311). Also shipped in [0.5.1](#051---2026-04-14).

## [0.5.5] - 2026-04-16

### Added

- **`drop_match_columns`** (#347): Automatically remove lookup match columns from INSERT after FK resolution. Prevents `Unknown column` errors when the destination table doesn't have the match columns. Enabled by default; set `drop_match_columns: false` to opt out.

## [0.5.4] - 2026-04-16

### Added

- **`destination_lookup`** (#345): Resolve foreign key values by querying the destination database during sync. When syncing related tables, child tables can now reference parent table auto-increment IDs without triggers or denormalized schemas. Supports MySQL, PostgreSQL, and ClickHouse destinations. Configure via `lookups` field in destination YAML with `on_miss: skip | fail | null`. Guide: `docs/guides/destination-lookup.md`.

## [0.5.1] - 2026-04-14

### Fixed

- **MySQL destination**: auto-serialize `dict`/`list` values to JSON strings before passing to pymysql (#311). Fixes BigQuery → MySQL reverse ETL where BigQuery JSON columns come back as Python `dict`/`list`. Backward compatible — strings, ints, and other types pass through unchanged.

## [0.5.0] - 2026-04-13

### Added

#### Sources

- **Snowflake source connector** (#162): Extract data from Snowflake using `snowflake-connector-python`. Supports account, user, password/password_env, database, schema, warehouse, and optional role. Install: `pip install drt-core[snowflake]`
- **MySQL source connector** (#19): Extract data from MySQL databases using pymysql. Supports host, port, dbname, user, password via env var. Backtick quoting for table names. Install: `pip install drt-core[mysql]`

#### Destinations

- **ClickHouse destination** (#166): Insert rows via `clickhouse-connect` (HTTP). Supports connection string, HTTPS via `secure` flag. Install: `pip install drt-core[clickhouse]`
- **Parquet file destination** (#171): Write to local Parquet files with snappy/gzip/zstd compression and partition columns. Install: `pip install drt-core[parquet]`
- **CSV/JSON/JSONL file destination** (#67): Write to local files using stdlib csv/json. No extra dependencies
- **Microsoft Teams destination** (#85): Incoming Webhook with plain text and Adaptive Card payloads
- **Jira destination** (#158): Create/update Jira issues via REST API v3 with Jinja2 templates
- **Linear destination** (#195): Create Linear issues via GraphQL API with Jinja2 templates
- **SendGrid email destination** (#194): Transactional emails via SendGrid v3 Mail Send API

#### CLI

- **`drt test` command** (#141): Post-sync data validation. Supports `row_count` (min/max) and `not_null` (columns) tests for DB destinations (PostgreSQL, MySQL, ClickHouse)
- **`--output json` flag** (#142): Structured JSON output for `drt run` and `drt status`. Designed for CI/scripting use
- **`--profile` CLI override** (#238): Runtime profile switching via `--profile` flag or `DRT_PROFILE` env var. Precedence: flag > env var > drt_project.yml
- **Improved `drt validate` errors** (#104): User-friendly error messages with YAML field paths instead of raw Pydantic tracebacks. Shows ✓/✗ per sync file
- **Dry-run summary** (#219): Enhanced `--dry-run` shows Source, Destination, Rows to sync, and Sync mode

#### Multi-environment support

- **`${VAR}` env var substitution** (#240): Use `${VAR}` syntax in `model:` field for environment-specific SQL queries
- **dbt manifest resolution** (#239): `ref('model')` now resolves from dbt `target/manifest.json` when available. Resolution order: SQL file > dbt manifest > profile-based expansion
- **`secrets.toml`** (#143): Local secret management via `.drt/secrets.toml` (dlt-like pattern). Resolution order: explicit value > env var > secrets.toml

#### Infrastructure

- **Dockerfile and docker-compose** (#161): `python:3.12-slim` image with `DRT_EXTRAS` build arg, non-root user
- **Codecov integration** (#103): Coverage badge, PR reports. Patch checks set to informational
- **Pre-commit hooks** (#105): ruff + mypy
- **Python 3.13 support** (#225): Added to CI matrix and classifiers
- **`duration_seconds` in SyncResult** (#226): Track sync execution time

### Tests

- 382+ tests (up from 170+ in v0.4.3)
- Source and destination protocol contract tests (#209, #210)
- Slack Block Kit tests (#97), state persistence tests (#100)
- CLI validate error case tests (#98)
- Codecov coverage at 64%

## [0.4.3] - 2026-04-02

### Added

- **ClickHouse source connector** (#156 by @msarwal345): High-performance source using `clickhouse-connect` (HTTP interface). Supports host, port, database, user, and password/password_env. Includes `examples/clickhouse_to_rest/`.
- **SQLite in `drt init` wizard** (#153): Users can now select `sqlite` as a source type during project initialization
- **README.ja.md** (#150 by @Ami-3110): Japanese translation of README with language toggle
- **Fractional rps test** (#144 by @Pranavv157): Additional RateLimiter test for non-integer request rates

### Fixed

- **Discord destination not wired in CLI** (#152): `type: discord` syncs now work correctly — was raising `ValueError` since v0.4.2
- **API_REFERENCE.md** (#154): Added missing SQLite source and Discord destination config examples
- **drt-init skill** (#155): Updated source type list to include all supported sources (redshift, sqlite)

## [0.4.2] - 2026-04-02

### Added

- **SQLite source connector** (#146 by @PFCAaron12): Zero-dependency source using Python's built-in `sqlite3` — ideal for testing, prototyping, and local development
- **Discord webhook destination** (#147 by @xingzihai): Send messages and rich embeds to Discord channels via webhooks, following the same pattern as the Slack destination

### Improved

- **Redshift unit tests** (#145 by @HoudaBelhad): Replaced fake test double with real `RedshiftSource` + mock-based tests covering connection, query execution, error handling, and protocol compatibility
- **RateLimiter boundary tests** (#125 by @kipra19821810-cloud): Added 11 boundary-value tests covering zero/negative rps, large values, rapid calls, and state management — regression tests for the v0.3.3 ZeroDivisionError fix

### Fixed

- **psycopg2 lazy import**: Moved top-level `from psycopg2 import sql` to method-level import, fixing CI failures in environments without psycopg2 installed

### Community

🎉 This release features contributions from **4 community members** — thank you @PFCAaron12, @xingzihai, @HoudaBelhad, and @kipra19821810-cloud!

## [0.4.1] - 2026-04-01

### Added

- **Upsert sync mode** (#130): `mode: upsert` for explicit intent in YAML — behaves like `mode: full` with `upsert_key`
- **SSL/TLS for DB destinations** (#131): Optional `ssl` config for PostgreSQL and MySQL with `ca_env`, `cert_env`, `key_env`
- **Connection string support** (#132): `connection_string_env` for PostgreSQL and MySQL — alternative to individual host/port/dbname params
- **dagster-drt: DagsterDrtTranslator** (#127): Customise asset keys, group names, deps, and metadata (follows dagster-dbt pattern)
- **dagster-drt: DrtConfig with dry_run** (#126): RunConfig controllable from Dagster UI, plus `drt_assets(dry_run=True)` for build-time defaults
- **dagster-drt: MaterializeResult** (#128): Assets return structured metadata (rows_synced, rows_failed, rows_skipped, dry_run, row_errors_count)

### Fixed

- **MySQL type: ignore** (#133): Replaced `# type: ignore[import-untyped]` with `types-PyMySQL` dev dependency

## [0.4.0] - 2026-03-31

### Added

- **Google Sheets destination** (#64): Overwrite or append mode. Service account or ADC auth. Install: `pip install drt-core[sheets]`
- **PostgreSQL destination** (#81): Upsert via `INSERT ... ON CONFLICT DO UPDATE`. Row-level error handling
- **MySQL destination** (#83): Upsert via `INSERT ... ON DUPLICATE KEY UPDATE`. Row-level error handling
- **dagster-drt integration** (#63): `integrations/dagster-drt/` package. Expose drt syncs as Dagster assets with `drt_assets()`
- **dbt manifest reader** (#65): `drt.integrations.dbt.resolve_ref_from_manifest()` resolves `ref()` from `target/manifest.json`
- **Google Sheets example** (#94): `examples/duckdb_to_google_sheets/`
- **dbt usage guide**: `docs/guides/using-with-dbt.md`

### Refactored

- **Type safety overhaul** (#80, #110): Eliminated 13 `type: ignore` annotations. `Destination.load()` config type `object` → `DestinationConfig`. `Source.extract()` config type narrowed to `ProfileConfig`. `DetailedSyncResult` removed in favor of `SyncResult`. All sources/destinations use `assert isinstance()` for type narrowing
- **Redshift lazy import** (#78): `RedshiftSource` no longer crashes when `psycopg2` is not installed

### Tests

- 136 tests total (up from 101 in v0.3.4)

## [0.3.4] - 2026-03-30

### Added

- **Redshift source** (#76, closes #20): Amazon Redshift connector via psycopg2. New `RedshiftProfile` with host/port/dbname/user/password_env/schema fields (port defaults to 5439). `drt init` wizard updated to support `redshift` source type. Install: `pip install drt-core[redshift]`.

## [0.3.3] - 2026-03-30

### Fixed

- **SQL injection** (#42): `cursor_field` now validated as a safe SQL identifier; `last_cursor_value` escaped with standard `''` quoting in incremental WHERE clauses
- **row_errors lost** (#43): `run_sync()` now aggregates `row_errors` across all batches
- **Numeric cursor comparison** (#44): Incremental cursor uses numeric comparison (`float()`) when possible — fixes `"9" > "10"` regression for integer/timestamp cursors
- **HTTP timeout** (#45): `httpx.Client(timeout=30.0)` added to all destinations (REST API, Slack, HubSpot, GitHub Actions) — prevents indefinite hangs
- **BasicAuth empty credentials** (#46): `BasicAuth` now raises `ValueError` when `username_env`/`password_env` are not set (was silently sending empty credentials)
- **Corrupted state.json** (#47): `JSONDecodeError` on corrupted `.drt/state.json` is caught; prints warning to stderr and resets to empty state instead of crashing all syncs
- **Slack retry** (#48): `SlackDestination` now uses `with_retry` — 429 rate limit responses are retried with backoff
- **Incremental cursor_field validation** (#49): `SyncOptions` raises `ValidationError` if `mode: incremental` is set without `cursor_field`
- **RateLimiter ZeroDivisionError** (#50): `RateLimiter.acquire()` returns immediately when `requests_per_second <= 0`

### Added

- **Destination unit tests** (#51): 10 new unit tests for `SlackDestination`, `HubSpotDestination`, `GitHubActionsDestination` (84 tests total)

### Refactored

- **DetailedSyncResult unification** (#52): Slack, HubSpot, and GitHub Actions destinations now use `DetailedSyncResult` + `RowError` — consistent row-level error reporting across all destinations

## [0.3.2] - 2026-03-30

### Fixed

- `load_profile()` が `profiles.yml` の `location` フィールドを `BigQueryProfile` に渡していなかった問題を修正。常にデフォルト `"US"` が使われていた。 ([#58](https://github.com/drt-hub/drt/issues/58), [#59](https://github.com/drt-hub/drt/pull/59))

## [0.3.1] - 2026-03-30

### Fixed

- **BigQuery location**: `profiles.yml` now supports a `location` field (e.g. `"EU"`, `"asia-northeast1"`); passed to `bigquery.Client()` so queries route to the correct regional endpoint. Defaults to `"US"` for backwards compatibility. ([#54](https://github.com/drt-hub/drt/issues/54))
- `drt init` wizard now prompts for dataset location when configuring a BigQuery profile.

## [0.3.0] - 2026-03-30

### Added

#### MCP Server

- `drt mcp run` — start a FastMCP server (stdio transport) for Claude Desktop, Cursor, and any MCP-compatible client
- 5 MCP tools: `drt_list_syncs`, `drt_run_sync`, `drt_get_status`, `drt_validate`, `drt_get_schema`
- Install: `pip install drt-core[mcp]`

#### AI Skills for Claude Code

- `.claude/commands/drt-create-sync.md` — `/drt-create-sync` skill: generate sync YAML from user intent
- `.claude/commands/drt-debug.md` — `/drt-debug` skill: diagnose and fix failing syncs
- `.claude/commands/drt-init.md` — `/drt-init` skill: guide through project initialization
- `.claude/commands/drt-migrate.md` — `/drt-migrate` skill: migrate from Census/Hightouch to drt

#### LLM-readable Docs

- `docs/llm/CONTEXT.md` — architecture, key concepts, state file format (optimized for LLM consumption)
- `docs/llm/API_REFERENCE.md` — all config fields with types, defaults, and full YAML examples

#### Row-level Error Details

- `RowError` dataclass: `batch_index`, `record_preview` (200-char PII-safe), `http_status`, `error_message`, `timestamp`
- `drt run --verbose` and `drt status --verbose` show per-row error details
- `RestApiDestination` now populates `row_errors` on each failure

### Tests

- 82 tests total (up from 53 in v0.2)
- MCP server tests auto-skip when `fastmcp` not installed

## [0.2.0] - 2026-03-30

### Added

#### Incremental Sync

- `sync.mode: incremental` — watermark-based incremental sync using a `cursor_field`
- Saves `last_cursor_value` in `.drt/state.json` after each run
- Injects `WHERE {cursor_field} > '{last_cursor_value}'` automatically on next run
- Works with both `ref('table')` and raw SQL models

#### Retry Configuration

- `sync.retry` is now fully configurable per-sync in YAML (`max_attempts`, `initial_backoff`, `backoff_multiplier`, `max_backoff`, `retryable_status_codes`)
- Previously used a hardcoded default; now reads from `SyncOptions.retry`

### Fixed

- Removed duplicate `RetryConfig` dataclass from `destinations/retry.py` (was shadowing the Pydantic model in `config/models.py`)

### Tests

- 6 new unit tests for incremental sync (resolver + engine)
- Integration test suite cleaned up: removed monkey-patching of internal `_DEFAULT_RETRY`

## [0.1.1] - 2026-03-29

### Fixed

- `drt --version` now correctly displays the installed package version (e.g. `0.1.1`) instead of the stale hardcoded value `0.1.0.dev0`. Version is now read dynamically via `importlib.metadata`.

## [0.1.0] - 2026-03-28

### Added

#### CLI

- `drt init` — interactive project wizard (supports BigQuery, DuckDB, PostgreSQL)
- `drt run` — run all syncs or a specific sync (`--select`)
- `drt run --dry-run` — preview without writing data
- `drt list` — list sync definitions
- `drt validate` — validate sync YAML configs
- `drt status` — show recent sync run results

#### Sources

- BigQuery (`pip install drt-core[bigquery]`)
- DuckDB (`pip install drt-core[duckdb]`)
- PostgreSQL (`pip install drt-core[postgres]`)

#### Destinations

- REST API (core) — generic HTTP with Jinja2 body templates, auth, rate limiting, retry
- Slack Incoming Webhook (core)
- GitHub Actions `workflow_dispatch` trigger (core)
- HubSpot Contacts / Deals / Companies upsert (core)

#### Configuration

- `profiles.yml` credential management (dbt-style, stored in `~/.drt/`)
- Declarative sync YAML with Jinja2 templating
- Auth: Bearer token, API key, Basic auth
- Rate limiting and exponential backoff retry
- `on_error: skip | fail` per sync
