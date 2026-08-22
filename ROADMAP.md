# Roadmap

> **SSoT for upcoming releases.** For shipped releases, see [CHANGELOG.md](CHANGELOG.md) and [GitHub Releases](https://github.com/drt-hub/drt/releases). For issue-level tracking, see each version's [milestone](https://github.com/drt-hub/drt/milestones).

Targets are indicative, not guarantees. Scope may shift between versions — when that happens, this file is updated first and issues are re-labeled to match.

---

## v0.7 — Production Ready ✅ Shipped 2026-05-06

Released as **v0.7.0** on 2026-05-06. See [CHANGELOG.md](CHANGELOG.md#070---2026-05-06) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.7.0) for the full feature list.

Tail items continue in [v0.7.1](#v071--production-ready-follow-up--shipped-2026-05-07) below.

---

## v0.7.1 — Production Ready Follow-up ✅ Shipped 2026-05-07

Released as **v0.7.1** on 2026-05-07. See [CHANGELOG.md](CHANGELOG.md#071---2026-05-07) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.7.1) for the full feature list.

Tail items continue in [v0.7.2](#v072--production-ready-follow-up-2--shipped-2026-05-11) below.

---

## v0.7.2 — Production Ready Follow-up #2 ✅ Shipped 2026-05-11

Released as **v0.7.2** on 2026-05-11. See [CHANGELOG.md](CHANGELOG.md#072---2026-05-11) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.7.2) for the full feature list.

Followup items continue in their own issues (#482 telemetry retention cleanup, #483 swap-path psycopg2.sql migration).

---

## v0.7.3 — Postgres Patch ✅ Shipped 2026-05-17

Released as **v0.7.3** on 2026-05-17. See [CHANGELOG.md](CHANGELOG.md#073---2026-05-17) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.7.3) for the full feature list.

Strict patch release — cherry-pick of PR #498 (Postgres schema-qualified `Identifier()` composition fix, closing #442) on top of the v0.7.2 line. No new features, no breaking changes. v0.8 work continues in parallel.

---

## v0.7.4 — MySQL Patch ✅ Shipped 2026-05-23

Released as **v0.7.4** on 2026-05-23. See [CHANGELOG.md](CHANGELOG.md#074---2026-05-23) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.7.4) for the full feature list.

Strict patch release — cherry-pick of PR #514 (MySQL `_quote_ident` applied across all SQL paths, closing #511) on top of the v0.7.3 release line. MySQL counterpart to the Postgres `Identifier()` fix that shipped in v0.7.3. PR #514 originally landed on `main` two days after the v0.7.3 tag, so the wheel published as `drt-core==0.7.3` did **not** contain it; v0.7.4 is the release that actually delivers the fix. No new features, no breaking changes. v0.8 work continues in parallel.

---

## v0.7.5 — Production Ready follow-up #3 + Tech Foundation Hardening ✅ Shipped 2026-05-25

Released as **v0.7.5** on 2026-05-25. See [CHANGELOG.md](CHANGELOG.md#075---2026-05-25) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.7.5) for the full feature list.

Closes the **Tech Foundation Hardening epic** ([#538](https://github.com/drt-hub/drt/issues/538), 11 child issues): CI nightly + publish gate + CodeQL + pip-audit + SBOM, DuckDB E2E harness + boundary tests, `ErrorFormatter` / `--detailed` / `--template` UX, plus load-bearing refactors (`SyncObserver` engine seam, destinations serializer + config base class consolidation, `cli/main.py` split Phase 1). Also ships the accumulated work since v0.7.4 — REST API source polish, sync catalog (#499 P1+P2), MCP test tool, OTel Phase 1 config, hardcoded secret detection, lookup ambiguity warning, orphan shadow cleanup, `drt init` "Next steps:" block. No new connectors, no breaking changes — drop-in upgrade from v0.7.2 / v0.7.3 / v0.7.4.

---

## v0.8.3 — Diff Polish ✅ Shipped 2026-07-27

Released as **v0.8.3** on 2026-07-27. See [CHANGELOG.md](CHANGELOG.md#083---2026-07-27) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.8.3) for the full feature list.

**Theme: seeing what a run will do — and what it quietly did.** `--dry-run --diff` now previews the **mirror `DELETE` pass** ([#693](https://github.com/drt-hub/drt/issues/693)) across all three strategies, read-only and labelled distinctly from a replace-mode rebuild — previously the destructive half of a `mode: mirror` sync was invisible until it ran. The same preview got cheaper: the destination lookup batches by source primary keys instead of scanning the whole table ([#470](https://github.com/drt-hub/drt/issues/470)). **`alerts.on_degraded`** ([#784](https://github.com/drt-hub/drt/issues/784)) adds thresholds on the failure modes that *succeed* (creeping error rate, SLA breach, empty source, growing DLQ), and **`drt test`** gains custom SQL `query` tests, `severity: warn`, and `--store-failures` ([#779](https://github.com/drt-hub/drt/issues/779)). Internally the Postgres/MySQL destinations shed their duplicated orchestration into a shared base ([#719](https://github.com/drt-hub/drt/issues/719), closed) and the MCP server / `schema.py` / `run.py` were split along the same lines ([#723](https://github.com/drt-hub/drt/issues/723)). No breaking changes — drop-in upgrade from v0.8.2.

*Deferred, still open:* `--diff-fields` (#471) and API-based SaaS diff (#472) remain feedback-gated parking spots; Delta/Iceberg predicate pushdown (#679) is a source-side perf item tracked separately.

---

## v0.8.2 — Navigable docs hosting (single-object `--inline`) ✅ Shipped 2026-07-21

Released as **v0.8.2** on 2026-07-21. See [CHANGELOG.md](CHANGELOG.md#082---2026-07-21) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.8.2) for the full feature list.

**A focused follow-up completing v0.8.1's `--inline` ([#818](https://github.com/drt-hub/drt/issues/818)).** v0.8.1 made each `drt docs` page self-contained (fixing *styling* on an authenticated GCS / S3 object URL) but left the site multi-object, so navigation still dead-ended — a per-object authenticated viewer isn't a web server and the relative inter-object links break. `drt docs generate --inline` now emits the **entire catalog as one navigable HTML object** ([#821](https://github.com/drt-hub/drt/issues/821), the Elementary single-file model): overview, DAG, and every sync / source / destination / tag become `<section>`s with in-page `#hash` navigation, so it both renders *and* navigates from a single authenticated object URL — zero sub-resource and zero inter-object requests. Per-page display byte-identical to the default multi-file output (unchanged). No breaking changes — drop-in upgrade from v0.8.1.

---

## v0.8.1 — Reverse-ETL activation (`match_policy`) + docs hosting + hardening ✅ Shipped 2026-07-21

Released as **v0.8.1** on 2026-07-21. See [CHANGELOG.md](CHANGELOG.md#081---2026-07-21) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.8.1) for the full feature list.

**A follow-up release on the v0.8.0 line, themed around reverse-ETL activation semantics.** **`sync.match_policy`** ([#757](https://github.com/drt-hub/drt/issues/757)) brings Census/Hightouch-style `update_only` / `create_only` to the upsert write path — touch only rows a CRM already has (enrichment, no junk contacts) or only insert new ones (seed once, never overwrite) — shipping on **Postgres** (clean `rowcount` skip detection) and **HubSpot** (POST-409 / PATCH-404), with match-policy skips named via a new `SyncResult.skipped_no_match` counter surfaced in `drt run` and `--output json`. **`drt docs generate --inline`** ([#818](https://github.com/drt-hub/drt/issues/818)) emits self-contained HTML (CSS/JS inlined, no `assets/`) so the catalog renders on an authenticated object store (GCS `storage.cloud.google.com` / S3 presigned URLs) where per-object auth 401s relative asset fetches — display byte-identical to the default multi-file output. **Manifest schema v2** ([#698](https://github.com/drt-hub/drt/issues/698)) carries per-sync run history, declared column facts (`field_mappings` + `mask`), and DLQ depth, with embedded error text redacted by default. Plus hardening: tracked mirror pre-provisions its `_drt_synced_keys` state table without a `CREATE` grant ([#695](https://github.com/drt-hub/drt/issues/695)), and HTTP retries honour the server's `Retry-After` header ([#769](https://github.com/drt-hub/drt/issues/769)). No breaking changes — drop-in upgrade from v0.8.0.

---

## v0.8.0 — `drt docs` site + developer-experience pull-in wave ✅ Shipped 2026-07-17

Released as **v0.8.0** on 2026-07-17. See [CHANGELOG.md](CHANGELOG.md#080---2026-07-17) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.8.0) for the full feature list.

**The release gate was the `drt docs` site** ([#499](https://github.com/drt-hub/drt/issues/499)), and it reaches its designed form: the static HTML catalog gains an ownership-zone lineage DAG rendered as **static SVG** from a deterministic layout engine ([#701](https://github.com/drt-hub/drt/issues/701)/[#796](https://github.com/drt-hub/drt/pull/796), replacing runtime Mermaid), a single-`:root` token design system with light/dark theming and connector badges ([#704](https://github.com/drt-hub/drt/pull/704)), empty/error states ([#751](https://github.com/drt-hub/drt/pull/751)), an as-written YAML tab ([#752](https://github.com/drt-hub/drt/pull/752)), an a11y/finishing pass ([#753](https://github.com/drt-hub/drt/pull/753)), **byte-identical regeneration** for diffability ([#697](https://github.com/drt-hub/drt/issues/697)), and two architecture ADRs under `docs/adr/` ([#500](https://github.com/drt-hub/drt/issues/500)). It is **security-hardened by default** ([#696](https://github.com/drt-hub/drt/issues/696)): destination labels and node ids no longer leak endpoints / phone numbers / emails / bucket names into a hosted site — the first hash-of-`describe()` node-id design was caught brute-forceable in review and replaced with safe-label-derived ids; MCP's `drt_get_manifest` inherits the safe default with a `full_labels` opt-in ([#807](https://github.com/drt-hub/drt/pull/807)). Alongside it, a wave of **dbt/dlt-familiar developer experience**: project `vars:` + `{{ var('name') }}` + `--vars` ([#783](https://github.com/drt-hub/drt/issues/783)), `drt build` (run + test in one pass, [#777](https://github.com/drt-hub/drt/issues/777)), `--fail-fast` / `--limit` / `--failed` on `drt run` ([#775](https://github.com/drt-hub/drt/issues/775)/[#774](https://github.com/drt-hub/drt/issues/774)/[#773](https://github.com/drt-hub/drt/issues/773)), dbt-style selection v2 (globs, `--exclude`, `destination:`, [#771](https://github.com/drt-hub/drt/issues/771)), incremental REST API extraction ([#767](https://github.com/drt-hub/drt/issues/767)), late-arrival `watermark.lag` ([#759](https://github.com/drt-hub/drt/issues/759)), and `drt deploy github-actions` scaffolding ([#785](https://github.com/drt-hub/drt/issues/785)). Config internals were split for maintainability (`models.py` → destination/sync-option modules [#798](https://github.com/drt-hub/drt/pull/798); profiles → `profiles.py` [#799](https://github.com/drt-hub/drt/pull/799)), and CI hardened so the full matrix runs on every PR ([#801](https://github.com/drt-hub/drt/issues/801)). No breaking changes — drop-in upgrade from v0.7.11.

---

## v0.7.11 — Databricks write fix + native paramstyle, docs-site engine, real-warehouse validation ✅ Shipped 2026-07-10

Released as **v0.7.11** on 2026-07-10. See [CHANGELOG.md](CHANGELOG.md#0711---2026-07-10) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.7.11) for the full feature list.

**The urgency lever is Databricks**: v0.7.10 wheels break every parameterised Databricks write on databricks-sql-connector >=3.0 (native paramstyle forwards `%s` unexpanded, [#706](https://github.com/drt-hub/drt/issues/706)), and this release ships both the interim fix and the forward migration to native `?` markers with a staging anti-join for mirror deletes ([#707](https://github.com/drt-hub/drt/issues/707)). **Snowflake key-pair authentication** (`private_key_env`, [#737](https://github.com/drt-hub/drt/issues/737)) lands just in time — new Snowflake accounts enforce MFA on password sign-ins (hit live on the org smoke account), and a `TYPE = SERVICE` user with an RSA key pair is the sanctioned programmatic path. Databricks writes are also **batched into multi-row `VALUES` chunks** ([#734](https://github.com/drt-hub/drt/issues/734)) — a 127–255× round-trip reduction that the 300-key mirror probe motivated. It also finally puts `drt docs generate --format html` ([#510](https://github.com/drt-hub/drt/issues/510)) in a published wheel (it missed the v0.7.10 tag — [#715](https://github.com/drt-hub/drt/issues/715)), joined by the docs-site UI follow-ups ([#704](https://github.com/drt-hub/drt/pull/704)) and the deterministic DAG layout engine ([#713](https://github.com/drt-hub/drt/pull/713)). MCP reaches CLI parity — DLQ/retry, docs manifest, profile diagnostics ([#718](https://github.com/drt-hub/drt/issues/718)); schema-aware serialization gains its Databricks STRUCT/ARRAY/MAP/VARIANT leg ([#317](https://github.com/drt-hub/drt/issues/317) via [#680](https://github.com/drt-hub/drt/pull/680)); and the [#654](https://github.com/drt-hub/drt/issues/654) real-warehouse smoke epic closes with Snowflake / Databricks / BigQuery all live-validated (both mirror legs included), plus provisioning scripts, runbook, and a daily cost digest ([#738](https://github.com/drt-hub/drt/pull/738)). README modernised + Japanese i18n retired ([#712](https://github.com/drt-hub/drt/pull/712)); destination-layer dedup tranches ([#719](https://github.com/drt-hub/drt/issues/719)/[#722](https://github.com/drt-hub/drt/issues/722)). No breaking changes — drop-in upgrade from v0.7.10.

---

## v0.7.10 — Co-writer-safe mirror + lakehouse sources + PII masking ✅ Shipped 2026-07-02

Released as **v0.7.10** on 2026-07-02. See [CHANGELOG.md](CHANGELOG.md#0710---2026-07-02) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.7.10) for the full feature list.

**Mirror grows co-writer-safe delete semantics — the biggest correctness gap vs the SaaS tools closes.** `sync.mirror.strategy: tracked` ([#686](https://github.com/drt-hub/drt/issues/686)) adopts Census/Hightouch semantics: deletions are computed against the keys drt itself previously synced (drt-managed `_drt_synced_keys` state table in the destination, first-run baseline, lost-state re-baseline), so `mode: mirror` is finally safe on operational tables the application also writes to. `sync.mirror.scope` ([#687](https://github.com/drt-hub/drt/issues/687)) adds the stateless variant for 1:N parent/child regeneration — deletes restricted to parents observed in the run. Both Postgres + MySQL first (#317-style phasing). Sources: **Delta Lake + Apache Iceberg** ([#172](https://github.com/drt-hub/drt/issues/172) / [#173](https://github.com/drt-hub/drt/issues/173)) read lakehouse tables from local / S3 / GCS. Engine: **PII masking** `sync.mask` ([#427](https://github.com/drt-hub/drt/issues/427)) with hash / redact / truncate ([#660](https://github.com/drt-hub/drt/issues/660)). Correctness: **#317 Layer 3 schema-aware serialization** lands for Postgres / MySQL (INFORMATION_SCHEMA introspection) and Snowflake (`PARSE_JSON`); Databricks leg in flight ([#680](https://github.com/drt-hub/drt/pull/680)). Destinations: **Klaviyo** ([#418](https://github.com/drt-hub/drt/issues/418)) + **Airtable** ([#419](https://github.com/drt-hub/drt/issues/419)). Growth-adjacent: **VS Code extension** ([#293](https://github.com/drt-hub/drt/issues/293)) with bundled-schema drift CI guard, refreshed Quickstart GIF ([#377](https://github.com/drt-hub/drt/issues/377)), official site + X badges, first Japanese doc under `docs/` ([#95](https://github.com/drt-hub/drt/issues/95)) + weekly i18n staleness audit. Hardening: `~/.drt` credentials `0o600` ([#650](https://github.com/drt-hub/drt/issues/650)), OTel Phase 3 engine spans + batch export, gated real-warehouse smoke harness ([#674](https://github.com/drt-hub/drt/issues/674)), `drt destinations` 9-connector listing fix. No breaking changes — drop-in upgrade from v0.7.9.

---

## v0.7.9 — Cloud Destinations land + Dead Letter Queue + `drt profile` ✅ Shipped 2026-06-17

Released as **v0.7.9** on 2026-06-17. See [CHANGELOG.md](CHANGELOG.md#079---2026-06-17) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.7.9) for the full feature list.

**The largest accumulation since v0.7.0 — the v0.8 "Cloud Destinations" half lands early on the 0.7 line.** Six new destinations: **Amazon S3** ([#168](https://github.com/drt-hub/drt/issues/168)), **Google Cloud Storage** ([#169](https://github.com/drt-hub/drt/issues/169)), **Azure Blob Storage** ([#170](https://github.com/drt-hub/drt/issues/170)) (csv/json/jsonl/parquet + gzip, on a shared blob serialiser), **Databricks Delta Lake** ([#167](https://github.com/drt-hub/drt/issues/167)), **BigQuery** ([#165](https://github.com/drt-hub/drt/issues/165) — the oldest open connector request, building on [@PFCAaron12](https://github.com/PFCAaron12)'s [#584](https://github.com/drt-hub/drt/pull/584)), and **Elasticsearch / OpenSearch** ([#420](https://github.com/drt-hub/drt/issues/420)). Reliability: the **Dead Letter Queue** + `drt retry` ([#278](https://github.com/drt-hub/drt/issues/278)) persists per-record failures for replay. CLI: **`drt profile`** ([#423](https://github.com/drt-hub/drt/issues/423)) manages credential profiles. Engine: **`sync.mode: replace`** (truncate + zero-downtime swap) on Snowflake ([#434](https://github.com/drt-hub/drt/issues/434)) and Databricks ([#643](https://github.com/drt-hub/drt/issues/643)), **`sync.field_mappings`** declarative column renaming ([#415](https://github.com/drt-hub/drt/issues/415)), and Snowflake made fully queryable for `--diff` / `lookups` / `drt test` ([#468](https://github.com/drt-hub/drt/issues/468)). Tooling/hygiene: a state-based **docs/skills/MCP drift audit** + weekly workflow (connector-docs backlog now burned to **zero**), MCP server catch-up (`drt_doctor` + `compute_diff` parity), OpenTelemetry Phase 2 (NoOpTracer global provider, [#531](https://github.com/drt-hub/drt/issues/531)), and the **`/drt-troubleshoot`** skill ([#369](https://github.com/drt-hub/drt/issues/369)). No breaking changes — drop-in upgrade from v0.7.8. **v0.8.0 now reduces to the Growth / README push** (hero redesign, Quickstart GIF, blogs, Discord/X, VS Code extension) + the INFORMATION_SCHEMA correctness epic ([#317](https://github.com/drt-hub/drt/issues/317)).

---

## v0.7.8 — Mixpanel destination + ClickHouse identifier fix + empty-batch contract completion ✅ Shipped 2026-06-05

Released as **v0.7.8** on 2026-06-05. See [CHANGELOG.md](CHANGELOG.md#078---2026-06-05) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.7.8) for the full feature list.

**Community follow-up patch.** Two contributor PRs accumulated since v0.7.7 — a new **Mixpanel destination** ([#608](https://github.com/drt-hub/drt/pull/608) by [@Pawansingh3889](https://github.com/Pawansingh3889), people_set + import_events endpoints, EU residency, deterministic `$insert_id`, closes [#417](https://github.com/drt-hub/drt/issues/417)) and a **ClickHouse `_quote_ident` identifier fix** ([#610](https://github.com/drt-hub/drt/pull/610) by [@yodakanohoshi](https://github.com/yodakanohoshi)) that closes the ClickHouse leg of the qualified-identifier fix family alongside Postgres ([#498](https://github.com/drt-hub/drt/pull/498)) / MySQL ([#514](https://github.com/drt-hub/drt/pull/514)). The ClickHouse fix is the urgency lever — v0.7.7 users running `database.table` syntax hit a server-side `Code: 62` syntax error from `get_row_count`'s malformed identifier. Also completes the **empty-batch contract suite** ([#604](https://github.com/drt-hub/drt/pull/604)–[#606](https://github.com/drt-hub/drt/pull/606), 25 of 25 registered destinations) which surfaced a real bug in `staged_upload.finalize()` fixed in the same PR, ships user-facing **`sync.mode: mirror` documentation** ([#607](https://github.com/drt-hub/drt/pull/607)), the post-#608 Mixpanel wiring ([#609](https://github.com/drt-hub/drt/pull/609)), and i18n marker bump ([#603](https://github.com/drt-hub/drt/pull/603)). BigQuery destination is in flight ([#584](https://github.com/drt-hub/drt/pull/584)) and will trigger v0.7.9. No breaking changes — drop-in upgrade from v0.7.7. v0.8 Cloud Destinations work continues in parallel.

---

## v0.7.7 — `sync.mode: mirror` across SQL destinations ✅ Shipped 2026-06-01

Released as **v0.7.7** on 2026-06-01. See [CHANGELOG.md](CHANGELOG.md#077---2026-06-01) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.7.7) for the full feature list.

The first user-facing addition since v0.7.6 is the **`sync.mode: mirror`** differential-delete sync mode ([#340](https://github.com/drt-hub/drt/issues/340)), shipping in four landings across **Postgres (#596)**, **MySQL (#597)**, **ClickHouse (#598)**, and **Snowflake (#599)** — all four SQL destinations now upsert source rows and then DELETE destination rows whose `upsert_key` was not observed in the source, without the TRUNCATE / re-insert overhead of `replace` mode. BigQuery follows once the contributor PR #584 lands (then v0.7.8). Also lands the **`cli/main.py` split completion** — Phase 2b PR (a) + PR (b) + tighten finish the 1706 → 164 LOC split (-90%) begun in v0.7.5 — plus a `FakeSource` + destination contract test framework (#592–#595), a CI `check-changelog-required` guard (#590), a GCS storage import mypy fix (#588), and CI install line extension that unlocked ~102 silently-skipped SQL destination tests (raised total coverage 82.68 → 85.29). No breaking changes — drop-in upgrade from v0.7.6. v0.8 Cloud Destinations work continues in parallel.

---

## v0.7.6 — Small follow-up ✅ Shipped 2026-05-28

Released as **v0.7.6** on 2026-05-28. See [CHANGELOG.md](CHANGELOG.md#076---2026-05-28) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.7.6) for the full feature list.

Two additive features accumulated since v0.7.5 — a new **Amplitude destination** (#574, Identify API + HTTP V2 events API) and a new **`tojson_safe` Jinja2 filter** (#580 / PR #581) that unblocks `datetime` / `Decimal` / `UUID` columns flowing through REST API `body_template` rendering — plus a CLI `--log-format` typer-compatibility fix (#578), a follow-up retrofit of `ErrorFormatter` stage detection to an engine-emitted attribute (#571, supersedes the traceback-walk heuristic from #544), and Phase 2a of the `cli/main.py` split (#572, continues #565's Phase 1). No breaking changes — drop-in upgrade from v0.7.5. v0.8 work continues in parallel.

---

## v0.8 — Cloud Destinations & Growth

**Theme:** DWH/Lakehouse destinations + community growth push. Most of the original connector scope shipped early on the v0.7.x line — v0.8.0 releases the `drt docs` site plus a DX pull-in wave.

**Release gate:** the `drt docs` site epic (#499) — ADR (#500) · static pre-laid DAG (#701) · default-safe connection labels (#696) · byte-identical output (#697) · post-#677 design follow-ups (#702). The tag is cut when the docs experience is coherent end-to-end.

**Scope:**
- **Docs site (release gate)** — `drt docs` sync catalog & lineage UI (#499 / #500 / #701 / #696 / #697 / #702) — *`generate --format mermaid|json|html` shipped v0.7.5–v0.7.11*
- **DX pull-ins** *(from the 2026-07-10 dbt/dlt/competitor gap batch, #755–#786)* — `watermark.lag` (#759) · REST API source incremental (#767) · selection v2: glob / union / `--exclude` / `destination:`/`source:` (#771) · `drt run --failed` (#773) · `drt run --limit` (#774) · `--fail-fast` (#775) · `drt build` (#777) · `drt deploy github-actions` (#785)
- **Docs / skills debt** — skills freshness sweep (#717) · `sync.mask` documentation (#716)
- **Growth / README (non-blocking)** — hero section redesign (#281) · "Why OSS Reverse ETL" blog (#284) · production use case doc (#375) · Discord (#378, blocked on the server itself existing — #294) · X account link (#379) — these ride alongside the release and do **not** gate the tag; Reddit/HN launch (#289) stays opportunistic post-v0.8. Growth / GTM issues are **deliberately left without a milestone**: they are date-driven rather than release-gating, and milestoning them would keep a shipped version milestone permanently open. Track them via the `growth` / `gtm` title prefixes, or a dedicated non-version milestone if that ever becomes worth it
- **Shipped early on the v0.7.x line** — cloud destinations: BigQuery (#165) · Databricks Delta Lake (#167) · S3 (#168) · GCS (#169) · Azure Blob (#170) · Snowflake (#164) — sources: REST API (#422) · Delta Lake (#172) · Iceberg (#173) — reliability/correctness: DLQ (#278) · schema-aware serialization (#317) · `sync.mode: mirror` (#340) — ecosystem: GitHub Action (#292) · VS Code extension (#293) — dev tooling: FakeSource (#364) · `drt_run_test` (#368) · `/drt-troubleshoot` (#369) · `/drt-changelog` (#372) · validate connection test (#367)

**Out of scope:** Enterprise boundary (→ v0.9), Rust engine work (→ v1.x), diff polish (→ v0.8.3), warehouse hardening follow-ups (→ v0.8.4).

*Deferred, moved to v0.9:* dbt exposures export (#781) — the one DX pull-in that didn't ship with the v0.8.x line; re-milestoned 2026-08-06 so v0.8 can close.

**Target:** 2026-07 · **Progress:** [milestone/5](https://github.com/drt-hub/drt/milestone/5)

---

## v0.8.4 — Warehouse hardening & security ✅ Shipped 2026-08-03

Released as **v0.8.4** on 2026-08-03. See [CHANGELOG.md](CHANGELOG.md#084---2026-08-03) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.8.4) for the full feature list.

**Theme: the warehouse legs go fast, safe, and fully symmetrical.** Mirror symmetry closes out in full: `strategy: tracked` + `scope` composition, and the SQL-JOIN state diff that replaces a Python-side full read of `_drt_synced_keys` ([#692](https://github.com/drt-hub/drt/issues/692) / [#694](https://github.com/drt-hub/drt/issues/694), both closed) — every SQL destination implementing `sync.mode: mirror` now supports both, on every dialect. **Query tagging** ([#768](https://github.com/drt-hub/drt/issues/768)) puts a `sync`/`run_id` SQL comment — plus native BigQuery/Snowflake/Databricks tagging — on every query drt issues, read or write. Extract robustness (streaming via server-side cursors, [#765](https://github.com/drt-hub/drt/issues/765); source-side retry, [#766](https://github.com/drt-hub/drt/issues/766)) and rate limiting v2 ([#769](https://github.com/drt-hub/drt/issues/769)) — both from the 2026-07-10 gap batch — land here too, alongside a sanctioned way to reset drt's own state (`--full-refresh` + `drt state show`/`reset`, [#776](https://github.com/drt-hub/drt/issues/776)). Three items originally scoped here shipped early once their urgency became clear, ahead of this milestone: Snowflake key-pair auth ([#737](https://github.com/drt-hub/drt/issues/737)) and Databricks batched writes ([#734](https://github.com/drt-hub/drt/issues/734)) in v0.7.11, and the tracked-mirror destination-privileges doc ([#695](https://github.com/drt-hub/drt/issues/695)) in v0.8.1; the dead-branch/docs-hardening cleanups ([#699](https://github.com/drt-hub/drt/issues/699)/[#703](https://github.com/drt-hub/drt/issues/703)) were already closed by the time this milestone was scoped. No breaking changes — drop-in upgrade from v0.8.3.

---

## v0.8.5 — Databricks composite-key mirror fix ✅ Shipped 2026-08-05

Released as **v0.8.5** on 2026-08-05. See [CHANGELOG.md](CHANGELOG.md#085---2026-08-05) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.8.5) for the full feature list.

Strict patch release — cherry-pick of the fix for [#908](https://github.com/drt-hub/drt/issues/908) on top of the v0.8.4 line. `sync.mode: mirror` on Databricks failed at finalize whenever `destination.upsert_key` had more than one column (Delta rejects the tuple-`IN` anti-join `#707` had used) — present since v0.7.11 and shipped in every release since, and unconditionally hit by any `tracked` + `scope` configuration on Databricks (`scope` must be a subset of `upsert_key`). Composite keys now go through `MERGE` instead; single-column keys are unchanged. Caught in review, not by the unit suite — a mock cursor can't reject invalid SQL, and the live smoke harness had never exercised a composite key or `tracked`/`scope` on Databricks; both gaps are now closed. No breaking changes — drop-in upgrade from v0.8.4.

---

## v0.9.0 — Engine Foundation ✅ Shipped 2026-08-11

Released as **v0.9.0** on 2026-08-11. See [CHANGELOG.md](CHANGELOG.md#090---2026-08-11) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.9.0) for the full feature list.

**Theme: ADR 0005's state-location foundation, delivered end to end.** [#756](https://github.com/drt-hub/drt/issues/756) ships all four steps — the `state.backend` config surface + factory, a working GCS backend, a working S3 backend, and the operator/migration docs — so run state, history, and the DLQ survive an ephemeral CI runner and are shareable across a team, with generation/ETag-preconditioned writes closing the same concurrent-writer failure class as [#919](https://github.com/drt-hub/drt/issues/919) for drt's own state. Write-path semantics grew three ways: first-class `run_id`/`sync_run_id` correlation plus opt-in `metadata_columns` bookkeeping ([#762](https://github.com/drt-hub/drt/issues/762)), declarative `computed_fields` ([#763](https://github.com/drt-hub/drt/issues/763)), and REST API `body_mode: batch` ([#770](https://github.com/drt-hub/drt/issues/770)). CI/testing depth: `state:modified` selection against a prior manifest baseline ([#772](https://github.com/drt-hub/drt/issues/772), [ADR 0006](docs/adr/0006-manifest-schema-v3.md)) and zero-credential `sync.unit_tests` ([#780](https://github.com/drt-hub/drt/issues/780)). Security: secret provider URIs for AWS/GCP Secret Manager and Vault ([#782](https://github.com/drt-hub/drt/issues/782)). `drt serve` gained a real delivery contract — coalescing instead of dropping concurrent triggers, `202` + poll instead of holding the request open, pluggable auth ([#854](https://github.com/drt-hub/drt/issues/854)). Two data-loss bugs closed, both with the same reconcile-against-fresh-state pattern: concurrent GCS watermark writers silently clobbering each other ([#919](https://github.com/drt-hub/drt/issues/919)) and `drt retry` silently dropping dead letters appended by a concurrent `drt run` ([#955](https://github.com/drt-hub/drt/issues/955)) — local (non-object-store) cross-process safety for both stores is tracked separately as [#963](https://github.com/drt-hub/drt/issues/963). The CLI/MCP parity gap that let `--limit`/`--fail-fast`/`--vars` ship without their MCP-tool equivalents for three releases is now a CI gate ([#871](https://github.com/drt-hub/drt/issues/871)), and all 11 gaps it found are closed ([#870](https://github.com/drt-hub/drt/issues/870)). Also: scope-aware SQL diff closing the last #694-part-2 memory-win gap ([#890](https://github.com/drt-hub/drt/issues/890)). No breaking changes — drop-in upgrade from v0.8.5.

**Deferred to unscheduled backlog** *(no milestone — pull in when a release theme fits)*: diff-based incremental ([#755](https://github.com/drt-hub/drt/issues/755)) and the "Schema management" cluster — managed destination tables ([#760](https://github.com/drt-hub/drt/issues/760)), column contracts ([#761](https://github.com/drt-hub/drt/issues/761)), dbt-derived contracts ([#896](https://github.com/drt-hub/drt/issues/896)) — all block on the same unbuilt warehouse managed-table primitive, now its own issue ([#960](https://github.com/drt-hub/drt/issues/960), filed 2026-08-11); windowed backfill ([#758](https://github.com/drt-hub/drt/issues/758)), pre/post-sync SQL hooks ([#764](https://github.com/drt-hub/drt/issues/764), no longer blocked now that #762's `run_id` plumbing has landed), run artifacts ([#778](https://github.com/drt-hub/drt/issues/778)); the older-backlog cluster — multi-destination fan-out ([#425](https://github.com/drt-hub/drt/issues/425)), `depends_on` sync graph ([#426](https://github.com/drt-hub/drt/issues/426)), built-in scheduler ([#428](https://github.com/drt-hub/drt/issues/428), needs re-litigating against [ADR 0004](docs/adr/0004-streaming-and-event-triggered-syncs.md)'s Tier 1 "no daemon" posture), upstream API change detection ([#649](https://github.com/drt-hub/drt/issues/649), still research-phase), reconciliation doc ([#825](https://github.com/drt-hub/drt/issues/825)); `match_policy` remaining destinations ([#757](https://github.com/drt-hub/drt/issues/757), open-ended tracking issue by design, no milestone); warehouse-backed state for SQL-queryable observability ([#920](https://github.com/drt-hub/drt/issues/920), deliberately split out of #756 per ADR 0005 — needs a destination write-grant conversation #756 doesn't); idempotency keys for fire-and-forget destinations ([#897](https://github.com/drt-hub/drt/issues/897), parked until a duplicate-side-effect report justifies it); ClickHouse batch insert ([#961](https://github.com/drt-hub/drt/issues/961), filed 2026-08-11, perf-only); local (non-object-store) DLQ/state cross-process safety ([#963](https://github.com/drt-hub/drt/issues/963), filed 2026-08-11). `#469` (`fetch_existing()` Protocol refactor) and `#781` (dbt exposures export) moved to [v0.10](#v010--enterprise-boundary--ecosystem) instead.

---

## v0.10 — Enterprise Boundary & Ecosystem

**Theme:** Open Core boundary design — interfaces for Enterprise features without implementing them in OSS — plus the dagster-drt integration depth and Rust-migration groundwork that share nothing scope-wise with [v0.9.0](#v090--engine-foundation--shipped-2026-08-11)'s concrete engine work. Split out 2026-08-03; see v0.9.0's note for why.

**Scope:**
- **Interfaces** — RBAC interface spec (**#298 — closed**, 2026-08-19) and audit log hooks (**#299 — closed**, 2026-08-19): landed together as [ADR 0008](docs/adr/0008-enterprise-boundary-rbac-and-audit-hooks.md), a new `PermissionChecker` Protocol (`drt/security/`) and a narrow `AuditLogger` Protocol scoped to `config_changed`/`secret_accessed` — the other three audit events reuse the existing `SyncObserver` Protocol rather than duplicating it. Design-only per both issues' scope: OSS defaults are no-ops (`AllowAllPermissionChecker`, `NoOpAuditLogger`), wired at representative hook points, not enforced anywhere in OSS · plugin system for third-party connectors (**#297 — closed**, 2026-08-20): entry-point discovery (`drt/plugins.py`, six `drt.*` groups) + `drt plugins list`, per [ADR 0009](docs/adr/0009-plugin-config-union-blocker.md). Ships the part that works today — closes the "requires an explicit operator-side import" gap ADR 0008 named for `SecretProvider`/`PermissionChecker`/`AuditLogger`/extra-`SyncObserver` registration. `drt.sources`/`drt.destinations` entry points are discovered and register in the connector registry, but — per ADR 0009 — a plugin connector cannot yet be named in a sync YAML: `SyncConfig.destination`/`load_profile()` validate `type` against a closed, hand-enumerated set before the registry is ever consulted. Closing that is scoped to a follow-up issue, [#997](https://github.com/drt-hub/drt/issues/997), not this one
- **Protocol stability** — review and freeze preparation (**#300 — closed**, 2026-08-19: audited all 17 `Protocol` interfaces, [ADR 0007](docs/adr/0007-protocol-stability-policy.md) defines what a breaking Protocol change is + the deprecation workflow, PRs #992/#993) · config encryption for secrets at rest (**#303 — done**, 2026-08-21: optional `pyrage`/age X25519 encryption for `.drt/secrets.toml`, `drt encrypt`/`decrypt`, and transparent in-memory loading via `DRT_SECRETS_KEY`) · replacing the hardcoded `_QUERYABLE_TYPES` list (**#469 — closed**, 2026-08-19: landed as a new `QueryableDestination` Protocol per ADR 0007's extension mechanism rather than the originally-proposed `Destination.fetch_existing()` — the issue's design predated ADR 0007 and its imagined `fetch_existing(match_columns, select_columns)` shape didn't match the query dispatch code that had grown since it was filed; `Destination.load()` itself was never touched) — *`drt cloud push` stub (#302) shipped early in v0.7 via PR #409*
- **Ecosystem** — dbt exposures export (**#781 — done**, 2026-08-21): `drt docs generate --format dbt-exposures` emits deterministic, ref-only exposure YAML to stdout so drt syncs appear downstream in dbt docs lineage, with a documented CI drift gate (moved from v0.9 2026-08-06 — it only landed there by deferral from v0.8, not thematic fit)
- **Performance** — benchmark suite (**#280 — done**, 2026-08-21: fixed 100 / 10,000 / 100,000-row local SQLite → real engine → file-destination scenarios, versioned JSON results with wall time / throughput / peak Python memory / destination-call count, and an importable unmeasured seam for the next profiler) + I/O vs CPU profiling (**#301 — done**, 2026-08-22: corrected cProfile attribution on those unchanged workloads found 91.88% / 86.41% / 84.46% combined CPU versus 8.12% / 13.59% / 15.54% local destination I/O; the large run was 38.95% `json.dumps`, 28.07% in-memory source extraction, and only a 17.45% non-JSON transformation residual that still includes non-engine work, so [ADR 0010](docs/adr/0010-rust-migration-decision.md) recommends against treating this deliberately I/O-light profile as evidence for a broad `engine/sync.py` rewrite and defers the final Rust/roadmap call)
- **Event-driven activation** — the streaming / event-triggered syncs ADR ([ADR 0004](docs/adr/0004-streaming-and-event-triggered-syncs.md), accepted) named its Tier 2 and Tier 3 recommendations as sanctioned follow-through. **dagster-drt sensors (#855) — closed**, on the Delta Lake and Iceberg variants (`build_drt_change_sensor()`, PR #974): both have a monotonic, side-effect-free change signal a cursor-diff sensor can poll. Snowflake `STREAM` and SQL Server Change Tracking did **not** ship the same way in #855 — building them surfaced that `SYSTEM$STREAM_HAS_DATA()` only resets on DML consumption, which a read-only polling sensor never provides, so the same cursor-diff shape would fire once and then latch permanently silent; making it work would mean a write-grant escalation on a source connection this ADR never scoped. Re-scoped to its own issue, [#975](https://github.com/drt-hub/drt/issues/975) (filed 2026-08-14) — and **#975 has since shipped both**: `SYSTEM$LAST_CHANGE_COMMIT_TIME('<table>')` for Snowflake and `CHANGE_TRACKING_CURRENT_VERSION()` for SQL Server, both verified against real accounts to have none of `STREAM_HAS_DATA()`'s consumption trap. Snowflake needs `watch_table=`/`minimum_interval_seconds=` (no single table on the profile, and — while the poll itself is metadata-only and carries no warehouse-compute cost, verified live in [#985](https://github.com/drt-hub/drt/issues/985) — a real per-poll connection/auth cost the object-storage signals don't have); SQL Server needs `watch_table=` too, but only to validate that specific table has Change Tracking enabled (`CHANGE_TRACKING_MIN_VALID_VERSION`) — its actual polled signal stays database-wide regardless. See ADR 0004's 2026-08-17 amendment and `docs/guides/event-driven-syncs.md`. Researching #855 also surfaced a real, previously-undiscovered bug: `drt.integrations.airflow`/`prefect`'s `run_drt_sync()` resolved the correct `state.backend` via `build_state_bundle()` but never wired it into an observer, so state/watermark/DLQ/history persistence was silently a no-op through that path on *any* backend, local included — fixed in [#976](https://github.com/drt-hub/drt/pull/976), same bug class as the dagster-drt state-wiring fix in #973 (that fix's own `history_manager` gap, found during #976's review, was fixed in #981/#982). **The three-tier user guide (#856) — closed**, [`docs/guides/event-driven-syncs.md`](docs/guides/event-driven-syncs.md), covering all three tiers and now the Snowflake/SQL-Server Tier 2 support above. Cross-process rate-limit coordination for orchestrator-launched sync processes (#921 — a sensor firing N `RunRequest`s each as its own process defeats #769's single-process shared bucket; ADR 0004's 2026-07-29 amendment folded this into #756, but **ADR 0005 corrected that** — neither of #756's backends can serve the low-latency atomic increment a token bucket needs) stays genuinely unscheduled with no committed design. The Tier 3 half of this — a hardened `drt serve` concurrency contract so a dropped trigger isn't silent event loss (#854) — **shipped ahead of schedule** (#902); its direct follow-on, OIDC JWT verification for the Pub/Sub push leg (#903), is unscheduled backlog below
- **Integrations** — dagster-drt: `DrtEventIterator` for chainable post-processing (#182) · `DrtSyncComponent` for declarative YAML (#183) · `@op` context support in `DagsterDrtResource` (#184)
- **State layout revisit** — [#948](https://github.com/drt-hub/drt/issues/948): [v0.9.0](#v090--engine-foundation--shipped-2026-08-11)'s #756 shipped `state.json` as one project-wide object per remote backend (byte-for-byte parity with `.drt/`, chosen over an earlier per-sync-split design for zero-migration tier reversibility) — revisit the split-object layout only if `--threads N` contention against that single object becomes a real, measured problem, not speculatively

**Unscheduled backlog** *(no milestone — pull in when a release theme fits)*: OIDC JWT verification for `drt serve`'s Pub/Sub push leg (#903) — direct follow-on to #854, belongs alongside #855/#856. Stripe timestamped-signature verification for `drt serve` (#969, filed 2026-08-14) — the other named gap from #854's HMAC auth work, same "filed separately rather than left as a CHANGELOG mention" treatment as #903. Local (non-object-store) DLQ/state cross-process safety (#963, filed 2026-08-11) — candidate for pulling into v0.10 alongside the Protocol stability review (#300) rather than staying unscheduled, since both touch the same state-layer surface; not decided yet.

**Out of scope:** Implementing RBAC/audit log in OSS, actual Cloud service backend, Rust migration itself.

**Target:** after v0.9 · **Progress:** [milestone/13](https://github.com/drt-hub/drt/milestone/13)

---

## v1.0 — Stable Release

**Theme:** Protocol freeze, semver guarantee, public launch.

**Scope:**
- Protocol freeze — Source / Destination / StateManager interfaces (#304)
- Migration guide v0.x → v1.0 (#305)
- v1.0 launch campaign — blog, HN, Reddit, X (#306)

**Target:** 2026-11 · **Progress:** [milestone/7](https://github.com/drt-hub/drt/milestone/7)

---

## v1.x — Rust Engine

The Rust migration decision is pending repository-owner review of [ADR 0010](docs/adr/0010-rust-migration-decision.md)'s benchmark data ([#301](https://github.com/drt-hub/drt/issues/301), closed 2026-08-22). ADR 0010 recommends against treating this local profile alone as justification for a broad `engine/sync.py` rewrite and identifies the representative evidence that could change that recommendation. Module boundaries remain drawn for a possible PyO3 transition — `engine/sync.py` is kept pure (no I/O side effects beyond protocol calls) — but no rewrite is committed or scheduled.
