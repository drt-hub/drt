# Roadmap

> **SSoT for upcoming releases.** For shipped releases, see [CHANGELOG.md](CHANGELOG.md) and [GitHub Releases](https://github.com/drt-hub/drt/releases). For issue-level tracking, see each version's [milestone](https://github.com/drt-hub/drt/milestones).

Targets are indicative, not guarantees. Scope may shift between versions — when that happens, this file is updated first and issues are re-labeled to match.

---

## v0.7 — Production Ready ✅ Shipped 2026-05-06

Released as **v0.7.0** on 2026-05-06. See [CHANGELOG.md](CHANGELOG.md#070---2026-05-06) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.7.0) for the full feature list.

Tail items continue in [v0.7.1](#v071--production-ready-follow-up) below.

---

## v0.7.1 — Production Ready Follow-up ✅ Shipped 2026-05-07

Released as **v0.7.1** on 2026-05-07. See [CHANGELOG.md](CHANGELOG.md#071---2026-05-07) and the [GitHub Release](https://github.com/drt-hub/drt/releases/tag/v0.7.1) for the full feature list.

Tail items continue in [v0.7.2](#v072--production-ready-follow-up-2) below.

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

**Theme:** DWH/Lakehouse destinations + community growth push.

**Scope:**
- **Cloud destinations** — BigQuery (#165) · Databricks Delta Lake (#167) · S3 Parquet/CSV (#168) · GCS (#169) · Azure Blob (#170) — *Snowflake (#164) shipped early in v0.7 via PR #353*
- **SaaS destinations** — *Zendesk (#421) shipped via PR #504 — pattern reference for future SaaS connectors*
- **Sources** — REST API (#422) ✅ *shipped via PR #474 — first non-database source, pattern reference for future API sources* · Delta Lake (#172) · Apache Iceberg (#173)
- **Reliability follow-on** — dead letter queue (#278) — *opt-in telemetry (#263) moved up to v0.7*
- **Correctness epic** — schema-aware serialization via INFORMATION_SCHEMA (#317)
- **Engine** — `sync.mode: mirror` differential delete (#340)
- **Growth / README** — hero section redesign (#281) · Quickstart GIF/asciinema (#282) · "Why OSS Reverse ETL" blog (#284) · production use case blog (#285) · Discord (#378) · X account link (#379) · Awesome lists (#290) — *Codespaces devcontainer (#283) and PyPI keywords (#307) shipped early in v0.7; Reddit/HN launch (#289) deferred to opportunistic timing post-v0.8*
- **Ecosystem** — GitHub Action (#292) · VS Code extension (#293)
- **Dev tooling** — FakeSource (#364) · `drt_run_test` MCP tool (#368) · `/drt-troubleshoot` skill (#369) · `/drt-changelog` repo skill (#372) · connection test in `drt validate` (#367)

**Out of scope:** Enterprise boundary (RBAC / audit log / plugin system → v0.9), Rust engine work (→ v1.x).

**Target:** 2026-07 · **Progress:** [milestone/5](https://github.com/drt-hub/drt/milestone/5)

---

## v0.8.1 — Diff Polish

**Theme:** Polish and follow-ups for the `--diff` feature shipped in v0.7.1.

**Scope:**
- **Diff UX** — `--diff-fields` column filter (#471) · API-based diff for upsert-keyed SaaS destinations (#472)
- **Diff perf** — batch lookup queries for large diff sets (#470)
- **Lookup correctness** — first-miss-wins YAML order semantics (#453)

**Out of scope:** New destinations, engine features unrelated to `--diff`.

**Target:** Cut from v0.8 once Cloud Destinations land · **Progress:** [milestone/10](https://github.com/drt-hub/drt/milestone/10)

---

## v0.9 — Enterprise Foundation

**Theme:** Open Core boundary design — interfaces for Enterprise features without implementing them in OSS.

**Scope:**
- **Interfaces** — RBAC interface spec (#298) · audit log hooks (#299) · plugin system for third-party connectors (#297)
- **Protocol stability** — review and freeze preparation (#300) · config encryption for secrets at rest (#303) — *`drt cloud push` stub (#302) shipped early in v0.7 via PR #409*
- **Observability** — OpenTelemetry traces + metrics for sync execution (epic #429) — *Phase 1 (config schema + `[otel]` extras) shipped early via PR #527; Phase 2 (NoOpTracer global provider, #531) and Phases 3–4 (engine instrumentation + counter metrics) continue in parallel with v0.8 work*
- **Performance** — benchmark suite (#280) + I/O vs CPU profiling for Rust migration decision (#301)

**Out of scope:** Implementing RBAC/audit log in OSS, actual Cloud service backend, Rust migration itself.

**Target:** 2026-09 · **Progress:** [milestone/6](https://github.com/drt-hub/drt/milestone/6)

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

Rewrite `engine/sync.py` in Rust via PyO3. Decision gated on benchmark data from v0.9 (#301). Module boundaries are already drawn for this transition — `engine/sync.py` is kept pure (no I/O side effects beyond protocol calls).
