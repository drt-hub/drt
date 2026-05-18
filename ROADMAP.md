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

## v0.8 — Cloud Destinations & Growth

**Theme:** DWH/Lakehouse destinations + community growth push.

**Scope:**
- **Cloud destinations** — BigQuery (#165) · Databricks Delta Lake (#167) · S3 Parquet/CSV (#168) · GCS (#169) · Azure Blob (#170) — *Snowflake (#164) shipped early in v0.7 via PR #353*
- **Lakehouse sources** — Delta Lake (#172) · Apache Iceberg (#173)
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
