# Roadmap

> **SSoT for upcoming releases.** For shipped releases, see [CHANGELOG.md](CHANGELOG.md) and [GitHub Releases](https://github.com/drt-hub/drt/releases). For issue-level tracking, see each version's [milestone](https://github.com/drt-hub/drt/milestones).

Targets are indicative, not guarantees. Scope may shift between versions — when that happens, this file is updated first and issues are re-labeled to match.

---

## v0.7 — Production Ready

**Theme:** Reliability, observability, and correctness for real production use.

**Scope:**
- **Reliability** — retry policy in sync YAML (#277) · graceful shutdown on SIGTERM/SIGINT (#279) · sync execution history as local JSON log + `drt status --history` tail (#276, scope-reduced)
- **Correctness** — `json_columns` explicit JSON serialization (#316) · FK existence check without value resolution (#354) · zero-downtime replace via staging table swap (#338)
- **DX** — `drt doctor` environment diagnostics (#264) · `--quiet` flag for `drt run` (#265)
- **Tests** — `on_error='fail'` and retry config tests across all destinations (#365)

**Out of scope (→ v0.8):** Cloud destinations (Snowflake / BigQuery / Databricks / S3 / GCS / Azure), dead letter queue, opt-in telemetry, benchmark suite, Growth/README refresh items, schema-aware serialization epic.

**Target:** 2026-05 · **Progress:** [milestone/4](https://github.com/drt-hub/drt/milestone/4)

---

## v0.8 — Cloud Destinations & Growth

**Theme:** DWH/Lakehouse destinations + community growth push.

**Scope:**
- **Cloud destinations** — Snowflake (#164) · BigQuery (#165) · Databricks Delta Lake (#167) · S3 Parquet/CSV (#168) · GCS (#169) · Azure Blob (#170)
- **Lakehouse sources** — Delta Lake (#172) · Apache Iceberg (#173)
- **Reliability follow-on** — dead letter queue (#278) · opt-in anonymous usage telemetry (#263)
- **Correctness epic** — schema-aware serialization via INFORMATION_SCHEMA (#317)
- **Engine** — `sync.mode: mirror` differential delete (#340)
- **Growth / README** — hero section redesign (#281) · Quickstart GIF/asciinema (#282) · Codespaces devcontainer (#283) · "Why OSS Reverse ETL" blog (#284) · production use case blog (#285) · Discord (#378) · X account link (#379) · PyPI keywords (#307) · Awesome lists (#290) · Reddit/HN launch (#289)
- **Ecosystem** — GitHub Action (#292) · VS Code extension (#293)
- **Dev tooling** — FakeSource (#364) · `drt_run_test` MCP tool (#368) · `/drt-troubleshoot` skill (#369) · `/drt-changelog` repo skill (#372) · connection test in `drt validate` (#367)

**Out of scope:** Enterprise boundary (RBAC / audit log / plugin system → v0.9), Rust engine work (→ v1.x).

**Target:** 2026-07 · **Progress:** [milestone/5](https://github.com/drt-hub/drt/milestone/5)

---

## v0.9 — Enterprise Foundation

**Theme:** Open Core boundary design — interfaces for Enterprise features without implementing them in OSS.

**Scope:**
- **Interfaces** — RBAC interface spec (#298) · audit log hooks (#299) · plugin system for third-party connectors (#297)
- **Protocol stability** — review and freeze preparation (#300) · config encryption for secrets at rest (#303) · `drt cloud push` stub (#302)
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
