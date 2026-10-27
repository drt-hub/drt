# ADR 0015 — Operational metadata in the catalog: owner, maturity and the freshness contract

- **Status:** Accepted 2026-10-27 (implements the agreed shape of [#1226](https://github.com/drt-hub/drt/issues/1226)).
- **Relates to:** [ADR 0013](0013-plan-apply-lineage.md) (manifest v4, `plan_id`);
  [ADR 0003](0003-manifest-schema-v2.md) / [ADR 0006](0006-manifest-schema-v3.md) (manifest versioning);
  `docs/guides/alerts.md` (`alerts.on_stale`, #1224); #697 (byte-identical regeneration).

## Context

The dbt exposures drt generates hard-code `owner: drt` and `maturity: high`, so dbt's
catalog cannot answer "who owns this reverse-ETL sync, and how stale may it be?". Since
v1.3 drt can say how stale a sync may be (`alerts.on_stale`) and which plan produced a
run (`runs[].plan_id`). The catalog should carry that, without breaking two properties:
the generated artifacts regenerate byte-identically, and personal data does not leak into
hosted or dbt-bound output (#696).

## Decision

1. **`meta` on a sync:** `{owner, owner_email, maturity: high|medium|low}`, strict (unknown
   keys rejected), no runtime effect. Absent `meta` keeps today's exposure output exactly.
2. **Manifest schema v5** (additive, omit-when-absent, v1-v4 load): `syncs[].meta` and
   `syncs[].freshness = {max_age_seconds, never_run}`. `owner` and `maturity` are authored
   for display and appear by default; `owner_email` is personal data and appears only with
   `--full-labels`.
3. **Freshness is a declaration, not a verdict.** A fresh/stale judgement depends on the
   clock; putting it in the manifest or exposures would make regeneration differ between
   runs and break the drift gate. The verdict stays `drt status --check`.
4. **dbt exposures** use `meta.owner` / `meta.maturity` and add `meta.drt.freshness`.
   The renderer **never emits the e-mail, whatever labels the manifest was built with**,
   and the CLI builds them without run state, so the last applied plan never reaches the
   dbt project either.
5. **The docs site** shows owner, maturity, the declared freshness and (when history is
   included) the last applied plan on the sync page, only when present, so existing
   output and visual baselines are unchanged.

## Consequences

- One additive public-shape change (v5) with the usual compatibility rule.
- Editing `meta` edits the sync file, so it changes the sync's `config_hash` like a change
  to `tags` or `description` (and invalidates a plan made before it). Accepted for
  simplicity; excluding `meta` from the fingerprint would be a separate decision.
- A "staleness badge" on the docs site is deliberately not built: it would need a
  generation-time clock and a non-deterministic page.
