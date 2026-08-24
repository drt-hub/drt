# ADR 0011 — Subtraction as positioning: what drt deliberately does not build

- **Status:** Proposed.
- **Issues:** none yet — this ADR precedes any issue; it exists to give
  future feature-request triage a written boundary to point to, the same
  role ADR 0008 plays for RBAC.
- **Relates to:** [ADR 0008](0008-enterprise-boundary-rbac-and-audit-hooks.md)
  (the existing precedent for "OSS core deliberately excludes X, and here is
  the exact escape hatch if that ever needs to change"); `CLAUDE.md`'s
  existing "Do not add a GUI or web UI" line, which this ADR gives a
  competitive rationale rather than a bare assertion.

## Context

drt's own docs already describe dlt and dbt as pipeline stages, not
competitors: "dlt loads data in, dbt transforms it, and drt activates it
back out." That framing is correct and should not change. **drt's actual
competitive set is other reverse-ETL tools** — commercial products (Census,
Hightouch, RudderStack Reverse ETL, Polytomic, and similar) that solve the
same problem drt solves: moving warehouse data back out to the SaaS tools a
team works in.

Those products, near-universally, bundle the sync mechanism with a second
and third product on top of it: a web UI for point-and-click field mapping
and monitoring, an audience/segmentation builder for building the record set
to sync, and a hosted runtime billed by rows or sync volume. The sync engine
itself — read a query, map fields, write to a destination API — is a small
fraction of what a user is actually paying for or maintaining.

This is a familiar shape: XML tried to be a document format, a schema
language, a transform language, and a query language in one spec, and JSON
won its niche by refusing to be any of the last three — it left validation,
transformation, and querying to other tools, and did one thing (data
interchange) well. **This ADR proposes drt's answer to reverse-ETL
incumbents follow the same shape deliberately, not by accident of what
hasn't been built yet.**

## Decision — four things drt's OSS core will not build

1. **No web UI or dashboard.** Already true (`CLAUDE.md`'s existing line).
   This ADR adds the reason: a UI is table stakes for every commercial
   competitor, and drt competing on UI polish is a fight against companies
   whose entire product *is* the UI. Config-as-code (YAML, git-reviewable,
   diffable) is the differentiated position, not a placeholder for a UI
   that hasn't been built yet.
2. **No hosted/managed runtime.** drt runs wherever the operator already
   runs code — GitHub Actions, Dagster, cron, a container — never as a
   drt-hub-operated service. This is what makes "no per-row billing"
   structurally true rather than a pricing choice that could change: there
   is no metered infrastructure to bill against in the first place.
3. **No audience/segmentation builder.** Building the record set to sync is
   a SQL/dbt-modeling problem, not a reverse-ETL problem — drt reads
   whatever query or model a sync config points it at (`sync.model` /
   `sync.query`) and syncs exactly that. Commercial tools fold segmentation
   into the sync tool because their user often isn't SQL-fluent; drt's user
   already has dbt (or equivalent) for that work, and duplicating it would
   be the same "one tool doing four jobs" mistake this ADR is naming.
4. **No proprietary connector catalog.** Destinations are Protocol
   implementations (`drt/destinations/base.py`), extensible via the plugin
   system (#297) without drt-hub as a gatekeeper. A closed connector catalog
   is a common commercial reverse-ETL lock-in mechanism; drt's answer is
   architectural, not a promise — anyone can ship a destination without
   asking.

None of these four are framed as "not yet" — they are the position. A
feature request that reintroduces any of them into drt-core should be
declined by default, citing this ADR, the same way an RBAC PR gets declined
by default citing ADR 0008 — not because the idea is bad, but because it
contradicts a decision already made on purpose.

## Escape hatch — this is a boundary on OSS core, not on drt as a company

Mirroring ADR 0008's exact shape: this ADR governs `drt-core` (this
repository) only. `project_startup_vision`'s two-leg plan (OSS core + a
separate closed application) is not foreclosed by this ADR — if a UI,
managed runtime, or audience-building layer is ever built, it belongs in
that separate closed product, consuming drt-core as a dependency, the same
relationship Enterprise RBAC has to `PermissionChecker` in ADR 0008. This
ADR is not a promise those four things are permanently out of drt's future
as a business; it is a promise they are permanently out of `drt-core`'s
scope as an OSS sync engine.

## Consequences

- **Positioning copy** (README tagline, GitHub repo description, docs
  landing page) should name the competitive set explicitly — "reverse ETL,
  without the dashboard, the audience builder, or the row-based bill" is a
  sharper claim than a feature-parity comparison table, and it's a claim
  competitors structurally cannot match without abandoning their own
  business model. Drafting that copy is out of scope for this ADR (it's a
  product-voice decision, not an architecture one) — this ADR only fixes
  the underlying claim the copy should make.
- **Contribution triage gets a written default.** `CONTRIBUTING.md` (or the
  PR-triage habits already tracked in this project's memory) can now point
  to this ADR when declining a UI/dashboard/billing/catalog PR, instead of
  relitigating the reasoning per PR.
- **Risk, named plainly:** a "we don't do X" pitch reads as feature-poor to
  a buyer doing a checkbox comparison against incumbents that do have a UI
  and an audience builder. This ADR does not resolve that risk — it is the
  tradeoff of choosing this position over a feature-parity race, accepted
  deliberately rather than discovered later.

## Follow-up

1. Draft candidate README/tagline/GitHub-description copy that names the
   reverse-ETL competitive set directly (not done in this ADR — product-voice
   decision for the repository owner).
2. Consider a short "Why not a UI?" / "Why not audience building?" FAQ entry
   in the docs site, linking here, for evaluators arriving from a
   commercial-tool comparison.
3. If a future issue proposes any of the four excluded items for
   `drt-core`, it should cite this ADR and either argue the ADR's premise
   has changed (competitive landscape, user base) or be redirected to the
   closed-product track named in the escape-hatch section above.
