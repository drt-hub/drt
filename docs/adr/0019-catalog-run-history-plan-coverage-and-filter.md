# ADR 0019 — Catalog run history, plan coverage and large-project filtering

- **Status:** Accepted 2026-11-07 (owner-approved v1.3.1 scope).
- **Relates to:** [ADR 0003](0003-manifest-schema-v2.md) and
  [ADR 0006](0006-manifest-schema-v3.md) (additive manifest versioning),
  [ADR 0013](0013-plan-apply-lineage.md) (plan/apply lineage),
  [ADR 0015](0015-catalog-operational-metadata.md) (deterministic operational metadata),
  and [ADR 0018](0018-api-based-plan-reads.md) (`api-reads`).

## Context

The manifest already carries bounded per-sync history, plan ids, guards, column
contracts and declared operational metadata, but the HTML catalog exposes only
the latest state and last plan. It also makes an operator leave the catalog to
learn whether a destination is plan-capable. Finally, the overview remains easy
to scan for a small project but its unfiltered navigation becomes slow to use as
the sync count grows.

These are catalog facts, not new runtime behavior. They must preserve safe-label
defaults, old-manifest loading, single-object `--inline` output and #697's
byte-identical regeneration guarantee.

## Decision

### Manifest schema v7

Schema v7 is an additive superset of v6. Every new field is omitted when absent,
and `Manifest.from_dict()` continues to load v1 through v6:

- `runs[].exact: boolean` records exact-replay apply lineage. `approved_by`
  remains available only with `--full-labels`.
- `destinations[].plan = {status, reason}` copies the declaration from
  `drt.engine.plan_coverage`. Supported statuses are `available`, `partial`,
  `api-reads`, `needs-api-diff` and `unavailable`. Undeclared/plugin destination
  types omit the block; absence does not claim unavailability.
- `syncs[].plan = {status, reason}` starts with that destination declaration and
  replaces it with `unavailable` plus the planner's sync-level blocker when the
  sync uses an unsupported match policy, diff extraction, or metadata key.
- `syncs[].columns_source = dbt|declared` identifies the effective column
  declaration. A hand-written `columns:` block that overrides a dbt contract is
  `declared`.
- Explicitly authored destination `manage_table` and `on_schema_change` values
  appear on the sync object, because syncs sharing one destination node may
  declare different policies. Pydantic defaults are not published as if the
  operator had declared them.

The HTML sync page renders a bounded run-history table only when `runs[]` exists.
It shows UTC time, status, row counts, plan id, guard outcome, exact replay and an
approver column only when an approver survived the safe-label policy. Plan
coverage appears on destination pages and in the sync page's existing State tab.
The destination page shows connector-level coverage; the sync page shows the
effective sync-level result. Contract provenance and managed-table declarations
appear only when present, and managed-table policy appears only on sync pages.

### Scale mode

At `SCALE_MODE_SYNC_THRESHOLD = 8`, the overview's existing search control gains
owner, maturity and tag facets. Search matches name, description, destination,
tags and owner. The controls are client-side progressive enhancement: without
JavaScript every sync remains visible, keyboard focus opens the facets, Escape
closes them, and no animation is required (so reduced-motion preferences are
inherently respected). The same code operates on multi-file and `--inline` hash
navigation. Projects below the threshold retain the small-project UI.

The existing visual fixture has ten syncs despite the original scope assumption
that it had fewer than eight. To preserve its Linux-only snapshots, scale mode
enhances the already-visible top-bar search and keeps the facet panel closed until
interaction; the resting layout does not move.

Multi-file pages retain only names, slugs and counts in their navigation JSON.
The larger search corpus and facet metadata are emitted solely as escaped DOM
attributes on the overview page, so catalog size grows linearly rather than
repeating every sync description in every generated page. Tag lists use JSON
arrays, preserving authored values that contain `|`.

### Determinism

No fresh/stale or other clock-relative verdict is added. Plan coverage and
managed-table settings are static declarations, history remains bounded and
newest-first, filter values are sorted, and the client receives only stable
manifest/config data. No runtime dependency is added.

## Consequences

- Catalog consumers can discover reviewability and provenance without opening
  project YAML or the generated coverage guide.
- Hosted default catalogs still withhold `owner_email` and `approved_by`; trusted
  catalogs opt in with `--full-labels`.
- A v6 consumer that ignores unknown fields continues to work, while consumers
  that pin a schema version can deliberately add v7 support.
- The catalog does not answer “is this sync stale now?”; that remains
  `drt status --check`.
