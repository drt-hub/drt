# ADR 0020 — Postgres raw-SQL source lineage

- **Status:** Accepted 2026-11-09.
- **Relates to:** [ADR 0001](0001-drt-docs-manifest.md),
  [ADR 0003](0003-manifest-schema-v2.md), and [issue #1045](https://github.com/drt-hub/drt/issues/1045).

## Context

`drt docs` previously represented every sync as reading from its configured
profile. That is the only source fact available for a `ref()` resolved through
dbt or a private `profiles.yml`, but it hides the physical relations read by a
hand-written model. Regexing `FROM` and `JOIN` is not sufficient: Postgres CTEs,
derived queries, quoted identifiers, and nested joins make aliases look like
tables.

The first slice requested in #1045 is Postgres only. The project cannot inspect
the operator's private profile safely, so an explicit `source.type: postgres` in
`drt_project.yml` is required before its raw SQL is parsed.

## Decision

Manifest schema v8 adds an optional `syncs[].sources: string[]` field. For a
supported raw Postgres `SELECT`, it contains every distinct physical relation,
sorted deterministically. The existing scalar `syncs[].source` remains present
and is the first relation, preserving a usable source for older consumers that
understand only one input.

The builder also replaces the generic profile source edge with one
`source_to_sync` edge per parsed relation. It reads a local
`syncs/models/<ref>.sql` override because the runtime resolves that file before
a dbt model. A plain `ref()` is not resolved: doing so would need a dbt manifest
or private profile and could claim a relation that was never inspected.

Extraction uses SQLGlot with the `postgres` dialect and its scope walker. Only
physical `Table` sources with identifier names are emitted; CTEs, derived
subqueries, and table-valued functions are excluded. The manifest records only
the canonical relation identifier, reconstructed from the catalog/database/name
identifiers; it never records model SQL or SQL comments.

Failure is intentionally conservative. Empty SQL, unrendered `var()` calls,
unavailable parser support, a parser failure, a non-read-only/non-query
statement, or multiple statements emits no partial lineage and keeps the
existing generic profile source/edge. Other source types retain the same
behavior without invoking the parser.

The HTML catalog, Mermaid output, and static DAG consume the complete source
edge set. Sync pages fan all inputs into the sync, source pages list every sync
that reads that relation, and the JSON manifest remains deterministic.

## Consequences

- A Postgres project that declares its source type gains accurate table-level
  lineage for raw models without affecting runtime extraction or writes.
- Consumers that need all source relations can opt into v8's `sync.sources`;
  v1-v7 manifests still load and reserialize without the new field.
- Unsupported dialects, dbt-resolved refs, and unparseable SQL keep the prior
  profile-level lineage rather than publishing a guess.
- Extending extraction to another dialect requires its own fixtures, parser
  policy, and manifest/UI review rather than treating this Postgres behavior as
  a universal SQL promise.
