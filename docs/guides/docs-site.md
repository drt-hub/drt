# Generated docs site

`drt docs generate` builds a deterministic catalog from `drt_project.yml`,
`syncs/*.yml` and, unless disabled, persisted state and history:

```bash
drt docs generate
open target/docs/index.html
```

The default HTML output is a portable multi-file site. Use `--inline` to emit a
single navigable `index.html` for authenticated GCS/S3 object URLs, where relative
asset and page requests cannot inherit the object's authorization:

```bash
drt docs generate --inline
```

Both modes show the same content. They make no runtime network requests and use
the same client-side filtering behavior.

## What the catalog shows

- source → sync → destination lineage and destination lookups;
- declared owner, maturity and freshness contract (never a clock-relative
  fresh/stale verdict);
- column contracts and whether the effective declaration was hand-written or
  derived from an enforced dbt contract;
- explicitly configured managed-table policy;
- declared `drt plan` coverage and its reason; and
- when state is included, latest state plus bounded per-sync run history,
  including plan ids, guard outcomes and exact-replay lineage.

The Overview also lists every sync with its docs-safe destination label, latest
status, persisted last-run date/time and effective plan coverage. Last-run
values come directly from the manifest and are never rendered as relative time;
a sync with no run state shows an em dash. On screens up to 860px wide, the same
table rows become stacked cards so every sync remains reachable without the
desktop sidebar.

Run history is newest-first and bounded by `--history-depth` (default 10). Use
`--no-state` for a repository-only artifact; this omits latest state, history and
DLQ depth while retaining config hashes and other declarations.

```bash
drt docs generate --history-depth 20
drt docs generate --no-state
```

The manifest is also available directly:

```bash
drt docs generate --format json
```

Schema v7 adds `runs[].exact`, destination plan coverage, column provenance and
explicit managed-table settings. All additions are omitted when absent, and drt
continues to load schema v1 through v6 manifests.

## Safe labels and personal data

Generated catalogs are safe by default: connection labels and error text are
redacted, and `owner_email` and run `approved_by` are omitted. For a trusted,
internal-only artifact, `--full-labels` opts into those values as well as verbatim
connection labels and error text:

```bash
drt docs generate --full-labels
```

Do not use that flag for a public bucket or committed artifact.

## Large catalogs

At eight syncs, a search with owner, maturity and tag facets appears directly
above the Overview's Syncs table. Text search covers sync name, description,
destination, tags and owner, and filters the table rows. Filtering is progressive
enhancement: with JavaScript disabled every sync remains visible, and the controls
work in both multi-file and `--inline` hash navigation. Escape closes the facet
panel from the search field, a select, or the Clear button. Filter metadata exists
only on the overview page, so multi-file output grows linearly with the catalog;
tags are encoded individually, including tags that contain `|`.

Destination pages report connector-level `drt plan` coverage. A sync page
reports the effective result after sync-level planner blockers and is the only
page that shows that sync's explicitly declared `manage_table` and
`on_schema_change` policy.

## Deterministic output

Catalog sections contain only sorted repository declarations and persisted
state. They do not compute “stale now” or use the generation clock for UI
verdicts, so regenerating an unchanged project produces byte-identical page
content. Use `drt status --check` for a current freshness verdict.
