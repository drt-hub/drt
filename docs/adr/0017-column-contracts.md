# ADR 0017 — Column contracts: declared columns enforced before load, optionally derived from dbt

- **Status:** Accepted 2026-10-28 (owner confirmed the proposals — see "Decisions").
- **Issues:** [#761](https://github.com/drt-hub/drt/issues/761), [#896](https://github.com/drt-hub/drt/issues/896) (epic [#1227](https://github.com/drt-hub/drt/issues/1227)).
- **Relates to:** [ADR 0016](0016-managed-destination-tables.md) (the destination-side drift policy);
  [ADR 0015](0015-catalog-operational-metadata.md) (catalog metadata, manifest versioning);
  #499 (docs site: column-level content); `drt test` (value-level assertions stay there).

## Context

drt sends whatever the model query returns. A renamed column or a type change (INT → STRING) flows
straight into HubSpot properties, ad-platform conversions or a production table, often succeeding
partially before it errors. Nothing validates the *data contract* between model and destination.
dbt already states the model's output in `target/manifest.json` when the model declares a contract,
but drt does not read it, so a hand-written drt-side `columns:` block drifts from the dbt model.

## Decision

### 1. Opt-in declaration

```yaml
columns:                      # presence enables the contract
  - {name: email, type: string, description: Primary identifier}
  - {name: lifetime_value, type: number}
contract:
  enforced: true              # fail the run before anything is loaded
  on_extra_columns: fail      # fail | ignore | drop
```

Portable types only: `string, number, integer, boolean, timestamp, date, json`, mapped per source.
No `columns:` block means no contract and today's behaviour exactly. Strict models (unknown keys
rejected); the JSON Schema for VS Code and MCP `drt_get_schema` is regenerated.

### 2. Enforcement is a pure, pre-load check

The check compares the declared columns with the first extracted batch's keys and value types (a
`LIMIT 0` metadata query where the source supports it) **before the first write**, so a violation
never produces a partial send. It is a pure function in the engine; the observer reports violations,
and the error names every offending column and the expected/actual type. It is not a value assertion
(null rates, ranges and uniqueness stay in `drt test`).

### 3. `contract.source: dbt` (#896)

```yaml
contract: {enforced: true, source: dbt}
```

Expected columns and types come from the dbt manifest entry that `model: ref(...)` resolves to
(project-relative `target/manifest.json`, or `--manifest-path`); drt never invokes dbt. A model with
no dbt-declared contract has nothing to derive from, so `source: dbt` is a **validate-time error**
for it rather than a silent no-contract. A hand-written `columns:` block, when present, wins, and
`drt validate` warns when it disagrees with the manifest. dbt data types are normalised to the
portable set; an unmappable dbt type is a validate error naming it. Generating a dbt contract from
drt is out of scope: dbt owns the model.

### 4. Catalog and docs

`columns[].description` and types appear on the sync page of `drt docs generate` and in the manifest
(additive, omit-when-absent, schema version bump per ADR 0003/0006/0015 rules). Run state is not
needed, so regeneration stays byte-identical.

### 5. Relation to destination drift

Contracts say what the **model** must emit; ADR 0016 says what to do when the **destination table**
differs. Declared types feed `manage_table` column creation; the two checks share the same
"after first batch, before first write" point and one error format.

## Non-goals

- Value-level assertions (`drt test`); automatic contract generation from a live run; reading
  `catalog.json` as a fallback for models without a dbt contract (candidate follow-up).
- A warehouse-resident history of contract violations (kept as an optional later extension on the
  ADR 0005 managed-table primitive; the single-run check needs no warehouse write).

## Consequences

- A second, optional config block; syncs without it are unaffected.
- `source: dbt` removes the duplicate declaration but couples validation to a built manifest, so
  `drt validate` must say clearly when the manifest is missing or stale.
- A type vocabulary is now a public contract: adding types later is compatible, renaming is not.

## Decisions

Confirmed by the maintainer on 2026-10-28, all as proposed:

1. `on_extra_columns` defaults to `fail`.
2. Seven portable types (no decimal/array yet).
3. `contract.source: dbt` with no manifest is a validate error.
4. Order: #761 core, then `source: dbt`, then ADR 0016.
