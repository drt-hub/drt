# ADR 0001 — `drt docs` manifest schema v1

- **Status:** Accepted (implemented; shipped across v0.7.5 – v0.7.11)
- **Issue:** [#500](https://github.com/drt-hub/drt/issues/500), under epic [#499](https://github.com/drt-hub/drt/issues/499)
- **Implementation:** `drt/docs/manifest.py` (schema), `drt/docs/builder.py` (producer)

## Context

The `drt docs` epic (#499) ships in phases — P1 Mermaid text, P2 `manifest.json`,
P3 static HTML site, P4 `drt docs serve` — and every phase needs the same facts:
which syncs exist, what they read and write, how they relate. Deriving those
facts inside each renderer would couple every output format to the config
parser and the state store, and would let the formats drift apart.

dbt's `manifest.json` is the precedent worth copying: one documented artifact
between "understand the project" and "render something", stable enough that
third parties build on it.

## Decision

### One intermediate artifact, one producer

`build_manifest()` is the only code that reads project config and run state for
documentation purposes. Every renderer (Mermaid, HTML, and the emitted
`manifest.json` itself) consumes a `Manifest` — never the config parser or
state manager directly. The MCP server's docs tool returns the same manifest,
so LLM consumers and the static site can never disagree.

### Schema v1 shape

Frozen dataclasses (`Project`, `Source`, `Destination`, `Sync`,
`SyncStateSnapshot`, `Edge`, `Manifest`), serialized by hand in
`to_dict()`/`from_dict()` (round-trip safe). Not pydantic: the builder is the
only writer, so there is nothing to validate on the way in, and frozen
dataclasses make accidental mutation in a renderer a `TypeError` instead of a
subtle bug.

The graph model is three node kinds plus typed edges:

```
sources ── source_to_sync ──> syncs ── sync_to_destination ──> destinations
                              syncs <────── lookup ─────────── syncs
```

`EdgeKind = source_to_sync | sync_to_destination | lookup`. Lookup edges are
heuristic — sync A's `lookups.*.table` matched against sync B's
`destination.table` (with a short-name alias for `schema.table`) draws
`B → A`. Heuristic is acceptable in v1 because a false negative only omits a
dashed edge; nothing downstream breaks.

### Versioning

`schema_version` is a single integer, currently `1`. Additive, optional fields
do **not** bump it; renames and removals do. `drt_version` rides alongside for
debugging, but consumers must key behaviour off `schema_version` only.

### Public names decoupled from runtime names

`SyncStateSnapshot` deliberately renames the runtime fields
(`last_run_at → last_sync_at`, `records_synced → rows_synced`): the manifest is
a public contract, `drt/state/manager.py` is not, and the persistence layer
must be free to evolve without a schema bump.

### State is opt-in

Run state (`include_state=True`, CLI default; `--no-state` to disable) attaches
a `state` block per sync; syncs that never ran omit the block entirely. The
split exists because the catalog half of the manifest is a **function of the
repo** (reproducible anywhere from the YAML alone) while state is a function of
one machine's `.drt/state.json` — CI-generated docs and locally generated docs
should differ only in the parts that honestly differ.

### Determinism

Same project in, same bytes out, with exactly one exception: `generated_at`
(ISO-8601 UTC) lives **only** in `manifest.json` — never in rendered pages —
so regenerating docs for an unchanged project produces a one-line git diff
(#697, enforced by `test_regeneration_is_byte_identical`). Iteration order is
pinned (sorted sync files, insertion-ordered node maps); the builder reads no
clocks (beyond that one stamp), randomness, or unordered sets.

### Destination identity and labels

Syncs that target the same destination share one node. v1 as shipped derived
the node id from a slug of `describe()` and carried `describe()` verbatim as
`label`. **Amended by #696:** labels are docs-safe by default (`describe_safe()`;
`--full-labels` restores verbatim output), and ids derive **only from the
safe label** — `dest_<slug(describe_safe())>` plus a deterministic `_2`/`_3`
suffix when distinct destinations share a safe label. The intermediate design
(`dest_<type>_<sha1(describe())[:8]>`) was rejected in review: a truncated
hash of a low-entropy value (phone number, email with a known domain) is
brute-forceable, so no function of the sensitive string may ship at all.
Distinctness is tracked by the full `describe()` in memory only; ids are
independent of the label mode, so `--full-labels` never rewires the graph.
Both changes are `schema_version`-neutral: field shapes unchanged, only
values.

## Consequences

- Renderers are independent and testable against hand-built manifests; the
  HTML suite never touches a real project.
- `manifest.json` ships inside the site output, so anything that applies to
  hosted pages (redaction, determinism) applies to the manifest too.
- Known v2 candidates, deliberately excluded from v1: `field_mappings`/`mask`
  exposure for column-level lineage (L2, #702), run/DLQ history for the DLQ
  badge (#698), and model-SQL source-table extraction (needs SQL parsing).
  Each is additive in spirit but reshapes `Sync`, so they land together as v2.
