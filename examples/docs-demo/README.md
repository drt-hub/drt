# docs-demo — showcase fixture for `drt docs generate`

A fictional but **fully valid** drt project used to demonstrate the generated
docs site (sync catalog + lineage). Nothing here connects anywhere: hosts and
credentials are env-var references that are never resolved, and
`.drt/state.json` is hand-authored so the catalog shows a realistic mix of
`success` / `partial` / `failed` runs.

```bash
cd examples/docs-demo
drt docs generate --format html   # writes target/docs/
open target/docs/index.html      # works on file:// — no server needed
```

What it exercises on purpose:

- 10 syncs across postgres / slack / hubspot destinations
- lookup lineage, including **two lookups into one sync** (`orders_to_pg`)
- `field_mappings`, `mask`, modes `full` / `incremental` / `upsert`
- run state with errors (`partial` + `failed`), tags, model SQL

This fixture backs the public demo of the docs site (see drt-web) and the
`test_docs_demo_example` guard, so keep it valid: `drt validate` should pass
here after any edit.
