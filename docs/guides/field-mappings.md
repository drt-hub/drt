# Field Mappings — Declarative Column Renaming

Source warehouse column names rarely match the field names your
destination expects (`user_id` in BigQuery vs `id` in the target table,
`full_name` vs `name`). Traditionally you'd alias in the source SQL —
`SELECT user_id AS id` — which couples the rename to the query and makes
it hard to reuse one query across syncs with different destination
shapes.

**`field_mappings`** moves the rename into the sync config, where it
belongs. Same first-class feature Census, Hightouch, and Polytomic
expose.

## Quick Start

```yaml
name: users_to_crm
model: |
  SELECT user_id, full_name, created_ts
  FROM `project.dataset.users`

destination:
  type: postgres
  host_env: PG_HOST
  dbname_env: PG_DB
  user_env: PG_USER
  password_env: PG_PASSWORD
  table: crm_users
  upsert_key: [id]            # references the MAPPED name (see ordering below)

sync:
  mode: upsert
  field_mappings:
    user_id: id               # source_column: destination_field
    full_name: name
    created_ts: created_at
```

The destination receives `{id, name, created_at}` — the source query
keeps its natural warehouse names, and the rename happens in flight.

## How it works

`field_mappings` is `{source_column: destination_field}`. Each record's
keys are remapped in a **single pass** just before the record reaches
the destination:

1. Source rows are extracted with their original column names.
2. Cursor tracking (for `mode: incremental`) reads `cursor_field` — a
   **source** column name.
3. Destination lookups (`lookups:`) resolve FK values using **source**
   column names.
4. **`field_mappings` renames keys** — the last transform before load.
5. The destination, `upsert_key`, and the `--diff` engine all see the
   **mapped** (destination) names.

This ordering is deliberate: source-side concerns (`cursor_field`,
`lookups`) reference the names the query produces, while
destination-side concerns (`upsert_key`, target columns) reference the
names after the rename.

```
extract → cursor tracking → lookups → field_mappings → destination.load()
            (source names)  (source)    (rename here)    (mapped names)
```

## Ordering example: remapping the cursor column

You can rename a column that's also the incremental cursor — the cursor
is tracked on the source name before the rename:

```yaml
sync:
  mode: incremental
  cursor_field: created_ts        # source column — watermark tracked here
  field_mappings:
    created_ts: created_at        # destination gets `created_at`
```

Watermark state stays keyed to `created_ts`'s values; the destination
writes `created_at`.

## Rules and edge cases

- **Best-effort per record.** If a mapping's source column is absent
  from a given row, the rename is simply not applied to that row. Genuine
  typos are surfaced separately (see Validation below) rather than
  silently dropped here.
- **No chaining.** `{a: b, b: c}` renames `a→b` and `b→c` from the
  *original* keys — it does not turn `a` into `c`. The remap is a single
  pass, so it's order-independent.
- **Keep targets distinct.** If two source columns map to the same
  destination name (`{first: name, second: name}`), the later key in the
  row wins (last-write-wins) and you lose data. This is almost always a
  mistake — give each mapping a distinct target.
- **Unmapped columns pass through** unchanged.

## Validation

`drt validate` will warn about `field_mappings` keys that don't match any
column the source query produces — a best-effort typo check that
activates when the source schema is introspectable. (Today, `drt
validate` performs static schema validation without live introspection,
so this warning is wired and ready but only fires once schema
introspection is available; until then, use `drt run --dry-run` to
confirm the rename produced the columns you expect.)

```bash
drt run --select users_to_crm --dry-run --diff   # preview the mapped records
```

## What it does not do

`field_mappings` renames keys only. It is intentionally **not**:

- **Type coercion** — values pass through unchanged. Cast in the source
  SQL or use `tojson_safe` for `datetime` / `Decimal` / `UUID` in
  templated destinations.
- **Computed / derived fields** — there's no expression evaluation; a
  mapping target is a plain name, not a formula.
- **Nested extraction** — no JSONPath into nested columns.

These are deliberately out of scope (tracked separately) to keep the
rename predictable and pure.

## See also

- [`docs/llm/API_REFERENCE.md`](../llm/API_REFERENCE.md) — full sync
  options reference
- [Destination Lookup](destination-lookup.md) — resolve FK values during
  sync (runs *before* `field_mappings`)
