# Write policy: fill only what is empty

`sync.match_policy` chooses **which rows** a sync may touch. `sync.write_policy`
chooses **which columns** it may overwrite.

```yaml
sync:
  mode: upsert
  write_policy: fill_empty            # default: overwrite
  write_policy_overrides:
    lifecycle_stage: overwrite        # always keep this one in sync
```

With `fill_empty`, an upsert writes a column only when the destination's current
value is **NULL or an empty string**. A value that is already there is never
replaced. That is the contract of most enrichment syncs: the warehouse value is a
best guess (third-party data, a model's inference), while an existing destination
value may have been entered or verified by someone.

`write_policy_overrides` sets a single column against the default, in either
direction: `fill_empty` by default with an `overwrite` override for the columns
that must always track the warehouse, or `overwrite` by default with a
`fill_empty` override for the one descriptive field you do not want to clobber.

## Exactly what counts as empty

| Column | Empty when |
|---|---|
| text (`TEXT`, `VARCHAR`, `CHAR(n)`, ...) | `NULL`, `''`, or **only spaces** |
| any other type (integer, boolean, date, ...) | `NULL` only |

A stored `0` or `false` is a **value**, not empty, so it is kept. Spaces-only
counts as empty because a `CHAR(n)` column pads with spaces and the database cannot
tell padding from content; a tab or a newline is a value. (The comparison casts the
column to text and trims spaces for the check only; the column's own type is never
changed. The diff reads the destination value and applies the same rule, so
`--dry-run --diff` and `drt plan` agree with the write.) Binary columns (`bytea`,
`BLOB`) are not meaningful here: leave them on `overwrite`.

## Behaviour

- **New rows are inserted in full.** The policy only applies when the row
  already exists.
- **Key columns** are never updated, so the policy never applies to them.
- Works with `match_policy: upsert` and `update_only` (a missing row is still
  skipped, never created). `create_only` never updates, so there is nothing to
  fill.
- `mode: replace` is rejected: it rebuilds the table, so there is no existing
  value to keep.
- A name in `write_policy_overrides` that is not a column of the destination
  table is an error and nothing is written: a typo would otherwise apply the
  default policy to the column you meant (overwriting, when the default is
  `overwrite`). This is checked against the destination's own columns, so it is
  exact however the source batches its rows; it needs schema introspection
  (`introspect_schema`, the default, and no `json_columns`). `drt plan` and
  `drt run --dry-run --diff` also check the names against the columns the source
  produces and report the sync as unavailable, with the reason, when one is
  missing.
- A key that repeats in the source behaves as the real writes do: the first row
  fills an empty column, and a later row finds it taken. The diff and the plan model
  that order too.

## Where it works

| Destination | `fill_empty` |
|---|---|
| PostgreSQL | yes: in the `ON CONFLICT ... DO UPDATE` expression (and `UPDATE` for `update_only`), no extra round trip |
| MySQL | yes: in the `ON DUPLICATE KEY UPDATE` expression (and `UPDATE` for `update_only`) |
| Others | refused up front with a clear message, never silently overwritten. Snowflake, Databricks, BigQuery and HubSpot follow ([#1238](https://github.com/drt-hub/drt/issues/1238)). |

The generated SQL for a fill column is, for Postgres (the target is aliased,
because an unqualified column is ambiguous with `EXCLUDED`),
`col = CASE WHEN t.col IS NULL OR btrim(t.col::text) = '' THEN EXCLUDED.col ELSE t.col END`,
and for MySQL
`col = IF(col IS NULL OR TRIM(CAST(col AS CHAR)) = '', VALUES(col), col)`.

## Seeing what is kept

`drt run --dry-run --diff` and `drt plan` apply the policy when they compare:
a column the destination already holds is not reported as an update, and the
number of kept values is shown (`N existing value(s) kept`, and `kept_values` in
`plan.json`'s `summary`). A row whose only differences are kept values is not
listed at all, because nothing would be written.

A normal `drt run` does not count kept values: the write is a single upsert
statement, which does not report per column what it left alone. Use the diff or
the plan to see them before writing.

## Not the same as

- **`match_policy: create_only`**: never updates *any* column of an existing row.
- **Filtering in the model SQL**: the warehouse does not know the destination's
  current value, and a read-then-write would be racy; `fill_empty` decides inside
  the write itself.
