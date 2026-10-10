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
| text (`TEXT`, `VARCHAR`, ...) | `NULL` or `''` |
| any other type (integer, boolean, date, ...) | `NULL` only |

A stored `0`, `false` or whitespace-only string is a **value**, not empty, so it
is kept. (The comparison casts the column to text for the check only; the
column's own type is never changed.)

## Behaviour

- **New rows are inserted in full.** The policy only applies when the row
  already exists.
- **Key columns** are never updated, so the policy never applies to them.
- Works with `match_policy: upsert` and `update_only` (a missing row is still
  skipped, never created). `create_only` never updates, so there is nothing to
  fill.
- `mode: replace` is rejected: it rebuilds the table, so there is no existing
  value to keep.
- A name in `write_policy_overrides` that no record in the batch carries is an
  error and nothing is committed: a typo on an override would otherwise apply the
  default policy to the column you meant.

## Where it works

| Destination | `fill_empty` |
|---|---|
| PostgreSQL | yes: in the `ON CONFLICT ... DO UPDATE` expression (and `UPDATE` for `update_only`), no extra round trip |
| MySQL | yes: in the `ON DUPLICATE KEY UPDATE` expression (and `UPDATE` for `update_only`) |
| Others | refused up front with a clear message, never silently overwritten. Snowflake, Databricks, BigQuery and HubSpot follow ([#1238](https://github.com/drt-hub/drt/issues/1238)). |

The generated SQL for a fill column is, for Postgres,
`col = CASE WHEN col IS NULL OR col::text = '' THEN EXCLUDED.col ELSE col END`,
and for MySQL
`col = IF(col IS NULL OR CAST(col AS CHAR) = '', VALUES(col), col)`.

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
