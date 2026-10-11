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

A stored `0`, `false`, empty binary value, or non-NULL semi-structured value is a
**value**, not empty, so it is kept. Spaces-only counts as empty for text because a
`CHAR(n)` column pads with spaces and the database cannot tell padding from content;
a tab or a newline is a value. The write tests the column's own database type before
trimming, and the diff reads the destination value and applies the same rule, so
`--dry-run --diff` and `drt plan` agree with the write. Every non-text column —
binary, numeric, boolean, date/time, array, map, struct, object, or variant — is
empty only when SQL `NULL`.

On Snowflake, `VARIANT` / `OBJECT` / `ARRAY` columns therefore count as empty only
when SQL `NULL` (a stored JSON `""` is a value; this relies on schema introspection,
which is on by default), and the destination must use
`mode: merge` (or `sync.mode: mirror`): `mode: insert` only appends, so
`fill_empty` is refused there rather than ignored. Repeated source keys in one
Snowflake batch are not collapsed (as with `overwrite`): keep the model output
unique per `upsert_key`.

Databricks has the same `mode: merge` requirement (`sync.mode: mirror` forces
that path). Its `STRUCT` / `ARRAY` / `MAP` / `VARIANT` columns are empty only
when SQL `NULL`; a stored semi-structured empty string, array, map or struct is a
value. Databricks stages each destination batch and runs one Delta `MERGE`,
without collapsing repeated keys. If multiple source rows in that batch match
the same existing target row, Delta rejects the ambiguous update; keep model
output unique per `upsert_key`.

## Behaviour

- **New rows are inserted in full.** The policy only applies when the row
  already exists.
- **Key columns** are never updated, so the policy never applies to them.
- Works with `match_policy: upsert` on every destination listed below, and with
  `update_only` on PostgreSQL and MySQL (a missing row is still skipped, never
  created). Snowflake and Databricks do not implement `update_only`; their
  existing engine guard refuses that policy before any I/O. `create_only` never
  updates, so there is nothing to fill.
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
| Snowflake | yes: in the VALUES-sourced `MERGE` update expression (including `mode: mirror` before its unchanged delete pass); requires the destination's `mode: merge` for ordinary upserts |
| Databricks | yes: in the staging-sourced Delta `MERGE`, composed with sparse-row presence flags (including `mode: mirror` before its unchanged delete pass); requires the destination's `mode: merge` for ordinary upserts |
| Others | refused up front with a clear message, never silently overwritten. BigQuery and HubSpot follow ([#1238](https://github.com/drt-hub/drt/issues/1238)). |

The generated SQL for a fill column is, for Postgres (the target is aliased,
because an unqualified column is ambiguous with `EXCLUDED`),
`col = CASE WHEN t.col IS NULL OR btrim(t.col::text) = '' THEN EXCLUDED.col ELSE t.col END`,
and for MySQL
`col = IF(col IS NULL OR TRIM(CAST(col AS CHAR)) = '', VALUES(col), col)`. Snowflake's
equivalent is
`col = CASE WHEN target.col IS NULL OR TRIM(target.col::STRING) = '' THEN source.col ELSE target.col END`.
Databricks uses
`col = CASE WHEN target.col IS NULL OR ((typeof(target.col) LIKE 'string%' OR typeof(target.col) LIKE 'varchar%' OR typeof(target.col) LIKE 'char%') AND TRIM(CAST(target.col AS STRING)) = '') THEN <presence-aware source expression> ELSE target.col END`,
so a sparse row that omitted the column still preserves the target value.

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
