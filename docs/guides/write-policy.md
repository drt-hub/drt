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
trimming. On PostgreSQL, MySQL, Snowflake, and Databricks, the diff reads the
destination value and applies the same rule, so `--dry-run --diff` and `drt plan`
agree with the write. BigQuery does not support destination diff/plan, as described
below. Every non-text column — binary, numeric, boolean, date/time, array, map,
struct, object, or variant — is empty only when SQL `NULL`.

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

BigQuery also requires destination `mode: merge` for an ordinary upsert;
`sync.mode: mirror` forces that MERGE path. BigQuery has no `typeof()` function,
so drt reads the target table schema once and trims only scalar `STRING` columns.
`BYTES`, `JSON`, `STRUCT` / `RECORD`, repeated fields, numeric, boolean,
date/time, and `GEOGRAPHY` columns are empty only when SQL `NULL`. If the target
table does not exist (`NotFound`), there is no schema yet, so every fill column is
treated as NULL-only rather than risking an invalid `TRIM` on a non-STRING type.
Any other schema lookup failure fails the batch. BigQuery's no-argument `TRIM` uses
BigQuery's native whitespace definition, so a STRING containing only other
whitespace can also count as empty there.

BigQuery does not collapse repeated source keys. Rows with the same signature
are loaded into one temp table and one `MERGE`; if multiple source rows match an
existing target row, BigQuery rejects the ambiguous update. Rows separated into
different sparse-signature runs execute as separate MERGEs, but model output
should still be unique per `upsert_key`. BigQuery is not a queryable destination,
so `drt plan` and `drt run --dry-run --diff` report destination diff/plan as
unsupported; they do not model its repeated keys, apply `fill_empty`, validate
override names, or count kept values.

HubSpot uses a read-then-filter path because its Objects API has no conditional
MERGE expression. Before each group of at most 100 rendered records, drt sends
one `batch/read` request for only the fill-only properties present in that group.
For an existing object, non-empty fill-only properties are removed from the
PATCH while overwrite properties and `id_property` remain. A new object is
POSTed in full. A 207 partial read is valid only when every absent result is
reported by `OBJECT_NOT_FOUND`; one error may group multiple IDs, but its
non-empty ID list must contain only requested inputs. Any other batch error,
status absent or not exactly `COMPLETE`, or missing/malformed `results` fails the
group closed and writes none of it. The batch read and PATCH-by-`idProperty`
require a unique-valued HubSpot property or `hs_object_id`. A non-unique key
such as `dealname` therefore fails the read with row errors and no writes. If a
supposedly missing object appears
before its POST and HubSpot returns 409, drt also fails that row closed and asks
you to re-run—it does not send an unfiltered PATCH.

Override names are lowercase and case-sensitive. drt validates them once
against HubSpot's properties API when the token permits it; a 401/403/404,
unusable response, transport failure, or exhausted 5xx retries skips validation
with one warning because private apps may lack schema access. Repeated IDs are
first-writer-wins within one destination load. Duplicates split across engine
batches rely on HubSpot returning the first write on the next batch's read, so a
small stale-read window remains possible. HubSpot rate limiting counts each
logical lookup/read/write, not its internal retries or the default-policy
POST-409-to-PATCH fallback.

## Behaviour

- **New rows are inserted in full.** The policy only applies when the row
  already exists.
- **Key columns / HubSpot's `id_property`** are never filtered out, so the
  policy never applies to them.
- Works with `match_policy: upsert` on every destination listed below, and with
  `update_only` on PostgreSQL, MySQL, and HubSpot (a missing row is still skipped,
  never created). Snowflake, Databricks, and BigQuery do not implement
  `update_only`; their existing engine guard refuses that policy before any I/O.
  `create_only` never updates, so there is nothing to fill.
- `mode: replace` is rejected: it rebuilds the table, so there is no existing
  value to keep.
- A name in `write_policy_overrides` that is not a column of the destination
  table—or, for HubSpot, not a property returned by the properties API when that
  best-effort lookup is available—is an error and nothing is written: a typo would otherwise apply the
  default policy to the column you meant (overwriting, when the default is
  `overwrite`). This is checked against the destination's own columns, so it is
  exact however the source batches its rows; it needs schema introspection
  (`introspect_schema`, the default, and no `json_columns`). On PostgreSQL,
  MySQL, Snowflake, and Databricks, `drt plan` and `drt run --dry-run --diff`
  also check the names against the columns the source produces and report the
  sync as unavailable, with the reason, when one is missing. BigQuery diff/plan
  is unsupported and therefore performs none of these preview checks.
- On PostgreSQL, MySQL, Snowflake, and Databricks, the diff and plan model
  repeated source keys in first-writer-wins order. Live PostgreSQL/MySQL writes
  match that model; Databricks can instead reject an ambiguous same-batch MERGE.

## Where it works

| Destination | `fill_empty` |
|---|---|
| PostgreSQL | yes: in the `ON CONFLICT ... DO UPDATE` expression (and `UPDATE` for `update_only`), no extra round trip |
| MySQL | yes: in the `ON DUPLICATE KEY UPDATE` expression (and `UPDATE` for `update_only`) |
| Snowflake | yes: in the VALUES-sourced `MERGE` update expression (including `mode: mirror` before its unchanged delete pass); requires the destination's `mode: merge` for ordinary upserts |
| Databricks | yes: in the staging-sourced Delta `MERGE`, composed with sparse-row presence flags (including `mode: mirror` before its unchanged delete pass); requires the destination's `mode: merge` for ordinary upserts |
| BigQuery | yes: in each typed-temp-table `MERGE`, with sparse runs preserved (including `mode: mirror` before its unchanged delete pass); requires the destination's `mode: merge` for ordinary upserts |
| HubSpot | yes: one fail-closed `batch/read` per ≤100 rendered records, then filtered PATCHes; new objects are POSTed in full |
| Others | refused up front with a clear message, never silently overwritten. |

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
BigQuery uses
``col = CASE WHEN T.col IS NULL OR TRIM(T.col) = '' THEN S.col ELSE T.col END``
for scalar `STRING` columns and omits the `TRIM` branch for every other type.

## Seeing what is kept

For PostgreSQL, MySQL, Snowflake, and Databricks, `drt run --dry-run --diff` and
`drt plan` apply the policy when they compare: a column the destination already
holds is not reported as an update, and the number of kept values is shown
(`N existing value(s) kept`, and `kept_values` in `plan.json`'s `summary`). A row
whose only differences are kept values is not listed at all, because nothing
would be written. BigQuery destination diff/plan is unsupported, so these preview
semantics and counts are not available for it. HubSpot also has no destination
diff/plan support ([#472](https://github.com/drt-hub/drt/issues/472)), so `drt plan`
and `drt run --dry-run --diff` remain unsupported for it. Its aggregated run
`SyncResult.kept_values` counts kept **properties** exactly; a row for which every
non-ID property is kept is an ordinary skip, not an error or a no-match skip.

A normal SQL `drt run` does not count kept values: the write is a single upsert
statement, which does not report per column what it left alone. Use the diff or
the plan to see them before writing. HubSpot can count them because it reads the
current properties before writing; `drt run` reports the aggregate as
`N properties kept (non-empty)`.

## Not the same as

- **`match_policy: create_only`**: never updates *any* column of an existing row.
- **Filtering in the model SQL**: the warehouse does not know the destination's
  current value. SQL destinations decide inside the write itself; HubSpot uses
  the API's batch read followed by a filtered PATCH and fails a POST-409 create
  race closed so it cannot overwrite a newly appeared record.

The HubSpot leg is verified against HTTP mocks only; no live HubSpot account was
available for this implementation.
