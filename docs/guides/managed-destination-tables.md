# Managed destination tables

PostgreSQL and MySQL destinations can opt into a pre-load schema check and narrowly managed DDL
([#760](https://github.com/drt-hub/drt/issues/760),
[ADR 0016](../adr/0016-managed-destination-tables.md)). This first release can create a missing
table or append nullable columns. It never drops, renames, or changes the type of a column.

```yaml
destination:
  type: postgres                 # or mysql
  table: public.users_enriched
  upsert_key: [id]
  manage_table: true             # strict boolean; default false
  on_schema_change: fail         # fail | ignore | append_new_columns
```

The first check runs after drt has a destination-facing batch and has applied field mappings,
masking, computed fields, and metadata columns, but before that batch is written. drt remembers
the observed column set for the run. If a later batch introduces a key, drt re-introspects and
applies the same `fail`, `ignore`, or `append_new_columns` policy before writing that batch.
Existing configs that declare neither key keep the previous path byte-for-byte: no new schema
probe or DDL. Set `on_schema_change` explicitly when you want drift checking on a pre-provisioned
table without granting CREATE permission.

## Behaviour

| Destination state | `fail` | `ignore` | `append_new_columns` |
|---|---|---|---|
| Table missing, `manage_table: true` | Create it | Create it | Create it |
| Table missing, `manage_table: false` | Error naming the table | Same | Same |
| Source has a new column | Error naming columns | Warn and load only shared columns | Add nullable columns, then load |
| Destination column absent from source | Allowed when nullable or defaulted; otherwise error | Same | Same |
| Incompatible type, when source metadata is available | Error with migration guidance | Not checked | Error with migration guidance |

Creation and appended columns use declared `sync.columns` types when present, then source cursor
metadata when a source exposes it, then the portable `TEXT` fallback. Declarations are mapped
through `sync.field_mappings` before destination DDL is built.

| `sync.columns[].type` | PostgreSQL | MySQL |
|---|---|---|
| `string` | `TEXT` | `TEXT` |
| `number` | `NUMERIC` | `DECIMAL(65, 30)` |
| `integer` | `BIGINT` | `BIGINT` |
| `boolean` | `BOOLEAN` | `BOOLEAN` |
| `timestamp` | `TIMESTAMPTZ` | `DATETIME(6)` |
| `date` | `DATE` | `DATE` |
| `json` | `JSONB` | `JSON` |

A MySQL string or fallback `upsert_key` uses `VARCHAR(255)` because MySQL cannot make an
unrestricted `TEXT` column a primary key. A newly created table gets a primary key from
`upsert_key`, so PostgreSQL `ON CONFLICT` and MySQL `ON DUPLICATE KEY` remain usable. For an
existing MySQL table, drt verifies before writing that one PRIMARY or UNIQUE index covers exactly
the configured `upsert_key` columns. PostgreSQL continues to rely on `ON CONFLICT` to report a
missing matching constraint; drt does not add or change constraints on an existing table.

Type compatibility is deliberately a coarse family check. It compares text, integer, number,
boolean, timestamp, date, JSON, binary, and array families; it does **not** compare integer width,
character length, decimal precision/scale, or timestamp timezone semantics. Pre-provision narrower
types carefully: the database remains responsible for rejecting overflow, rounding, truncation, or
timezone-loss cases that pass the family check.

`append_new_columns` always adds nullable columns. `ignore` removes unshared columns from every
batch in the run and emits a warning. A non-null destination column with no default must still be
present in the source under every policy.

## Dry runs and plans

`drt run --dry-run` prints each `DDL would run` statement and includes `ddl` in JSON output; it does
not execute DDL. `drt plan` includes the same safely quoted statements in its terminal, Markdown,
and sealed JSON output. Apply refuses if recomputing the plan produces different DDL. A missing
table without `manage_table: true` is an unavailable plan result, not an unhandled planner error.

Executed statements are recorded as the omit-when-absent `ddl` field in run history, including
warehouse-backed `_drt_history` stores when their optional column can be added. MCP `drt_plan`,
`drt_run_sync`, and `drt_apply` responses expose the same `ddl` list. A DLQ retry that executes DDL
also returns and prints it and writes a history entry for that schema change.

Exact replay stores rows, not connector cursor metadata. DDL typed only by optional source metadata
therefore cannot be reproduced and exact apply fails clearly. Declare the columns in `sync.columns`
when a managed-table plan must support `--include-values`; config-declared portable types are fully
reproducible.

## Privileges

- Table creation requires `CREATE` on the target schema/database.
- Appending columns requires `ALTER` on the target table.
- The pre-load check requires access to `information_schema.columns`; MySQL upserts also read
  `information_schema.statistics`.

Grant only the operation selected by the config. A role without DDL privileges can keep using a
pre-provisioned table with neither key, or enable drift checking with `manage_table: false` and
`on_schema_change: fail` / `ignore` while retaining only metadata-read and data-write grants. DDL
permission failures stop before the affected batch is written and name the missing `CREATE` or
`ALTER` privilege. Other DDL failures retain the database error after best-effort credential
redaction.

## Non-goals and transaction boundary

- Snowflake, BigQuery, Databricks, ClickHouse, and non-SQL destinations reject these keys in this
  release instead of silently ignoring them.
- Column drops, renames, type migrations, and constraint/index management on existing tables are
  not performed.
- DDL is not atomic with the load. drt commits compatible DDL before writing rows; a later load
  failure leaves the created table or appended nullable columns in place.
