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

The check runs after drt has the first extracted batch and has applied field mappings, masking,
computed fields, and metadata columns, but before the first destination write. Existing configs
that declare neither key keep the previous path byte-for-byte: no new schema probe or DDL. Set
`on_schema_change` explicitly when you want drift checking on a pre-provisioned table without
granting CREATE permission.

## Behaviour

| Destination state | `fail` | `ignore` | `append_new_columns` |
|---|---|---|---|
| Table missing, `manage_table: true` | Create it | Create it | Create it |
| Table missing, `manage_table: false` | Error naming the table | Same | Same |
| Source has a new column | Error naming columns | Warn and load only shared columns | Add nullable columns, then load |
| Destination column absent from source | Allowed when nullable or defaulted; otherwise error | Same | Same |
| Incompatible type, when source metadata is available | Error with migration guidance | Not checked | Error with migration guidance |

Creation uses source cursor type metadata when a source exposes it through drt's optional metadata
capability. Otherwise columns use the portable `TEXT` fallback; a MySQL fallback `upsert_key` uses
`VARCHAR(255)` because MySQL cannot make an unrestricted `TEXT` column a primary key. A newly
created table gets a primary key from `upsert_key`, so the existing PostgreSQL `ON CONFLICT` and
MySQL `ON DUPLICATE KEY` writes remain usable. drt does not add or change constraints on an existing
table.

`append_new_columns` always adds nullable columns. `ignore` removes unshared columns from every
batch in the run and emits a warning. A non-null destination column with no default must still be
present in the source under every policy.

## Dry runs and plans

`drt run --dry-run` prints each `DDL would run` statement and includes `ddl` in JSON output; it does
not execute DDL. `drt plan` includes the same safely quoted statements in its terminal, Markdown,
and sealed JSON output. Apply refuses if recomputing the plan produces different DDL. A missing
table without `manage_table: true` is an unavailable plan result, not an unhandled planner error.

Executed statements are recorded as the omit-when-absent `ddl` field in run history, including
warehouse-backed `_drt_history` stores when their optional column can be added.

## Privileges

- Table creation requires `CREATE` on the target schema/database.
- Appending columns requires `ALTER` on the target table.
- The pre-load check requires access to `information_schema.columns`.

Grant only the operation selected by the config. A role without DDL privileges can keep using a
pre-provisioned table with neither key, or enable drift checking with `manage_table: false` and
`on_schema_change: fail` / `ignore` while retaining only metadata-read and data-write grants. DDL
permission failures stop before the first row write and name the missing `CREATE` or `ALTER`
privilege.

## Non-goals and transaction boundary

- Snowflake, BigQuery, Databricks, ClickHouse, and non-SQL destinations reject these keys in this
  release instead of silently ignoring them.
- Column drops, renames, type migrations, and constraint/index management on existing tables are
  not performed.
- Types declared by column contracts are tracked separately in #761.
- DDL is not atomic with the load. drt commits compatible DDL before writing rows; a later load
  failure leaves the created table or appended nullable columns in place.

