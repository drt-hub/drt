# ADR 0016 — Managed destination tables: `manage_table` and `on_schema_change`

- **Status:** Accepted 2026-10-28 (owner confirmed the proposals — see "Decisions").
- **Issue:** [#760](https://github.com/drt-hub/drt/issues/760) (epic [#1227](https://github.com/drt-hub/drt/issues/1227)).
- **Relates to:** [ADR 0005](0005-state-location-and-write-grants.md) (warehouse writes are opt-in; cursor
  incremental stays first-class); [ADR 0017](0017-column-contracts.md) (declared columns inform the
  types); #317 (`introspect_schema()`); #542 (boundary tests documenting today's failures); #695
  (privilege docs).

## Context

SQL/warehouse destinations need the target table to exist with the right columns, and the INSERT
column list comes from the first record's keys. Two consequences: every new sync starts with
hand-written DDL, and when the model adds or drops a column drt fails inside the driver (or loads a
subset) — there is no drt-level drift policy. #760 asks for an opt-in create-if-missing and an
explicit policy for new columns.

Two commitments from ADR 0005 must not break: the read-only, cursor-based path stays first-class
(this is an *addition*), and turning the option back off must keep working (no one-way door).

## Decision

### 1. Two independent, opt-in keys on SQL destinations

```yaml
destination:
  type: postgres
  table: public.users_enriched
  manage_table: true            # default false: CREATE TABLE IF NOT EXISTS before the first load
  on_schema_change: fail        # fail (default) | ignore | append_new_columns
```

`manage_table` absent or false is exactly today's behaviour. `on_schema_change` is meaningful with
or without `manage_table`. Non-SQL destinations reject both keys at validate time (an error, not a
warning: silently ignoring a drift policy is how drift gets through).

### 2. The check runs before any record is loaded

After the first extracted batch (the same point ADR 0017's contract check uses) drt compares the
record keys with the table's columns from `introspect_schema()` and decides *before* the first
write:

| Situation | `fail` | `ignore` | `append_new_columns` |
|---|---|---|---|
| Table missing, `manage_table: true` | create | create | create |
| Table missing, `manage_table: false` | error naming the table | same | same |
| Source has a column the table lacks | error naming the columns | load the shared columns, warn | `ALTER TABLE ADD COLUMN` (nullable), then load |
| Table has a column the source lacks | allowed if nullable/defaulted, else error | same | same |
| Column type incompatible | error with guidance | not checked | error with guidance |

Removals and type migrations are **never** performed by drt: they fail with guidance. That is the
line that keeps this safe enough to be opt-in rather than dangerous.

### 3. Types

Created columns take their type from the declared contract (ADR 0017) when there is one, otherwise
from the source cursor metadata, otherwise a portable fallback (`TEXT` / `VARIANT` / `STRING`).
Nothing is inferred by sampling values. Added columns are always nullable.

### 4. Dry-run, plan and privileges

- `--dry-run` and `drt plan` report the DDL they *would* run and never execute it. A plan whose
  destination table is missing is reported as such, not as an error.
- Required privileges (CREATE, ALTER) are documented per warehouse; a missing privilege is a
  pre-load error naming it, extending the #695 guide.
- Executed DDL is recorded on the run (additive history field, omit-when-absent, ADR 0013 style)
  so "who added this column, when" is answerable.

### 5. Phasing

Postgres and MySQL first, then Snowflake, BigQuery, Databricks (the order #317 and #340 used).
`plan_coverage` gets a line per destination as it lands. Each warehouse's DDL dialect is behind the
existing `sql_base` seam; no new abstraction.

## Non-goals

- Dropping or renaming columns, type migrations, index/constraint management (`upsert_key` must
  already be a constraint the engine can use; creating one is out of scope).
- DDL for SaaS destinations (HubSpot properties etc.) — future per-connector work.
- Atomicity of DDL with the load: most warehouses auto-commit DDL, so the order is
  check → DDL → load, and a failed load leaves the (compatible) new columns in place.

## Consequences

- One config surface, default off; existing syncs unchanged.
- The pre-load error for drift replaces driver stack traces with a message naming columns.
- drt can now alter a customer's table when asked: the opt-in, the nullable-only additions and the
  recorded DDL exist to keep that auditable.

## Decisions

Confirmed by the maintainer on 2026-10-28, all as proposed:

1. `on_schema_change` defaults to `fail`.
2. A missing table with `manage_table: false` is a clear pre-load error.
3. First release: Postgres + MySQL.
4. Executed DDL is recorded in run history (additive).
