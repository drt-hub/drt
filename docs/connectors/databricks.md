# Databricks Destination

> Write data back to Databricks Delta Lake tables via the Databricks
> SQL Connector. Supports **INSERT**, **MERGE** (upsert), and
> **`sync.mode: mirror`** (upsert + DELETE-missing) — the same modes
> as the [Snowflake destination](snowflake.md).

## YAML Example — INSERT (append)

```yaml
destination:
  type: databricks
  host_env: DATABRICKS_HOST
  http_path_env: DATABRICKS_HTTP_PATH
  token_env: DATABRICKS_TOKEN
  catalog: main
  schema: default
  table: user_events
  mode: insert
```

## YAML Example — MERGE (upsert)

```yaml
destination:
  type: databricks
  host_env: DATABRICKS_HOST
  http_path_env: DATABRICKS_HTTP_PATH
  token_env: DATABRICKS_TOKEN
  catalog: main
  schema: default
  table: user_scores
  mode: merge
  upsert_key: [user_id]
```

## YAML Example — Mirror (upsert + delete-missing)

```yaml
destination:
  type: databricks
  host_env: DATABRICKS_HOST
  http_path_env: DATABRICKS_HTTP_PATH
  token_env: DATABRICKS_TOKEN
  catalog: main
  schema: analytics
  table: active_users
  upsert_key: [user_id]   # required for mirror

sync:
  mode: mirror            # forces MERGE write path + end-of-sync DELETE
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"databricks"` | — | Required |
| `host_env` | string | — | Env var name holding the workspace hostname (e.g. `dbc-abc12345-1234.cloud.databricks.com`) |
| `http_path_env` | string | — | Env var name holding the SQL warehouse HTTP path (e.g. `/sql/1.0/warehouses/abc123def456`) |
| `token_env` | string | — | Env var name holding a Databricks personal access token (PAT, starts with `dapi`) |
| `catalog` | string | — | Unity Catalog catalog name. Use `hive_metastore` for legacy workspaces. |
| `schema` | string | — | Database/schema name within the catalog |
| `table` | string | — | Target Delta Lake table name |
| `mode` | `insert` \| `merge` | `insert` | Write mode. `merge` requires `upsert_key`. |
| `upsert_key` | list[str] \| null | null | Column list that uniquely identifies a row. Required for `mode: merge` and for `sync.mode: mirror`. Composite keys supported (`[tenant_id, user_id]`). |

## Authentication

Databricks SQL Connector authenticates via a **personal access token**
(PAT). Generate one in the Databricks workspace at User Settings →
Developer → Access tokens.

```bash
export DATABRICKS_HOST=dbc-abc12345-1234.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/abc123def456
export DATABRICKS_TOKEN=dapi-xxxxxxxxxxxxxxxxxxxx
```

The token-bearing principal needs:
- `USAGE` on the catalog and schema
- `MODIFY` on the target table
- `CREATE` on the schema (for the merge-path staging table — see below)

> **OAuth M2M / service principal flows** are not yet supported.
> Track [#634](https://github.com/drt-hub/drt/issues) if you need
> them — for now use a PAT on a service-principal-scoped account.

## Write modes

### `mode: insert`

Issues one `INSERT INTO catalog.schema.table (...) VALUES (...)` per
record. Best for **append-only** workloads — event streams, audit
logs, telemetry.

Errors at the row level are tracked in `result.row_errors`; the
sync continues for the rest of the batch unless `on_error: fail`.

### `mode: merge`

Creates a uniquely-named **Delta scratch table**
(`catalog.schema.__drt_staging_<table>`), stages rows into it via
per-row `INSERT`, then issues `MERGE INTO target USING staging ON
<upsert_key> WHEN MATCHED THEN UPDATE WHEN NOT MATCHED THEN INSERT`,
and finally `DROP TABLE` the staging table.

**Why a Delta scratch table and not `CREATE TEMP TABLE`?** Databricks
Delta Lake doesn't have session-local tables — the standard
`CREATE TEMP TABLE` syntax isn't supported. A uniquely-named scratch
table in the same catalog.schema is the idiomatic shape. The
`__drt_staging_*` prefix makes it identifiable in audit logs.

**Composite keys** are supported (`upsert_key: [tenant_id, user_id]`)
— the `ON` clause becomes `target.tenant_id = source.tenant_id AND
target.user_id = source.user_id`.

If every column is in `upsert_key`, the MERGE skips the `UPDATE` clause
(no non-key columns to update); the resulting statement is effectively
`INSERT-IF-NOT-EXISTS`.

### `sync.mode: mirror`

Mirrors the source table to the destination: upserts source rows AND
deletes destination rows whose `upsert_key` was not observed in the
source. Forces the MERGE write path regardless of `config.mode`, so
the only YAML changes needed are:

```yaml
destination:
  ...
  upsert_key: [user_id]   # required
sync:
  mode: mirror
```

End-of-sync, drt issues a single
`DELETE FROM catalog.schema.table WHERE upsert_key NOT IN (observed)`
against the destination. Composite keys use the
`WHERE (c1, c2) NOT IN ((v1a, v1b), (v2a, v2b), ...)` form.

**Safety guard**: if no batch ever produced records (source returned
zero rows), the DELETE is skipped entirely — protects against wiping
the destination when the source is transiently empty.

Mirror semantics fit the same shape as Postgres / MySQL / ClickHouse /
Snowflake mirror destinations (see #340) — same `upsert_key` contract,
same source-key-cardinality memory bound on `_mirror_keys`.

### `sync.mode: replace` ([#643](https://github.com/drt-hub/drt/issues/643))

Rebuilds the destination table from the current source snapshot. Two strategies:

**`replace_strategy: truncate`** (default) — `TRUNCATE TABLE` once, then INSERT every batch.

```yaml
sync:
  mode: replace            # replace_strategy defaults to truncate
```

**`replace_strategy: swap`** — zero-downtime. Delta has no `ALTER TABLE … SWAP WITH`, so drt stages every batch into a shadow `<table>__drt_swap` (`CREATE OR REPLACE TABLE … AS SELECT * … WHERE 1=0` clones the schema), then at end-of-sync runs an atomic `INSERT OVERWRITE <table> SELECT * FROM <shadow>` and drops the shadow. `INSERT OVERWRITE` commits a new Delta table version atomically (snapshot isolation), so readers always see the full old or full new data — never a half-written table — and the **target table object is preserved** (grants, properties, clustering / liquid clustering all survive; the table itself is never recreated).

```yaml
sync:
  mode: replace
  replace_strategy: swap
```

- **First run** (target table doesn't exist yet) falls through to a direct write into the target and skips the swap.
- **Interrupted swaps** leave a `<table>__drt_swap` shadow; `drt clean --orphans` lists and drops them (only `__drt_swap`-suffixed tables are eligible).

Swap requires `mode: replace` (enforced by config validation). The same `replace_strategy: swap` is supported on Postgres, MySQL, ClickHouse, and Snowflake.

## Sync modes

| `sync.mode` | Behaviour on Databricks |
|-------------|--------------------------|
| `full` | Re-extracts every run + writes via `config.mode` (insert / merge). |
| `incremental` | Watermark-based — extracts rows with `cursor_field > last_value`, writes via `config.mode`. |
| `upsert` | Same as `incremental` but with `upsert_key` enforced. |
| `mirror` | Forces the MERGE write path + end-of-sync DELETE. `upsert_key` required. |
| `replace` | Full table replace. `replace_strategy: truncate` (default) TRUNCATEs + re-inserts; `replace_strategy: swap` is zero-downtime via a shadow + atomic `INSERT OVERWRITE`. See above. |

## Notes

- Requires `pip install drt-core[databricks]` (depends on `databricks-sql-connector>=3.0`).
- Target table must be a **Delta Lake table**. Non-Delta formats fail at `MERGE INTO` time with a Databricks server error.
- Empty batches short-circuit before any `databricks.sql` import or warehouse call — the same "no driver was imported" contract used by the SQL destinations (#595). A run with zero source rows produces zero warehouse statements.
- Errors during the connect step (missing env vars, bad token, network) raise immediately; errors during row INSERT are captured per-row and surface in `result.row_errors`.
- The `__drt_staging_*` scratch table is dropped at the end of every merge run. If a sync is interrupted mid-merge, the next run's `CREATE OR REPLACE TABLE` overwrites it cleanly.
- Unity Catalog three-part names are the default. Workspaces still on Hive Metastore should use `catalog: hive_metastore`.

## References

- [Databricks SQL Connector for Python](https://docs.databricks.com/dev-tools/python-sql-connector.html)
- [Delta Lake MERGE INTO](https://docs.databricks.com/delta/merge.html)
- [Personal access tokens](https://docs.databricks.com/dev-tools/auth/pat.html)
- Sibling SQL destinations with the same shape: [Snowflake](snowflake.md), [PostgreSQL](postgres.md), [MySQL](mysql.md), [ClickHouse](clickhouse.md)
