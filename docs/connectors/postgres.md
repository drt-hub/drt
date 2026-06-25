# PostgreSQL Destination

> Upsert records into PostgreSQL tables using `INSERT ... ON CONFLICT DO UPDATE`.

## YAML Example

```yaml
destination:
  type: postgres
  host_env: PG_HOST
  port: 5432
  dbname_env: PG_DATABASE
  user_env: PG_USER
  password_env: PG_PASSWORD
  table: public.user_scores
  upsert_key: [user_id]
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"postgres"` | — | Required |
| `connection_string_env` | string \| null | null | Env var with full connection string (takes precedence) |
| `host` / `host_env` | string | — | Hostname (direct or env var) |
| `port` | int | `5432` | Port number |
| `dbname` / `dbname_env` | string | — | Database name |
| `user` / `user_env` | string | — | Username |
| `password` / `password_env` | string | — | Password |
| `table` | string | — | Target table (e.g., `public.users`) |
| `upsert_key` | list[str] | — | Columns for `ON CONFLICT` (must be unique constraint) |
| `ssl` | SslConfig \| null | null | SSL/TLS config |
| `lookups` | dict \| null | null | FK resolution via destination DB query |

## Authentication

**Option 1: Individual fields (recommended)**
```yaml
host_env: PG_HOST
dbname_env: PG_DATABASE
user_env: PG_USER
password_env: PG_PASSWORD
```

**Option 2: Connection string**
```yaml
connection_string_env: DATABASE_URL
# e.g. postgresql://user:pass@host:5432/dbname
```

**SSL:**
```yaml
ssl:
  enabled: true
  ca_env: PG_SSL_CA      # path to CA cert
  cert_env: PG_SSL_CERT   # path to client cert
  key_env: PG_SSL_KEY     # path to client key
```

## Common Patterns

**Upsert by email:**
```yaml
table: public.contacts
upsert_key: [email]
```

**Replace mode (TRUNCATE + INSERT):**
```yaml
sync:
  mode: replace
```

**Zero-downtime replace via staging swap:**
```yaml
sync:
  mode: replace
  replace_strategy: swap   # default: truncate
```

drt creates a shadow table `{table}__drt_swap`, populates it across batches, then atomically renames it to the original. Readers of the original table never see an empty state.

Requirements: the destination user needs `CREATE TABLE`, `ALTER TABLE`, and `DROP TABLE` privileges.

Caveats:
- Tables with dependent views, materialized views, or triggers are **not recommended** for swap mode — the rename can break dependent objects.
- If a sync is killed before completion, an orphan shadow may remain. Drop manually with `DROP TABLE {table}__drt_swap`. Auto-cleanup is tracked in [#433](https://github.com/drt-hub/drt/issues/433).

Same `replace_strategy: swap` is supported on MySQL (atomic `RENAME TABLE`) and ClickHouse (atomic `EXCHANGE TABLES`, requires CH 21.8+).

**Mirror mode (differential delete, [#340](https://github.com/drt-hub/drt/issues/340) — v0.7.7+):**

```yaml
sync:
  mode: mirror
destination:
  type: postgres
  table: public.employees
  upsert_key: [employee_id]   # required for mirror
```

Mirror upserts every source row (same as `full`), then issues a single `DELETE FROM <table> WHERE upsert_key NOT IN (observed keys)` from `finalize_sync()`. Use when **rows disappearing from the source should disappear from the destination** — e.g., a master table where deletions in the source system (employees / SKUs / permissions) need to propagate without the TRUNCATE / re-insert cost of `replace` mode.

Comparison:

| Mode | New rows | Updated rows | Removed-from-source rows | Cost shape |
|---|---|---|---|---|
| `upsert` / `full` | upsert | upsert | stay in destination | upsert per row |
| `replace` | INSERT | INSERT | DELETEd as side effect | TRUNCATE + INSERT all |
| **`mirror`** | upsert | upsert | **DELETEd by upsert_key NOT IN (...)** | upsert per row + 1 DELETE |

Safety guards:

- **Empty source short-circuit** — if no batch ever delivered records, the DELETE is skipped. A transient empty source (auth failure mid-extract, vendor outage) cannot wipe the destination.
- **Failed rows excluded from the key set** — only successfully upserted keys count as "observed source state"; a row that failed during upsert won't cause its destination counterpart to be deleted.
- **`upsert_key` required** — load() raises `ValueError` before any write if `destination.upsert_key` is empty.
- **Composite keys supported** — `upsert_key: [tenant_id, employee_id]` produces `WHERE (tenant_id, employee_id) NOT IN ((...), (...))`.

Memory constraint: the in-process key set is memory-bound to source key cardinality. For tables with **more than a few million keys** the temp-table strategy ([#340 follow-up](https://github.com/drt-hub/drt/issues/340)) will be more appropriate. Mirror as shipped today is appropriate for small/medium reference tables.

Same `sync.mode: mirror` is supported on **MySQL** (explicit `%s` placeholder list), **ClickHouse** (`ALTER TABLE ... DELETE WHERE` mutation with `mutations_sync=1`), and **Snowflake** (forces the MERGE write path regardless of `config.mode`). BigQuery follows once contributor PR [#584](https://github.com/drt-hub/drt/pull/584) lands.

**FK resolution with destination_lookup:**
```yaml
lookups:
  department_id:
    table: departments
    match: { name: department_name }
    select: id
    on_miss: skip
```

## Complex types (JSON / JSONB / arrays)

`dict` and `list` values need different wire formats depending on the destination column: a `JSONB` column wants JSON, a native `ARRAY` column wants the list handed to the driver's array adapter. A bare Python `list` is ambiguous — it could mean either.

**Schema-aware by default (`introspect_schema: true`).** At sync start drt reads `INFORMATION_SCHEMA.COLUMNS` for the target table once and routes each value by the column's real type:

- `json` / `jsonb` column → the dict **or list** is JSON-encoded (via `psycopg2.extras.Json`),
- `ARRAY` column → the list passes through to psycopg2's array adapter,
- anything else → unchanged.

This resolves the list→JSONB-vs-ARRAY ambiguity with **no configuration**. Introspection is best-effort: if `information_schema` isn't readable or the table doesn't exist yet, drt falls back to its prior behaviour (encode dicts, pass lists through). Disable with `introspect_schema: false`.

`json_columns` is an explicit **override** that always wins over introspection — list the columns allowed to hold JSON and unlisted complex values raise an early, pointing error:

```yaml
json_columns: [profile, preferences]
```

## Notes

- Requires `pip install drt-core[postgres]` (uses `psycopg2`)
- `upsert_key` columns must have a UNIQUE constraint on the target table
- `drt test` validators (row_count, not_null, freshness, unique, accepted_values) work with PostgreSQL
- `--dry-run` shows row count diff for `mode: replace`