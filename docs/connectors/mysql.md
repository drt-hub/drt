# MySQL Destination

> Upsert records into MySQL tables using `INSERT ... ON DUPLICATE KEY UPDATE`.

## YAML Example

```yaml
destination:
  type: mysql
  host_env: MYSQL_HOST
  port: 3306
  dbname_env: MYSQL_DATABASE
  user_env: MYSQL_USER
  password_env: MYSQL_PASSWORD
  table: interviewer_learning_profiles
  upsert_key: [user_id, company_id]
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"mysql"` | — | Required |
| `connection_string_env` | string \| null | null | Env var with full connection string (takes precedence) |
| `host` / `host_env` | string | — | Hostname (direct or env var) |
| `port` | int | `3306` | Port number |
| `dbname` / `dbname_env` | string | — | Database name |
| `user` / `user_env` | string | — | Username |
| `password` / `password_env` | string | — | Password |
| `table` | string | — | Target table (e.g. `mydb.scores` for schema-qualified) |
| `upsert_key` | list[str] | — | Columns for `ON DUPLICATE KEY UPDATE` (must have a UNIQUE / PRIMARY KEY constraint) |
| `ssl` | SslConfig \| null | null | SSL/TLS config |
| `json_columns` | list[str] \| null | null | Columns containing JSON values (encoded as JSON strings via `_serializer`) |

## Authentication

**Option 1: Individual fields (recommended)**
```yaml
host_env: MYSQL_HOST
dbname_env: MYSQL_DATABASE
user_env: MYSQL_USER
password_env: MYSQL_PASSWORD
```

**Option 2: Connection string**
```yaml
connection_string_env: MYSQL_URL
# e.g. mysql://user:pass@host:3306/dbname
```

**SSL (mTLS):**
```yaml
ssl:
  enabled: true
  ca_env: MYSQL_SSL_CA      # path to CA cert
  cert_env: MYSQL_SSL_CERT  # path to client cert
  key_env: MYSQL_SSL_KEY    # path to client key
```

## Common Patterns

**Upsert by composite key:**
```yaml
table: user_skills
upsert_key: [user_id, skill_id]
```

The MySQL connector serialises `dict` / `list` values to JSON strings via the shared `_serializer` module (consolidated in v0.7.5 alongside Postgres) — both encode through `json.dumps`.

**Schema-aware by default (`introspect_schema: true`).** At sync start drt reads `INFORMATION_SCHEMA.COLUMNS` for the target table once and routes each value by the column's real type — so a `dict`/`list` lands as JSON in a `JSON` column with **no configuration**. Introspection is best-effort: if `information_schema` isn't readable (locked-down grants) or the table doesn't exist yet, drt silently falls back to encoding every complex value. Set `introspect_schema: false` to disable it.

Use `json_columns` to **override** introspection with an explicit allowlist — it always takes priority and surfaces an early error when an unlisted column carries a complex value:

```yaml
json_columns: [preferences, metadata]
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

drt creates a shadow table `{table}__drt_swap`, populates it across batches, then issues a single atomic `RENAME TABLE` from `finalize_sync` — MySQL guarantees the multi-table `RENAME TABLE original TO old, shadow TO original` is atomic in one statement, so readers of the original table never see an empty or half-populated state.

Requirements: the destination user needs `CREATE`, `ALTER`, `DROP`, and `RENAME` privileges on the schema.

Caveats:
- Tables with dependent views or triggers are **not recommended** for swap mode — the rename can break dependent objects.
- If a sync is killed before completion, an orphan shadow may remain. Drop manually with `DROP TABLE {table}__drt_swap`. Auto-cleanup is tracked in [#433](https://github.com/drt-hub/drt/issues/433).

Same `replace_strategy: swap` is supported on Postgres (transactional `ALTER TABLE ... RENAME`) and ClickHouse (atomic `EXCHANGE TABLES`, requires CH 21.8+).

**Mirror mode (differential delete, [#340](https://github.com/drt-hub/drt/issues/340) Step 2 — v0.7.7+):**

```yaml
sync:
  mode: mirror
destination:
  type: mysql
  table: hr.employees
  upsert_key: [employee_id]   # required for mirror
```

Mirror upserts every source row (same as `full`), then issues a single end-of-sync DELETE that removes destination rows whose `upsert_key` was not observed in the source. The MySQL implementation builds the `NOT IN` list with explicit `%s` placeholders because pymysql does not auto-expand a `tuple-of-tuples` parameter the way psycopg2 does:

- **single-column** form: `DELETE FROM \`<table>\` WHERE \`<key>\` NOT IN (%s, %s, ...)` with a flat values list
- **composite** form: `DELETE FROM \`<table>\` WHERE (\`c1\`, \`c2\`) NOT IN ((%s, %s), (%s, %s), ...)` with values flattened row-major

The same `_quote_ident` helper that v0.7.4 ([PR #514](https://github.com/drt-hub/drt/pull/514)) hardened across all replace / insert / upsert / row-count paths is reused for the DELETE — `mydb.scores` correctly renders as `` `mydb`.`scores` `` regardless of which mode you use.

Mirror is appropriate when **rows disappearing from the source should disappear from the destination** — master tables (employees / SKUs / permissions) where deletions need to propagate without the TRUNCATE / re-insert cost of `replace` mode.

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
- **Composite keys supported** — `upsert_key: [tenant_id, employee_id]` produces `WHERE (tenant_id, employee_id) NOT IN (...)`.

Memory constraint: the in-process key set is memory-bound to source key cardinality. For tables with **more than a few million keys** the temp-table strategy ([#340 follow-up](https://github.com/drt-hub/drt/issues/340)) will be more appropriate. Mirror as shipped today is appropriate for small/medium reference tables.

**Tracked mirror (`mirror.strategy: tracked`, [#686](https://github.com/drt-hub/drt/issues/686)) — for tables the application also writes to:**

```yaml
sync:
  mode: mirror
  mirror:
    strategy: tracked   # default: "destination" (the behaviour described above)
```

Instead of diffing against the whole destination table (only correct when drt exclusively owns it), `tracked` persists the set of `upsert_key` tuples drt has itself synced in a drt-managed `_drt_synced_keys` table (created lazily in the target's database) and deletes only `previously-synced − current-source` keys — so rows the application wrote are never deletion candidates. First run baselines without deleting; lost state re-baselines with a WARN; target delete + state rewrite share one transaction. See the [Postgres tracked-mirror section](postgres.md) for the full semantics — the MySQL implementation is identical apart from placeholder building (explicit `%s` lists).

**Required destination privileges ([#695](https://github.com/drt-hub/drt/issues/695)):** tracked mirror needs `DELETE` on the target plus grants on the state table, beyond the `mode: full` set (`SELECT, INSERT, UPDATE`). This bit a real least-privilege MySQL rollout (`(1142, "CREATE command denied to user … for table '_drt_synced_keys'")`):

```sql
-- target table: DELETE is the tracked-mirror addition
GRANT SELECT, INSERT, UPDATE, DELETE ON `analytics`.`scores` TO 'retl_user'@'%';
-- state table: pre-provision it once as an admin to skip the CREATE grant
CREATE TABLE IF NOT EXISTS `analytics`.`_drt_synced_keys` (
  sync_name VARCHAR(255) NOT NULL,
  key_hash  CHAR(64)     NOT NULL,
  key_json  TEXT         NOT NULL,
  PRIMARY KEY (sync_name, key_hash)
);
GRANT SELECT, INSERT, DELETE ON `analytics`.`_drt_synced_keys` TO 'retl_user'@'%';
-- table-level grants can be issued before the table exists
```

Two MySQL-specific traps:

1. **The DELETE grant fails late, not on the first run.** The first run baselines and issues no deletes, so a missing `DELETE` privilege only detonates on the *first real generation change* — potentially weeks after rollout. Grant it up front.
2. **`CREATE TABLE IF NOT EXISTS` still needs `CREATE` — MySQL checks the privilege *before* the existence check**, so pre-creating `_drt_synced_keys` alone did not historically avoid the grant. Since v0.8.x drt first probes `information_schema.tables` and **skips the CREATE entirely when the table already exists**, so an admin can pre-provision the state table (SQL above) and run the sync user with **no DDL privilege** — the sanctioned pattern for "no CREATE for app users" hardening.

**Scoped mirror (`mirror.scope`, [#687](https://github.com/drt-hub/drt/issues/687)):** `scope: [parent_id]` restricts the mirror DELETE to rows whose scope values appeared in this run's source — the stateless fit for 1:N regeneration (delete stale children under regenerated parents, never touch rows under unobserved parents). See the [Postgres scoped-mirror section](postgres.md) for the full semantics.

Same `sync.mode: mirror` is supported on **Postgres** (Step 1 — psycopg2's tuple-of-tuples auto-expansion), **ClickHouse** (Step 3 — `ALTER TABLE ... DELETE WHERE` mutation with `mutations_sync=1`), and **Snowflake** (Step 4 — forces the MERGE write path regardless of `config.mode`). BigQuery follows once contributor PR [#584](https://github.com/drt-hub/drt/pull/584) lands. `mirror.strategy: tracked` is currently **Postgres + MySQL only**.

## Schema-qualified table identifiers

MySQL identifier quoting applies consistently across all paths thanks to the `_quote_ident` helper hardened in v0.7.4 ([PR #514](https://github.com/drt-hub/drt/pull/514)):

- `scores` → `` `scores` ``
- `mydb.scores` → `` `mydb`.`scores` ``

This means you can write `table: mydb.scores` without worrying about reserved words, mixed case, or schema-qualified addressing being mis-quoted on any of the replace / insert / upsert / row-count / swap / mirror paths.

## Notes

- Requires `pip install drt-core[mysql]` (uses `pymysql`)
- `upsert_key` columns must have a UNIQUE or PRIMARY KEY constraint on the target table
- `drt test` validators (row_count, not_null, freshness, unique, accepted_values) work with MySQL
- `--dry-run` shows row count diff for `mode: replace`
- The connection uses `charset: utf8mb4` and `autocommit: false` by default so multi-statement work runs in a single transaction
