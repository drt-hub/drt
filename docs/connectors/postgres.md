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

**Match policy (`match_policy`, [#757](https://github.com/drt-hub/drt/issues/757)) — only-update / only-create:**

```yaml
sync:
  mode: upsert
  match_policy: update_only   # upsert (default) | update_only | create_only
destination:
  type: postgres
  table: public.contacts
  upsert_key: [id]
```

By default `mode: upsert` (and `full` / `incremental`) both *update* rows that already exist and *insert* rows that don't. `match_policy` narrows that to one side — the reverse-ETL activation cases where creating or overwriting is exactly wrong:

- **`update_only`** — only touch rows that already exist in the destination; rows with no match are **skipped, not inserted**. The enrichment case: push warehouse-computed scores into CRM contacts sales already created, without spraying a new junk contact for every warehouse row. Emitted as `UPDATE <table> SET <non-key cols> WHERE <upsert_key>`; requires at least one non-key column to update.
- **`create_only`** — only insert rows that don't yet exist; existing rows are **left untouched**. Seed an audience once and never overwrite fields a rep has since hand-edited. Emitted as `INSERT ... ON CONFLICT (<upsert_key>) DO NOTHING`.

Skipped rows are counted in the run's **`skipped`** total (`drt run` prints `… N skipped`), not as errors. `match_policy` composes with `field_mappings`, `mask`, and `lookups`. It is **rejected for `mode: replace`** (the TRUNCATE makes only-update / only-create meaningless) and **`mode: mirror`** (its delete pass is a separate design) at validate time, and a destination that doesn't implement it **fails fast** rather than silently upserting. Prior art: Census / Hightouch "Update Only" / "Create Only" sync behaviours.

> **Availability:** shipped on the **Postgres** destination first (clean `rowcount` semantics for the skip count). MySQL and the SaaS/CRM destinations (HubSpot, Salesforce, Intercom, …) — where `update_only` is most valuable — follow as per-destination PRs on the same engine seam ([#757](https://github.com/drt-hub/drt/issues/757)).

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

**Tracked mirror (`mirror.strategy: tracked`, [#686](https://github.com/drt-hub/drt/issues/686)) — for tables the application also writes to:**

```yaml
sync:
  mode: mirror
  mirror:
    strategy: tracked   # default: "destination" (the behaviour described above)
```

The default (`destination`) strategy diffs against the **whole destination table**, which is only correct when drt exclusively owns it. The canonical Reverse ETL destination, though, is a product's operational database where the application also inserts rows — and there, a destination diff would delete application-written rows. `strategy: tracked` closes that gap with Census-style semantics: drt persists the set of `upsert_key` tuples **it has itself synced** in a drt-managed `_drt_synced_keys` table (created lazily in the target table's schema), and each run deletes only `previously-synced − current-source` keys. Rows drt never wrote are never deletion candidates.

Tracked-specific behaviour:

- **First run baselines** — records the key set, deletes nothing (the second and subsequent runs account for deletions).
- **Lost / missing state re-baselines** — a WARN is logged and no deletes happen that run; the state is rebuilt from the current run.
- **The diff runs in SQL, not Python ([#694 part 2](https://github.com/drt-hub/drt/issues/694))** — this run's keys are staged into a scratch table and `previously-synced − current-source` is computed with a `NOT EXISTS` join against `_drt_synced_keys`, so a state table with millions of rows is never read into memory just to compute a diff that's typically small. Only the diffed rows (and the genuinely-new keys to insert) ever reach drt's process. **This assumes unscoped tracked mirror** — see the `mirror.scope` note below for why scoped runs don't get the same guarantee yet ([#890](https://github.com/drt-hub/drt/issues/890)).
- **Target delete + state rewrite are one transaction** — they commit or roll back together, so the bookkeeping can't drift from the data.
- **State survives ephemeral runners** — it lives in the destination next to the data (`sync_name`, `key_hash`, `key_json` — one row per synced key, scoped per sync), not in local `.drt/` state.
- **Key types** — int / str keys round-trip exactly; non-JSON-native key types (datetime, Decimal, UUID) are stringified in the state table, a documented limitation.
- The empty-source and failed-rows guards above apply to tracked as well — a transient empty source also leaves the tracked baseline untouched.

Choose `destination` when drt owns the table (slightly cheaper: no state I/O). Choose `tracked` when anything else writes to the table. Supported on every SQL destination that implements `sync.mode: mirror` at all — Postgres, MySQL (#686), Snowflake, ClickHouse, and Databricks ([#692](https://github.com/drt-hub/drt/issues/692), closing the family — mirror mode itself isn't yet available on BigQuery).

**Required destination privileges ([#695](https://github.com/drt-hub/drt/issues/695)):** tracked mirror needs two grants beyond the `mode: full` set (`SELECT, INSERT, UPDATE` on the target). A least-privilege user hardened for `full` writes will otherwise fail — and, because of trap #1 below, often not until weeks later:

```sql
-- target table: DELETE is the tracked-mirror addition
GRANT SELECT, INSERT, UPDATE, DELETE ON marketing.scores TO retl_user;
-- state table: pre-provision it once as an admin to skip the CREATE grant entirely
CREATE TABLE IF NOT EXISTS marketing._drt_synced_keys (
  sync_name VARCHAR(255) NOT NULL,
  key_hash  CHAR(64)     NOT NULL,
  key_json  TEXT         NOT NULL,
  PRIMARY KEY (sync_name, key_hash)
);
GRANT SELECT, INSERT, DELETE ON marketing._drt_synced_keys TO retl_user;
```

Two traps worth calling out:

1. **The DELETE grant fails late, not on the first run.** The first run baselines and issues no deletes, so a missing `DELETE` privilege stays invisible until the *first real generation change* — potentially weeks after rollout. Grant it up front.
2. **The state table is lazily created — but drt checks existence first.** drt runs `CREATE TABLE IF NOT EXISTS _drt_synced_keys` on the target schema when the table is absent, so by default the sync user needs `CREATE`. Since v0.8.x drt first probes `to_regclass('<schema>._drt_synced_keys')` and **skips the CREATE entirely when the table already exists** — so an admin can pre-provision `_drt_synced_keys` (SQL above) and run the sync user with **no DDL privilege at all**, the sanctioned pattern for "no CREATE for app users" environments.

**Scoped mirror (`mirror.scope`, [#687](https://github.com/drt-hub/drt/issues/687)) — for 1:N regeneration:**

```yaml
sync:
  mode: mirror
  mirror:
    scope: [parent_id]
```

The stateless fit for the parent + child-link shape: a parent entity is periodically regenerated together with its child rows, so stale children **under that parent** must go — but rows under parents *not present in this run* (other pipelines, the application) must not be touched. With `scope`, the mirror DELETE becomes `WHERE parent_id IN (observed parents) AND upsert_key NOT IN (observed keys)` — every run recomputes the diff within the observed scope, so there is no state to lose. A scope column missing from the model output fails fast before any write. Composite scopes (`scope: [tenant_id, parent_id]`) are supported. `scope` still assumes drt owns all rows *under the observed parents* — if co-writers touch the same parents, combine it with `strategy: tracked` (below). Supported everywhere `sync.mode: mirror` is: Postgres, MySQL, Snowflake, ClickHouse, and Databricks ([#692](https://github.com/drt-hub/drt/issues/692)).

**Tracked + scoped mirror ([#694](https://github.com/drt-hub/drt/issues/694)) — co-writer-safe 1:N regeneration:**

```yaml
sync:
  mode: mirror
  mirror:
    strategy: tracked
    scope: [parent_id]
```

Composing the two: `scope` must be a subset of `destination.upsert_key` (`upsert_key: [parent_id, id]` for the example above) — a run touching one parent leaves every other parent's tracked state untouched (not wiped by a blanket rewrite). Scope values are derived from the already-tracked key rather than stored in a separate state-table column — the reason for the subset requirement, and why no `_drt_synced_keys` migration is needed for tables created before #694. A scope column that isn't part of `upsert_key` fails fast, same as the missing-column check above.

**Scoped runs don't yet get #694 part 2's memory guarantee.** The SQL-side diff query (previous bullet) filters only by `sync_name`, not by scope — scope-filtering happens in Python *after* the SQL diff. The final set of rows deleted is still correct (scope membership and current-run membership are independent conditions, so filtering the diff by scope afterward is equivalent to filtering the full previous set by scope first), but when a run only touches one scope out of many historically-tracked ones, the SQL diff's result is close to the *entire* previous state for that sync — not the small diff the unscoped case gets. Tracked with [#890](https://github.com/drt-hub/drt/issues/890); fixing it needs dialect-specific JSON extraction from `key_json` (scope isn't a separate column) or reopening the schema-migration tradeoff above.

`scope` alone (without `tracked`) still assumes drt owns all rows under the observed parents.

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

## As a source — retry on transient extract failures ([#766](https://github.com/drt-hub/drt/issues/766))

When Postgres is the **source** of a sync, opening the connection and running the model query
are retried automatically (3 attempts, exponential backoff from 1s, capped at 60s). No
configuration — it is always on, and separate from the `sync.retry` knobs that govern the load
side.

**Retried** (psycopg2):

- `OperationalError` — connection refused, `server closed the connection unexpectedly`,
  `terminating connection due to administrator command` (failover, restart, idle-timeout reaper).
- `InterfaceError` — the driver's own connection object went bad.

**Not retried:** `ProgrammingError` (bad SQL, missing relation, denied privilege), `DataError`,
`IntegrityError`. psycopg2 makes all of these *siblings* of `OperationalError` under
`DatabaseError` (PEP 249's hierarchy), so drt matches the two specific classes rather than the
base — testing the base would happily retry a typo in your SQL three times.

**Authentication failures are never retried**, even though psycopg2 files them *under*
`OperationalError` (`InvalidPassword`, `InvalidAuthorizationSpecification`, SQLSTATE class `28`).
Three rapid attempts with a wrong credential can trip an account-lockout policy — turning a
config typo into an outage.

⚠️ **Scope: connection + query execution + fetching the result set only.** A failure *after the
first row has been yielded* is not retried and fails the sync: the engine has already handed
those rows to the destination, and loaded rows cannot be un-sent. Re-running would duplicate
them; skipping them would need a stable ordering and offset the query doesn't promise. That is a
checkpointing problem, not a retry problem. See
[API_REFERENCE](../llm/API_REFERENCE.md#source-side-retry-766).

## As a source — streaming extraction ([#765](https://github.com/drt-hub/drt/issues/765))

Rows are read through a **server-side (named) cursor** in batches rather than buffered whole with
`fetchall()`, so peak memory tracks the batch instead of the result set — an order of magnitude
lower on a 300k-row extract. Measured figures for every source are collected in
[Extraction memory](../research/extraction-memory.md).

```yaml
# ~/.drt/profiles.yml
pg:
  type: postgres
  host: localhost
  dbname: analytics
  user: analyst
  password_env: PG_PASSWORD
  fetch_size: 10000        # rows per server round trip (default: 10000)
```

`fetch_size` trades round trips against memory. Memory scales with `fetch_size x row width`, not
with the number of rows, so **lower it for very wide rows** (large `JSONB`, long text) rather than
for big tables — a 300M-row table costs the same as a 300k-row one at the same width. Raising it
buys progressively less.

There is deliberately no setting that restores the old buffer-everything behaviour: holding the
whole result set client-side is the problem this removes.

⚠️ **The connection is now held open for the whole load**, not just the extract — the cursor lives
on the server and dies with its session. Two consequences worth knowing: a sync is more exposed to
an idle-session reaper or a failover mid-load (that failure is *not* retried, per the scope note
above), and long-running syncs hold a Postgres backend for their duration. If your server enforces
a short `idle_in_transaction_session_timeout`, a slow destination can now trip it.

## Notes

- Requires `pip install drt-core[postgres]` (uses `psycopg2`)
- `upsert_key` columns must have a UNIQUE constraint on the target table
- `drt test` validators (row_count, not_null, freshness, unique, accepted_values, query) work with PostgreSQL
- `--dry-run` shows row count diff for `mode: replace`