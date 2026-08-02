# ClickHouse Destination

> Insert records into ClickHouse tables via the HTTP interface using `clickhouse-connect`.
> Deduplication is handled by ClickHouse's `ReplacingMergeTree` engine at merge time;
> the destination performs simple INSERTs.

## YAML Example

```yaml
destination:
  type: clickhouse
  host_env: TARGET_CH_HOST
  port: 8123
  database_env: TARGET_CH_DATABASE
  user_env: TARGET_CH_USER
  password_env: TARGET_CH_PASSWORD
  table: analytics_scores
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"clickhouse"` | — | Required |
| `connection_string_env` | string \| null | null | Env var with full DSN (takes precedence) |
| `host` / `host_env` | string | — | Hostname (direct or env var) |
| `port` | int | `8123` | HTTP interface port. **Use `8443` for HTTPS** and set `secure: true`. |
| `database` / `database_env` | string | — | Database name |
| `user` / `user_env` | string | `default` | Username |
| `password` / `password_env` | string | `""` | Password |
| `table` | string | — | Target table (e.g. `analytics_scores` or `db.analytics_scores`) |
| `upsert_key` | list[str] \| null | null | **Informational only** for the INSERT path — drt does not enforce or create `ReplacingMergeTree` tables. **Required** for `sync.mode: mirror` (used to identify which rows to DELETE). |
| `secure` | bool | `false` | Use HTTPS/TLS for the connection. Set the port explicitly for your deployment (typically `8443`). |

## Authentication

**Option 1: Individual fields (recommended)**
```yaml
host_env: TARGET_CH_HOST
database_env: TARGET_CH_DATABASE
user_env: TARGET_CH_USER
password_env: TARGET_CH_PASSWORD
```

**Option 2: Connection string (DSN)**
```yaml
connection_string_env: CLICKHOUSE_DSN
# e.g. clickhouse+http://user:pass@host:8123/db
```

**HTTPS:**
```yaml
secure: true
port: 8443
```

## Deduplication strategy

ClickHouse deduplicates with `ReplacingMergeTree` at **merge time** — the destination INSERTs rows as-is and the table engine collapses duplicates by the `ORDER BY` keys when it merges parts. Create the destination table with:

```sql
CREATE TABLE analytics_scores (
    user_id UInt64,
    score Float64,
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY user_id;
```

`upsert_key` on the destination config is **informational only** for the INSERT path — drt does not enforce or create `ReplacingMergeTree`. It is only consumed by `sync.mode: mirror` (see below).

## Common Patterns

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

drt creates a shadow table `{table}__drt_swap` via `CREATE TABLE shadow AS original` — which clones the engine, partitioning, `ORDER BY`, and column definitions — populates it across batches, then issues `EXCHANGE TABLES original AND shadow` from `finalize_sync`. `EXCHANGE TABLES` is **atomic since ClickHouse 21.8**, so readers of the original table never see an empty state. The (now-orphan) shadow that holds the previous generation's data is dropped in a separate transaction.

Caveats:
- `EXCHANGE TABLES` requires ClickHouse **≥ 21.8**.
- If a sync is killed before completion, an orphan shadow may remain. Drop manually with `DROP TABLE {table}__drt_swap`. Auto-cleanup is tracked in [#433](https://github.com/drt-hub/drt/issues/433).

Same `replace_strategy: swap` is supported on Postgres (transactional `ALTER TABLE ... RENAME`) and MySQL (atomic `RENAME TABLE` in a single statement).

**Mirror mode (differential delete, [#340](https://github.com/drt-hub/drt/issues/340) Step 3 — v0.7.7+):**

```yaml
sync:
  mode: mirror
destination:
  type: clickhouse
  table: analytics.scores
  upsert_key: [user_id]   # required for mirror (was informational only)
```

Mirror INSERTs every source row (same as `full`), then issues a single `ALTER TABLE ... DELETE WHERE <upsert_key> NOT IN (<observed>)` **mutation** from `finalize_sync` that removes destination rows whose key was not observed. The mutation runs with `mutations_sync=1` so the call blocks until the mutation finishes.

The ClickHouse implementation uses clickhouse_connect's native `{name:Type}` parameter substitution with `Array(String)` (single-column key) or `Array(Tuple(String, ...))` (composite key) — so unlike Postgres / MySQL where the placeholder list is assembled manually, the call site is a single parameter dict. Both column references and parameter values are coerced via `toString()` so the comparison works regardless of the source column type — at the cost of not using any index on the upsert_key column. Mirror mode is therefore intended for **small/medium reference tables**, not high-volume fact tables.

> Mutations in ClickHouse **rewrite affected parts** and are expensive. The temp-table strategy ([#340 follow-up](https://github.com/drt-hub/drt/issues/340)) is the planned shape for high-cardinality cases.

Comparison:

| Mode | New rows | Updated rows | Removed-from-source rows | Cost shape |
|---|---|---|---|---|
| `upsert` / `full` | INSERT (dedup at merge) | INSERT (dedup at merge) | stay in destination | INSERT per row |
| `replace` | INSERT | INSERT | DELETEd as side effect | TRUNCATE + INSERT all |
| **`mirror`** | INSERT (dedup at merge) | INSERT (dedup at merge) | **DELETEd via `ALTER TABLE ... DELETE` mutation** | INSERT per row + 1 mutation |

Safety guards:

- **Empty source short-circuit** — if no batch ever delivered records, the DELETE mutation is skipped. A transient empty source (auth failure mid-extract, vendor outage) cannot wipe the destination.
- **Failed rows excluded from the key set** — only successfully INSERTed keys count as "observed source state"; a row that failed during INSERT won't cause its destination counterpart to be deleted.
- **`upsert_key` required at load time** — `load()` raises `ValueError` before any INSERT if mirror mode is requested without a populated `upsert_key`. Fail-fast: the misconfiguration is surfaced before any row touches ClickHouse.
- **Composite keys supported** — `upsert_key: [tenant_id, user_id]` produces `(toString(\`tenant_id\`), toString(\`user_id\`)) NOT IN {keys:Array(Tuple(String, String))}`.

Memory constraint: the in-process key set is memory-bound to source key cardinality. Mirror as shipped today is appropriate for small/medium reference tables.

Same `sync.mode: mirror` is supported on **Postgres** (Step 1), **MySQL** (Step 2), and **Snowflake** (Step 4). BigQuery follows once contributor PR [#584](https://github.com/drt-hub/drt/pull/584) lands.

**Tracked mirror (`mirror.strategy: tracked`, [#686](https://github.com/drt-hub/drt/issues/686)/[#692](https://github.com/drt-hub/drt/issues/692)) — for tables the application also writes to:**

```yaml
sync:
  mode: mirror
  mirror:
    strategy: tracked   # default: "destination" (the NOT IN behaviour above)
```

Same Census-style semantics as the other three dialects (see the [Postgres tracked-mirror section](postgres.md) for the full write-up): drt persists the set of `upsert_key` tuples it has itself synced in a drt-managed `_drt_synced_keys` table, and each run deletes only `previously-synced − current-source` keys.

Two ClickHouse-specific notes:

- The state table is created with `ENGINE = MergeTree ORDER BY (sync_name, key_hash)` — ClickHouse requires an explicit engine, unlike Postgres/MySQL/Snowflake. Existence is checked via `EXISTS TABLE` (skip CREATE when a pre-provisioned table is already there, mirroring [#695](https://github.com/drt-hub/drt/issues/695)'s pattern on the other dialects), and reads/writes go through `client.query()`/`client.insert()`/mutation `ALTER TABLE ... DELETE`, matching how the rest of this connector already talks to ClickHouse.
- **No cross-statement transaction.** The other three dialects commit the target DELETE and the state rewrite together; ClickHouse mutations and inserts here are each their own statement. The target DELETE always runs *first*, so a failure partway through degrades safely — either a stale key gets deleted again next run (a harmless no-op) or the state table reads back empty and the existing "no prior state" baseline path takes over (WARN, re-baseline, no wrongful deletes) rather than ever deleting something it shouldn't.

**Scoped mirror (`mirror.scope`, [#687](https://github.com/drt-hub/drt/issues/687)/[#692](https://github.com/drt-hub/drt/issues/692)):** `scope: [parent_id]` restricts the mirror DELETE to rows whose scope values appeared in this run's source — the fit for 1:N regeneration. Composable with `strategy: tracked` ([#694](https://github.com/drt-hub/drt/issues/694)) provided `scope` is a subset of `upsert_key`. See the [Postgres scoped-mirror section](postgres.md) for the full semantics — identical on ClickHouse, built on the same `{name:Type}`-parameterized mutation shape already used for the plain mirror DELETE above.

## Identifier quoting

ClickHouse identifier quoting applies consistently across all SQL command paths thanks to the `_quote_ident` helper introduced in v0.7.7 ([PR #598](https://github.com/drt-hub/drt/pull/598)) for the mirror DELETE and extended in v0.7.8 ([PR #610](https://github.com/drt-hub/drt/pull/610)) to every remaining path:

- `scores` → `` `scores` ``
- `analytics.scores` → `` `analytics`.`scores` ``

This means `table: analytics.scores` renders correctly on `TRUNCATE TABLE`, `DROP TABLE`, `CREATE TABLE ... AS`, `EXCHANGE TABLES`, `client.insert(table, ...)` (clickhouse-connect interpolates the table argument raw into `INSERT INTO {table} ... FORMAT Native` so the destination pre-quotes it), `get_row_count`, and the mirror `ALTER TABLE ... DELETE`. Reserved words, mixed case, and database-qualified addressing all work without per-path quoting concerns.

The pre-v0.7.8 `get_row_count` path used `".`".join(...)` and rendered `` `db.`scores` `` (3 backticks) — a syntax error on the server that surfaced as `Code: 62` against ClickHouse 24.8 ([#512](https://github.com/drt-hub/drt/issues/512)). Upgrade to `drt-core>=0.7.8` if you use database-qualified table addressing on ClickHouse.

## As a source — retry on transient extract failures ([#766](https://github.com/drt-hub/drt/issues/766))

When ClickHouse is the **source** of a sync, opening the connection and running the model query
are retried automatically (3 attempts, exponential backoff from 1s, capped at 60s). No
configuration — it is always on, and separate from the `sync.retry` knobs that govern the load
side.

**Retried** (`clickhouse_connect.driver.exceptions`):

- `OperationalError` — "an unexpected disconnect occurs".
- `InterfaceError` — "errors related to the database interface rather than the database itself".
- Additionally, because ClickHouse is reached over an **HTTP interface**, raw `httpx` failures
  can surface instead of a driver class. Those need no special handling: drt's retry loop
  natively catches `httpx.TransportError` and the retryable status codes (429/500/502/503/504),
  and the driver-exception classification is purely additive to that.

**Not retried:** `ProgrammingError` (table not found, syntax error), `DataError`,
`IntegrityError`, `NotSupportedError`. clickhouse-connect follows PEP 249, so these are siblings
of `OperationalError` under `DatabaseError` — matching the specific classes keeps a SQL typo from
being retried. Note `StreamClosedError` subclasses `ProgrammingError`, so it is correctly treated
as permanent.

⚠️ **Scope: connection + query execution + fetching the result set only.** A failure *after the
first row has been yielded* is not retried and fails the sync — those rows are already loaded
into the destination and cannot be un-sent. See
[API_REFERENCE](../llm/API_REFERENCE.md#source-side-retry-766) for the full rationale.

## Notes

- Requires `pip install drt-core[clickhouse]` (uses `clickhouse-connect`)
- **Query tagging** ([#768](https://github.com/drt-hub/drt/issues/768)): `TRUNCATE`/DDL/mirror-`DELETE` statements get a leading `/* drt app=drt sync=<name> run_id=<id> ... */` comment by default. The row-write path itself (`client.insert()`) is a streaming API call, not SQL text, so it isn't tagged — see `query_tagging` in `docs/llm/API_REFERENCE.md`.
- Each record is inserted individually to enable row-level error tracking (consistent with PostgreSQL and MySQL destination patterns)
- For deduplication on the INSERT path, **create the destination table with `ReplacingMergeTree`** — `upsert_key` on the config is informational only for non-mirror modes
- `drt test` validators (row_count, not_null, freshness, unique, accepted_values, query) work with ClickHouse
- `--dry-run` shows row count diff for `mode: replace`
