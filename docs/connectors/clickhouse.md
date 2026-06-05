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

## Identifier quoting

ClickHouse identifier quoting applies consistently across all SQL command paths thanks to the `_quote_ident` helper introduced in v0.7.7 ([PR #598](https://github.com/drt-hub/drt/pull/598)) for the mirror DELETE and extended in v0.7.8 ([PR #610](https://github.com/drt-hub/drt/pull/610)) to every remaining path:

- `scores` → `` `scores` ``
- `analytics.scores` → `` `analytics`.`scores` ``

This means `table: analytics.scores` renders correctly on `TRUNCATE TABLE`, `DROP TABLE`, `CREATE TABLE ... AS`, `EXCHANGE TABLES`, `client.insert(table, ...)` (clickhouse-connect interpolates the table argument raw into `INSERT INTO {table} ... FORMAT Native` so the destination pre-quotes it), `get_row_count`, and the mirror `ALTER TABLE ... DELETE`. Reserved words, mixed case, and database-qualified addressing all work without per-path quoting concerns.

The pre-v0.7.8 `get_row_count` path used `".`".join(...)` and rendered `` `db.`scores` `` (3 backticks) — a syntax error on the server that surfaced as `Code: 62` against ClickHouse 24.8 ([#512](https://github.com/drt-hub/drt/issues/512)). Upgrade to `drt-core>=0.7.8` if you use database-qualified table addressing on ClickHouse.

## Notes

- Requires `pip install drt-core[clickhouse]` (uses `clickhouse-connect`)
- Each record is inserted individually to enable row-level error tracking (consistent with PostgreSQL and MySQL destination patterns)
- For deduplication on the INSERT path, **create the destination table with `ReplacingMergeTree`** — `upsert_key` on the config is informational only for non-mirror modes
- `drt test` validators (row_count, not_null, freshness, unique, accepted_values) work with ClickHouse
- `--dry-run` shows row count diff for `mode: replace`
