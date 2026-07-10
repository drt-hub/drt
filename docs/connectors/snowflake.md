# Snowflake Destination

> INSERT (append) or MERGE (upsert) rows into Snowflake tables using `snowflake-connector-python`.

## YAML Example

```yaml
destination:
  type: snowflake
  account_env: SF_ACCOUNT      # e.g. "acct.us-east-1.aws"
  user_env: SF_USER
  password_env: SF_PASSWORD
  database: ANALYTICS
  schema: PUBLIC               # YAML key — model field is schema_
  table: USER_SCORES
  warehouse: COMPUTE_WH
  mode: merge                  # "insert" (default) | "merge"
  upsert_key: [id]             # required when mode: merge
```

## Configuration

| Field | Type | Default | Description |
|---|---|---|---|
| `type` | `"snowflake"` | — | Required |
| `account_env` | string | — | Env var holding the Snowflake account identifier (e.g. `acct.us-east-1.aws`). **Required** |
| `user_env` | string | — | Env var holding the username. **Required** |
| `password_env` | string \| null | null | Env var holding the password. One of `password_env` / `private_key_env` is required. |
| `private_key_env` | string \| null | null | Env var holding the **PEM (PKCS#8) private key contents** for key-pair auth (#737) — the path for `TYPE = SERVICE` users. Takes precedence over `password_env`. |
| `private_key_passphrase_env` | string \| null | null | Env var holding the private key passphrase, if the key is encrypted. |
| `database` | string | — | Database name. **Required** |
| `schema` | string | — | Schema name (YAML key; model field is `schema_` to avoid colliding with `BaseModel.schema()` under mypy strict). **Required** |
| `table` | string | — | Target table name. **Required** |
| `warehouse` | string | — | Warehouse to use for the connection. **Required** |
| `mode` | `"insert"` \| `"merge"` | `"insert"` | Write strategy on the destination side. `insert` = append; `merge` = upsert via staging-table-plus-MERGE (requires `upsert_key`). Orthogonal to `sync.mode`. |
| `upsert_key` | list[str] \| null | null | Columns to match on in the `MERGE INTO ... USING ... ON` clause. Required when `mode: merge`. |
| `lookups` | dict \| null | null | FK resolution against the destination (same shape as Postgres/MySQL/ClickHouse — see [Destination Lookup](../guides/destination-lookup.md)). Added in v0.7.9 (#468). |

> The YAML key is `schema:` for ergonomics, but the model field on `SnowflakeDestinationConfig` is `schema_` (alias) — `BaseModel.schema()` is a built-in pydantic method that would otherwise shadow a plain `schema` attribute under mypy strict mode.

## Authentication

**Key-pair (recommended):** new Snowflake accounts enforce MFA on password sign-ins, so programmatic access should use a `TYPE = SERVICE` user with an RSA key pair. Generate a key, register the public half on the user, and point `private_key_env` at an env var holding the **PEM private key contents**:

```bash
openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out sf_key.p8 -nocrypt
openssl rsa -in sf_key.p8 -pubout   # → RSA_PUBLIC_KEY value (strip the PEM headers)
export SF_PRIVATE_KEY="$(cat sf_key.p8)"
```

```sql
CREATE USER drt_writer TYPE = SERVICE DEFAULT_ROLE = ...;
ALTER USER drt_writer SET RSA_PUBLIC_KEY = 'MII...';
```

```yaml
destination:
  type: snowflake
  account_env: SF_ACCOUNT
  user_env: SF_USER
  private_key_env: SF_PRIVATE_KEY   # wins over password_env when both are set
  ...
```

**Password (legacy):** still supported via `password_env` for human-type users / older accounts, but Snowflake is deprecating password-only sign-ins — expect it to stop working for non-MFA users.

Both paths apply to the **source profile** too (`private_key_env` on the `snowflake` profile in `~/.drt/profiles.yml`). OAuth is not yet supported.

## Common Patterns

### Insert mode (append)

```yaml
destination:
  type: snowflake
  mode: insert     # default — can omit
  ...
sync:
  mode: full       # or "incremental"
```

Each row is INSERTed individually into `<database>.<schema>.<table>`. Use this when the destination table tolerates duplicates (e.g. append-only event tables) or when deduplication happens downstream.

### Merge mode (upsert)

```yaml
destination:
  type: snowflake
  mode: merge
  upsert_key: [id]
  ...
```

drt creates a session-scoped `TMP_<TABLE>` staging table (`CREATE TEMP TABLE TMP_<TABLE> LIKE <fully-qualified-table>`), INSERTs the batch's rows into the staging table, then issues a single `MERGE INTO <target> USING TMP_<TABLE> ON <upsert_key>` that updates matched rows and inserts unmatched ones. The staging table is dropped automatically at session end.

Requirements:
- `upsert_key` columns identify a logical primary key — drt's `ON` clause uses them verbatim.
- The destination user needs `CREATE TEMP TABLE`, `INSERT`, `UPDATE`, and `MERGE` privileges on the target schema.

### Mirror mode (differential delete, [#340](https://github.com/drt-hub/drt/issues/340) Step 4 — v0.7.7+)

```yaml
destination:
  type: snowflake
  # config.mode here can be left at the default ("insert") — sync.mode:
  # mirror forces the MERGE write path regardless. You only need to set
  # the upsert_key.
  upsert_key: [employee_id]
  ...
sync:
  mode: mirror
```

Mirror **forces the MERGE write path regardless of `config.mode`** — mirror semantics intrinsically require upsert, so users only need to set `destination.upsert_key` and `sync.mode: mirror`. Each batch is staged + MERGEd into the target (same as `mode: merge`); at end-of-sync `finalize_sync` issues a single `DELETE FROM <database>.<schema>.<table> WHERE key NOT IN (collected)` that removes destination rows whose `upsert_key` was not observed in the source.

`finalize_sync` also drives the `replace_strategy: swap` atomic SWAP ([#434](https://github.com/drt-hub/drt/issues/434), see [Replace mode](#replace-mode-434)); for `insert` / `merge` / `truncate`-replace it returns `None` and the engine's existing dispatch is unchanged.

The Snowflake connector uses `%s` placeholders (same family as psycopg2 / pymysql) and does **not** auto-expand a tuple-of-tuples, so the DELETE placeholder shape is built explicitly, identical to MySQL Step 2:

- **single-column** form: `DELETE FROM <table_fq> WHERE col NOT IN (%s, %s, ...)` with a flat values list
- **composite** form: `DELETE FROM <table_fq> WHERE (c1, c2) NOT IN ((%s, %s), (%s, %s), ...)` with values flattened row-major

Mirror is appropriate when **rows disappearing from the source should disappear from the destination** — master tables (employees / SKUs / permissions) where deletions need to propagate without the full-table rebuild cost of `replace` mode (see [Replace mode](#replace-mode-434) for that path).

Comparison:

| Mode | New rows | Updated rows | Removed-from-source rows | Cost shape |
|---|---|---|---|---|
| `upsert` / `full` + `config.mode: merge` | MERGE | MERGE | stay in destination | MERGE per batch |
| `upsert` / `full` + `config.mode: insert` | INSERT | INSERT (duplicate) | stay in destination | INSERT per row |
| **`mirror`** (forces MERGE) | MERGE | MERGE | **DELETEd by upsert_key NOT IN (...)** | MERGE per batch + 1 DELETE |

Safety guards:

- **Empty source short-circuit** — if no batch ever delivered records, the DELETE is skipped. A transient empty source (auth failure mid-extract, vendor outage) cannot wipe the destination.
- **Failed rows excluded from the key set** — only successfully staged keys count as "observed source state"; a row that failed during the staging INSERT won't cause its destination counterpart to be deleted.
- **`upsert_key` required at load time** — `load()` raises `ValueError` before any INSERT touches Snowflake when mirror mode is requested without a populated `upsert_key`. Fail-fast.
- **Composite keys supported** — `upsert_key: [tenant_id, user_id]` produces `WHERE (tenant_id, user_id) NOT IN (...)`.

Memory constraint: the in-process key set is memory-bound to source key cardinality. Mirror as shipped today is appropriate for small/medium reference tables.

Same `sync.mode: mirror` is supported on **Postgres** (Step 1), **MySQL** (Step 2), and **ClickHouse** (Step 3). BigQuery follows once contributor PR [#584](https://github.com/drt-hub/drt/pull/584) lands.

## Replace mode ([#434](https://github.com/drt-hub/drt/issues/434))

`sync.mode: replace` rebuilds the destination table from the current source snapshot. Two strategies:

**`replace_strategy: truncate`** (default) — `TRUNCATE TABLE` once at the start of the sync, then INSERT every batch.

```yaml
sync:
  mode: replace            # replace_strategy defaults to truncate
```

**`replace_strategy: swap`** — zero-downtime via Snowflake's atomic `ALTER TABLE … SWAP WITH`. drt builds a shadow table `<table>__drt_swap` with `CREATE OR REPLACE TABLE … LIKE <table>` (which carries clustering keys), writes every batch into the shadow, then at end-of-sync swaps the shadow over the original in a single atomic step and drops the old table. Readers see either the full old table or the full new table — never an empty or half-written one.

```yaml
sync:
  mode: replace
  replace_strategy: swap
```

- **Grants are preserved** — `SWAP WITH` exchanges the underlying objects, not the names, so **role privileges (grants)** on the original table name survive the swap. No grant re-application needed.
- **Clustering keys are carried** by `CREATE … LIKE` — but **masking / row-access policies and tags are not** (the shadow is built fresh via `LIKE`, which doesn't copy them). If your target table relies on column policies, re-apply them after the swap or front the table with a policy-bearing view.
- **First run** (target table doesn't exist yet) falls through to a direct write into the target and skips the swap.
- **Interrupted swaps** leave a `<table>__drt_swap` shadow; `drt clean --orphans` lists and drops them (only `__drt_swap`-suffixed tables are eligible).

Swap requires `mode: replace` (enforced by config validation). The same `replace_strategy: swap` is supported on Postgres, MySQL, and ClickHouse.

## Semi-structured columns (VARIANT / OBJECT / ARRAY)

`dict` and `list` values bound for a Snowflake `VARIANT` / `OBJECT` / `ARRAY` column can't be inserted as plain parameters — Snowflake needs them parsed with `PARSE_JSON`. By default (`introspect_schema: true`) drt reads `INFORMATION_SCHEMA.COLUMNS` for the target table **once per sync**, detects the semi-structured columns, and rewrites the INSERT to wrap them:

```sql
-- a VARIANT column "payload" is loaded as:
INSERT INTO db.schema.t (id, payload) SELECT %s, PARSE_JSON(%s)
```

so a `dict`/`list` lands as proper semi-structured data instead of a stringified `repr` — with **no configuration**. When no column needs wrapping, the INSERT is the unchanged `VALUES (...)` form. Introspection is best-effort: if `information_schema` isn't readable for the role, drt falls back to binding values directly. Disable with `introspect_schema: false`.

## Notes

- Requires `pip install drt-core[snowflake]` (uses `snowflake-connector-python`)
- Tables are addressed fully-qualified as `<database>.<schema>.<table>` (e.g. `ANALYTICS.PUBLIC.USER_SCORES`)
- The `schema:` YAML key maps to `schema_` on the model — see the model alias note above
- `upsert_key` columns identify a logical primary key for `mode: merge` and `sync.mode: mirror`
- **Queryable (v0.7.9, [#468](https://github.com/drt-hub/drt/issues/468)):** Snowflake is now wired into the query infrastructure used by Postgres/MySQL/ClickHouse, which unlocks three things — `drt test` validators (row_count, not_null, freshness, unique, accepted_values) run real queries against the target table; `drt run --dry-run --diff` produces a **true record-level diff** instead of falling back to sample mode; and `lookups` FK resolution works. Test/diff queries address the table fully-qualified (`<database>.<schema>.<table>`).
- `--dry-run` is honoured — `destination.load()` is never called when dry_run is on
