# PostgreSQL → PostgreSQL (Mirror Mode)

Mirror an active-employee directory from an HR data warehouse into an
operational Postgres instance, using `sync.mode: mirror` so that
employees who leave the warehouse query result also disappear from
the destination table — without TRUNCATE/re-insert overhead.

## What it does

1. Reads `hr.dim_employees_curated WHERE employment_status = 'active'`
   from the HR warehouse
2. Upserts every returned row into `public.ops_employees` on the
   operational Postgres instance (by `employee_id`)
3. At end of sync, issues **one DELETE** that removes destination rows
   whose `employee_id` was not in the source result set — typically
   employees who were terminated or transferred since the last sync

The third step is what makes `mirror` different from `upsert` / `full`
(which leave terminated employees in the destination forever) and from
`replace` (which TRUNCATEs the whole table and re-INSERTs everything
on every run, even when 99% of rows are unchanged).

## Why `mirror` and not `replace`?

- **Cheaper writes**: only changed rows hit the destination (upsert);
  one final DELETE removes departed rows
- **No empty window**: the destination table stays populated throughout
  — `replace` (truncate strategy) briefly empties it
- **Safer on empty source**: if the warehouse query returns zero rows
  for any reason (auth failure mid-extract, vendor outage), the DELETE
  is skipped entirely — a transient empty source can't wipe the
  destination

`replace` is still the right choice when you want the destination
schema to track the source schema exactly (added/removed columns).
`mirror` only diffs rows, not columns.

## Setup

### 1. Configure your HR warehouse source

```bash
drt init   # select "postgres" as source, or use the profile below
```

Edit `~/.drt/profiles.yml`:

```yaml
hr_warehouse:
  type: postgres
  host: warehouse.internal
  port: 5432
  dbname: hr
  user: drt_reader
  password_env: HR_PG_PASSWORD
```

### 2. Set destination credentials

The destination Postgres connection is read at sync time from
environment variables (see `syncs/mirror_active_employees.yml`):

```bash
export OPS_PG_HOST=ops-db.internal
export OPS_PG_DBNAME=operations
export OPS_PG_USER=drt_writer
export OPS_PG_PASSWORD=...
export HR_PG_PASSWORD=...
```

### 3. Ensure the destination table exists with a unique constraint

`mirror` mode requires `upsert_key` to map to a unique constraint on
the destination table — the same requirement as plain upsert.

```sql
CREATE TABLE IF NOT EXISTS public.ops_employees (
    employee_id           TEXT PRIMARY KEY,
    full_name             TEXT NOT NULL,
    department            TEXT,
    manager_employee_id   TEXT,
    work_email            TEXT,
    updated_at            TIMESTAMPTZ
);
```

### 4. Run

```bash
drt run mirror_active_employees
```

## Watching it work

`drt run --log-format json` prints structured events. On a sync where
3 employees were added, 2 updated, and 1 terminated since the last
run, you should see roughly:

```
{"event": "sync_started", "sync_name": "mirror_active_employees", ...}
{"event": "batch_loaded", "success": 5, "failed": 0, ...}    # added + updated
{"event": "finalize_started", "stage": "mirror_delete", ...} # the DELETE
{"event": "sync_complete", "success": 5, "failed": 0, ...}
```

Only the upserts show up in the per-batch `success` count. The DELETE
issued by `finalize_sync()` is a single statement and is reported as
the finalize stage — it does not inflate the `success` / `failed`
counters (those reflect what the engine handed to `load()`, not what
the destination internally re-derived).

## How `mirror` differs from other modes

| Mode | New rows | Updated rows | Removed-from-source rows | Cost shape |
|---|---|---|---|---|
| `upsert` / `full` | upsert | upsert | stay in destination | upsert per row |
| `replace` | INSERT | INSERT | DELETEd as side effect | TRUNCATE + INSERT all |
| **`mirror`** | upsert | upsert | **DELETEd by upsert_key NOT IN (...)** | upsert per row + 1 DELETE |

## Safety guards built into mirror

- **Empty source short-circuit** — if no batch ever delivered records,
  the end-of-sync DELETE is skipped. Protects against catastrophic
  destination wipes when the source returns zero rows by accident
  (auth failure, vendor outage, etc.).
- **Failed rows excluded from the key set** — a row that failed during
  upsert is not tracked as "observed source state", so its destination
  counterpart will not be DELETEd. Combined with `on_error: fail` in
  this example, the sync stops at the first error rather than letting
  partial data drive the diff.
- **`upsert_key` required** — `load()` raises `ValueError` before any
  write if the destination has no `upsert_key`.

## Memory note

The set of observed upsert keys is held in process memory for the
duration of the sync. For tables with **more than a few million keys**
the temp-table strategy ([#340 follow-up](https://github.com/drt-hub/drt/issues/340))
will be more appropriate. For small/medium reference tables
(employees, products, regions, dimension tables) the application-side
diff shipped today is the right shape.

## Same pattern on other SQL destinations

`sync.mode: mirror` is supported on **Postgres**, **MySQL**,
**ClickHouse** (via an `ALTER TABLE ... DELETE` mutation with
`mutations_sync=1`), and **Snowflake** (mirror forces the MERGE write
path regardless of `destination.mode`). To adapt this example to one
of the others, only the destination block changes — the model and
`sync.mode: mirror` line stay the same.
