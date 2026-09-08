# Warehouse-backed state, history, and DLQ (Postgres, Snowflake, Databricks)

[Remote state on GCS or S3](remote-state.md) makes run state, execution history, and the dead
letter queue (DLQ) durable and shareable across a team — but the data stays opaque JSON/JSONL
objects, not something you can query. `state.backend: warehouse` is a second, separate tier:
it writes the same three things into tables in an existing warehouse connection instead, so
`drt`'s own operational data becomes SQL-queryable alongside the data the project already syncs.

This is [ADR 0005](../adr/0005-state-location-and-write-grants.md)'s step 4 — the
observability half of the state-location split, distinct from GCS/S3's durability half (step 2).
Postgres ([#920](https://github.com/drt-hub/drt/issues/920)), Snowflake
([#1106](https://github.com/drt-hub/drt/issues/1106)), and Databricks
([#1108](https://github.com/drt-hub/drt/issues/1108)) connections are supported today; see
[#1107](https://github.com/drt-hub/drt/issues/1107) for the remaining dialect (BigQuery), blocked
on live-verifiable credentials rather than deferred indefinitely. `state.idempotency`/
`state.audit_trail.enabled` remain Postgres-only ([#1099](https://github.com/drt-hub/drt/issues/1099)/
[#1100](https://github.com/drt-hub/drt/issues/1100)) — enabling either on a Snowflake or
Databricks `connection_profile` fails loudly at startup rather than silently doing nothing.

## Quick start

```yaml
# drt_project.yml
state:
  backend: warehouse
  connection_profile: pg_analytics   # names an entry in profiles.yml
```

```yaml
# ~/.drt/profiles.yml
pg_analytics:
  type: postgres
  host: localhost
  dbname: analytics
  user: analyst
  password_env: PG_PASSWORD
  managed_schema: _drt   # default shown — see below
```

`connection_profile` names an **existing** profile — this backend does not take its own
connection fields (no `host`/`user`/`password` under `state:`) the way `gcs`/`s3` take their own
`bucket`. It reuses whatever connection that profile already resolves to, whether or not that
same profile is also used as a sync's source.

Install the matching extra before running drt — a base `drt-core` install does not pull in
`psycopg2`:

```bash
pip install 'drt-core[postgres]'
```

## Configuration reference

| Field | Default | Required | Meaning |
|---|---|---|---|
| `backend` | `local` | no | Set to `warehouse` to enable this tier. |
| `connection_profile` | `null` | yes, for `warehouse` | Name of a `profiles.yml` entry. Rejected under every other backend. |

The managed schema name is **not** a `state:` field — it lives on the connection profile itself
(`managed_schema`, default `_drt` on every dialect — `PostgresProfile.managed_schema`,
`SnowflakeProfile.managed_schema` inside `database`, `DatabricksProfile.managed_schema` inside
`catalog`; see [the Postgres connector guide](../connectors/postgres.md#as-a-source--drts-own-managed-bookkeeping-schema-960)),
not a second, independent `state.schema` knob. Two projects that intentionally share one
connection should give each project's profile a distinct `managed_schema` — the same
isolation convention `gcs`/`s3` already use via a distinct `prefix` (see
[Concurrency and known limitations](#concurrency-and-known-limitations) below).

## Tables

Three tables, created under `managed_schema`, one per persistence surface:

| Table | Shape | Backs |
|---|---|---|
| `_drt_runs` | One row per sync, upserted | `StateStore` — `drt status`, `--full-refresh`, incremental cursors |
| `_drt_history` | One row per run, append-only | `HistoryStore` — `drt status --history`, the docs manifest's `runs` data |
| `_drt_dlq` | One row per dead-letter entry | `DlqBackend` — `drt retry` |

These are drt-managed tables — do not write to them directly except for read-only observability
queries. Their exact columns are not a stable public API.

## Escape hatch and reversibility

Same discipline as the managed-table primitive underneath this backend
([#960](https://github.com/drt-hub/drt/issues/960)): an admin can pre-create the schema and all
three tables, grant the sync role no `CREATE` privilege at all, and every write still succeeds —
drt probes for existence before issuing any `CREATE` statement, live-verified with a role that
has `CREATE` fully revoked.

```sql
CREATE SCHEMA _drt;
CREATE TABLE _drt._drt_runs (
    sync_name TEXT PRIMARY KEY,
    last_run_at TEXT NOT NULL,
    records_synced BIGINT NOT NULL,
    status TEXT NOT NULL,
    error TEXT,
    last_cursor_value TEXT
);
CREATE TABLE _drt._drt_history (
    sync_name TEXT NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT NOT NULL,
    duration_seconds DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL,
    records_synced BIGINT NOT NULL,
    records_failed BIGINT NOT NULL,
    errors JSONB NOT NULL DEFAULT '[]',
    cursor_value_used TEXT,
    dry_run BOOLEAN NOT NULL DEFAULT FALSE,
    run_id TEXT,
    sync_run_id TEXT
);
CREATE TABLE _drt._drt_dlq (
    id TEXT PRIMARY KEY,
    sync_name TEXT NOT NULL,
    record JSONB NOT NULL,
    error_message TEXT NOT NULL,
    http_status INTEGER,
    ts TEXT NOT NULL,
    attempts INTEGER NOT NULL,
    sync_run_id TEXT
);
GRANT USAGE ON SCHEMA _drt TO retl_user;
GRANT SELECT, INSERT, UPDATE, DELETE ON _drt._drt_runs, _drt._drt_history, _drt._drt_dlq
  TO retl_user;
```

Note there is deliberately no `BIGSERIAL`/identity column anywhere in this schema: an implicit
sequence needs its own `GRANT USAGE` that the plain table grants above don't cover, which the
live escape-hatch test caught as a real failure (`permission denied for sequence`) before this
shape was settled on. `_drt_history` needs no id column at all; `_drt_dlq` orders by
`(ts, id)` instead of a sequence.

Reversible by design ([ADR 0005](../adr/0005-state-location-and-write-grants.md#decision)
Decision 4): switch `state.backend` back to `local`/`gcs`/`s3` — the three tables are simply no
longer read or written, and `DROP TABLE`/`DROP SCHEMA` when convenient (or leave them, harmlessly
idle) is entirely operator-driven. No code path deletes them automatically.

**One case needs care before switching.** `state.backend` and `sync.watermark.storage` are
independent scopes (see [Project state and sync watermarks are
independent](remote-state.md#project-state-and-sync-watermarks-are-independent)) — except for an
incremental sync that leaves `sync.watermark.storage` unset. That sync falls back to reading its
cursor from the project's own `StateStore` (`_drt_runs` here), so switching `state.backend` away
from `warehouse` mid-flight points that fallback at a *different*, empty store: the next run has
no stored cursor and will either replay from the beginning or, with `watermark.default_value`
set, resume from that default rather than where the warehouse-backed run actually left off.
Syncs with an explicit `sync.watermark.storage` are unaffected. Reseed the new store's cursor
(`drt run --cursor-value <value>` once) or copy `_drt_runs`' `last_cursor_value` for the affected
syncs before switching.

## Concurrency and known limitations

Every write is atomic per statement, though the mechanism differs by dialect. On Postgres, a
single `INSERT ... ON CONFLICT DO UPDATE` or a plain `DELETE` is atomic via Postgres's own row
lock, and `DlqBackend.replace()` wraps its delete-and-replace in one transaction. Snowflake has no
`ON CONFLICT`, so writes use `MERGE` instead, and `replace()` wraps its `DELETE`-then-`INSERT` in
an explicit transaction (`conn.autocommit(False)`) since this connector otherwise autocommits each
statement individually. Databricks/Delta Lake has **no multi-statement transactions at all** — a
stronger constraint than Snowflake's autocommit default, and one with no scratch-table workaround
either (an operator may pre-provision the three tables and grant only DML, the same escape hatch
Postgres/Snowflake preserve). `replace()` instead deletes ids absent from the new set by explicit
id and upserts the rest via a `MERGE` sourced from a `VALUES` table constructor, chunked to a
parameter budget — a single chunk is one atomic Delta commit, but a `replace()` spanning more than
one chunk is not atomic as a whole. What's still guaranteed: a crash mid-`replace()` never leaves
the queue empty or destroys entries outside the chunk that failed. In every case, unlike the
[GCS/S3 backends](remote-state.md), there is no client-side read-modify-write cycle to retry, so
this backend **never raises `StateContentionError`** — the failure class that error exists to
prevent (two writers silently clobbering each other's read-modify-write) cannot happen when the
database does the read-modify-write atomically server-side.

Two limitations are shared with every other state backend, not unique to `warehouse` — checked
against `gcs`/`s3`'s own behavior rather than solved here:

- **No enforced per-project namespace.** Rows key by `sync_name` alone. Two drt projects
  sharing one `connection_profile` + `managed_schema` with a common sync name will collide,
  exactly like two projects sharing one GCS/S3 bucket with no `prefix` set would. Give each
  project's profile its own `managed_schema` the same way you'd give each project its own
  `prefix`.
- **Concurrent runs of the same sync can move a cursor backward.** A slower run's write can
  commit after a faster, newer run's — atomic per statement, but still last-writer-wins across
  statements. `drt/engine/observer.py`'s single-process monotonicity check does not close this
  cross-process window for any backend; it needs a compare-and-set primitive none of the four
  backends' `StateStore` implementations currently have.

## Project state and sync watermarks are independent

Same rule as [remote state](remote-state.md#project-state-and-sync-watermarks-are-independent):
`state.backend` and `sync.watermark.storage` are different scopes. A project can use
`state.backend: warehouse` while an individual sync keeps `watermark.storage: local` (or
`gcs`/`bigquery`), and vice versa.

## What this isn't

- **Not a replacement for GCS/S3 durability.** If the only problem is "a CI runner's disk
  disappears," [remote state](remote-state.md) solves that without a warehouse write grant —
  see ADR 0005's Decision 2 for why that ordering matters.
- **Not every dialect yet.** Postgres, Snowflake, and Databricks; BigQuery remains open — see
  the issues linked at the top of this guide.
- **Not warehouse-managed watermark storage.** `sync.watermark.storage: bigquery` is a separate,
  already-shipped mechanism predating this backend, for a different Protocol
  (`WatermarkStorage`, not `StateStore`).

## See also

- [ADR 0005 — Where drt's state lives, and what it costs the operator](../adr/0005-state-location-and-write-grants.md)
- [Remote state on GCS or S3](remote-state.md)
- [Postgres connector — managed bookkeeping schema](../connectors/postgres.md#as-a-source--drts-own-managed-bookkeeping-schema-960)
- [Sync execution history](sync-history.md)
- [Dead Letter Queue](dead-letter-queue.md)
