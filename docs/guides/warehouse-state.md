# Warehouse-backed state, history, and DLQ (Postgres)

[Remote state on GCS or S3](remote-state.md) makes run state, execution history, and the dead
letter queue (DLQ) durable and shareable across a team — but the data stays opaque JSON/JSONL
objects, not something you can query. `state.backend: warehouse` is a second, separate tier:
it writes the same three things into tables in an existing warehouse connection instead, so
`drt`'s own operational data becomes SQL-queryable alongside the data the project already syncs.

This is [ADR 0005](../adr/0005-state-location-and-write-grants.md)'s step 4 — the
observability half of the state-location split, distinct from GCS/S3's durability half (step 2).
Postgres is the only supported connection today; see
[#1106](https://github.com/drt-hub/drt/issues/1106) (Snowflake),
[#1107](https://github.com/drt-hub/drt/issues/1107) (BigQuery), and
[#1108](https://github.com/drt-hub/drt/issues/1108) (Databricks) for the other dialects, each
blocked on live-verifiable credentials rather than deferred indefinitely.

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

## Configuration reference

| Field | Default | Required | Meaning |
|---|---|---|---|
| `backend` | `local` | no | Set to `warehouse` to enable this tier. |
| `connection_profile` | `null` | yes, for `warehouse` | Name of a `profiles.yml` entry. Rejected under every other backend. |

The managed schema name is **not** a `state:` field — it lives on the connection profile itself
(`PostgresProfile.managed_schema`, default `_drt`, see
[the Postgres connector guide](../connectors/postgres.md#as-a-source--drts-own-managed-bookkeeping-schema-960)),
not a second, independent `state.schema` knob. Two projects that intentionally share one
Postgres database should give each project's profile a distinct `managed_schema` — the same
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
Decision 4): switch `state.backend` back to `local`/`gcs`/`s3` at any time — the three tables are
simply no longer read or written, and `DROP TABLE`/`DROP SCHEMA` when convenient (or leave them,
harmlessly idle) is entirely operator-driven. No code path deletes them automatically.

## Concurrency and known limitations

Every write is a single `INSERT ... ON CONFLICT DO UPDATE` or a plain `DELETE` — atomic per
statement via Postgres's own row lock. Unlike the [GCS/S3 backends](remote-state.md), there is no
client-side read-modify-write cycle to retry, so this backend **never raises
`StateContentionError`** — the failure class that error exists to prevent (two writers silently
clobbering each other's read-modify-write) cannot happen when the database does the
read-modify-write atomically server-side. `DlqBackend.replace()` is fully transactional: the
delete and every replacement row commit together, so a failure partway through leaves the
original queue untouched rather than partially erased.

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
- **Not multi-dialect yet.** Postgres only; see the issues linked at the top of this guide.
- **Not warehouse-managed watermark storage.** `sync.watermark.storage: bigquery` is a separate,
  already-shipped mechanism predating this backend, for a different Protocol
  (`WatermarkStorage`, not `StateStore`).

## See also

- [ADR 0005 — Where drt's state lives, and what it costs the operator](../adr/0005-state-location-and-write-grants.md)
- [Remote state on GCS or S3](remote-state.md)
- [Postgres connector — managed bookkeeping schema](../connectors/postgres.md#as-a-source--drts-own-managed-bookkeeping-schema-960)
- [Sync execution history](sync-history.md)
- [Dead Letter Queue](dead-letter-queue.md)
