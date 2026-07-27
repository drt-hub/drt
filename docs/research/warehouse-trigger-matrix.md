# Warehouse trigger matrix — event-driven syncs

- **Issue:** [#786](https://github.com/drt-hub/drt/issues/786)
- **Companion to:** [ADR 0004 — Streaming / event-triggered syncs](../adr/0004-streaming-and-event-triggered-syncs.md)
- **Status:** Research. This document supplies the factual input ADR 0004 marked
  provisional; it does **not** make an architectural decision. The decision stays
  in the ADR.

## Purpose

For each source drt supports, this answers one question: **what is the cheapest
signal that says "data changed, a sync should run"?** ADR 0004 recommends *not*
building a native watcher, and lists a provisional matrix as raw material for
this one. Each row below was researched independently against official vendor
documentation and the shipped connector code, then compared to that provisional
reading — [What changed](#what-changed-against-the-adrs-provisional-matrix)
records the differences.

Every claim is cited: a vendor documentation URL, or a `file:line` reference into
this repository. Where multiple mechanisms exist, one is named **Preferred** and
the rest are listed as alternatives with their trade-offs.

### How to read the recommendation column

The tiers are ADR 0004's, not new:

- **Tier 1** — warehouse-native scheduling invokes the drt CLI. No drt-side
  runtime. Safe to document today.
- **Tier 2** — a Dagster sensor evaluates the cheap signal and yields a
  `RunRequest`. Requires sensors to be added to `dagster-drt` (the package ships
  `assets.py` / `resource.py` / `specs.py` / `translator.py` and exports seven
  symbols, none of them a sensor —
  `integrations/dagster-drt/dagster_drt/__init__.py:1-14`). *Gated on #756, #769.*
- **Tier 3** — a push source delivers to a hardened `drt serve`. *Gated on #769.*

"Practical?" answers whether **event-driven activation** is realistic for that
source in a CLI-first tool — not whether the signal itself works.

## Summary matrix

| Source | Preferred trigger | Push/Poll | Latency | Infra needed | Recommendation | Practical? |
|---|---|---|---|---|---|---|
| **PostgreSQL** | `max(updated_at)` poll | Poll | Poll interval | None (index only) | Tier 1 / Tier 2 | Poll: yes. Push: no |
| **MySQL** | `max(updated_at)` / `MAX(id)` poll | Poll | Poll interval | None (index only) | Tier 1 / Tier 2 | Poll: yes. Push: no |
| **Snowflake** | `STREAM` + `SYSTEM$STREAM_HAS_DATA()` in a `TASK` `WHEN` clause | Poll | 10 s floor; 1–5 min realistic | Stream + task; **warehouse required to check** | Tier 1 / Tier 2 | Yes |
| **BigQuery** | `APPENDS()` TVF poll, cursored on `_CHANGE_TIMESTAMP` | Poll | Poll interval | None beyond credentials | Tier 1 / Tier 2 | Yes (Tier 3 also viable) |
| **Databricks** | Workflows **table update** trigger | Push (managed) | ~1–2 min | Unity Catalog + a Job | Tier 1 | Yes |
| **Delta Lake** | `DeltaTable(...).version()` | Poll | Poll interval; sub-second check | `deltalake` lib + storage creds | Tier 1 / Tier 2 | Yes — cheapest in matrix |
| **Apache Iceberg** | `current-snapshot-id` from `metadata.json` | Poll | Poll interval; sub-second check | pyiceberg + catalog creds | Tier 1 / Tier 2 | Yes |
| **ClickHouse** | `system.parts` metadata probe | Poll | Poll interval | None | Tier 1 / Tier 2 | Yes |
| **DuckDB** | File `mtime` | Poll | Checkpoint-delayed | None (local `stat`) | Not a target | No — not the use case |
| **SQLite** | `PRAGMA data_version` | Poll | Sub-second | Held-open connection | Not a target | No — not the use case |
| **Redshift** | `max(updated_at)` poll | Poll | Poll interval | None | Tier 1 / Tier 2 | Poll only; no push exists |
| **SQL Server** | Change Tracking (`CHANGETABLE`) | Poll | Poll interval | CT enabled per DB + table | Tier 1 / Tier 2 | Yes — purpose-built |
| **REST API** | **None — polling *is* the extract** | Poll | Schedule / rate limits | None | Tier 1 (or Tier 3 if the API has webhooks) | No distinct trigger exists |

Latency figures are the *mechanism's* floor, not an SLA. "Poll interval" means the
mechanism imposes no floor of its own — freshness is whatever cadence you schedule,
and the constraint is cost, not capability.

## Per-source detail

### PostgreSQL

**Preferred: poll `max(updated_at)`** through the existing psycopg2 connection.
`PostgresSource.extract()` runs a caller-supplied query and streams rows with no
cursor or incremental logic of its own (`drt/sources/postgres.py:27-37,50-65`), so
a cursor poll needs no new connector code — only an indexed column.

Alternatives, both push, both rejected for drt-owned use:

- **`LISTEN`/`NOTIFY`** — sub-second to an *already-listening* client, but
  notifications are **not durable**: a disconnected listener silently misses
  events with no replay, so a reconciling poll is still required behind it. Needs
  a user-installed trigger (schema write access) plus a held connection. Payload
  caps at 8000 bytes; the queue caps at 8 GB.
- **Logical decoding** — lowest latency, but full CDC machinery: `wal_level=logical`
  (restart), replication slots, a `REPLICATION` role, a `pg_hba.conf` entry, and a
  persistent consumer. An unconsumed slot retains WAL indefinitely, risking disk
  exhaustion or a forced shutdown to avoid XID wraparound. ADR 0004 already places
  CDC under "Still out".

`xmin` is a fallback when no timestamp column exists, but it is a 32-bit XID with
wraparound risk, unindexed, and not safely ordered against concurrent commits —
a last resort, not an authoritative cursor.

**Limitation:** an `updated_at` poll is blind to `DELETE`s, and to bulk loads that
bypass whatever maintains the column.

*Sources: [NOTIFY](https://www.postgresql.org/docs/current/sql-notify.html) ·
[logical decoding](https://www.postgresql.org/docs/current/logicaldecoding-explanation.html) ·
[system columns](https://www.postgresql.org/docs/current/ddl-system-columns.html) ·
[replication config](https://www.postgresql.org/docs/current/runtime-config-replication.html)*

### MySQL

**Preferred: poll `max(updated_at)`** (with `ON UPDATE CURRENT_TIMESTAMP`), or
`MAX(id)` for append-only tables. `MySQLSource.extract()` is a plain pymysql
query-and-yield with no cursor, watermark, or binlog awareness
(`drt/sources/mysql.py:26-36`).

MySQL has **no `LISTEN`/`NOTIFY` equivalent**. Its only push signal is the binary
log, read over the replication protocol by a client registering as a replica
(`binlog_format=ROW`, `REPLICATION SLAVE` + `REPLICATION CLIENT` grants, a
persistent connection holding a GTID/position bookmark). Sub-second after commit,
but it is exactly the daemon-per-source shape ADR 0004 rejects — justified only
when a bus like Debezium/Kafka already exists downstream.

A trigger-populated audit table makes each poll cheaper (smaller scan target) at
the cost of DDL the user installs and maintains — the same trade-off as Postgres's
`NOTIFY` trigger.

**Limitations:** `ON UPDATE CURRENT_TIMESTAMP` does not fire on `DELETE`. There is
no cheap "has anything changed" probe between "full poll" and "full replication
connection" — nothing analogous to `SYSTEM$STREAM_HAS_DATA`.

*Sources: [binary log](https://dev.mysql.com/doc/refman/8.0/en/binary-log.html) ·
[replication formats](https://dev.mysql.com/doc/refman/8.0/en/replication-formats.html) ·
[replication privileges](https://dev.mysql.com/doc/refman/8.0/en/replication-privilege-checks.html) ·
[TIMESTAMP auto-init](https://dev.mysql.com/doc/refman/8.0/en/timestamp-initialization.html)*

### Snowflake

**Preferred: a `STREAM` on the source table, checked by `SYSTEM$STREAM_HAS_DATA()`
in a `TASK`'s `WHEN` clause.** A stream is a metadata-only offset marker (no data
copy), and the `WHEN` clause exists precisely so a task can skip its body — and
avoid spinning its warehouse — when nothing changed. `SnowflakeSource` itself has
no cursor logic (`drt/sources/snowflake.py:28-89`).

Tasks schedule down to `'10 SECONDS'`, but 1–5 minutes is realistic once
scheduling overhead is counted — squarely ADR 0004's Tier 1 band, not streaming.

**Cost is the caveat, and "cheap to poll" is relative, not free:** querying a
stream — and even `DESCRIBE STREAM` / `SHOW STREAMS` to inspect its offset —
**requires an active virtual warehouse**, billed per second. Worse, once
`SYSTEM$STREAM_HAS_DATA` returns TRUE the stream **must** be consumed via DML, or
it keeps returning TRUE, re-firing the task and re-spinning the warehouse
indefinitely.

Alternatives: **Snowflake Alerts + a `WEBHOOK` notification integration** can push
to a hardened `drt serve` (Tier 3), but the condition is still evaluated by
Snowflake-side scheduled compute, so it creates no genuinely new push capability —
it wraps a poll. **Snowpipe Streaming** is the wrong layer entirely: it is an
*ingestion* API for writing *into* Snowflake, not a change signal for extraction.

**Limitations:** `SYSTEM$STREAM_HAS_DATA` can return **false positives** (rows
inserted, updated, then deleted back to their original state); view-backed streams
have documented higher false-positive rates; an unconsumed stream goes stale past
the source table's retention window (Snowflake extends this up to 14 days for
inactive streams as a grace period, not indefinitely).

*Sources: [SYSTEM$STREAM_HAS_DATA](https://docs.snowflake.com/en/sql-reference/functions/system_stream_has_data) ·
[streams](https://docs.snowflake.com/en/user-guide/streams-intro) ·
[managing streams](https://docs.snowflake.com/en/user-guide/streams-manage) ·
[tasks](https://docs.snowflake.com/en/user-guide/tasks-intro) ·
[alerts](https://docs.snowflake.com/en/user-guide/alerts) ·
[Snowpipe Streaming](https://docs.snowflake.com/en/user-guide/snowpipe-streaming/data-load-snowpipe-streaming-overview)*

### BigQuery

**Preferred: poll the `APPENDS()` table-valued function, cursored on
`_CHANGE_TIMESTAMP`.** It requires no enablement, returns the original columns
plus `_CHANGE_TIMESTAMP` / `_CHANGE_TYPE`, and — being plain SQL — slots into
`BigQuerySource.extract()`'s existing arbitrary-query path with zero new connector
code (`drt/sources/bigquery.py:22-28`, which has no cursor mechanism today).

Alternatives:

- **`tables.get().lastModifiedTime`** — a *free* metadata call, cheaper still, but
  coarse: it fires on schema-only changes and carries no row cursor. (Compare
  `lastModifiedTime` directly; the `etag` is not guaranteed to change.)
- **Audit logs → Pub/Sub → hardened `drt serve` (Tier 3)** — genuine push,
  seconds-scale. BigQuery Data Access audit logs (`TableDataChange`) are on by
  default and incur no logging charge; a Log Router sink publishes to a Pub/Sub
  topic (the sink's writer identity needs `roles/pubsub.publisher`). A Pub/Sub
  **push** subscription then POSTs to `drt serve` — so *no persistent subscriber
  process* is needed on either side.
- **`CHANGES()` TVF** — adds UPDATE/DELETE/TRUNCATE, but requires
  `enable_change_history=TRUE` set in advance and costs more on delete-heavy
  tables.

**Limitations:** `APPENDS`/`CHANGES` are bounded by the table's time-travel window
and unsupported on views, clones, snapshots, external and wildcard tables. More
importantly for a push-only design: BigQuery does **not** emit Data Access events
for *failed* jobs, so a change tied to a failed-then-retried job can be missed
silently — which is why polling remains the safer primary signal even here.

*Sources: [change history](https://docs.cloud.google.com/bigquery/docs/change-history) ·
[BigQuery audit logs](https://docs.cloud.google.com/bigquery/docs/reference/auditlogs) ·
[log routing](https://docs.cloud.google.com/logging/docs/export/bigquery) ·
[Eventarc + BigQuery](https://docs.cloud.google.com/eventarc/standard/docs/run/bigquery)*

### Databricks

**Preferred: a Workflows "table update" trigger** on the source table, whose job
invokes the drt CLI (or `drt serve`) as a task. This is the one genuine
**platform-managed push** in the matrix: Databricks' control plane watches table
metadata and starts the job itself, so drt owns no runtime.
`DatabricksSource` is a stateless SQL-warehouse extractor with no cursor today
(`drt/sources/databricks.py:26-39,58-83`).

Note what "push" means precisely here: Databricks evaluates changes on a
best-effort **~1-minute internal loop**, so this is *managed* polling that pushes
to the job trigger — not a broker-free event. Configurable options include
minimum time between triggers and a debounce after the last change.

Alternatives: a Dagster sensor polling `DESCRIBE HISTORY` through the connector
drt already has (Tier 2, more portable, costs warehouse time per poll); a
scheduled Databricks SQL Alert posting to `drt serve` (Tier 3).

**Cost:** the native trigger adds no Databricks charge beyond cloud-provider costs
for listing tables and reading updates. Alert- or `DESCRIBE HISTORY`-based polling
spins a SQL warehouse per poll unless one stays warm — real money at tight
intervals.

**Limitations:** Unity Catalog only; max 10 managed/Delta tables per trigger; 1,000
concurrent job triggers per workspace without file events; view monitoring is
restricted; system-table monitoring is Beta. The ~1–2 min figure is best-effort
and community-reported, **not a documented SLA**.

*Sources: [table update triggers](https://docs.databricks.com/aws/en/jobs/trigger-table-update) ·
[announcement](https://www.databricks.com/blog/announcing-table-update-triggers-lakeflow-jobs) ·
[SQL alerts](https://docs.databricks.com/aws/en/sql/user/alerts/) ·
[change data feed](https://docs.databricks.com/aws/en/tables/features/change-data-feed)*

### Delta Lake

**Preferred: poll `DeltaTable(location, storage_options=...).version()`** — the
monotonic transaction-log version — and compare against the last-seen value.
**This is the cheapest signal in the matrix.** Constructing `DeltaTable` reads
`_delta_log/_last_checkpoint` and replays only the commits after that checkpoint,
not the whole log: a handful of small object-storage calls, metadata-only, no data
scan.

drt already calls exactly this — `DeltaTable(config.location,
storage_options=options).version()` at `drt/sources/deltalake.py:67` — but only
inside `test_connection()`, to prove connectivity. It is **not** wired to
incrementality: `extract()` loads the entire table via `.to_pyarrow_table()` every
run and filters in DuckDB afterwards (`drt/sources/deltalake.py:40-59`). The
mechanism is already present and already proven to work; only the wiring is
missing.

Alternatives: **cloud storage event notifications** (S3 Event Notifications, Azure
Event Grid, GCS Pub/Sub) on `_delta_log/*.json` writes — push-adjacent, but noisy:
one commit can write several objects and events fire per object, so a consumer must
dedupe and re-read the log to know whether a commit is complete. **Change Data
Feed** only if row-level detail is needed; for pure "did it change" it is strictly
more expensive (requires `delta.enableChangeDataFeed=true` in advance, writes extra
`_change_data` files).

**Limitations:** version-only tells you *that* something changed, not *what*.
Optimistic-concurrency retries and compaction can bump the version without new
logical data. CDF cannot be retrofitted onto history.

*Sources: [delta-rs API](https://delta-io.github.io/delta-rs/python/api_reference.html) ·
[change data feed](https://docs.delta.io/delta-change-data-feed/) ·
[Delta protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md)*

### Apache Iceberg

**Preferred: poll `current-snapshot-id`** from the table's `metadata.json`, via
`catalog.load_table(...)` (or `table.refresh()` then
`table.metadata.current_snapshot_id`). This reads one small metadata file — no
manifest lists, no manifests, no data files.

drt loads a pyiceberg table at `drt/sources/iceberg.py:51-52`, confirming the
signal is reachable from code that already runs. Worth noting what that line
actually does today: `catalog.load_table(config.table).scan().to_arrow()` — an
**unbounded full-table scan** with no row filter and no snapshot bound, fully
materialized before DuckDB filters it. So drt not only doesn't read the snapshot
id, it currently reads everything.

The **Iceberg REST Catalog OpenAPI spec has no notification, webhook, event, or
subscription endpoints** — `loadTable` is the only change-detection primitive, and
it is purely request/response. Iceberg has no format-level push.

The one alternative is catalog-specific: **AWS Glue Data Catalog** emits *Table
State Change* events to EventBridge for Iceberg tables registered in Glue. Real
push, but Glue-only — not portable to REST, Hive, or SQL catalogs.

**Limitation:** snapshot expiration and compaction can retire a snapshot before a
slow poller observes it. The poll interval must stay well inside the table's
retention window; a missed snapshot is not recoverable as an audit trail, only as
a current-vs-last-id difference.

*Sources: [pyiceberg API](https://py.iceberg.apache.org/api/) ·
[REST catalog spec](https://iceberg.apache.org/rest-catalog-spec/) ·
[table spec](https://iceberg.apache.org/spec/) ·
[Glue → EventBridge](https://docs.aws.amazon.com/eventbridge/latest/ref/events-ref-glue.html)*

### ClickHouse

**Preferred: probe `system.parts`** —
`SELECT max(modification_time), count() FROM system.parts WHERE database = ? AND
table = ? AND active = 1`. This reads the in-memory metadata catalog rather than
scanning column data, making it cheaper than `max(updated_at)` unless that column
happens to be the sort key. `ClickHouseSource.extract()` has no cursor logic
(`drt/sources/clickhouse.py:26-37`).

**Do not use `system.tables.metadata_modification_time`** — per ClickHouse
maintainer guidance it reflects the `.sql` file's modification time (typically the
last `ALTER TABLE`) and **does not fire on `INSERT`**, making it useless for
data-change detection. This is the kind of plausible-looking wrong answer a matrix
is for.

ClickHouse has **no push mechanism**: no `LISTEN`/`NOTIFY`, no CDC feed for
external consumers. Materialized Views chain `INSERT`→`INSERT` internally but never
push out; Refreshable Materialized Views are themselves scheduled polls that
re-run the full query, adding latency rather than removing it.

**Cost:** negligible self-hosted (in-memory catalog). On ClickHouse Cloud, queries
are usage-billed, so the metadata probe's much smaller byte count matters at high
poll frequency.

**Limitations:** `modification_time` also bumps on background merges and mutations
that add no rows, and on ReplicatedMergeTree replicas fetching parts from peers —
both false positives. Mitigate by tracking `sum(rows)` / part count alongside it.

*Sources: [system.parts](https://clickhouse.com/docs/operations/system-tables/parts) ·
[system.tables](https://clickhouse.com/docs/operations/system-tables/tables) ·
[maintainer note on `metadata_modification_time`](https://github.com/ClickHouse/ClickHouse/discussions/63958) ·
[refreshable MVs](https://clickhouse.com/docs/materialized-view/refreshable-materialized-view)*

### DuckDB

**Preferred: file `mtime`** on the `.duckdb` file — the only option. Both drt's
connector (`drt/sources/duckdb.py:32`) and the profile
(`drt/config/profiles.py:30-32`, a local path or `:memory:`) treat this as a local
file.

DuckDB is **single-writer by file lock** — a second read-write connection fails
outright — and exposes **no `PRAGMA data_version` equivalent** and no
update-hook/callback API. There is no CDC on the native format.

**Event-driven is not the use case.** A single-writer local file is a
dev/test/small-scale pattern; sub-minute activation against it is not a scenario
worth optimizing.

**Limitation:** `mtime` is checkpoint-delayed — writes land in a WAL and reach the
main file's blocks only at checkpoint (default around 16 MB of WAL, or clean
shutdown), so `mtime` can lag real writes substantially.

MotherDuck is a genuinely hosted, infrastructure-backed product, but drt has no
MotherDuck profile — `duckdb.connect("md:...")` only works as an accident of
passthrough. MotherDuck's own documented freshness pattern is itself a scheduled
`max(timestamp)` poll, not a push, so it does not change the verdict.

*Sources: [concurrency](https://duckdb.org/docs/current/connect/concurrency) ·
[pragmas](https://duckdb.org/docs/current/configuration/pragmas) ·
[concurrent transactions](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions) ·
[CDC discussion](https://github.com/duckdb/duckdb/discussions/12408)*

### SQLite

**Preferred: `PRAGMA data_version`** on a held-open connection — an integer that
changes when *any other* connection commits, including from other processes. It is
cheaper and strictly more reliable than file `mtime`.
`SQLiteSource` connects per extract via stdlib `sqlite3` (`drt/sources/sqlite.py:30`).

The subtlety that makes this the right answer: **in WAL mode, writes append to a
separate `-wal` file** and only transfer to the main database at checkpoint
(default ~1000 pages). The main file's `mtime` can stay flat while `-wal` grows —
so an `mtime` poll misses changes that `data_version` catches.

`sqlite3_update_hook()` exists in the C API and fires per row, but it is
same-process instrumentation, not a cross-process trigger: it requires an open
connection *at write time*, fires only inside that connection's process, and is
not exposed usefully by Python's stdlib `sqlite3` for an external poller.

**Event-driven is not the use case**, same as DuckDB — even though the signal here
is better.

**Limitation:** `data_version` is only meaningful compared against prior reads from
the *same* long-lived connection; a fresh connection has no baseline.

*Sources: [PRAGMA data_version](https://sqlite.org/pragma.html#pragma_data_version) ·
[update hook](https://sqlite.org/c3ref/update_hook.html) ·
[WAL mode](https://sqlite.org/wal.html)*

### Amazon Redshift

**Preferred: poll `max(updated_at)`** (or a count probe). `RedshiftSource` is a
thin psycopg2 wrapper — Redshift is Postgres-wire-compatible — running the
caller's SQL with no cursor logic (`drt/sources/redshift.py:38-52`).

**No table-level push exists**, and this is worth stating explicitly because
Redshift's EventBridge integration looks like it might provide one and does not:

- **Serverless** events (`aws.redshift-serverless`) are namespace/workgroup/rate/
  config/security/lifecycle only — no table signal — and delivery is explicitly
  *best effort*.
- **Zero-ETL** events cover the health of an Aurora/RDS→Redshift pipeline, not
  arbitrary table writes.
- **Data API** events fire on statement completion — a job signal, and only if the
  write used the Data API.

The system-table route (`STL_INSERT` / `STL_QUERY`) can answer "when was this
table last written", but `SVV_TABLE_INFO` has no last-modified column (only
`create_time`), `SVV_TABLE_INFO` needs a superuser grant, and AWS's own guidance
notes STL tables retain just 2–5 days and recommends unloading them to S3 for
longer history. More moving parts than a `max(updated_at)` probe, for no gain here.

**Cost:** provisioned clusters bill per node-hour regardless of query volume, so
polling is effectively free at the margin. **Redshift Serverless bills per
RPU-second with a 60-second minimum charge per query** — frequent polling has real
incremental cost there.

*Sources: [integration events](https://docs.aws.amazon.com/redshift/latest/mgmt/integration-event-notifications.html) ·
[Serverless EventBridge events](https://docs.aws.amazon.com/eventbridge/latest/ref/events-ref-redshift-serverless.html) ·
[Data API events](https://docs.aws.amazon.com/redshift/latest/mgmt/data-api-monitoring-events.html) ·
[SVV_TABLE_INFO](https://docs.aws.amazon.com/redshift/latest/dg/r_SVV_TABLE_INFO.html) ·
[pricing](https://aws.amazon.com/redshift/pricing/)*

### Microsoft SQL Server

**Preferred: Change Tracking (CT)** — store `CHANGE_TRACKING_CURRENT_VERSION()` as
the cursor, then read `CHANGETABLE(CHANGES dbo.MyTable, @last_version)` for the
primary keys changed since. `SQLServerSource` is stateless pymssql with no cursor
logic (`drt/sources/sqlserver.py:1-72`).

**SQL Server earns its own row rather than the generic-poll bucket.** CT is a
purpose-built change signal, closer in kind to Snowflake's `STREAM` than to a
`max(updated_at)` scan:

- It **catches deletes**, which an `updated_at` poll structurally cannot.
- It needs **no app-owned timestamp column**.
- It is ordered by **committed transaction**, not a mutable client timestamp.
- It is available on **every edition, including Express** — unlike CDC
  (Standard/Enterprise/Developer only).
- It is far lighter than CDC: a synchronous side-table write storing current PK +
  version, with no transaction-log reader and no SQL Server Agent job.

SQL Server does have a real push primitive — **Query Notifications /
`SqlDependency`** over Service Broker — but it is architecturally the same shape as
Postgres `LISTEN`/`NOTIFY`: the notification lands in a Service Broker queue that a
consumer must actively drain (`WAITFOR (RECEIVE ...)`), requiring a long-lived
connection plus non-trivial setup (`ALTER DATABASE ... SET ENABLE_BROKER` is off by
default, plus queue/service/contract objects). Not receivable by a short-lived CLI
invocation.

Alternatives: **CDC** only when full before/after row history is genuinely needed
and Agent is already running; **`max(updated_at)`** for zero setup, accepting
delete-blindness.

**Limitations:** CT's retention window is finite — if the sync interval exceeds
`CHANGE_RETENTION` the cursor becomes invalid and a full resync is required, so
`CHANGE_TRACKING_MIN_VALID_VERSION` must be checked. CT reports *which* PKs changed
(optionally which columns), not values — drt still reads current rows via a join.
Requires compatibility level ≥ 90.

*Sources: [about Change Tracking](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-tracking-sql-server) ·
[enable/disable CT](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-tracking-sql-server) ·
[CHANGETABLE](https://learn.microsoft.com/en-us/sql/relational-databases/system-functions/changetable-transact-sql) ·
[track data changes (CT vs CDC)](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/track-data-changes-sql-server) ·
[Query Notifications](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql/query-notifications-in-sql-server)*

### REST API sources

**There is no trigger, and this is a structural fact rather than a gap.** For a
generic REST API, the extraction request *is* the cheapest possible "did anything
change" check — there is no lighter call that carries the same information. Any
separate probe would add a round trip and learn nothing the extract wouldn't.

The code bears this out: `RestApiSource` issues
`client.request(method="GET", url=url_with_params, ...)` inside `_extract_impl`
(`drt/sources/rest_api.py:132-137`, method spanning `:85-175`), and there is **no
ETag, `If-Modified-Since`, `HEAD`-probe, or webhook handling anywhere in the
module**. Incremental support exists (#767): when `incremental.start_param` is
configured, the watermark is injected as a query parameter
(`drt/sources/rest_api.py:42-65,119-130`); without it, incremental mode silently
re-pulls the full endpoint each run, warned at `:59-64`.

> Note on the ADR's citation: the provisional matrix cites `rest_api.py:31`, which
> is the `extract()` signature. The HTTP call itself — the thing that "is" the
> extract — is at `:132-137`. The claim holds; the line reference is better as
> `:132`.

**The cost model is API quota, not trigger overhead.** Polling more often to cut
staleness directly multiplies request volume against the target's rate limits —
which is precisely the concern ADR 0004's #769 gate exists for.

**Preferred: Tier 1** — schedule the sync as frequently as the API's rate limits
and the incremental watermark tolerate.

**Alternative:** if the *specific* SaaS API offers webhooks (Stripe, GitHub, and
Shopify are well-known examples), receive them via a hardened `drt serve` (Tier 3)
and let it invoke normal extraction. Note carefully that this is a property of that
one API, not a capability of drt's generic `rest_api` connector, which is
configured to work against arbitrary endpoints and therefore cannot assume webhook
support exists.

## What this means for ADR 0004

### The structural pattern holds

ADR 0004's central claim — *"every cheap signal is a poll, and every push signal is
already a message bus the user runs"* — survives the research, with one refinement
worth stating precisely.

Of the four push mechanisms found, three are the ADR's pattern exactly: BigQuery's
is Pub/Sub (a bus); Postgres `NOTIFY` and SQL Server Query Notifications both
require a long-lived consumer draining a queue. The fourth, **Databricks' table
update trigger, is the closest thing to a counter-example** — a platform-managed
push that needs no broker and no drt-side consumer. But it does not undermine the
ADR: Databricks implements it as *its own managed ~1-minute polling loop* that then
starts a job. It is the platform's scheduler, not a broker-free event stream, and
it delivers by invoking drt as a process that starts, syncs, and exits — which is
Tier 1 working as designed.

### Falsification conditions: neither is met

ADR 0004 commits to two conditions that would require revisiting the no-watcher
recommendation. Checked explicitly:

1. **"A cheap push signal that is not already a message bus the user runs."**
   Not found. Databricks comes closest and is managed polling that invokes the CLI;
   BigQuery's push is Pub/Sub; Postgres and SQL Server both need a durable consumer.
   No warehouse will push to a broker-free drt consumer in a way that adds
   capability rather than duplicating a bus.
2. **"A signal whose cost only makes sense amortised across a long-lived
   connection."** Not found. Every preferred signal is cheap *per poll* — a
   metadata read, a version integer, a small indexed query. The one family where
   per-connection setup dominates (logical decoding, binlog replication) is CDC,
   which the ADR already excludes on separate grounds.

**The no-watcher recommendation stands, and this document changes none of ADR
0004's conclusions.** If anything the case is slightly stronger: the cheapest
signal for 11 of 13 sources is a poll, and the two purpose-built signals
(Snowflake `STREAM`, SQL Server Change Tracking) are *designed* to be polled
cheaply — which is exactly the shape a Dagster sensor provides.

### Notes for the gated tiers

Neither note is a recommendation to act now; both are inputs for when #756 and
#769 clear.

- **Tier 2 sensors have two nearly-free starting points.** Delta Lake's
  `version()` is already called in shipped code
  (`drt/sources/deltalake.py:67`) and Iceberg's snapshot id is reachable from a
  table drt already loads (`drt/sources/iceberg.py:51-52`). Both are metadata-only
  reads. These are the cheapest first sensors to write, matching ADR 0004's
  follow-up item 2.
- **Snowflake's cost model deserves care in any future sensor.** Because checking a
  stream requires an active warehouse, and an unconsumed stream keeps reporting
  data, a naive frequent-poll sensor could hold a warehouse awake — the opposite of
  the cost saving the `WHEN` clause exists to provide.

### Open-core boundary

Nothing here moves the line ADR 0004 draws, and the research supports that
placement. Every preferred signal is a query or metadata read performed by the same
connector code that already ships, so freshness remains a property of the sync
engine — which `OPEN_CORE.md` lists under *What's Always Free* ("Sync engine — core
orchestration, batching, rate limits, retry logic, cursor management"), with the
commitment that "if it ships in drt-core, it's free forever". What stays
legitimately enterprise is unchanged: **hosting** an always-on component, under the
existing "Cloud hosting / drt Cloud" boundary item.

Equally, nothing here requires a GUI, a daemon, or a resident process in drt-core —
consistent with CLAUDE.md's "this is a CLI-first tool".

## What changed against the ADR's provisional matrix

ADR 0004 stated that where the two disagree, this matrix wins. The differences are
all refinements of mechanism — **no architectural conclusion changed**.

| Source | Provisional reading | This matrix | Why |
|---|---|---|---|
| PostgreSQL | `LISTEN`/`NOTIFY`; logical decoding (push only) | **`max(updated_at)` poll** preferred; both push options demoted | The provisional row listed no poll, though the ADR's own "every cheap signal is a poll" pattern applies here too. `NOTIFY` is not durable, so it needs a poll behind it regardless |
| ClickHouse | grouped as "`max(updated_at)` or count probe" | **`system.parts` metadata probe** | Cheaper (in-memory catalog, no column scan) and more precise. Also rules out `system.tables.metadata_modification_time`, which looks right but does not fire on `INSERT` |
| SQL Server | grouped with ClickHouse/MySQL/Redshift as "no native change feed worth targeting; generic and slow" | **Change Tracking — its own row** | CT is purpose-built, delete-aware, version-cursored, and available on all editions. Closer to Snowflake `STREAM` than to a generic scan; the grouping undersold it |
| SQLite | grouped with DuckDB as "file mtime" | **`PRAGMA data_version`** | Strictly better: in WAL mode the main file's `mtime` can stay flat while writes accumulate in `-wal`. DuckDB has no equivalent, so the two engines differ |
| Redshift | "no native change feed worth targeting" | Same conclusion, **with EventBridge explicitly ruled out** | Redshift's EventBridge events are lifecycle/job-level, not table-level — worth recording so the next reader doesn't re-investigate a dead end |
| BigQuery | "Table change notifications → Pub/Sub; `APPENDS` TVF" | Same options, **`APPENDS` named preferred**; push path identified as audit-logs-via-Log-Router | There is no dedicated "table change notification" feature — the push path is Data Access audit logs routed to Pub/Sub |
| Databricks | "Table triggers / DLT; Delta commit version" | **Workflows table update trigger** preferred, characterised as *managed polling* | Confirms the mechanism and pins down what "push" means: Databricks polls internally ~1 min, then starts the job |
| REST API | non-option, cited `rest_api.py:31` | Same conclusion, citation refined to `:132-137` | Line 31 is the `extract()` signature; the HTTP call is at `:132` |
| Snowflake, Delta Lake, Iceberg, MySQL, DuckDB | as stated | **Confirmed** | Snowflake gains the warehouse-cost and false-positive caveats; Delta/Iceberg citations verified (`deltalake.py:67`, `iceberg.py:51-52`), with the finding that both signals are reachable but unused for incrementality today |

## Method and limits

Each row was researched independently against the vendor's official documentation
and the connector source in this repository, then compared with ADR 0004's
provisional reading. Every mechanism claim carries a citation; latency figures are
mechanism floors quoted from vendor documentation, not measured benchmarks, and
none should be read as an SLA.

What this document does **not** do: benchmark anything against live warehouses,
recommend an implementation, or revisit ADR 0004's decision. Two figures are
explicitly weaker than the rest and flagged in place — Databricks' ~1–2 minute
trigger latency (community-reported, no documented SLA) and BigQuery's
audit-log-to-Pub/Sub delivery time (typically seconds, no published SLA).
