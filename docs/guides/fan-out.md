# Fan out one model to multiple destinations

The supported fan-out pattern is one independently named sync per destination.
Give every leg the same `model` and tag, then select the tag when you want to
run the legs together:

```bash
drt run --select tag:customer_fanout
```

Each leg remains an ordinary sync. That is important: its destination config,
progress, failures, retries, and operational history are unambiguous even when
another leg succeeds or fails differently.

## Define one sync per destination

For example, these two files activate the same dbt model into two independent
file destinations. `file` is used here so the configuration is runnable
without credentials; the pattern is the same for SaaS, queue, and database
destinations.

```yaml
# syncs/active_customers_to_csv.yml
name: active_customers_to_csv
tags: [customer_fanout]
model: ref('active_customers')

destination:
  type: file
  path: ./output/active_customers.csv
  format: csv

sync:
  mode: full
  batch_size: 100
  on_error: fail
```

```yaml
# syncs/active_customers_to_jsonl.yml
name: active_customers_to_jsonl
tags: [customer_fanout]
model: ref('active_customers')

destination:
  type: file
  path: ./output/active_customers.jsonl
  format: jsonl

sync:
  mode: full
  batch_size: 100
  on_error: fail
```

Validate and run both legs:

```bash
drt validate --strict
drt run --select tag:customer_fanout
```

The zero-credential [fan-out example](../../examples/fan-out/) contains this
configuration as a complete mini-project.

## One model, independent reads

Each selected sync resolves and reads its model independently. Two legs mean
two source queries; drt does not extract once and broadcast a shared snapshot.
`--threads 2` can make those queries overlap, but it does not combine them.

If every destination must see an identical source snapshot, materialize the
model as a table (or another stable warehouse relation) before running the
fan-out, and do not rebuild it until all legs finish. For dbt, run or build the
model first and use the same exact ref in every sync:

```yaml
model: ref('active_customers')
```

drt resolves that ref through `target/manifest.json`. Keep the compiled dbt
artifact at that default project-relative path for ordinary model resolution:

```bash
dbt build --select active_customers
drt validate --strict
drt run --select tag:customer_fanout
```

An ephemeral model or a query over changing base tables can return different
rows to different legs. Tags do not add snapshot isolation.

## Progress and recovery belong to each leg

The sync name is the durable identity. Each fan-out leg has its own:

- latest state and run status;
- incremental watermark;
- dead-letter queue (when `sync.dlq.enabled: true`);
- run history (when history is enabled); and
- idempotency-ledger namespace (when the configured state backend provides
  the ledger).

There is no group watermark. For incremental fan-out, every leg reads from and
advances its own cursor. A leg that falls behind resumes from its own
watermark; another leg's success cannot advance it.

### Partial failure

Fan-out is not atomic. If `active_customers_to_csv` succeeds and
`active_customers_to_jsonl` fails, the CSV write and its persisted progress are
not rolled back. The JSONL leg retains its own recovery position.

Choose the recovery command for the work you intend to repeat:

```bash
# Re-run syncs whose last persisted status is not success.
drt run --failed

# Re-extract and run exactly one leg.
drt run --select active_customers_to_jsonl

# Replay only that leg's persisted dead-letter rows; do not re-read the model.
drt retry active_customers_to_jsonl
```

`--failed` uses the last persisted status. A fatal exception can happen before
a new status is persisted, so use `--select <leg>` when you know exactly which
leg must be re-extracted. `drt retry <leg>` only applies to records already in
that leg's enabled DLQ.

An ordinary `drt run` with no `--select` starts from every sync in the project.
It therefore runs every fan-out leg again, including legs that already
succeeded (and it also runs unrelated project syncs unless excluded). Use
destination-native upsert or idempotency behavior when repeat delivery is a
concern.

## Concurrency and shared destination quotas

The default is one worker. Add threads only when the source and destinations
can safely handle concurrent work:

```bash
drt run --select tag:customer_fanout --threads 2
```

Within one drt process, destination requests that resolve to the same
connector `rate_limit_key()` share one limiter bucket. The key represents the
real quota holder—for example, an API host, portal, integration token, or
workspace depending on the connector. If matching keys request different
rates, the strictest positive rate and smallest burst win.

This coordination covers `--threads`; separate drt processes do not coordinate
by default. Confirm that legs sharing a vendor quota also resolve to the same
connector quota key, and size source concurrency independently because rate
limiting does not reduce the number of source queries. See [Rate
limiting](rate-limiting.md) for configuration and the cross-process boundary.

## What this pattern does not provide

drt-core deliberately has no:

- `destinations:` list on a sync;
- fan-out group construct beyond ordinary tags;
- shared extraction or cached broadcast between legs;
- cross-destination transaction or rollback; or
- `depends_on` graph or dependency scheduler.

A tag is a selection label, not a new execution or recovery unit. Do not infer
ordering from a shared tag, model lineage, or sync filename order. In
particular, `--threads` may run selected legs concurrently.

## When you need ordering

Put each drt sync in an orchestrator task and declare the dependency there.
The orchestrator decides whether to block, retry, or continue after an upstream
failure; drt keeps responsibility for each sync's source-to-destination work
and recovery state.

- [dagster-drt](../../integrations/dagster-drt/) exposes drt syncs as Dagster
  assets with dependencies, subsetting, and observability.
- [Using drt with Airflow](using-with-airflow.md) shows one sync per Airflow
  task and task-to-task ordering.
- [Using drt with Prefect](using-with-prefect.md) shows the equivalent task and
  flow integration.
- [CI/CD integration](ci-cd-integration.md) covers explicit CI workflows; for a
  small serial chain, separate `drt run --select <sync>` commands with `&&` so
  a failed upstream command prevents the downstream write.

Do not use `drt run --select tag:<name>` as a dependency graph. It selects the
set of syncs to attempt; it does not create edges between them.
