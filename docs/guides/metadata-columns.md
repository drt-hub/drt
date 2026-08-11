# Metadata Columns — Engine-Injected Bookkeeping Columns

drt writes destination rows byte-for-byte from the source: no marker of
*when* a row was synced, *which run* produced it, or *which sync* owns it.
That makes a stale-looking CRM row or a DWH-to-DWH table with mixed writers
hard to debug — the answer lives in local `.drt/history` files a CI runner
throws away between runs, not in the row itself.

**`metadata_columns`** adds opt-in engine-owned columns to every record a
sync writes — `_drt_synced_at`, `_drt_run_id`, `_drt_sync_name` (names are
yours to choose). Prior art: dlt's `_dlt_load_id` + `_dlt_id`, dbt's
`invocation_id`.

## Quick start

```yaml
name: users_to_crm
model: |
  SELECT id, email, plan
  FROM `project.dataset.users`

destination:
  type: hubspot
  api_key_env: HUBSPOT_API_KEY
  object_type: contacts

sync:
  mode: upsert
  metadata_columns:
    synced_at: _drt_synced_at
    run_id: _drt_run_id
```

Each row HubSpot receives now carries `_drt_synced_at` (this run's UTC
start timestamp) and `_drt_run_id` (this invocation's id) alongside its
real columns.

**The target column must already exist on the destination.** This is dict
enrichment, not DDL — a SQL destination without that column fails the
write the same way any other unknown-column write would, governed by
`sync.on_error`.

## The three fields

| Key | Column holds | Default |
| --- | --- | --- |
| `synced_at` | This run's UTC start timestamp, ISO 8601. One value per `run_sync()` call — every row a run writes shares it, whether the run took one batch or a hundred. | not added |
| `run_id` | The CLI-invocation-level id, auto-generated once per `drt run` invocation and shared by every sync it runs. `null` for library callers of `run_sync()` that don't pass one — same nullability as `SyncResult.run_id`. | not added |
| `sync_name` | The sync's own name — useful once multiple syncs write into a shared table and rows need to name their owner. | not added, off by default even when the other two are set |

All three are independently opt-in: set only the ones you want.
`metadata_columns` absent entirely (the default) adds nothing — zero
behavior change for every sync that doesn't configure it.

## Order: last, after masking

```
extract → cursor tracking → lookups → computed_fields → field_mappings → mask → metadata_columns → load()
                                        (source names)     (rename)      (dest names)  (engine-injected)
```

`metadata_columns` runs **after** [`mask`](pii-masking.md), unlike
[`computed_fields`](computed-fields.md) which runs first. The column name
is already destination-facing — you write it directly in
`metadata_columns:`, not derived from a source column — so there's nothing
for [`field_mappings`](field-mappings.md) to rename. And the value is
drt's own bookkeeping, not source data, so there's nothing for a `mask`
rule to obscure. Placing it last also means it can never collide with a
computed field's *input*: a `computed_fields` template can't accidentally
read `row._drt_run_id`, because that column doesn't exist yet when
`computed_fields` runs.

## Values are stable across a run, not per-record

`synced_at` and `run_id` are computed once when the sync starts and reused
for every batch — a 100k-row sync across many batches writes the identical
`_drt_synced_at` to every row, matching how a "load id" is meant to work
(one per load, correlating every row it touched). If you need per-record
timestamps, that's what your source's own `updated_at`/similar column is
for — `metadata_columns` answers "when did drt write this," not "when did
the source last change it."

## Where it flows

Because the enrichment happens as a plain dict update on the engine-common
payload — the same seam `computed_fields` uses — it reaches every
destination that takes a `list[dict]`, not just SQL ones: file/blob
destinations (Parquet, CSV, S3, GCS) get the columns in the written file
exactly like a CRM gets them in the request body. `--dry-run --diff`
previews show them too, since the diff is built from the same
post-transform records the real load would send.

## Interaction with retry

A record that fails and lands in the [Dead Letter Queue](dead-letter-queue.md)
already has its metadata columns baked in from the original attempt —
`drt retry` resends the stored record verbatim, so `_drt_synced_at` /
`_drt_run_id` on a successfully-retried row reflect *when the row first
failed*, not the retry. This is deliberate: it keeps the columns useful for
tracing "when did this first go wrong" rather than being silently
overwritten by whichever attempt eventually succeeded.

## See also

- [Computed Fields](computed-fields.md) — user-templated derived columns
  (runs *first*, opposite end of the pipeline from `metadata_columns`)
- [Field Mappings](field-mappings.md) — rename columns
- [PII Masking](pii-masking.md) — obscure fields (runs *before*
  `metadata_columns`)
- [`docs/llm/API_REFERENCE.md`](../llm/API_REFERENCE.md) — full sync
  options reference
