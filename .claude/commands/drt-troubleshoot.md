
Systematically isolate which layer of a drt setup is failing, then hand off
the specific fix.

`/drt-debug` is reactive — the user has an error message and wants it fixed.
`/drt-troubleshoot` is for when the symptom is vague ("nothing happens",
"the data looks wrong", "I just set this up and want to confirm it works").
Walk the layers **top to bottom**, confirming each one is green before
moving on. The first layer that fails is where the problem lives — stop
there and fix it (or invoke `/drt-debug` for a known error pattern).

## The checklist

Run these in order. Each step has a "✅ green when" condition — only proceed
to the next step once the current one is green.

### 1. Environment

```bash
drt doctor
```

Checks Python version, the `~/.drt/profiles.yml` file, the active
`drt_project.yml`, installed extras, and common env-var gotchas (v0.7+).

- **✅ green when:** every line is ✅ (extras you don't use can be ❌ — only
  the ones your source/destination need must be installed).
- **🔴 common failures:** missing `drt-core[<extra>]` for your connector;
  `~/.drt/profiles.yml` not found (run `drt init`); wrong Python (<3.10).

### 2. Profile + credentials

```bash
drt profile list                 # confirm the profile in drt_project.yml exists
drt profile show <name>          # inspect it (secrets masked)
echo "$YOUR_PASSWORD_ENV"        # confirm the referenced env vars are actually set
```

- **✅ green when:** the profile named in `drt_project.yml` exists in
  `profiles.yml`, and every `${VAR}` / `*_env` it references resolves to a
  non-empty value in the shell.
- **🔴 common failures:** profile name mismatch (`drt run --profile <name>`
  or `DRT_PROFILE` to override); env var unset or exported in a different
  shell; secret hardcoded in YAML instead of an env reference (`drt validate`
  flags this since v0.7.5).

### 3. Connectivity

```bash
drt profile test <name>          # round-trip the SOURCE connection (SELECT 1)
drt validate                     # also surfaces connection issues where supported
```

`drt profile test` is the cheapest way to separate "can't reach the
warehouse" from "the data is wrong" — it runs a real connection check against
the source. For the destination side, `drt validate` exercises a live
connection where supported; otherwise a one-off manual check works (`psql` /
`curl -X POST <url>` / the warehouse console).

- **✅ green when:** the source warehouse and the destination both accept a
  connection with the configured credentials.
- **🔴 common failures:** wrong host/port/account; firewall / VPN; expired
  token; for BigQuery, `GOOGLE_APPLICATION_CREDENTIALS` unset or pointing at
  a stale keyfile; for non-US BigQuery datasets, a missing `location`.

### 4. Config validity

```bash
drt validate                     # JSON-Schema + semantic checks on every sync YAML
drt list                         # confirm your sync is actually discovered
```

- **✅ green when:** `drt validate` reports 0 errors and `drt list` shows the
  sync you expect.
- **🔴 common failures:** sync file outside `syncs/`; YAML indentation; a
  `model: ref('table')` pointing at a table that doesn't exist; an
  `upsert`/`mirror` mode without the required `upsert_key`; deprecation
  warnings (v0.7.2+) that will become errors.

### 5. Dry run (the data preview)

```bash
drt run --select <name> --dry-run            # config parses, rows extract, nothing is written
drt run --select <name> --dry-run --diff     # record-level preview for queryable destinations (v0.7.1+)
```

This is the single most useful step for "the sync runs but the data looks
wrong" — it shows exactly what would be written without touching the
destination.

- **✅ green when:** the row count is what you expect and the previewed
  records / `--diff` look correct.
- **🔴 common failures:** 0 rows (the source query / `model` filters
  everything out, or `mode: incremental` already consumed the watermark —
  check `drt status`, replay with `--cursor-value`); `{{ row.field }}`
  referencing a column the source doesn't return (use `tojson_safe` for
  datetime/Decimal/UUID, v0.7.6+); wrong column names in the template.

### 6. First real run

```bash
drt run --select <name> --limit 10           # sampled real send (#774) — load ≤10 rows, eyeball the destination
drt run --select <name> --verbose            # row-level error detail
drt status                                   # what actually happened
drt status --output json                     # machine-readable, for CI
```

`--limit N` really loads at most N rows (Census/Hightouch "test with sample
records"), so you can inspect them in the destination UI before opening the
tap. The watermark never advances on a sampled run, and it's refused for
`mode: mirror` / `replace` (a sampled mirror would delete the rows it skipped).
When verifying a whole project, add `--fail-fast` (#775) so a systemic failure
(expired credential, warehouse down) stops after the first sync instead of
burning quota — skipped syncs report `status: "skipped"` in `--output json`.

- **✅ green when:** `result.success` equals the dry-run row count and
  `result.failed` is 0.
- **🔴 common failures:** rate limit (429 — lower `rate_limit.requests_per_second`;
  HubSpot max 9/s, GitHub Actions 5/s); per-row auth/permission errors
  (`on_error: skip` to see the full failure count instead of stopping at the
  first); partial success where some rows fail validation downstream.
- **Recovering partial failures:** enable the dead letter queue
  (`sync.dlq.enabled: true` + `on_error: skip`, v0.7.9+) so failed records
  persist to `.drt/dlq/<sync>.jsonl` instead of being dropped. `drt status`
  shows the queue depth; `drt retry <sync>` re-sends just the failures once
  you've fixed the root cause. See `docs/guides/dead-letter-queue.md`.
- **Re-running whole syncs that failed:** `drt retry` replays *records* from
  the DLQ; `drt run --failed` (#773) re-runs the *syncs* whose last status
  wasn't `success` — the sync-level recovery loop after you've fixed config.
  It exits 0 (with a note) when nothing failed, so it's safe in CI.

### 7. Post-sync correctness

```bash
drt test --select <name>                     # freshness / unique / accepted_values tests, if defined
```

- **✅ green when:** all declared tests pass (or there are none — that's not a
  failure, just no assertions).
- **🔴 common failures:** the sync reported success but downstream `unique` /
  `freshness` tests fail — the data moved but isn't what was expected. This
  usually points back to the source query (step 5), not the sync itself.

## When you've found the failing layer

- **A specific error message** → switch to `/drt-debug` for the known-pattern
  fix.
- **Silent / wrong-data** → the dry-run step (5) almost always localises it;
  fix the source query or template and re-run the checklist from step 5.
- **Still stuck after the checklist** → capture `drt doctor`, the sync YAML,
  `drt_project.yml`, and `drt run --verbose --output json` output, and open a
  discussion / issue with that bundle.

## Reference

- `docs/llm/CONTEXT.md` — architecture and key concepts
- `docs/llm/API_REFERENCE.md` — all config fields
- `docs/connectors/` — per-connector auth, sync modes, and gotchas
- `/drt-debug` — the companion skill for fixing a specific error once this
  checklist has localised it
