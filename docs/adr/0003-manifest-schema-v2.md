# ADR 0003 — `drt docs` manifest schema v2

- **Status:** Accepted
- **Issue:** [#698](https://github.com/drt-hub/drt/issues/698), plus the
  column-lineage and DLQ-badge data needs tracked in
  [#808](https://github.com/drt-hub/drt/issues/808)
- **Implementation:** `drt/docs/manifest.py` (schema), `drt/docs/builder.py`
  (producer)

## Context

Schema v1 carries a single latest-state snapshot per sync, so the docs site's
"Recent runs" panel is really *latest state per sync* sorted by time — it
cannot show a Duration column, cannot show two runs of the same sync, and a
per-sync run timeline has no data to draw from. Meanwhile run history has been
persisted since #276 (`.drt/history/<sync>.jsonl`, one `HistoryEntry` per
execution) and the Dead Letter Queue since #278 — the facts exist, the
manifest just doesn't carry them.

ADR 0001 closed with three v2 candidates deliberately excluded from v1:
`field_mappings`/`mask` exposure for column-level lineage, run/DLQ history,
and model-SQL source-table extraction. Each is additive in spirit but
reshapes `Sync`, so they were to land together as v2. This ADR lands the
first two; SQL parsing stays out.

## Decision

### Three additive blocks per sync, one version bump

```
sync.runs       [ …last N HistoryEntry-shaped records, newest first… ]
sync.fields     [ {name, source_name, mask}, … ]   (declared columns only)
sync.dlq_depth  int                                 (current DLQ depth)
```

v2 is a **pure superset of v1**: nothing is renamed or removed, every v1
consumer keeps working unchanged, and `schema_version: 2` is the signal that
the new blocks may be present. No migration is required — the bump exists
because the *shape of `Sync`* changed, per ADR 0001's rule that consumers key
behaviour off `schema_version` only.

### The machine/repo split extends to the new blocks

ADR 0001 split the manifest into a catalog half (a function of the repo) and
a state half (a function of one machine). The new blocks sort the same way:
`runs` and `dlq_depth` ride the existing `include_state` switch
(`--no-state` omits all three machine-local blocks), while `fields` derives
purely from sync YAML and is always emitted, like tags and edges.

### Public names stay decoupled from runtime names

`SyncRun` is the public shape of `HistoryEntry`, minus `sync_name` (runs nest
under their sync) and `dry_run` (always False on disk — reserved).
`SyncField` is keyed by the **destination-side** name: `sync.mask` keys
already reference the post-rename field name, and reverse ETL's write side is
the identity that matters. `source_name` carries the pre-rename column (equal
to `name` when no rename is declared). Only declared columns appear — drt
does not parse model SQL, so `fields` is never the full column set and the
renderer must not present it as one.

### Error text joins the redaction policy

The #698 interlock: `errors[]` strings (and the pre-existing
`state.last_error`, which v1 shipped verbatim — a real gap) come straight
from connector exceptions and routinely embed the very things #696 keeps out
of labels: DSNs, hosts, e-mail addresses, credential fragments. Free text has
no key structure to anchor on, so `_redact_error_text()` is a pattern sweep —
URLs, e-mails, phone numbers, credential-ish `key=value` pairs — deliberately
over-eager, because for a hosted artifact over-redaction is a cosmetic bug
and under-redaction is a leak. `--full-labels` bypasses it: same switch, same
trust model as labels.

### History depth is bounded and tunable

`history_depth` defaults to 10, is exposed as `--history-depth` on the CLI
and `history_depth` on the MCP tool (parity per #718), and `0` disables runs
without disabling state. Reads go through `HistoryManager.read(sync_name=…)`,
which touches only that sync's file; per-entry error lists are already capped
at 5 by the history layer, so manifest growth is bounded at roughly
`syncs × depth` small records.

### Still out

Model-SQL source-table extraction (needs SQL parsing) and everything
presentational — run timelines, sparklines, badges are renderer concerns
(#808), not schema. `cursor_value_used` is carried per run for incremental
debugging, matching the precedent set by `state.last_cursor_value` in v1.

## Consequences

- The docs site can now render a true cross-sync run log with Duration,
  per-sync run timelines, the DLQ badge, and declared column-level lineage
  (#808) with no further schema work.
- `manifest.json` grows by up to `depth` records per sync; the default of 10
  keeps a 50-sync project's manifest in the tens of kilobytes.
- Error text in hosted docs may be over-redacted ("user: 42" masks the 42).
  Accepted: fail-closed is the #696 precedent, and `--full-labels` exists for
  trusted hosting.
- A v1-shaped consumer that pins `schema_version == 1` will refuse a v2
  manifest despite being able to read it. Accepted: that is what the version
  field is for, and the alternative (never bumping on additive reshapes)
  makes the number meaningless.
