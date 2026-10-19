# ADR 0013 — Plan/apply lineage: `plan_id`, approver and guard overrides in run history and the manifest

- **Status:** Accepted 2026-10-20 (owner confirmed the three open questions; implementation follows the v1.2 release).
- **Issue:** [#1222](https://github.com/drt-hub/drt/issues/1222) (epic [#1227](https://github.com/drt-hub/drt/issues/1227)).
- **Relates to:** [ADR 0003](0003-manifest-schema-v2.md) / [ADR 0006](0006-manifest-schema-v3.md)
  (manifest versioning: additive field → version bump, omit-when-absent, older manifests stay loadable);
  [ADR 0007](0007-protocol-stability-policy.md) (`HistoryStore` is frozen; `ComplianceAuditTrail` is not);
  [#1264](https://github.com/drt-hub/drt/issues/1264) (optional additive warehouse history columns, stack 1/3).

## Context

v1.2 made a sync reviewable (`drt plan`) and made the write conditional on that review (`drt apply`). What is
still missing is the *record* that connects the two afterwards: given a row in run history, which plan was
reviewed, who approved it, and did anyone force a change guard?

Today that information exists in three disconnected places only:

- the single-use claim file under `.drt/applied_plans/<plan_id>.json` (plan id, run id, approver, state) — local to
  the workspace, not queryable, lost on an ephemeral CI runner;
- `target/drt/run_results.json` (`plan_id`, `approved_by`, the forced guards) — overwritten on every run;
- `HistoryEntry.guards` — only the forced-guard outcome, with no plan or approver.

`run_id` already correlates a run across history, the DLQ and the compliance audit trail (#762). So the missing
piece is small: put `plan_id` and `approved_by` on the history entry, and expose them where history is already
exposed.

## Decision

### 1. Two additive `HistoryEntry` fields

```
plan_id      str | None   # the plan this run applied; None for an ordinary `drt run`
approved_by  str | None   # who approved it (drt apply --approved-by / MCP approved_by); None otherwise
```

Both default to `None`, are omitted from the JSONL when absent, and read back as `None` from entries written
before the fields existed — exactly the pattern `run_id`, `sync_run_id` and `guards` already follow, so no
migration is needed for local and object-store history. `HistoryStore` is frozen (ADR 0007), and adding an
optional dataclass field is not a breaking change.

Where the values come from: `drt apply` (CLI and MCP) already knows both. They travel the same way
`force_guards` and the reviewed guard numbers already do — as keyword-only `run_sync()` parameters carried on
`_RunContext` — and the engine copies them onto the `HistoryEntry` it appends. An ordinary `drt run` passes
nothing, so its history is unchanged.

### 2. Warehouse history tables

`plan_id` and `approved_by` join `guards` in `OPTIONAL_HISTORY_COLUMNS`
(`drt/state/_history_columns.py`, #1264). Existing tables are migrated in place on the first append, a read never
alters a table, and a role that cannot `ALTER` keeps working without them. No new mechanism.

### 3. Manifest schema v4 (additive)

`SyncRun` is "the public shape of `HistoryEntry`" (ADR 0003). Adding fields to the history entry therefore changes
the public shape of the manifest, so — as with `config_hash` in v3 — the version moves even though the change is
additive.

```
runs[].plan_id      str, omitted when absent      # opaque plan identifier (a digest-derived id)
runs[].guards       object, omitted when absent   # {"tripped": [...], "forced": bool}; numbers only
runs[].approved_by  str, omitted when absent      # ONLY with --full-labels (see below)
```

v4 is a pure superset of v3: nothing is renamed or removed, `Manifest.from_dict()` keeps accepting v1–v3 records
(the new keys are simply absent), `config_hash` and the `state:modified` baseline are untouched, and keys with no
value are omitted rather than written as `null`.

**`approved_by` is a person's name, so it is treated like the other labels the manifest redacts by default
(#696).** A hosted docs site is a public surface; a plan id is an opaque digest, an approver's name is personal
data. Default manifests carry `plan_id` and `guards` but not `approved_by`; `drt docs generate --full-labels`
(and MCP `drt_get_manifest(full_labels=true)`) include it. `guards.tripped[].message` contains only numbers and
guard names.

### 4. Surfaces

- `drt status` / `drt status --history`: show the last applied plan per sync (`plan <id> approved by <name>`),
  from the run history; JSON output gains the same keys.
- MCP `drt_get_status` / `drt_get_history`: the same fields.
- `run_results.json` and the claim file are unchanged (they already carry these values).

### 5. What this ADR deliberately does not do

- **No change to `ComplianceAuditTrail`.** The audit table already stores `run_id`; history now stores
  `plan_id` for the same `run_id`, so "which record went where, under which approved plan" is one join
  (`audit.run_id = history.run_id`). Duplicating `plan_id` onto every delivered-record row would multiply
  storage on the largest table for a value that is constant per run, and the audit Protocol is not yet frozen
  but its table has the same pre-provisioned-table constraints as history. If a compliance requirement ever needs
  the value on the audit row itself, the optional-column mechanism makes that a small follow-up.
- **No docs-site (HTML) change.** The visual-regression baselines would all move; showing the last applied plan
  and staleness on the sync page belongs to [#1226](https://github.com/drt-hub/drt/issues/1226).
- **No new state store**, no daemon, no hosted approval record (ADR 0004, ADR 0011).

## Alternatives considered

- **Join history to the claim file at read time.** Rejected: the claim file is workspace-local and absent on the
  ephemeral runners where lineage matters most.
- **Put `plan_id` on every audit-trail row.** Rejected for now (see §5); revisit on demand.
- **Always include `approved_by` in the manifest.** Rejected: leaks a name into a hosted catalog by default.
  `--full-labels` is the existing opt-in for exactly this class of value.

## Consequences

- One additive public-shape change (manifest v4) with a documented, tested compatibility rule; older consumers
  keep working and newer ones can rely on `plan_id` when present.
- Lineage works wherever history is stored (local, GCS/S3, warehouse), because it rides the history entry.
- drt-web needs no change: its generator re-runs `drt docs generate` from the in-repo demo; the demo state has
  no plan ids, so the demo output is unchanged apart from `schema_version`.
- The `drt docs` HTML is unchanged in this ADR's implementation, so no visual-regression baseline moves.

## Resolved questions

Confirmed by the owner on 2026-10-20:

1. `approved_by` is withheld from the default manifest; it appears only with `--full-labels`.
2. `drt status` shows the last applied plan only behind `--history`; the default output is unchanged.
3. `ComplianceAuditTrail` is unchanged. Audit rows join history on `run_id`; `plan_id` can be added later as an additive change.
