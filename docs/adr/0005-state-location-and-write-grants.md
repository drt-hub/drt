# ADR 0005 — Where drt's state lives, and what it costs the operator

- **Status:** Accepted 2026-08-03. Both #755 and #756 were written assuming an
  answer — "drt writes managed tables into the source warehouse" — that this ADR
  finds only half right, and an implementation plan for #756 (dated
  2026-07-22, kept locally under the gitignored `docs/plans/`, not tracked in
  this repo) had already been drafted on that assumption. That plan has been
  revised to this ADR's ordering; the issues themselves still need the
  corrections listed under [Follow-up issues](#follow-up-issues).
- **Issues:** [#755](https://github.com/drt-hub/drt/issues/755),
  [#756](https://github.com/drt-hub/drt/issues/756)
- **Amended:** 2026-09-05 — competitive research into how Hightouch, Segment,
  RudderStack, Census, Polytomic, Rivery, dlt, and Meltano handle
  warehouse-write opt-ins confirmed this ADR's core positions rather than
  changing them: the tiered, reversible, non-paywalled design (Decisions 1 and
  4) has no equivalent among the reverse-ETL vendors researched — Hightouch's
  Basic→Lightning upgrade is explicitly one-way, and Segment/RudderStack
  require write access unconditionally with no tiering at all. Two corrections
  and one addition: (1) #755's and #920's core mechanisms (warehouse
  snapshot-diff, SQL-queryable sync history) are **already shipped** by
  Hightouch/Segment/RudderStack and Census/Hightouch respectively — they
  should be scheduled and described as closing a table-stakes gap, not as
  differentiation, correcting this ADR's original "none is warranted on this
  axis" framing to be more precise: the *axis* was right, but #755/#920
  specifically are catch-up, not the exception. (2) Genuine, currently
  unclaimed differentiation exists one layer up the same write-access stack:
  a warehouse-backed idempotency ledger for fire-and-forget destinations
  ([#1099](https://github.com/drt-hub/drt/issues/1099), no researched vendor
  offers this) and a compliance audit trail with owned retention/purge
  ([#1100](https://github.com/drt-hub/drt/issues/1100) — Hightouch has the
  same table shape in `hightouch_audit.Changelog` but frames unmanaged PII
  retention as the *customer's* problem, which is exactly the gap #1100
  closes as a product feature instead). (3) Postgres logical replication's
  publication+slot create/drop symmetry is adopted as the concrete
  reversibility precedent for Decision 4 — cleaner than anything found in the
  reverse-ETL space itself, where scoped-schema designs (RudderStack,
  Segment) isolate blast radius but ship no downgrade path at all. Full
  research and issue-by-issue notes: [#755](https://github.com/drt-hub/drt/issues/755#issuecomment-5551408063),
  [#920](https://github.com/drt-hub/drt/issues/920#issuecomment-5551408667),
  [#960](https://github.com/drt-hub/drt/issues/960#issuecomment-5551409522).
- **Relates to:** [ADR 0004](0004-streaming-and-event-triggered-syncs.md) — whose
  Tier 2 gate is #756, and whose #769 amendment this ADR corrects.
- **Implementation:** none directly. This ADR sets the ordering and the
  permission posture that #755/#756 implement; see
  [Follow-up issues](#follow-up-issues).

## Context

Everything drt remembers between runs lives on local disk under `.drt/` — run
state (`drt/state/manager.py`), history (`drt/state/history.py`), and the DLQ
(`drt/state/dlq.py`). Watermarks are the single exception, with `gcs` and
`bigquery` storage backends already shipped (`drt/state/watermark.py`).

The deployment model drt actively promotes — GitHub Actions cron via
drt-action, ephemeral containers, Dagster/Airflow workers, Cloud Run Jobs —
destroys that disk after every run. The symptoms are concrete and reported
from production (#756, @K-Masuda-SL): `drt status` is blank in a fresh
checkout, `drt retry` cannot see yesterday's failures, and a laptop and a
runner hold disjoint histories.

v0.9 puts two features on the table that both need somewhere durable to write:
#756 (run state, history, DLQ) and #755 (snapshot-diff incremental). As
written, **both assume drt creates and writes managed tables in the source
warehouse.** That assumption is what this ADR examines, because it is not free:
it requires a write grant on the source, and "the data team hands you a
read-only service account" is a common starting condition for exactly the
teams drt targets.

## Evidence

**Hightouch** ships two engines, and the split is along this exact line:

| | Basic | Lightning |
|---|---|---|
| Warehouse permission | **read-only** | read + write, schema creation |
| Diff computed | on Hightouch infrastructure | in the customer's warehouse |
| State lives | Hightouch's servers | `hightouch_planner`, `hightouch_audit` |
| Scale | small models | 100K+ rows, "up to 100 times faster" |
| Reversible | upgrade any time | **cannot return to Basic** |

Permission granularity is documented too: planner table names change every run,
so Lightning needs write on the *whole schema* — with a documented escape hatch
where the operator pre-creates the schema and grants only create/delete table.

**Segment Reverse ETL** is the same shape: a managed `_segment_reverse_etl`
schema requiring read and write.

**Census** could not be verified. `docs.getcensus.com` now 301s to Fivetran's
Activations documentation, which states neither the permission requirement nor
the state location. #755's prior-art claim that Census keeps state tables in
the source warehouse should be treated as **unverified**, and Census is a
weaker reference than it was now that the product has been absorbed.

**dlt** persists pipeline state *in the destination* (`_dlt_pipeline_state`,
`_dlt_loads`). The principle worth stealing is "put state where you already
have a write grant" — but it does not transfer directly, because dlt's
destination is always a data store while drt's is frequently a Slack webhook
or a CRM with no storage at all. drt already applies the principle where it
*can*: `_drt_synced_keys` (tracked mirror, #686) lives in the destination when
the destination is SQL.

**dbt** contributes two structures. Snapshots are near-isomorphic to #755 —
`strategy: check` + `check_cols: all` is the same feature as
`incremental_strategy: diff` + `hash_columns: all` — and dbt requires warehouse
write because it is a transformation tool, so it pays that cost without
argument. But `state:modified` deliberately does *not* use a state service: it
diffs against a **baseline artifact file**. dbt's answer to "CI needs to know
what changed" was files, not infrastructure.

## The asymmetry that decides it

Every SaaS vendor above has a third location: their own infrastructure. That is
what lets Hightouch offer a read-only tier at all — Basic's state has somewhere
to live that is neither the customer's warehouse nor a runner's disk.

**drt has no third location.** Which means drt's local tier is not equivalent
to Hightouch Basic; it is strictly weaker:

| | read-only source | survives CI | team-shared |
|---|---|---|---|
| Hightouch Basic | yes | yes | yes |
| **drt local (today)** | yes | **no** | **no** |

drt's equivalent of "the vendor's infrastructure" is **object storage the
operator already owns** — GCS or S3. drt already speaks this (`GCSWatermarkStorage`),
and the workaround reported in #756 (hand-rolling `.drt/history` persistence to
a GCS object around `drt docs generate`) is precisely this tier, built by hand
because drt does not offer it.

This splits #756's motivation in two, and the halves have different costs:

1. **Durability and sharing** — CI-safe state, `drt retry` across runners, one
   view for laptop and runner. **Object storage is sufficient. No warehouse
   write grant is needed.**
2. **SQL-queryable observability** — Hightouch's Warehouse Sync Logs: run
   history as warehouse tables the team can join against. **Requires the
   warehouse, and cannot be had any other way.**

#755 has no such split. A multi-million-row diff cannot be computed against a
JSON blob in object storage; pushing it down as SQL in the warehouse is the
entire point of the feature. #755 is structurally a Lightning-engine-shaped
capability and must be treated as one.

## Decision

1. **State location is tiered, and the tier is a config choice — not a product
   tier and not a paywall.** `local` (default, unchanged) → `gcs`/`s3`
   (durable, CI-safe, shared, no warehouse write) → `warehouse` (adds
   SQL-queryable observability, requires a write grant).

2. **#756 splits along that seam.** The CI-durability half ships first on
   object storage. Warehouse-backed state becomes a separate, later, opt-in
   issue whose justification is observability, not durability. Shipping them
   as one issue would put a warehouse write grant in front of a fix for a
   problem that does not need one.

3. **#755 requires warehouse write, and says so plainly in its docs** — with
   the pre-created-schema escape hatch Hightouch documents, since "let this
   tool create schemas" is a harder review than "here is a schema, use it".
   **Cursor-based incremental remains a first-class supported path**, not a
   deprecated fallback: it is the read-only-source answer, and it is the honest
   equivalent of Hightouch Basic.

4. **Tiers must be reversible.** Changing one YAML line and dropping a schema
   returns a project to the previous tier. Hightouch explicitly cannot do this
   ("You can't move to the Basic engine once Lightning is configured"); drt can,
   because the configuration is declarative and lives in Git rather than in a
   vendor's control plane. This is a design constraint on the implementation,
   not an aspiration — it is cheap if designed in and expensive if retrofitted.

5. **Borrow dbt's vocabulary rather than inventing.** drt's users are dbt
   users; `check_cols` is a word they already know. New names here are pure
   learning cost with no differentiation payoff.

**On differentiation:** none is warranted on this axis. State and diff plumbing
is table stakes, and being novel about it is a cost. The novelty budget belongs
on the axis dbt's artifacts point at — declarative YAML in Git, artifacts as
files (#772, #778, drt-action). Item 4 is the one exception, and it is
differentiation that falls out of the architecture rather than being bolted on.

## Consequences

**[ADR 0004](0004-streaming-and-event-triggered-syncs.md)'s Tier 2 gate clears
earlier than its table implies.** That gate reads "a sensor in an orchestrator
and a CI run genuinely cannot share a watermark today" — a durability and
sharing problem, satisfied in full by the object-storage half. Tier 2 does not
need to wait for warehouse-backed state.

**ADR 0004's #769 amendment needs re-scoping, and this is a correction.** That
amendment folded the cross-process rate-limit residual (N Dagster-launched
processes hitting one endpoint with N buckets) into #756 on the reasoning that
it "needs shared state". It does — but not state of this kind. A token bucket
needs low-latency atomic increment; object storage offers no cheap
compare-and-swap and a per-acquire round trip, and a warehouse table is worse
on both counts. **Neither half of #756 closes that residual.** It should be
lifted back out into its own issue rather than left folded into an issue that
structurally cannot close it.

**Implementation ordering** follows from the split:

| | | warehouse write |
|---|---|---|
| 1 | State-manager Protocols + factory (no behaviour change) | not required |
| 2 | Object-storage backend for state / history / DLQ | **not required** |
| 3 | Warehouse managed-table primitive (shared by 4 and 5) | required |
| 4 | Warehouse state backend (SQL observability) | required |
| 5 | #755 diff-based incremental | required |

The operator-visible payoff of #756 lands at step 2, before any permission
conversation. Step 1 is a prerequisite regardless of this ADR's outcome: the
three managers are constructed directly at roughly fourteen call sites with no
factory, so no backend selection can be honoured until that is centralised.
*(Half-landed already: #900, merged the day after this ADR was opened,
extracted the `StateStore` / `HistoryStore` / `DlqBackend` Protocols with
back-compat aliases and a set-equality drift test against each local
implementation's public API. The factory half — routing a backend choice to
a concrete implementation at the roughly fourteen call sites above — is still
open; `drt/state/manager.py:150` carries the placeholder comment for it.)*

**The Protocol freeze (#304 / v0.10) inherits whatever step 1 produces.**
#900's Protocols are what it inherits from; #297's third-party plugin system
may put external implementations behind them.

## Falsification condition

This ADR is wrong if object storage cannot carry history and DLQ at realistic
volume. The specific risk is write amplification: if the backend rewrites a
whole object per run (the shape `GCSWatermarkStorage` uses today), a project
with many syncs and frequent runs pays a full read-modify-write per sync per
run.

Concurrent runners clobbering each other with last-write-wins is a real,
currently-shipped bug in that same shape — `GCSWatermarkStorage.save()`
(`drt/state/watermark.py:111-117`) loads, mutates, and calls
`upload_from_string()` with no generation precondition, so two saves that
race lose one silently — but it is not evidence against this ADR. GCS
supports preconditioned writes (`if_generation_match`) and S3 now has
conditional writes too, so the fix is a retry-on-precondition-failure loop in
the client, not a change of tier. If anything this strengthens Decision 2:
the object-storage tier *can* be made safe for concurrent writers, which is
what "team-shared" in Decision 1 requires, it just isn't yet. Tracked as
[#919](https://github.com/drt-hub/drt/issues/919), independent of #756 —
today's bug, not a #756 design question.

If a prototype shows that per-run cost or contention makes the object-storage
tier unusable at, say, 50 syncs on a 15-minute cadence, then durability must
move to the warehouse, the split in Decision 2 collapses, and the ordering
above is wrong. That measurement should happen during step 2 rather than after.

@K-Masuda-SL offered in #756 to test a backend prototype against a real
GCP / Cloud Run Jobs + Dagster deployment; that is the right environment to
check this in, since it is the topology the failure mode targets.

## Follow-up issues

1. **Split #756** into the object-storage durability half and the
   warehouse-observability half.
2. **Correct #755's prior-art section** — drop or qualify the unverified Census
   claim; add dbt snapshots as the closer structural reference.
3. **New issue: read-only source support** — state cursor-based incremental's
   status as the supported read-only path, and tier reversibility as a design
   goal, so neither is quietly lost during implementation.
4. **Re-scope the #769 cross-process residual** out of #756 per the correction
   above, and amend ADR 0004's gate table accordingly.
5. **New issues from the 2026-09-05 amendment**: [#1099](https://github.com/drt-hub/drt/issues/1099)
   (warehouse idempotency ledger) and [#1100](https://github.com/drt-hub/drt/issues/1100)
   (compliance audit trail) — both gated on #960, both flagged as the
   genuinely-unclaimed differentiation this ADR's "none is warranted" framing
   didn't anticipate finding one layer up the stack.
