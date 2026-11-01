# ADR 0018 — API-based plan reads for upsert-keyed SaaS destinations

- **Status:** Proposed 2026-11-02 (design draft; owner review pending — see "Open questions").
- **Issue:** [#472](https://github.com/drt-hub/drt/issues/472) (v1.3).
- **Relates to:** [ADR 0007](0007-protocol-stability-policy.md) (additive capability Protocols);
  [ADR 0013](0013-plan-apply-lineage.md) (plan/apply lineage); [ADR 0014](0014-exact-replay-apply.md);
  `drt/engine/plan_coverage.py` (`needs-api-diff`); #1238 (`fill_empty`, the HubSpot batch read).

## Context

`drt plan` is honest about coverage: a destination that cannot answer "what exists today" is
reported *plan unavailable, with a reason*. Nine built-in SaaS destinations are declared
`needs-api-diff` (HubSpot, Salesforce Bulk, Notion, Intercom, Zendesk, Jira, Linear, Airtable,
Google Sheets, Elasticsearch): the vendor has a keyed read API, drt just does not call it for
planning. This is the most visible limit of v1.2 and was stated up front in the launch notes.

#472 was parked as feedback-gated because of cost: one API call per source row makes a preview
slow, rate-limited and, on metered plans, billable. Two things changed. HubSpot's **batch read by
id** (up to 100 objects per call, already used by `fill_empty` in #1238) cuts the cost by two
orders of magnitude. And `plan`/`apply` makes the preview a first-class artifact, so a bounded,
explicit cost is acceptable where an implicit one was not.

## Decision

### 1. An additive capability Protocol, never a search

A new optional Protocol (ADR 0007: additive, the frozen `Destination` is untouched), e.g.
`KeyedReadDestination`:

- `max_read_batch` — keys per call (HubSpot: 100).
- `read_current(keys, columns, config) -> {key: row | None}` — the **current values of the
  columns the sync writes**, by exact key.

Only **strongly consistent, exact-key reads** qualify (read by id / unique property). Search
endpoints are index-backed and eventually consistent (a record created a second ago may be
missing), which would produce a plan that says *create* for a row that exists. Fail closed, as
`fill_empty` already does: an ambiguous match, an uncorrelated result or an incomplete batch is a
plan-unavailable result with the reason, never a guessed plan.

### 2. Cost is explicit, bounded and part of the plan

A plan makes `ceil(rows / max_read_batch)` read calls. The sync (or `--api-reads`, see open
question 1) opts in and carries a **call budget** (`max_calls`, default 200 ≈ 20,000 HubSpot
rows). Over budget the plan is *unavailable: would need N calls, budget M* — never a partial plan,
which would be a dishonest one. The plan records `api_reads: {calls, rows}` (additive field,
sealed by the existing HMAC), and `drt apply` re-verifies by replanning, so an apply costs the
same reads again; the docs say so plainly. Reads go through the destination's existing rate limiter
and retry policy.

### 3. Comparison is conservative

Vendors normalise values (HubSpot returns strings, lower-cases emails, formats dates). Each
implementation supplies a normaliser for the columns it writes; a row is **unchanged** only when
the normalised current value equals the normalised new value, otherwise it is an **update**.
The plan may over-report updates but never hides one, and entries produced this way are marked
`fidelity: conservative` so a reviewer knows an "update" can be a formatting difference.

### 4. Creates and updates only

SaaS destinations have no safe "list everything" read, so `mirror` and `replace` stay
*unavailable* (nothing can be diffed for deletes). Upsert, insert-only and `match_policy` syncs
are planned; deletes are never reported for these destinations.

### 5. Coverage, schema and surfaces

- `plan_coverage` gains a status for "planned through bounded API reads" (name in open question
  3); a destination moves there only when it implements the Protocol, and the existing test that
  checks the declaration against the code gains the matching assertion. Plan schema/version bump
  per the plan-schema rules.
- Rollout is **per connector, each behind a live verification** (the v1.1 rule): HubSpot first
  (the batch read exists), then Salesforce (SOQL by key), then Notion/Intercom/Zendesk/Jira/
  Linear/Airtable as their keyed reads allow. Google Sheets and Elasticsearch are separate
  designs (a sheet is read whole; ES has `mget`).
- MCP: see open question 4. `drt_plan` today spends no API calls; this would change that.

## Non-goals

- Search-based reads, caching reads across runs, reading rows the sync does not write.
- Previewing vendor-side side effects (workflows triggered by a write).
- `--diff-fields` (#471), and dry-run `--diff` for SaaS (it keeps its sample mode unless asked).

## Consequences

- The largest remaining plan-coverage gap closes one connector at a time; each flips a line in the
  generated coverage matrix.
- A plan can now cost API quota. The opt-in, the budget and the recorded `api_reads` exist so that
  cost is a decision, not a surprise.
- Verification needs live vendor accounts (a HubSpot developer account first).

## Open questions for the owner

1. **Opt-in surface.** Proposed: a sync-level key (reviewable in the YAML and identical for `plan`
   and `apply`), e.g. `plan: {api_reads: {max_calls: 200}}`. Alternative: a `--api-reads` flag, which
   keeps YAML untouched but lets `plan` and `apply` disagree.
2. **Default budget.** Proposed 200 calls (≈ 20,000 HubSpot rows); lower for metered APIs.
3. **Coverage status name** for "planned through bounded API reads" (proposed `api-reads`;
   `needs-api-diff` stays for connectors that have not implemented it).
4. **MCP.** Proposed: not exposed in the first release (an agent should not spend vendor quota
   unprompted); revisit with an explicit per-call budget.
5. **First connector and account.** Proposed HubSpot, which needs a developer account for the live
   verification (the same account unblocks #1237).
