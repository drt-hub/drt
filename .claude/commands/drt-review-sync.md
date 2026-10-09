
Review a sync's change set, then apply it only with human approval.

## The rule

**You plan and explain. A human approves. Never call `drt_apply` (or run
`drt apply`) on your own initiative.** Planning is read-only; applying writes to
the destination and cannot be undone by drt.

## Steps

1. **Plan** (read-only):
   - MCP: `drt_plan(sync_name="<name>")`
   - CLI: `drt plan <name> --out plan.json` (add `--output markdown` for a PR comment)
2. **Read the result before summarising it.**
   - `available: false` (or exit 1): the sync cannot be planned, and the reason says why (destination cannot report its contents, `match_policy` or `incremental_strategy: diff` not planned yet, a failed extraction). Do not guess a plan; offer `drt run --dry-run --diff` instead.
   - `summary`: counts of `create`, `insert`, `update`, `replace`, `delete`. Lead with the deletes: they are the one-way door.
   - `guards.tripped`: a `sync.guards` limit was exceeded. Say so plainly; do not suggest `force_guards` as the fix.
   - `entries` shows keys and changed column names, never values. If `entries_truncated`, say how many were not shown (`entries_total`).
3. **Explain to the human** in plain language: how many rows, what kinds of change, anything surprising (a large delete count, many updates to few columns). You may point at suspicious keys, but you cannot verify business meaning from keys alone; say what you could not check.
4. **Wait for an explicit approval** from the human, and take their name for `approved_by`. A vague "looks fine" about something else is not approval of this plan.
5. **Apply** only after approval:
   - MCP: `drt_apply(plan_id="<plan_id>", approved_by="<who approved>")`
   - CLI: `drt apply plan.json --approved-by "<who>"` (prompts; `--auto-approve` only in CI where a human already approved the plan, e.g. by merging a PR)
6. **Report what happened**, not what you hoped: `applied`, the run id, success/failed counts.

## When apply refuses

Nothing was written. Read the reason and tell the human; do not retry blindly.

| Message | Meaning | What to do |
|---|---|---|
| "no longer matches" + drift report | the source or destination changed since the plan | `drt_plan` again and show the new changes; never lower the drift tolerance to push it through |
| "already applied" / "being applied" | a plan is single-use | for "being applied", check the destination first; otherwise plan again |
| "older than max_age" | stale plan | plan again |
| "Change guards tripped" | the change is bigger than the configured limit | show the human the limit and observed value; only `force_guards` if they explicitly accept it |
| "environment differs" / "different plan key" | planned somewhere else | plan here, or use the same `DRT_PLAN_KEY` in both places |
| "sync definition changed" | the YAML or model SQL changed | plan again |

## Things to know

- A plan is a **guard in front of a normal run**, not a transaction: apply recomputes the plan and refuses on any difference, then runs the sync, which writes every extracted row. The source can still change in the short window after verification.
- Plans never contain row values; keys can be hashed with `redact_keys` (or `--redact-keys`).
- Apply is not for backfills or one-off fixes you have not planned; use `drt run --dry-run --diff` for exploration.
- Recommended client setup: allow `drt_plan` freely and put `drt_apply` behind an approval prompt.
