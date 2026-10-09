# Plan and apply: review a sync before it writes

`drt plan` computes what a sync **would** change and saves it as a reviewable
file. It is the same read-only comparison as `drt run --dry-run --diff`, but it
keeps the **complete** list of changed keys instead of a sample, in a
versioned, deterministic document you can attach to a pull request or hand to a
reviewer.

```bash
drt plan orders_to_pg --out plan.json
drt plan orders_to_pg --out plan.json --detailed-exitcode   # 0 = no changes, 2 = changes
drt plan orders_to_pg --out plan.json --output markdown     # markdown on stdout (PR comment / CI summary), full plan in the file
```

`drt plan` never writes to the destination, never advances a watermark and
never persists run state. Applying a plan is a separate step, `drt apply`
(see below).

## What is in `plan.json`

| Field | Meaning |
|---|---|
| `digest` | Derived from content only: the sync name, the config and environment fingerprints, and the entries. The same sync, environment and changes give the same digest. |
| `plan_id` | Unique per plan file (digest + cursor hash + `created_at`), so a plan made later over the same changes is a new plan. |
| `seal` | Hash of the whole document. It catches accidental edits (including to `created_at` and `drt_version`); it is not a signature. |
| `created_at` | The one wall-clock field; part of `plan_id` and `seal`, not of `digest`. |
| `fingerprints.config_hash` | Hash of the sync file and the model SQL it references. |
| `fingerprints.environment_hash` | Keyed hash of what the sync resolves to here: resolved config, project vars and profile. Only the hash is stored, never the values. |
| `fingerprints.cursor_hash` | Hash of the incremental cursor (never the raw value). |
| `summary` | Counts of `create`, `insert`, `update`, `replace`, `delete`. |
| `entries[]` | `{key, action, value_hash, changed_columns, delete_reason}` per changed record. `value_hash` is a keyed hash of the row to be written, so a changed value is detected without the value being stored. |

Entries are sorted, so the content of a plan is stable across runs. The JSON Schema is in
[`docs/schemas/plan.schema.json`](../schemas/plan.schema.json).

### Actions

- `create` / `update`: an upsert-style write that adds a new key or changes an existing one.
- `insert`: an append-only destination always adds a row, even if the key exists.
- `replace`: `mode: replace` rebuilds the row; omitted columns reset.
- `delete`: a `mirror` or `replace` run removes this key (`delete_reason` says why).

## Markdown output

`--output markdown` is meant for a PR comment or a CI job summary. It shows the
summary and the first 50 entries; the full list is in the JSON plan (use
`--out`). Keys, column names and reasons are rendered as inline code, long
values are shortened to 120 characters, and control characters appear as
`\\xNN`, so a record's data cannot add headings or links to the comment.

## What is hidden

Row **values** never appear. `changed_columns` lists names only. Key values are
shown so a reviewer can tell which record changes; use `--redact-keys` to hash
them, and any column in `sync.mask` is always hashed.

Every hash in a plan (keys, row values, the cursor, the environment) is an
HMAC-SHA256 keyed with a secret that is **not** in the file, so someone who only
has `plan.json` cannot confirm a guess such as a particular email address. The
key is `DRT_PLAN_KEY` if set, otherwise a random key drt creates once in
`.drt/plan.key` (mode 0600; keep `.drt/` out of version control). `drt apply`
needs the same key: set the same `DRT_PLAN_KEY` secret in the CI jobs that plan
and apply, or apply from the workspace that made the plan. Without the key a
plan can still be reviewed, but it cannot be applied.

## When a plan is unavailable

A plan is never partial. `drt plan` exits **1**, writes no file, and prints the
reason when:

- the destination cannot report what it currently contains (most SaaS
  destinations), or the set of rows a mirror would delete could not be read;
- the extraction did not complete (failed rows, interruption);
- `destination.upsert_key` is missing;
- the sync uses something a plan cannot represent yet: `match_policy:
  update_only` / `create_only`, `incremental_strategy: diff` (snapshot
  extraction writes scratch tables, so it is not read-only), or an engine
  metadata column inside `upsert_key`.

## Change guards

`sync.guards` sets limits on how much one run may change:

```yaml
sync:
  mode: mirror
  guards:
    max_creates: 10000
    max_deletes: 500
    max_delete_pct: 10      # of the rows the delete pass looked at
    max_updates_pct: 50     # of the source rows
```

`drt plan` reports a tripped guard (text, markdown and the `guards` block of
`plan.json`) without changing its exit code. `drt apply` evaluates the guards on
the **recomputed** plan, not the file, and refuses to write when one trips; it
names the guard, the observed value and the limit. `--force-guards` applies
anyway, prints what it overrode, and records it in `run_results.json` and the
plan's claim file.

- A percentage that cannot be evaluated trips instead of passing. Delete
  percentages need a count of the rows the delete pass looked at: destination
  keys (`strategy: destination`), tracked keys (`strategy: tracked`) or the
  table (`mode: replace`). `strategy: diff` does not report one, so use
  `max_deletes` there.
- Unknown keys under `guards` are rejected, so a typo cannot disable a guard.
- **`drt run` does not enforce guards yet.** Today they protect the plan/apply
  path only; enforcing them before a plain run's `DELETE` is the next step.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | Plan computed (with `--detailed-exitcode`: no changes) |
| 1 | Error, or the plan is unavailable |
| 2 | Plan computed and changes are present (`--detailed-exitcode` only) |

## Applying a plan

```bash
drt plan orders_to_pg --out plan.json      # review plan.json (or the markdown)
drt apply plan.json                        # prompts; --auto-approve in CI
drt apply plan.json --auto-approve --max-age 2h
```

`drt apply` **recomputes** the plan through the same code `drt plan` uses and
only writes if the result matches (verify-by-replan). That keeps row values out
of the plan file and reuses the exact comparison that produced it.

Nothing is written, and the command exits 1, when:

| Situation | Why |
|---|---|
| the file was edited or truncated | the `seal`, `digest` and `plan_id` are recomputed |
| the plan is older than `--max-age` (default 24h), or dated in the future | the world has had time to move |
| the plan came from another drt **major** version | formats are only promised within a major |
| the sync file, or the model SQL it references, changed | the plan describes a different sync |
| the environment differs (profile, project vars, environment variables, resolved destination) | the same keys and actions could write different values somewhere else |
| the incremental watermark moved | the plan covers a different window (pass `--cursor-value` if the plan was made with one) |
| the change set differs, including a changed **value** | a **drift report** lists the entries that disappeared or appeared; it never prints values |
| the plan was already claimed | a plan is single-use |

`--allow-drift-pct N` proceeds if at most N percent of the planned entries
drifted. The default, 0, refuses any difference at all. Without a terminal,
`--auto-approve` is required.

### What apply guarantees, and what it does not

`drt apply` is a **guard in front of a normal run**, not a transaction and not
a filtered write.

- After verification it runs the normal `drt run` path (rate limiting, DLQ,
  history, watermarks, alerts). That path extracts again and writes **every
  extracted row**, not only the entries listed in the plan. Rows that were
  already equal are upserted too, so triggers and "updated at" columns behave
  exactly as they do for `drt run`.
- The source can still change between the verification extraction and the write.
  Verification narrows that window; it does not close it.
- For incremental syncs the write is pinned to the cursor the plan was verified
  over, not to whatever the watermark says by then.
- Writing exactly the verified rows needs the planned payloads to be stored,
  which is the later exact-replay option
  ([#1221](https://github.com/drt-hub/drt/issues/1221)).

### Plans that cannot be applied

A plan is verified by recomputing it, so output that changes by itself between
two extractions never verifies. A model that returns `CURRENT_TIMESTAMP`,
`random()` or another volatile value in a written column produces plans that
report drift every time. Keep volatile expressions out of the written columns
(use `metadata_columns.synced_at` for a run timestamp: those columns are
excluded from the comparison), or wait for exact replay
([#1221](https://github.com/drt-hub/drt/issues/1221)). Any change to the resolved
config, a project var or the profile (including a rotated literal credential)
is treated as a different environment, which is deliberately conservative.

### Single use

Before the write, `drt apply` creates `.drt/applied_plans/<plan_id>.json`
atomically (`O_EXCL`) with state `pending`, then updates it to `success` or
`failed`. A second apply of the same plan, concurrent or later, finds the file
and refuses. A `pending` file means another apply is running or one stopped
part-way: check the destination before doing anything else.

This claim lives in the workspace. It stops repeated and concurrent applies
there, but two machines with separate workspaces do not see each other's claims;
coordinating those needs a shared claim store, which is not built yet. The apply
run gets a normal `run_id`; the claim file and `run_results.json` link it to the
`plan_id`.
