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
| `digest` | Derived from content only: the sync name, its config fingerprint and the entries. The same sync definition and the same changes give the same digest. |
| `plan_id` | Derived from the digest and the cursor hash, so two plans over the same changes but a different incremental window have different ids. |
| `created_at` | The one wall-clock field; excluded from `plan_id` and `digest`. |
| `fingerprints.config_hash` | Hash of the sync file and the model SQL it references. |
| `fingerprints.cursor_hash` | Hash of the incremental cursor (never the raw value). |
| `summary` | Counts of `create`, `insert`, `update`, `replace`, `delete`. |
| `entries[]` | `{key, action, changed_columns, delete_reason}` per changed record. |

Entries are sorted, so a plan is stable across runs. The JSON Schema is in
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
them, and any column in `sync.mask` is always hashed. The hash is an unsalted
SHA-256 prefix: it lets you compare two plans, but it does not make a
low-entropy key such as an email address unguessable.

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
only writes if the result is the same change set (verify-by-replan). That keeps
row values out of the plan file and reuses the exact comparison that produced
it. The write itself is the normal `drt run` path: rate limiting, DLQ, history,
watermarks and alerts behave as they do for `drt run`.

Nothing is written, and the command exits 1, when:

| Situation | Why |
|---|---|
| the file was edited or truncated | `digest` and `plan_id` are recomputed from the entries |
| the plan is older than `--max-age` (default 24h) | the world has had time to move |
| the plan came from another drt **major** version | formats are only promised within a major |
| the sync file, or the model SQL it references, changed | the plan describes a different sync |
| the incremental watermark moved | the plan covers a different window (the plan stores only a hash of the cursor) |
| the change set differs | a **drift report** lists the keys that appeared, disappeared or changed action |
| the plan was already applied | a plan is single-use |
| history is disabled | single use is recorded in run history, so it cannot be enforced |

`--allow-drift-pct N` proceeds if at most N percent of the planned keys
drifted (default 0); the drift is printed. Without a terminal, `--auto-approve`
is required.

**How "already applied" works.** The plan's `plan_id` is used as the `run_id`
of the apply run, so it lands in run history and in
`target/drt/run_results.json`. A plan is applied when history holds an entry for
that sync with `run_id == plan_id`. No new state is stored.

**Limits.** Extraction runs once to verify and once to write, so the source can
still change in between; verify-by-replan narrows that window, it does not make
the write transactional. Exact replay of the planned rows is a later opt-in.
