# Plan: review a sync before it writes

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
never persists run state. Applying a plan (`drt apply`) is a separate step
([#1217](https://github.com/drt-hub/drt/issues/1217)).

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
