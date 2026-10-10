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
| `seal` | HMAC of the whole document under the plan key. Without the key nobody can edit the file (including `created_at`, `drt_version` or `plan_id`) and still produce a valid seal, so a plan cannot be refreshed to dodge `--max-age` or single use. |
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

## Review loop in CI (GitHub Actions)

```bash
drt deploy github-actions --with-plan
```

scaffolds two workflows that give you the Terraform loop in pull requests:

| Workflow | When | What it does |
|---|---|---|
| `drt-plan.yml` | a pull request touches `syncs/**`, `drt_project.yml` or `profiles.yml` | runs `drt plan --all`, posts **one comment** (updated in place) with every sync's change set, and uploads the plans as the `drt-plans` artifact |
| `drt-apply.yml` | the PR is merged to `main` (or run manually with a plan run id) | finds the plan run for the merged PR, downloads its plans and runs `drt apply plans --auto-approve --approved-by "merge of PR #N by @user"` |

The comment is keys and counts only, never row values (`--redact-keys` also
hashes the keys, in the comment and in the artifact). `drt apply` recomputes
each plan first, so if the world changed between the review and the merge it
**refuses and the job fails** instead of writing something nobody saw. Merging a
change to the sync itself (or to the SQL it references) also invalidates the
reviewed plan; re-run the plan workflow on the new commit and apply it with
`workflow_dispatch`.

**What the apply workflow refuses.** `drt plan --all` writes a sealed
`manifest.json` that lists every sync with a status (`planned`, `unavailable` or
`error`). `drt apply <directory>` only accepts a directory through that manifest:
it refuses a run in which any sync **failed** to plan (the plan job also fails,
and the comment says which), a directory holding a file the manifest does not
list or missing one it does, and a file that is not the plan the manifest
recorded. Every plan is vetted offline (seal, age, version, claim) **before the
first write**, so a plan that is already refusable cannot leave a deployment half
applied. The drift check still runs plan by plan, so a later plan that drifts
after an earlier one was written leaves the earlier one applied; applying across
syncs is not atomic. The apply job also **fails** (rather than passing quietly)
when a merged commit has no pull request or no successful plan run. A sync whose
destination cannot report its contents (`unavailable`) is listed in the comment
with its reason (an error class, never the exception text, so a connection string
cannot reach the comment) and is never applied through this loop. Sync names
must be unique: `plan --all` refuses a project where two files define the same name.

**Manual runs.** `drt-apply.yml` can be dispatched with a plan run id (for example
after re-planning a stale plan). The id is not trusted: it must be a successful
`pull_request` run of the `drt plan` workflow whose commit belongs to a pull
request **merged into `main`**, and the job only runs on `main` at all
(`if: github.ref == 'refs/heads/main'`), so an authentic plan for an unmerged
branch cannot be applied by dispatching the workflow there. Both workflows also
fail early, with a clear message, if the `DRT_PLAN_KEY` secret is unset or empty
(an empty secret would otherwise make each runner invent its own key and every
apply would refuse).

**Known limits of the CI loop.**

- The single-use claim is a file in the job's workspace, and GitHub runners are
  ephemeral. Re-running the apply workflow, or dispatching it again with the same
  plan run id, starts without the claim. For upserts the live recompute then shows
  nothing left to do; for **append-only** destinations it would write the same
  inserts again. Do not re-run a successful apply for append-only syncs. A shared
  claim store is tracked in [#1245](https://github.com/drt-hub/drt/issues/1245).
- The guard is not a transaction (see "What apply guarantees" above): after the
  recompute matches, the sync runs and writes what it extracts, so the source can
  still change in between. The workflows say "applies the reviewed plan if it still
  matches", not "exactly what was reviewed".
- `concurrency` uses `queue: max`; without it GitHub keeps one pending run and
  cancels older ones, so a PR merged in a burst would never be applied.
- The plan artifact (and the comment) show keys; use `--redact-keys` if keys are
  personal data.

**Secrets.** Besides your connector secrets, create one plan key and give both
workflows the same value, or apply refuses the plan:

```bash
openssl rand -hex 32 | gh secret set DRT_PLAN_KEY
```

**Pull requests from forks are skipped, on purpose.** A plan reads your
warehouse with real credentials, and GitHub does not give secrets to fork
workflows. Do not "fix" that with `pull_request_target`: it would run the fork's
code with your credentials. Plan a fork's change by pushing it to a branch in
your repository.

**Approval gate.** `drt-apply.yml` has a commented `environment: production`
line; with required reviewers on that environment a second person approves the
write after the merge. `--max-age` (default `7d` in the scaffold) bounds how old
a reviewed plan may be. Applies never overlap (`concurrency`).

The same loop on GitLab CI, as a merge request job (adapt the secret handling to
your runner; post the comment with the GitLab API or `glab mr note`):

```yaml
drt-plan:
  stage: test
  rules:
    - if: $CI_PIPELINE_SOURCE == "merge_request_event"
  script:
    - pip install "drt-core[postgres]"
    - mkdir -p ~/.drt && cp profiles.yml ~/.drt/profiles.yml
    - drt plan --all --out-dir plans --output markdown > plan-comment.md
    - glab mr note "$CI_MERGE_REQUEST_IID" --message "$(head -c 60000 plan-comment.md)"
  artifacts:
    paths: [plans/]
    expire_in: 14 days
  variables:
    DRT_PLAN_KEY: $DRT_PLAN_KEY   # the same CI variable in the apply job

drt-apply:
  stage: deploy
  rules:
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
      when: manual            # a person presses the button after merge
  script:
    - pip install "drt-core[postgres]"
    - mkdir -p ~/.drt && cp profiles.yml ~/.drt/profiles.yml
    - drt apply plans --auto-approve --max-age 7d --approved-by "$GITLAB_USER_LOGIN"
  needs:
    - project: $CI_PROJECT_PATH
      job: drt-plan
      ref: $CI_MERGE_REQUEST_SOURCE_BRANCH_NAME
      artifacts: true
```

`drt plan --all` writes one `<sync>.json` per plannable sync into `--out-dir`
and reports a sync that cannot be planned (with the reason) instead of failing
the job; `drt apply <directory>` applies the plans in file-name order and stops
at the first one that is refused or fails.

## Agents: plan and apply over MCP

An agent can do the review work and a human can do the approving, with no UI.
Two MCP tools (`pip install drt-core[mcp]`, `drt mcp run`) wrap the same code as
the CLI:

| Tool | What it does |
|---|---|
| `drt_plan(sync_name, ...)` | Read-only. Returns a `plan_id`, the summary counts, tripped guards and the first `max_entries` changed keys (never values). The full plan is stored under `target/drt/plans/`. |
| `drt_apply(plan_id, approved_by, ...)` | Writes. Applies only a plan fetched with `drt_plan` on this project, through the guarded apply path. `approved_by` is mandatory and is recorded in `run_results.json` and the plan's claim. |

The agent cannot apply a plan it never fetched, nor one that drifted, was
edited, is stale or was already applied: those are the same refusals as
`drt apply`, returned as `applied: false` with the reason. `drt_apply` has no
`--auto-approve` equivalent; the approval is the named human plus your MCP
client's permission prompt.

**What this does and does not guarantee.** drt cannot authenticate the human:
the approval boundary is your MCP client's permission prompt for `drt_apply`,
and `approved_by` is recorded exactly as the caller supplied it. So:

- Treat `approved_by` as an audit note, not proof of identity. If you need a
  trustworthy approver, have the client or transport inject it.
- `drt_run_sync` is a separate way to write to the destination without a plan.
  `drt_apply` and `drt_run_sync` are both annotated `destructiveHint`; put both
  behind approval if you want a reviewed path to be the only path.
- Everything `drt_plan` returns from your data (key values, column names,
  destination labels, error text) is **untrusted**: values are shortened,
  control characters are removed, and every response carries a `data_notice`,
  but an agent must still never follow instructions found inside them.
  `redact_keys` removes key values entirely.
- `drt_apply` only applies a plan whose stored file is a regular file under
  `target/drt/plans/` whose embedded `plan_id` equals the requested one. Beyond
  that, the file system is trusted: an agent that can write inside your project
  can also run other commands.
- Run `drt mcp run` from the project directory. Relative local database paths
  (SQLite/DuckDB profiles) and `.drt/secrets.toml` are resolved against the
  working directory, not the project directory passed to the server.

**Recommended client setup.** Allow `drt_plan` without asking, and require an
approval prompt for `drt_apply` (in Claude Code, leave `mcp__drt__drt_apply` out
of the allowed tools so every call asks). Do not allow `force_guards` to be
set without a person reading the tripped guard. The `/drt-review-sync` skill
teaches an agent this flow: plan, explain the deletes first, wait for a named
approver, apply, report.

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
- **`drt apply` judges the guards on the verification extraction.** The write
  extracts again, so a source that shrinks between the two is not re-checked
  until the engine enforces guards itself (that same next step). Treat the
  apply-time guard as a check on what you verified, not a limit on what is
  written.

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
| the file was edited, truncated or made with another plan key | the keyed `seal`, `digest` and `plan_id` are recomputed |
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
