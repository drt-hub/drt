# State-aware Selection — Run Only the Syncs a PR Changed

A PR-preview job that reacts to any drt project change traditionally has two
choices: dry-run the whole project, or run nothing. In a project with many
syncs, changing one `syncs/*.yml` file can therefore spend warehouse time and
destination API quota previewing every unrelated sync. This is the motivating
case from [#772](https://github.com/drt-hub/drt/issues/772).

`state:modified` compares the current project with a previously generated drt
manifest and selects only current syncs whose definitions changed. It is drt's
file-backed answer to "which syncs did this PR change?"

## Quick start

Generate or restore a baseline `manifest.json`, then pass it to the selector:

```bash
drt run --select state:modified --state ci-baseline/manifest.json --dry-run --diff
```

Use `state:new` when the job should select only brand-new syncs, excluding
existing syncs whose definitions changed. Both selectors and `--state` also
work with `drt build`, `drt test`, and `drt validate`.

## Selector behavior

The baseline is the `manifest.json` written by:

```bash
drt docs generate --format json --output ci-baseline
```

Each current sync is compared by name with `syncs[].config_hash` in that
manifest:

| Selector | Selects |
|---|---|
| `state:new` | A current sync whose name is absent from the baseline. |
| `state:modified` | Every `state:new` sync, plus a sync whose current and baseline hashes differ. A `null`/missing hash on either side is treated as uncertainty and therefore as modified. |

A sync present only in the baseline has no current definition to run, so it is
silently ignored. `--state <path>` is required whenever a `state:` selector is
used. Like the other selector methods, state selectors can be repeated with
`--select` to form a union or used with `--exclude` to subtract matches.

## What gets hashed, precisely

For each sync, drt hashes:

1. the raw bytes of its `syncs/*.yml` file exactly as written on disk, before
   `${VAR}` substitution or `var()` rendering; and
2. when `model:` is a `ref()`, the raw bytes of the referenced local
   `syncs/models/<name>.sql` file.

The fingerprint does not contain resolved configuration, profile values,
watermarks, run history, or other runtime state. A comment-only or
whitespace-only edit changes the raw bytes and therefore marks the sync as
modified. Including the local model bytes means an edit to referenced model SQL
also marks the sync as modified without resolving SQL through a live profile.

Hashing files before substitution has two important consequences.

### Secrets stay out when configuration uses indirection

Use the recommended pattern: keep an environment variable name or placeholder
in versioned configuration, such as `token_env: API_TOKEN` or `token:
${API_TOKEN}`, and never put the literal credential in the file. With that
structure, the secret value cannot enter the hash input. This is the same
structural approach used by dbt (credentials live outside the project),
Hightouch Git Sync, and Census Git-backed Models (versioned files reference
credentials held outside Git); it avoids maintaining an error-prone list of
which connector fields are secrets.

This guarantee depends on using indirection. If a literal secret is hardcoded
in a sync file, its bytes are part of the hash input. `drt validate` warns about
hardcoded secrets, but does not make them impossible.

### Environment-only changes are invisible

Rotating the value of `API_TOKEN`, changing a `DRT_VAR_*`, or otherwise changing
only the environment does **not** change the file bytes and therefore does not
select the sync. dbt documents and accepts the
[same `var`/`env_var` state-comparison blind spot](https://docs.getdbt.com/reference/node-selection/state-comparison-caveats);
drt makes the same trade-off so a baseline generated in one environment remains
comparable in another.

This is about the *value* an already-resolvable variable holds, not about
whether it resolves at all: manifest generation loads every sync file through
the same strict, error-collecting loader `drt validate` uses, and a sync whose
`${VAR}`/`var()` substitution fails outright is dropped from the manifest
entirely — not included with a missing `config_hash`. Whatever generates your
baseline needs every referenced variable set, or that sync silently never gets
a baseline entry and shows up as changed on every run. See the CI recipe below
for where this actually bites.

This does not apply to the `name:` field itself. Comparison is keyed by the
**resolved** sync name — if `name:` contains `${VAR}`/`var()` and that value
changes between the baseline environment and the current one, the resolved
name changes too, and the fingerprint map no longer has an entry under the
current name: the sync shows up as `state:new`/`state:modified` even though
its file bytes are byte-identical. Keep `name:` literal (no `${VAR}`/`var()`)
if you rely on `state:modified` — templating any other field is unaffected.

A change to project-wide inputs such as `drt_project.yml`'s `vars:` or the
selected profile is also not detected. `state:modified` is a per-sync-file
change detector, not a whole-project change detector. Do not use it as the sole
correctness gate for project-wide configuration or environment changes.

## Missing and incompatible baselines

A missing, unreadable, or unparseable `--state` file is not an error. drt logs a
warning and treats every current sync as both new and modified. That fail-open
behavior lets the first CI run succeed before any baseline artifact has been
saved.

A baseline that parses successfully but has manifest schema version 1 or 2 is
different: those versions predate `config_hash` and cannot answer the
comparison. drt exits with a clear error asking you to regenerate the baseline
with a current version. This is deliberately a hard failure rather than a
guess. A schema-v3 baseline with a `null` hash for an individual sync remains
usable; that sync is conservatively treated as modified.

## GitHub Actions recipe

Baseline management is a CI responsibility. The following pair of workflows
saves a manifest on every push to `main`, then finds the latest successful
baseline workflow run and downloads its artifact in a pull request. GitHub
artifacts are scoped to workflow runs, which is why the PR workflow resolves a
`run-id` before calling `actions/download-artifact`.

```yaml
# .github/workflows/drt-state-baseline.yml
name: drt state baseline
on:
  push:
    branches: [main]

permissions:
  contents: read

jobs:
  baseline:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install drt
        run: pip install drt-core

      - name: Generate baseline manifest
        run: drt docs generate --format json --output ci-baseline
        env:
          # Every ${VAR}/DRT_VAR_* a sync file references must resolve here,
          # even though this step never connects to a destination. Manifest
          # generation loads every sync file the same way `drt run` does
          # (drt/docs/builder.py -> load_syncs_safe()); a sync whose
          # substitution fails is skipped, not included with a placeholder —
          # it silently drops out of the baseline entirely, not just its
          # config_hash. Every later PR job then sees that sync as new on
          # every single run, since it never has a baseline entry to compare
          # against. Match this to the same secrets the PR-preview job below
          # sets.
          GOOGLE_APPLICATION_CREDENTIALS: ${{ secrets.GCP_SA_KEY_PATH }}
          HUBSPOT_TOKEN: ${{ secrets.HUBSPOT_TOKEN }}

      - name: Upload baseline manifest
        uses: actions/upload-artifact@v4
        with:
          name: drt-state-baseline
          path: ci-baseline/manifest.json
```

```yaml
# .github/workflows/drt-pr-preview.yml
name: drt PR preview
on:
  pull_request:
    paths:
      - 'syncs/**'

permissions:
  actions: read
  contents: read

jobs:
  preview:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install drt
        run: pip install drt-core[bigquery]  # add your source extras

      - name: Find latest main-branch baseline
        id: baseline
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          run_id="$(
            gh run list \
              --repo "$GITHUB_REPOSITORY" \
              --workflow drt-state-baseline.yml \
              --branch main \
              --event push \
              --status success \
              --limit 1 \
              --json databaseId \
              --jq '.[0].databaseId // empty' \
              2>/dev/null || true
          )"
          echo "run_id=$run_id" >> "$GITHUB_OUTPUT"

      - name: Download baseline manifest
        if: steps.baseline.outputs.run_id != ''
        continue-on-error: true  # an expired artifact uses drt's first-run fallback
        uses: actions/download-artifact@v4
        with:
          name: drt-state-baseline
          path: ci-baseline
          github-token: ${{ github.token }}
          run-id: ${{ steps.baseline.outputs.run_id }}

      - name: Preview changed syncs
        run: >-
          drt run
          --select state:modified
          --state ci-baseline/manifest.json
          --dry-run
          --diff
        env:
          GOOGLE_APPLICATION_CREDENTIALS: ${{ secrets.GCP_SA_KEY_PATH }}
          HUBSPOT_TOKEN: ${{ secrets.HUBSPOT_TOKEN }}
```

If no successful baseline workflow exists yet, the download step is skipped. If
the run exists but its artifact has expired or is unavailable, the download is
allowed to fail. Either case leaves the path missing and intentionally exercises
the first-run behavior above: all current syncs are treated as modified and the
preview still runs.

## What this isn't

- **Not `dbt --defer`.** drt has no cross-sync references to defer to. A
  selected sync still reads from its configured source and previews or writes
  against its real destination.
- **Not a whole-project change detector.** Changes limited to
  `drt_project.yml`, profiles, or environment values are outside the per-sync
  fingerprint.
- **Not automatic baseline management.** drt reads the path you give it; you
  decide where and how the manifest artifact is saved, retained, and restored.

## See also

- [ADR 0006 — manifest schema v3](../adr/0006-manifest-schema-v3.md)
- [CI/CD Integration Guide](ci-cd-integration.md)
- [Rate limiting — batching changed syncs into one process](rate-limiting.md#zero-code-mitigation-batch-changed-syncs-into-one-process)
- [Issue #772](https://github.com/drt-hub/drt/issues/772)
