# CI/CD Integration Guide

Run drt in your CI/CD pipeline to automate data activation with proper testing and error handling.

## GitHub Actions

### Fastest path: `drt deploy github-actions`

Since #785, drt scaffolds the workflow for you — from your project root:

```bash
drt deploy github-actions --schedule "40 3 * * *"
```

This writes `.github/workflows/drt-sync.yml` wired to [`drt-hub/drt-action`](https://github.com/drt-hub/drt-action), with:

- **connector extras inferred** from your `profiles.yml` + sync destinations (e.g. `extras: "snowflake"`)
- **every required secret enumerated** in the step's `env:` block — each `*_env` reference and `${VAR}` placeholder in your project becomes `NAME: ${{ secrets.NAME }}`, and the command prints the matching `gh secret set NAME` checklist

Use `--select` / `--profile` to scope it, `--dry-run` to preview, `--force` to overwrite. The sections below cover hand-rolled workflows for anything the scaffold doesn't fit.

### Basic: run syncs on push to main

```yaml
# .github/workflows/drt-sync.yml
name: drt sync
on:
  push:
    branches: [main]
    paths:
      - 'syncs/**'
      - 'drt_project.yml'

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'

      - name: Install drt
        run: pip install drt-core[bigquery]  # add your source extras

      - name: Validate configs
        run: drt validate --output json

      - name: Dry run
        run: drt run --dry-run --output json

      - name: Run syncs
        run: drt run --output json
        env:
          GOOGLE_APPLICATION_CREDENTIALS: ${{ secrets.GCP_SA_KEY_PATH }}
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}

      - name: Run tests
        run: drt test --output json
```

### Advanced: validate on PR, sync on merge

```yaml
# .github/workflows/drt-validate.yml
name: drt validate
on:
  pull_request:
    paths:
      - 'syncs/**'

jobs:
  validate:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install drt-core
      - run: drt validate
      - run: drt run --dry-run
```

### State-aware PR previews

To preview only the syncs a pull request changed, save a JSON manifest after
each push to `main`, then restore the latest successful artifact in the PR job.
The manifest carries the per-sync hashes used by `state:modified`.

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
      - run: pip install drt-core
      - run: drt docs generate --format json --output ci-baseline
      - uses: actions/upload-artifact@v4
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
      - run: pip install drt-core[bigquery]  # add your source extras

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

If there is no baseline artifact yet (or it has expired), the download is
skipped or allowed to fail; drt then warns, treats every current sync as
modified, and completes the first preview. See [State-aware
Selection](state-modified-selector.md) for exact hash semantics, baseline
compatibility, and the environment/project-wide change caveats.

### Fail on dbt exposure drift

Commit the generated exposures file to the dbt project, then regenerate it on
pull requests and let `git diff --exit-code` fail when a sync's dbt lineage is
stale:

```yaml
- name: Check drt dbt exposures
  run: |
    mkdir -p models/exposures
    drt docs generate --format dbt-exposures \
      > models/exposures/drt_exposures.yml
    git diff --exit-code -- models/exposures/drt_exposures.yml
```

Run this from the drt project root. The exporter includes only exact `ref(...)`
models that are not shadowed by `syncs/models/<name>.sql`, sorts exposures by
sync name, and leaves raw-SQL and locally overridden syncs out with YAML
comments explaining why. HTML page URLs are computed relative to dbt's served
`target/` root without checking whether `target/docs` happens to exist, so a
clean checkout and a workspace with generated HTML produce the same exposure
file. The exporter never writes into the dbt project on its own and does not
call the dbt Cloud API.

### Scheduled sync (cron)

```yaml
on:
  schedule:
    - cron: '0 */6 * * *'  # every 6 hours

jobs:
  sync:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install drt-core[bigquery]
      - run: drt run --output json
        env:
          GOOGLE_APPLICATION_CREDENTIALS: ${{ secrets.GCP_SA_KEY_PATH }}
```

## GitLab CI

```yaml
# .gitlab-ci.yml
stages:
  - validate
  - sync

variables:
  PIP_CACHE_DIR: "$CI_PROJECT_DIR/.cache/pip"

cache:
  paths:
    - .cache/pip

validate:
  stage: validate
  image: python:3.12-slim
  script:
    - pip install drt-core
    - drt validate
    - drt run --dry-run
  rules:
    - if: $CI_MERGE_REQUEST_ID

sync:
  stage: sync
  image: python:3.12-slim
  script:
    - pip install drt-core[bigquery]
    - drt run --output json
    - drt test
  rules:
    - if: $CI_COMMIT_BRANCH == "main"
```

## Key CLI flags for CI

| Flag | Purpose |
|------|---------|
| `--output json` | Machine-readable output for parsing in scripts |
| `--dry-run` | Preview without writing data (safe for PR checks) |
| `--select <name>` | Run a specific sync (globs work: `--select 'users_*'`) |
| `--select tag:<tag>` | Run syncs by tag (e.g., `tag:hourly`); repeat `--select` to union |
| `--select destination:<type>` | Run syncs by destination type (e.g., `destination:hubspot`) |
| `--select state:modified` | Run syncs added or changed since a baseline manifest |
| `--select state:new` | Run only syncs absent from a baseline manifest |
| `--state <path>` | Baseline `manifest.json` for `state:modified` / `state:new` |
| `--exclude <selector>` | Subtract syncs from the selection (same grammar) |
| `--failed` | Re-run only syncs that failed in the previous invocation (exit 0 when nothing failed) |
| `--fail-fast` | Stop scheduling after the first failure — one systemic error, one red build, minimal quota burn |
| `--limit N` | Sampled run: send at most N rows per sync (watermarks frozen; refused for mirror/replace) |
| `--threads N` | Parallel execution for faster pipelines |
| `--log-format json` | Structured logs for log aggregators |

## Persisting state across ephemeral runs

A GitHub Actions or GitLab CI job starts from a fresh checkout, so it has no
`.drt/` directory from the previous run. Without remote state, `drt status`
has no prior runs to show, `drt retry` cannot see the previous DLQ, and DLQ
inspection is effectively a no-op. Add a project-level GCS or S3 backend to
`drt_project.yml` so every runner shares the same state:

```yaml
name: my-project
profile: default
state:
  backend: gcs
  bucket: my-drt-state
  prefix: ci/my-project
```

See [Remote state on GCS or S3](remote-state.md) for installation, credentials,
IAM, S3 configuration, and migration from an existing `.drt/` directory.

## Exit codes

| Code | Meaning |
|------|---------|
| `0` | All syncs succeeded |
| `1` | One or more syncs failed |

Use exit codes to gate deployments or trigger alerts.

## Parsing JSON output

```bash
# Check if any sync failed
result=$(drt run --output json)
failed=$(echo "$result" | jq '.failed')

if [ "$failed" -gt 0 ]; then
  echo "::error::$failed sync(s) failed"
  exit 1
fi
```

## Secrets management

Store credentials as CI secrets, not in your repo:

```yaml
# GitHub Actions
env:
  SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
  HUBSPOT_TOKEN: ${{ secrets.HUBSPOT_TOKEN }}

# GitLab CI — use CI/CD Variables (Settings → CI/CD → Variables)
```

drt supports `${VAR}` env substitution in sync YAML and `secrets.toml` for local development. See the [README](https://github.com/drt-hub/drt#quickstart) for details.

## Tips

- **Validate on every PR** — catch config errors before merge
- **Dry-run before real sync** — especially for `mode: replace`
- **Use tags** — `drt run --select tag:hourly` for cron jobs, `tag:daily` for nightly
- **Monitor with `--log-format json`** — pipe to Datadog, CloudWatch, or any log aggregator
- **Pin drt version** — `pip install drt-core==0.6.0` for reproducible builds
