# CI/CD Integration Guide

Run drt in your CI/CD pipeline to automate data activation with proper testing and error handling.

## GitHub Actions

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
| `--select <name>` | Run a specific sync |
| `--select tag:<tag>` | Run syncs by tag (e.g., `tag:hourly`) |
| `--threads N` | Parallel execution for faster pipelines |
| `--log-format json` | Structured logs for log aggregators |

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