# Using drt with dbt

drt can read your dbt project's `target/manifest.json` to resolve `ref()` model references to fully-qualified table names.

## Setup

No extra installation needed. Just run `dbt run` before `drt run` so that `target/manifest.json` exists.

## How it works

When drt encounters `ref('my_model')` in a sync definition, it:

1. Checks `syncs/models/my_model.sql` for a custom query
2. Reads `target/manifest.json` for dbt's fully-qualified relation name
3. Falls back to `SELECT * FROM <dataset>.my_model`

## Example workflow

```bash
# 1. Run dbt to build models
dbt run

# 2. Run drt to activate data
drt run
```

## Pipeline automation

Use Dagster, Airflow, or a simple script to chain them:

```bash
dbt run && drt run
```

Or with [dagster-drt](../integrations/dagster-drt/):

```python
from dagster import Definitions
from dagster_dbt import dbt_assets
from dagster_drt import drt_assets

defs = Definitions(
    assets=[*dbt_assets, *drt_assets("path/to/project")],
)
```

## Publish drt syncs in dbt lineage

Generate dbt exposures for every drt sync whose `model` is an exact `ref(...)`:

```bash
mkdir -p models/exposures
drt docs generate --format dbt-exposures \
  > models/exposures/drt_exposures.yml
```

Each exposure depends on the referenced dbt model and records the drt sync,
destination type, and sync mode under `meta.drt`. Its URL is computed
deterministically for the matching HTML sync page under `--output` (default:
`target/docs`) and is relative to the `target/` root served by `dbt docs serve`,
so the default URL starts with `docs/sync/` whether or not HTML docs have been
generated yet. Raw-SQL models are omitted and named in YAML comments because
drt deliberately does not guess lineage by parsing SQL. A `ref(...)` with a
matching `syncs/models/<name>.sql` override is omitted too: that local SQL takes
precedence at runtime, so publishing the dbt ref would claim false lineage.
If `--output` points outside `target/`, the HTML page is not reachable through
`dbt docs serve`, so the exporter omits the URL. The command only writes to
stdout; you choose and commit the destination file yourself.

The output is sorted by sync name and byte-identical for an unchanged project,
so it can be regenerated and checked for drift in CI. See [CI/CD
Integration](ci-cd-integration.md#fail-on-dbt-exposure-drift).
