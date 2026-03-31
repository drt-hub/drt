# Using drt with dbt

drt can read your dbt project's `target/manifest.json` to resolve `ref()` model references to fully-qualified table names.

## Setup

No extra installation needed. Just run `dbt run` before `drt run` so that `target/manifest.json` exists.

## How it works

When drt encounters `ref('my_model')` in a sync definition, it:

1. Checks `syncs/models/my_model.sql` for a custom query
2. Falls back to `SELECT * FROM <dataset>.my_model`
3. (Future) Reads `target/manifest.json` for dbt-resolved table names

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
from dagster import Definitions, job
from dagster_dbt import DbtCliResource, dbt_assets
from dagster_drt import drt_assets

defs = Definitions(
    assets=[*dbt_assets, *drt_assets("path/to/project")],
)
```
