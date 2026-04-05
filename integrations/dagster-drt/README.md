# dagster-drt

[![PyPI](https://img.shields.io/pypi/v/dagster-drt)](https://pypi.org/project/dagster-drt/)
[![dagster-drt downloads](https://img.shields.io/pepy/dt/dagster-drt?label=dagster-drt%20downloads)](https://pepy.tech/projects/dagster-drt)

Community-maintained [Dagster](https://dagster.io/) integration for [drt](https://github.com/drt-hub/drt) (data reverse tool).

Expose drt syncs as Dagster assets with full observability — metrics, dependencies, subsetting, and dry-run support.

## Installation

```bash
pip install dagster-drt
```

## Quick Start

```python
from dagster import AssetExecutionContext, Definitions
from dagster_drt import drt_assets, DagsterDrtResource

@drt_assets(project_dir="path/to/drt-project")
def my_syncs(context: AssetExecutionContext, drt: DagsterDrtResource):
    yield from drt.run(context=context)

defs = Definitions(
    assets=[my_syncs],
    resources={"drt": DagsterDrtResource(project_dir="path/to/drt-project")},
)
```

## API Overview

| Component | Purpose |
|---|---|
| `@drt_assets` | Decorator — creates `@multi_asset` from drt syncs |
| `build_drt_asset_specs()` | Spec-only generation (for Pipes / custom execution) |
| `DagsterDrtResource` | Execution resource with `.run()` |
| `DagsterDrtTranslator` | Customise how syncs map to assets |
| `DrtConfig` | Per-run config (dry-run) from Dagster UI |

## Features

### @drt_assets Decorator

Creates a Dagster `multi_asset` with `can_subset=True` from drt sync definitions:

```python
@drt_assets(
    project_dir=".",
    sync_names=["sync_a", "sync_b"],  # optional filter
    group_name="reverse_etl",         # optional group override
    partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"),
    pool="drt_pool",                  # optional concurrency control
)
def my_syncs(context: AssetExecutionContext, drt: DagsterDrtResource):
    yield from drt.run(context=context)
```

**Parameters:**

| Parameter | Type | Default | Description |
|---|---|---|---|
| `project_dir` | `str \| Path` | required | Path to drt project root |
| `sync_names` | `list[str] \| None` | `None` | Filter to specific syncs |
| `dagster_drt_translator` | `DagsterDrtTranslator \| None` | `None` | Custom translator |
| `name` | `str \| None` | `None` | Op name |
| `group_name` | `str \| None` | `None` | Group name override |
| `partitions_def` | `PartitionsDefinition \| None` | `None` | Partitions |
| `backfill_policy` | `BackfillPolicy \| None` | auto `single_run` | Backfill policy |
| `pool` | `str \| None` | `None` | Concurrency pool |

### DagsterDrtResource

Execution resource that yields `MaterializeResult` per sync:

```python
DagsterDrtResource(
    project_dir=".",  # optional if @drt_assets has it
    dry_run=False,    # default dry-run mode
)
```

- Auto-resolves `project_dir` from `@drt_assets` metadata
- Filters to `context.selected_asset_keys` for subset execution
- Supports `dry_run` override per-run: `drt.run(context=ctx, dry_run=True)`

### DagsterDrtTranslator

Customise how drt syncs map to Dagster assets. Override `get_asset_spec()`:

```python
from dagster_drt import DagsterDrtTranslator, drt_assets

class MyTranslator(DagsterDrtTranslator):
    def get_asset_spec(self, data):
        default = super().get_asset_spec(data)
        return default.replace_attributes(
            group_name="reverse_etl",
            owners=["team:data"],
        )

@drt_assets(project_dir=".", dagster_drt_translator=MyTranslator())
def my_syncs(context, drt):
    yield from drt.run(context=context)
```

Legacy per-attribute methods (`get_asset_key`, `get_group_name`, etc.) still work but emit deprecation warnings. Migrate to `get_asset_spec()`.

### build_drt_asset_specs (Pipes / Custom Execution)

Generate specs without execution logic — use with Dagster Pipes for remote execution:

```python
from dagster import multi_asset
from dagster_drt import build_drt_asset_specs

specs = build_drt_asset_specs(project_dir=".", sync_names=["my_sync"])

@multi_asset(specs=specs, can_subset=True)
def my_drt_assets(context, pipes: PipesCloudRunJobClient):
    return pipes.run(
        context=context,
        job_name="drt-runner",
        command=["drt", "run", "--sync", "my_sync"],
    ).get_results()
```

This is the same pattern as dagster-dlt's `build_dlt_asset_specs()`.

### MaterializeResult Metadata

Assets return `MaterializeResult` with structured metadata visible in the Dagster UI:

- `sync_name` — sync identifier
- `rows_synced` — successful row count
- `rows_failed` — failed row count
- `rows_skipped` — skipped row count
- `dry_run` — whether dry-run was active
- `row_errors_count` — number of row-level errors (details in logs)

### Asset Kinds

Assets are tagged with `kinds={"drt", "<destination_type>"}` (e.g. `{"drt", "rest_api"}`), visible in the Dagster UI asset graph.

## Usage with dagster-dbt

```python
from dagster import Definitions
from dagster_dbt import dbt_assets, DbtCliResource
from dagster_drt import drt_assets, DagsterDrtResource

@dbt_assets(manifest=dbt_project.manifest_path)
def my_dbt_assets(context, dbt: DbtCliResource):
    yield from dbt.cli(["build"], context=context).stream()

@drt_assets(project_dir="path/to/drt-project")
def my_drt_syncs(context, drt: DagsterDrtResource):
    yield from drt.run(context=context)

defs = Definitions(
    assets=[my_dbt_assets, my_drt_syncs],
    resources={
        "dbt": DbtCliResource(project_dir=dbt_project),
        "drt": DagsterDrtResource(project_dir="path/to/drt-project"),
    },
)
```

## Migration from v0.1

v0.2 introduces the `@drt_assets` decorator, `DagsterDrtResource`, and `build_drt_asset_specs()`. The old `drt_assets()` function is renamed to `drt_assets_legacy()` and emits a deprecation warning.

**Before (v0.1):**

```python
from dagster_drt import drt_assets
defs = Definitions(assets=drt_assets(project_dir="."))
```

**After (v0.2):**

```python
from dagster_drt import drt_assets, DagsterDrtResource

@drt_assets(project_dir=".")
def my_syncs(context, drt: DagsterDrtResource):
    yield from drt.run(context=context)

defs = Definitions(
    assets=[my_syncs],
    resources={"drt": DagsterDrtResource(project_dir=".")},
)
```

## License

Apache-2.0
