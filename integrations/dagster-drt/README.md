# dagster-drt

[![PyPI](https://img.shields.io/pypi/v/dagster-drt)](https://pypi.org/project/dagster-drt/)

Community-maintained [Dagster](https://dagster.io/) integration for [drt](https://github.com/drt-hub/drt) (data reverse tool).

Expose drt syncs as Dagster assets with full observability — metrics, dependencies, and dry-run support.

## Installation

```bash
pip install dagster-drt
```

## Quick Start

```python
from dagster import Definitions
from dagster_drt import drt_assets

defs = Definitions(assets=drt_assets(project_dir="path/to/drt-project"))
```

## Features

### DagsterDrtTranslator

Customise how drt syncs map to Dagster assets. Subclass and override methods:

```python
from dagster import AssetKey
from dagster_drt import DagsterDrtTranslator, drt_assets

class MyTranslator(DagsterDrtTranslator):
    def get_group_name(self, sync_config):
        return "reverse_etl"

    def get_deps(self, sync_config):
        deps_map = {
            "trigger_bq_meeting_gha": [AssetKey("bq_meeting_cloudsql")],
        }
        return deps_map.get(sync_config.name, [])

assets = drt_assets(
    project_dir="pipeline/data-reverse",
    dagster_drt_translator=MyTranslator(),
)
```

**Available methods:**

| Method | Default | Purpose |
|--------|---------|---------|
| `get_asset_key(sync_config)` | `AssetKey(f"drt_{name}")` | Asset key |
| `get_group_name(sync_config)` | `None` | Group name |
| `get_description(sync_config)` | `sync_config.description` | Asset description |
| `get_deps(sync_config)` | `[]` | Upstream dependencies |
| `get_metadata(sync_config)` | `{}` | Static metadata |

### Dry-Run Support (DrtConfig)

Control dry-run mode per-run from the Dagster UI, or set a build-time default:

```python
# Build-time default: all syncs in dry-run mode
assets = drt_assets(project_dir="...", dry_run=True)

# Override per-run via Dagster UI → Run Config:
# ops:
#   drt_my_sync:
#     config:
#       dry_run: false
```

### MaterializeResult

Assets return `MaterializeResult` with structured metadata visible in the Dagster UI:

- `sync_name` — sync identifier
- `rows_synced` — successful row count
- `rows_failed` — failed row count
- `rows_skipped` — skipped row count
- `dry_run` — whether dry-run was active
- `row_errors_count` — number of row-level errors (details in logs)

### Filtering Syncs

```python
# Only expose specific syncs as assets
assets = drt_assets(
    project_dir="...",
    sync_names=["sync_a", "sync_b"],
)
```

## Usage with dagster-dbt

```python
from dagster import Definitions
from dagster_dbt import dbt_assets
from dagster_drt import drt_assets, DagsterDrtTranslator

defs = Definitions(
    assets=[*my_dbt_assets, *drt_assets("path/to/drt-project")],
)
```

## API Reference

### `drt_assets()`

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `project_dir` | `str \| Path` | required | Path to drt project root |
| `sync_names` | `list[str] \| None` | `None` | Filter to specific syncs |
| `dagster_drt_translator` | `DagsterDrtTranslator \| None` | `None` | Custom translator |
| `dry_run` | `bool` | `False` | Default dry-run mode |

Returns: `list[AssetsDefinition]`

## License

Apache-2.0
