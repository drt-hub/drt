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
| `build_drt_change_sensor()` | Fire a run when the project's source table changes (Delta/Iceberg/Snowflake/SQL Server) |
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

### build_drt_change_sensor (event-driven activation)

Fires a `RunRequest` when the project's source table changes — Tier 2 of
[ADR 0004](https://github.com/drt-hub/drt/blob/main/docs/adr/0004-streaming-and-event-triggered-syncs.md).
Polls a cheap, metadata-only change signal and compares it against Dagster's
own sensor cursor, so no drt-side state is involved in the decision to fire.

```python
from dagster_drt import build_drt_change_sensor

change_sensor = build_drt_change_sensor(
    project_dir=".",
    asset_selection=[my_syncs],   # or job=my_job
    minimum_interval_seconds=60,
)

defs = Definitions(
    assets=[my_syncs],
    sensors=[change_sensor],
    resources={"drt": DagsterDrtResource(project_dir=".")},
)
```

**Supported: `deltalake`, `iceberg`, `snowflake`, `sqlserver` profiles**
(`DeltaTable.version()` / `current_snapshot().snapshot_id` /
`SYSTEM$LAST_CHANGE_COMMIT_TIME('<table>')` / `CHANGE_TRACKING_CURRENT_VERSION()`
— all side-effect-free reads compared for equality only, never ordering, so
an opaque unique token works as well as a genuinely monotonic counter).
`STREAM` + `SYSTEM$STREAM_HAS_DATA()` was Snowflake's originally-proposed
signal and is **not** what's used here — it only resets on DML consumption,
which a read-only polling sensor never provides, so a cursor-diff sensor
built around it would fire once and then latch permanently silent.
`SYSTEM$LAST_CHANGE_COMMIT_TIME` doesn't have that problem (verified against
a real account, [#975](https://github.com/drt-hub/drt/issues/975)), which is
why it's the one wired up instead — but it does carry a real, ongoing
compute cost the other three don't (it reuses the profile's `warehouse=`,
and Snowflake auto-resumes a suspended warehouse for any query by default).
A Snowflake profile requires two extra arguments: `watch_table=` (a
`SnowflakeProfile` has no single table of its own, unlike Delta/Iceberg) and
`minimum_interval_seconds=` (a deliberate poll-cadence choice rather than
inheriting Dagster's default, given the compute cost above):

```python
change_sensor = build_drt_change_sensor(
    project_dir=".",
    asset_selection=[my_syncs],
    watch_table="MY_DB.MY_SCHEMA.MY_TABLE",
    minimum_interval_seconds=300,
)
```

SQL Server also requires `watch_table=` — not because the polled signal
needs a table (`CHANGE_TRACKING_CURRENT_VERSION()` is database-scoped, so it
fires on any tracked table's change, coarser than the other three but not
unsafe), but to *validate* that the specific table is itself change-tracked.
`ALTER TABLE ... ENABLE CHANGE_TRACKING` is a separate opt-in on top of the
database-level `ALTER DATABASE ... SET CHANGE_TRACKING = ON` — without
checking `watch_table` against `CHANGE_TRACKING_MIN_VALID_VERSION`, a table
that was never individually enabled would silently never advance the
signal, even while the database-wide version keeps moving from *other*
tracked tables (caught in Codex review). See
[`docs/guides/event-driven-syncs.md`](https://github.com/drt-hub/drt/blob/main/docs/guides/event-driven-syncs.md)
for the full picture. Any other profile type raises `NotImplementedError` at
evaluation time; a supported profile missing a required argument or
returning a `NULL` signal raises `ValueError`; a missing optional driver
(none of `snowflake-connector-python`/`pymssql`/`deltalake`/`pyiceberg` are
in the base install) raises `ImportError` — all three are failed sensor
ticks, not a silent permanent skip.

**Deployment note:** the sensor evaluates inside the Dagster **daemon**
process, not inside a job run's own container, so the daemon's host needs
the source profile credentials available (same `~/.drt/profiles.yml` /
secret-provider-URI lookup `drt run` uses).

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

#### Reporting results from Cloud Run Jobs

When running drt inside a Cloud Run Job via Pipes, parse `drt run --output json` and report to the Pipes context:

```python
# entrypoint_wrapper.py (runs inside CRJ container)
import json
import subprocess

from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as context:
    proc = subprocess.run(
        ["drt", "run", "--select", "my_sync", "--output", "json"],
        capture_output=True, text=True, check=True,
    )
    for sync in json.loads(proc.stdout)["syncs"]:
        context.report_asset_materialization(
            metadata={
                "rows_extracted": sync["rows_extracted"],
                "rows_synced": sync["rows_synced"],
                "rows_failed": sync["rows_failed"],
                "duration_seconds": sync["duration_seconds"],
            }
        )
```

CRJ container only needs `drt-core` and `dagster-pipes` — `dagster-drt` is not required on the remote side.

### MaterializeResult Metadata

Assets return `MaterializeResult` with structured metadata visible in the Dagster UI:

| Field | Type | Description |
|---|---|---|
| `sync_name` | text | Sync identifier |
| `rows_extracted` | int | Source query row count (for skip detection) |
| `rows_synced` | int | Destination success count |
| `rows_failed` | int | Destination failure count |
| `rows_skipped` | int | Skipped row count |
| `duration_seconds` | float | Sync execution time |
| `dry_run` | bool | Whether dry-run was active |
| `row_errors_count` | int | Row-level error count (details in logs) |

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
