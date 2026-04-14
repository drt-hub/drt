# Using drt with Prefect

Run drt syncs as Prefect tasks. No extra package needed — drt's Prefect integration is built into `drt-core`. Works with Prefect 2.x and 3.x.

## Option 1: Pre-decorated task (recommended)

```python
from prefect import flow
from drt.integrations.prefect import drt_sync_task

@flow
def reverse_etl_flow():
    drt_sync_task(
        sync_name="sync_users",
        project_dir="/path/to/drt-project",
    )

    drt_sync_task(
        sync_name="sync_orders",
        project_dir="/path/to/drt-project",
    )
```

## Option 2: Decorate the helper yourself

For more control over task name, retries, tags, etc.:

```python
from prefect import flow, task
from drt.integrations.prefect import run_drt_sync

sync_users = task(run_drt_sync, name="sync-users", retries=3)

@flow(name="reverse-etl")
def my_flow():
    sync_users(
        sync_name="sync_users",
        project_dir="/path/to/drt-project",
    )
```

## Return value

Both forms return a dict ready for result passing:

```json
{
  "sync_name": "sync_users",
  "status": "success",
  "rows_synced": 42,
  "rows_failed": 0,
  "duration_seconds": 1.5,
  "dry_run": false,
  "errors": []
}
```

## Multi-environment with `--profile`

```python
@flow
def prd_flow():
    drt_sync_task(
        sync_name="sync_users",
        project_dir="/path/to/drt-project",
        profile="prd",  # overrides drt_project.yml
    )
```

## Installation

Install Prefect alongside drt-core in your environment:

```bash
pip install drt-core prefect
```

No extra `drt-core[prefect]` extra is needed — the integration is pure Python and only imports Prefect at runtime.
