# Using drt with Apache Airflow

Run drt syncs as Airflow tasks. No extra package needed — drt's Airflow integration is built into `drt-core`.

## Option 1: PythonOperator (recommended)

Use `run_drt_sync()` with Airflow's built-in `PythonOperator`:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

from drt.integrations.airflow import run_drt_sync

with DAG(
    "drt_syncs",
    schedule="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    sync_users = PythonOperator(
        task_id="sync_users",
        python_callable=run_drt_sync,
        op_kwargs={
            "sync_name": "sync_users",
            "project_dir": "/path/to/drt-project",
        },
    )

    sync_orders = PythonOperator(
        task_id="sync_orders",
        python_callable=run_drt_sync,
        op_kwargs={
            "sync_name": "sync_orders",
            "project_dir": "/path/to/drt-project",
            "dry_run": False,
        },
    )

    sync_users >> sync_orders
```

### Return value (XCom)

`run_drt_sync()` returns a dict that is automatically pushed to XCom:

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

### Multi-environment with `--profile`

```python
PythonOperator(
    task_id="sync_users_prd",
    python_callable=run_drt_sync,
    op_kwargs={
        "sync_name": "sync_users",
        "project_dir": "/path/to/drt-project",
        "profile": "prd",  # overrides drt_project.yml
    },
)
```

## Option 2: DrtRunOperator

If you prefer a dedicated operator (requires Airflow at runtime):

```python
from drt.integrations.airflow import DrtRunOperator

with DAG(...) as dag:
    sync_task = DrtRunOperator(
        task_id="sync_users",
        sync_name="sync_users",
        project_dir="/path/to/drt-project",
    )
```

`DrtRunOperator` supports Airflow's `template_fields` for `sync_name`, `project_dir`, and `profile`.

## Google Cloud Composer

For Cloud Composer, install `drt-core` in your Composer environment:

```bash
# Via requirements.txt
drt-core[bigquery]>=0.5.0

# Or via gcloud
gcloud composer environments update MY_ENV \
  --update-pypi-package drt-core[bigquery]>=0.5.0
```

Then use `PythonOperator` as shown above.
