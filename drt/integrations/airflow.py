"""Airflow integration — run drt syncs as Airflow tasks.

Two usage patterns:

1. Standalone helper (no Airflow dependency in drt-core):

    from drt.integrations.airflow import run_drt_sync

    with DAG(...) as dag:
        PythonOperator(
            task_id="sync_users",
            python_callable=run_drt_sync,
            op_kwargs={"sync_name": "sync_users", "project_dir": "/path/to/project"},
        )

2. DrtRunOperator (requires Airflow at runtime):

    from drt.integrations.airflow import DrtRunOperator

    with DAG(...) as dag:
        DrtRunOperator(
            task_id="sync_users",
            sync_name="sync_users",
            project_dir="/path/to/project",
        )
"""

from __future__ import annotations

from typing import Any

from drt.integrations._runner import run_drt_sync  # re-export for backward compat

__all__ = ["run_drt_sync", "DrtRunOperator"]

_airflow_operator_cls: type | None = None


class DrtRunOperator:
    """Airflow Operator that runs a drt sync.

    Inherits from ``airflow.models.BaseOperator`` at runtime.
    If Airflow is not installed, this class is still importable
    but cannot be used as an operator.

    Example::

        from drt.integrations.airflow import DrtRunOperator

        sync_task = DrtRunOperator(
            task_id="sync_users",
            sync_name="sync_users",
            project_dir="/path/to/drt-project",
        )
    """

    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        global _airflow_operator_cls  # noqa: PLW0603

        try:
            from airflow.models import BaseOperator
        except ImportError as e:
            raise ImportError(
                "DrtRunOperator requires Apache Airflow. "
                "Use run_drt_sync() with PythonOperator instead, "
                "or install Airflow: pip install apache-airflow"
            ) from e

        if _airflow_operator_cls is None:

            class _DrtRunOperator(BaseOperator):  # type: ignore[misc]
                """Airflow operator that runs a drt sync."""

                template_fields = ("sync_name", "project_dir", "profile")

                def __init__(
                    self,
                    sync_name: str,
                    project_dir: str = ".",
                    dry_run: bool = False,
                    profile: str | None = None,
                    **kwargs: Any,
                ) -> None:
                    super().__init__(**kwargs)
                    self.sync_name = sync_name
                    self.project_dir = project_dir
                    self.dry_run = dry_run
                    self.profile = profile

                def execute(self, context: Any) -> dict[str, Any]:
                    return run_drt_sync(
                        sync_name=self.sync_name,
                        project_dir=self.project_dir,
                        dry_run=self.dry_run,
                        profile=self.profile,
                    )

            _airflow_operator_cls = _DrtRunOperator

        return _airflow_operator_cls(*args, **kwargs)
