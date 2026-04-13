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

from pathlib import Path
from typing import Any


def run_drt_sync(
    sync_name: str,
    project_dir: str = ".",
    dry_run: bool = False,
    profile: str | None = None,
) -> dict[str, Any]:
    """Run a drt sync and return the result as a dict.

    Designed to be called from Airflow's PythonOperator.
    Returns a dict suitable for XCom push.

    Args:
        sync_name: Name of the sync to run.
        project_dir: Path to the drt project directory.
        dry_run: If True, extract but don't write to destination.
        profile: Override profile name (default: from drt_project.yml).

    Returns:
        Dict with sync_name, status, rows_synced, rows_failed, duration_seconds.

    Raises:
        ValueError: If sync_name is not found.
        RuntimeError: If sync fails completely.
    """
    from drt.cli.main import _get_destination, _get_source, _resolve_profile_name
    from drt.config.credentials import load_profile
    from drt.config.parser import load_project, load_syncs
    from drt.engine.sync import run_sync
    from drt.state.manager import StateManager

    pdir = Path(project_dir)
    project = load_project(pdir)
    resolved_profile = _resolve_profile_name(profile, project.profile)
    prof = load_profile(resolved_profile)
    syncs = load_syncs(pdir)

    matched = [s for s in syncs if s.name == sync_name]
    if not matched:
        raise ValueError(f"No sync named '{sync_name}' found in {pdir}")

    sync = matched[0]
    source = _get_source(prof)
    dest = _get_destination(sync)
    state_mgr = StateManager(pdir)

    result = run_sync(sync, source, dest, prof, pdir, dry_run, state_mgr)

    status = (
        "success" if result.failed == 0
        else "partial" if result.success > 0
        else "failed"
    )

    return {
        "sync_name": sync_name,
        "status": status,
        "rows_synced": result.success,
        "rows_failed": result.failed,
        "duration_seconds": result.duration_seconds,
        "dry_run": dry_run,
        "errors": result.errors[:10],
    }


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
        try:
            from airflow.models import BaseOperator
        except ImportError as e:
            raise ImportError(
                "DrtRunOperator requires Apache Airflow. "
                "Use run_drt_sync() with PythonOperator instead, "
                "or install Airflow: pip install apache-airflow"
            ) from e

        # Dynamically create a class that inherits from BaseOperator
        if not hasattr(cls, "_airflow_cls"):

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

            cls._airflow_cls = _DrtRunOperator

        return cls._airflow_cls(*args, **kwargs)
