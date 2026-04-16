"""Prefect integration — run drt syncs as Prefect tasks.

Built-in integration (no separate package). Works with Prefect 2.x and 3.x.

Two usage patterns:

1. Use the pre-decorated task directly:

    from prefect import flow
    from drt.integrations.prefect import drt_sync_task

    @flow
    def my_flow():
        drt_sync_task(sync_name="sync_users", project_dir="/path/to/project")

2. Decorate the plain helper yourself (more control over task name, retries, etc.):

    from prefect import flow, task
    from drt.integrations.prefect import run_drt_sync

    my_sync = task(run_drt_sync, name="sync-users", retries=3)

    @flow
    def my_flow():
        my_sync(sync_name="sync_users", project_dir="/path/to/project")
"""

from __future__ import annotations

from typing import Any

from drt.integrations._runner import run_drt_sync  # re-export

__all__ = ["run_drt_sync", "drt_sync_task"]


def drt_sync_task(
    sync_name: str,
    project_dir: str = ".",
    dry_run: bool = False,
    profile: str | None = None,
) -> dict[str, Any]:
    """Run a drt sync as a Prefect task.

    Applies ``@prefect.task`` to :func:`run_drt_sync` lazily so that drt-core
    doesn't require Prefect as a dependency.

    Raises:
        ImportError: If Prefect is not installed.
    """
    try:
        from prefect import task
    except ImportError as e:
        raise ImportError(
            "drt_sync_task requires Prefect. "
            "Use run_drt_sync() with your own @task decorator, "
            "or install Prefect: pip install prefect"
        ) from e

    global _decorated_task
    if _decorated_task is None:
        _decorated_task = task(name="drt_sync")(run_drt_sync)

    result: dict[str, Any] = _decorated_task(
        sync_name=sync_name,
        project_dir=project_dir,
        dry_run=dry_run,
        profile=profile,
    )
    return result


_decorated_task: Any = None
