"""Shared sync runner for orchestrator integrations (Airflow, Prefect, etc.).

Provides the pure `run_drt_sync()` helper used by framework-specific wrappers.
Keep this free of any orchestrator dependency.
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

    Designed to be called from orchestrator tasks (Airflow PythonOperator,
    Prefect @task, etc.). Returns a dict suitable for result passing.

    Args:
        sync_name: Name of the sync to run.
        project_dir: Path to the drt project directory.
        dry_run: If True, extract but don't write to destination.
        profile: Override profile name (default: from drt_project.yml).

    Returns:
        Dict with sync_name, status, rows_synced, rows_failed,
        duration_seconds, dry_run, errors.

    Raises:
        ValueError: If sync_name is not found.
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

    status = "success" if result.failed == 0 else "partial" if result.success > 0 else "failed"

    return {
        "sync_name": sync_name,
        "status": status,
        "rows_synced": result.success,
        "rows_failed": result.failed,
        "duration_seconds": result.duration_seconds,
        "dry_run": dry_run,
        "errors": result.errors[:10],
    }
