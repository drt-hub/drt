"""Shared sync runner for orchestrator integrations (Airflow, Prefect, etc.).

Provides the pure `run_drt_sync()` helper used by framework-specific wrappers.
Keep this free of any orchestrator dependency.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.config.sync_options import SyncConfig
    from drt.state.manager import StateStore


def _watermark_storage(sync: SyncConfig, project_dir: Path) -> Any:
    """Resolve a sync's configured watermark backend, or ``None``.

    Mirrors ``drt.cli._helpers.get_watermark_storage`` — duplicated rather
    than imported so this module stays free of the CLI's ``typer`` import
    graph, per the module docstring.
    """
    from drt.state.watermark import (
        BigQueryWatermarkStorage,
        GCSWatermarkStorage,
        LocalWatermarkStorage,
    )

    wm = sync.sync.watermark
    if wm is None:
        return None
    if wm.storage == "local":
        return LocalWatermarkStorage(project_dir)
    elif wm.storage == "gcs":
        assert wm.bucket is not None
        assert wm.key is not None
        return GCSWatermarkStorage(bucket=wm.bucket, key=wm.key)
    elif wm.storage == "bigquery":
        assert wm.project is not None
        assert wm.dataset is not None
        return BigQueryWatermarkStorage(project=wm.project, dataset=wm.dataset)
    return None


def run_drt_sync(
    sync_name: str,
    project_dir: str = ".",
    dry_run: bool = False,
    profile: str | None = None,
    state_manager: StateStore | None = None,
) -> dict[str, Any]:
    """Run a drt sync and return the result as a dict.

    Designed to be called from orchestrator tasks (Airflow PythonOperator,
    Prefect @task, etc.). Returns a dict suitable for result passing.

    Args:
        sync_name: Name of the sync to run.
        project_dir: Path to the drt project directory.
        dry_run: If True, extract but don't write to destination.
        profile: Override profile name (default: from drt_project.yml).
        state_manager: Share a state store across calls. Typed as the
            ``StateStore`` Protocol (#756) so a remote backend substitutes for
            the local one; ``LocalStateManager``'s thread-safety is an instance
            lock, so callers running syncs concurrently in one process
            (``drt serve``) must pass a shared instance. The default per-call
            instance is fine for one-run-per-process callers (Airflow, Prefect).

    Returns:
        Dict with sync_name, status, rows_synced, rows_failed,
        duration_seconds, dry_run, errors.

    Raises:
        ValueError: If sync_name is not found.
    """
    from drt.cli.main import _get_destination, _get_source, _resolve_profile_name
    from drt.config.credentials import load_profile
    from drt.config.parser import load_project, load_syncs
    from drt.engine.observer import CompositeObserver, DlqObserver, StatePersistingObserver
    from drt.engine.sync import run_sync
    from drt.state.factory import build_state_bundle

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
    # Always build the bundle — even when the caller supplies their own
    # state_manager (shared-instance case, see the docstring) — so the DLQ
    # store below is available either way; only .state is overridden.
    bundle = build_state_bundle(project, pdir)
    state_mgr = state_manager if state_manager is not None else bundle.state
    wm_storage = _watermark_storage(sync, pdir)

    observers: list[Any] = [StatePersistingObserver(state_mgr, wm_storage)]
    if not dry_run and sync.sync.dlq is not None and sync.sync.dlq.enabled:
        observers.append(DlqObserver(bundle.dlq, max_records=sync.sync.dlq.max_records))

    result = run_sync(
        sync,
        source,
        dest,
        prof,
        pdir,
        dry_run,
        state_mgr,
        watermark_storage=wm_storage,
        observer=CompositeObserver(observers),
        history_manager=bundle.history,
        history_retention_days=project.history.retention_days,
    )

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
