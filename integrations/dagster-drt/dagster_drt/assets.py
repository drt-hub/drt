"""Expose drt syncs as Dagster assets."""
from __future__ import annotations
from pathlib import Path
from typing import Any
from dagster import AssetExecutionContext, AssetsDefinition, asset

def drt_assets(
    project_dir: str | Path,
    sync_names: list[str] | None = None,
) -> list[AssetsDefinition]:
    from drt.config.parser import load_syncs
    project_path = Path(project_dir)
    syncs = load_syncs(project_path)
    if sync_names:
        syncs = [s for s in syncs if s.name in sync_names]

    assets_list: list[AssetsDefinition] = []
    for sync_config in syncs:
        sync_name = sync_config.name
        sync_desc = sync_config.description

        @asset(name=f"drt_{sync_name}", description=sync_desc)
        def _asset_fn(context: AssetExecutionContext, _sync_cfg: Any = sync_config) -> dict[str, Any]:
            from drt.config.credentials import load_profile
            from drt.config.parser import load_project
            from drt.cli.main import _get_source, _get_destination
            from drt.engine.sync import run_sync
            from drt.state.manager import StateManager

            project = load_project(project_path)
            profile = load_profile(project.profile)
            source = _get_source(profile)
            destination = _get_destination(_sync_cfg)
            state_mgr = StateManager(project_path)

            result = run_sync(_sync_cfg, source, destination, profile, project_path, state_manager=state_mgr)
            context.log.info(f"drt sync '{_sync_cfg.name}': {result.success} synced, {result.failed} failed")
            return {"success": result.success, "failed": result.failed}

        assets_list.append(_asset_fn)

    return assets_list
