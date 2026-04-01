"""Expose drt syncs as Dagster assets."""

from pathlib import Path
from typing import Any, Union

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    Config,
    MaterializeResult,
    MetadataValue,
    asset,
)

from dagster_drt.translator import DagsterDrtTranslator


class DrtConfig(Config):
    """Run configuration for drt assets — configurable from Dagster UI.

    Follows the same pattern as dagster-dbt's ``--full-refresh`` flag.
    """

    dry_run: bool = False


def drt_assets(
    project_dir: Union[str, Path],
    sync_names: Union[list[str], None] = None,
    dagster_drt_translator: DagsterDrtTranslator | None = None,
    dry_run: bool = False,
) -> list[AssetsDefinition]:
    """Create Dagster assets from drt sync definitions.

    Args:
        project_dir: Path to drt project root.
        sync_names: Optional filter. If None, discovers all syncs.
        dagster_drt_translator: Translator for customising asset keys, groups,
            deps, and metadata. If None, uses the default translator.
        dry_run: Default dry-run mode. Can be overridden per-run via
            ``DrtConfig`` in the Dagster UI.

    Returns:
        List of Dagster asset definitions.
    """
    from drt.config.parser import load_syncs

    translator = dagster_drt_translator or DagsterDrtTranslator()
    project_path = Path(project_dir)
    syncs = load_syncs(project_path)
    if sync_names:
        syncs = [s for s in syncs if s.name in sync_names]

    assets_list: list[AssetsDefinition] = []
    for sync_config in syncs:
        asset_key = translator.get_asset_key(sync_config)
        group_name = translator.get_group_name(sync_config)
        description = translator.get_description(sync_config)
        deps = translator.get_deps(sync_config)
        static_metadata = translator.get_metadata(sync_config)

        asset_kwargs: dict[str, Any] = {
            "key": asset_key,
            "description": description,
        }
        if group_name is not None:
            asset_kwargs["group_name"] = group_name
        if deps:
            asset_kwargs["deps"] = deps
        if static_metadata:
            asset_kwargs["metadata"] = static_metadata

        def _make_asset_fn(
            _sync_cfg: Any, _default_dry_run: bool
        ) -> AssetsDefinition:
            @asset(**asset_kwargs)
            def _asset_fn(
                context: AssetExecutionContext,
                config: DrtConfig,
            ) -> MaterializeResult:
                from drt.cli.main import _get_destination, _get_source
                from drt.config.credentials import load_profile
                from drt.config.parser import load_project
                from drt.engine.sync import run_sync
                from drt.state.manager import StateManager

                effective_dry_run = config.dry_run or _default_dry_run

                project = load_project(project_path)
                profile = load_profile(project.profile)
                source = _get_source(profile)
                destination = _get_destination(_sync_cfg)
                state_mgr = StateManager(project_path)

                result = run_sync(
                    _sync_cfg,
                    source,
                    destination,
                    profile,
                    project_path,
                    dry_run=effective_dry_run,
                    state_manager=state_mgr,
                )

                context.log.info(
                    f"drt sync '{_sync_cfg.name}': "
                    f"{result.success} synced, {result.failed} failed, "
                    f"{result.skipped} skipped (dry_run={effective_dry_run})"
                )

                # Log row-level error details for debugging
                for row_error in result.row_errors:
                    context.log.warning(
                        f"Row error in '{_sync_cfg.name}': {row_error}"
                    )

                return MaterializeResult(
                    metadata={
                        "sync_name": MetadataValue.text(_sync_cfg.name),
                        "rows_synced": MetadataValue.int(result.success),
                        "rows_failed": MetadataValue.int(result.failed),
                        "rows_skipped": MetadataValue.int(result.skipped),
                        "dry_run": MetadataValue.bool(effective_dry_run),
                        "row_errors_count": MetadataValue.int(
                            len(result.row_errors)
                        ),
                    }
                )

            return _asset_fn

        assets_list.append(_make_asset_fn(sync_config, dry_run))

    return assets_list
