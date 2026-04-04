"""Expose drt syncs as Dagster assets.

Provides two APIs:

- ``@drt_assets`` decorator (new, recommended) — wraps ``@multi_asset``
- ``drt_assets()`` function (legacy) — returns ``list[AssetsDefinition]``
"""

import warnings
from collections.abc import Callable
from pathlib import Path
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetsDefinition,
    BackfillPolicy,
    Config,
    MaterializeResult,
    MetadataValue,
    PartitionsDefinition,
    multi_asset,
)

from dagster_drt.specs import build_drt_asset_specs
from dagster_drt.translator import DagsterDrtTranslator


class DrtConfig(Config):
    """Run configuration for drt assets — configurable from Dagster UI.

    Follows the same pattern as dagster-dbt's ``--full-refresh`` flag.
    """

    dry_run: bool = False


# ------------------------------------------------------------------
# New API: @drt_assets decorator
# ------------------------------------------------------------------


def drt_assets(
    *,
    project_dir: str | Path,
    sync_names: list[str] | None = None,
    dagster_drt_translator: DagsterDrtTranslator | None = None,
    name: str | None = None,
    group_name: str | None = None,
    op_tags: dict[str, Any] | None = None,
    partitions_def: PartitionsDefinition | None = None,
    backfill_policy: BackfillPolicy | None = None,
    pool: str | None = None,
) -> Callable[[Callable[..., Any]], AssetsDefinition]:
    """Decorator that creates a Dagster ``multi_asset`` from drt syncs.

    Follows the same pattern as ``@dbt_assets`` and ``@dlt_assets``.

    Usage::

        @drt_assets(project_dir=".")
        def my_syncs(context: AssetExecutionContext, drt: DagsterDrtResource):
            yield from drt.run(context=context)

    Args:
        project_dir: Path to drt project root.
        sync_names: Optional filter. If None, discovers all syncs.
        dagster_drt_translator: Translator for customising asset specs.
        name: Optional name for the multi_asset op.
        group_name: Optional group name override for all assets.
        op_tags: Optional tags for the op.
        partitions_def: Optional partitions definition for time-based or
            static partitioning.
        backfill_policy: Optional backfill policy. If ``partitions_def`` is
            set and this is None, defaults to ``BackfillPolicy.single_run()``.
        pool: Optional concurrency pool name for limiting parallel execution.

    Returns:
        A decorator that wraps the function as a ``multi_asset``.
    """
    specs = build_drt_asset_specs(
        project_dir=project_dir,
        sync_names=sync_names,
        dagster_drt_translator=dagster_drt_translator,
    )

    if group_name is not None:
        # Detect conflict: raise if translator already set group names.
        conflicting = [s for s in specs if s.group_name is not None and s.group_name != group_name]
        if conflicting:
            keys = [str(s.key) for s in conflicting]
            raise ValueError(
                f"group_name='{group_name}' conflicts with translator-set "
                f"group names on assets: {keys}. Set group_name in the "
                f"translator or in @drt_assets, not both."
            )
        specs = [s.replace_attributes(group_name=group_name) for s in specs]

    multi_asset_kwargs: dict[str, Any] = {
        "specs": specs,
        "can_subset": True,
    }
    if name is not None:
        multi_asset_kwargs["name"] = name
    if op_tags is not None:
        multi_asset_kwargs["op_tags"] = op_tags
    if partitions_def is not None:
        multi_asset_kwargs["partitions_def"] = partitions_def
        if backfill_policy is None:
            backfill_policy = BackfillPolicy.single_run()
    if backfill_policy is not None:
        multi_asset_kwargs["backfill_policy"] = backfill_policy
    if pool is not None:
        multi_asset_kwargs["pool"] = pool

    def decorator(fn: Callable[..., Any]) -> AssetsDefinition:
        return multi_asset(**multi_asset_kwargs)(fn)

    return decorator


# ------------------------------------------------------------------
# Legacy API: drt_assets() function
# ------------------------------------------------------------------


def drt_assets_legacy(
    project_dir: str | Path,
    sync_names: list[str] | None = None,
    dagster_drt_translator: DagsterDrtTranslator | None = None,
    dry_run: bool = False,
) -> list[AssetsDefinition]:
    """Create Dagster assets from drt sync definitions.

    .. deprecated::
        Use the ``@drt_assets`` decorator instead for multi_asset support.

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
    from dagster import asset

    from drt.config.parser import load_syncs

    warnings.warn(
        "drt_assets_legacy() is deprecated. Use the @drt_assets decorator "
        "with DagsterDrtResource instead.",
        DeprecationWarning,
        stacklevel=2,
    )

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

        def _make_asset_fn(_sync_cfg: Any, _default_dry_run: bool) -> AssetsDefinition:
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

                for row_error in result.row_errors:
                    context.log.warning(f"Row error in '{_sync_cfg.name}': {row_error}")

                return MaterializeResult(
                    metadata={
                        "sync_name": MetadataValue.text(_sync_cfg.name),
                        "rows_synced": MetadataValue.int(result.success),
                        "rows_failed": MetadataValue.int(result.failed),
                        "rows_skipped": MetadataValue.int(result.skipped),
                        "dry_run": MetadataValue.bool(effective_dry_run),
                        "row_errors_count": MetadataValue.int(len(result.row_errors)),
                    }
                )

            return _asset_fn

        assets_list.append(_make_asset_fn(sync_config, dry_run))

    return assets_list
