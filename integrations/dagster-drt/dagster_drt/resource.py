"""DagsterDrtResource — Dagster resource for executing drt syncs.

Follows the same pattern as dagster-dlt's ``DagsterDltResource``.
Encapsulates execution logic so that ``@drt_assets`` function bodies
remain thin and the execution strategy can be swapped in the future.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    ConfigurableResource,
    MaterializeResult,
    MetadataValue,
)

from dagster_drt.specs import META_KEY_PROJECT_DIR, META_KEY_SYNC_NAME


class DagsterDrtResource(ConfigurableResource):
    """Dagster resource that executes drt syncs.

    Usage::

        @drt_assets(project_dir=".")
        def my_syncs(context: AssetExecutionContext, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        defs = Definitions(
            assets=[my_syncs],
            resources={"drt": DagsterDrtResource(project_dir=".")},
        )

    Attributes:
        project_dir: Path to drt project root. If empty, auto-retrieved
            from ``@drt_assets`` metadata.
        dry_run: Default dry-run mode. Can be overridden per-run via
            ``DrtConfig`` in the Dagster UI.
    """

    project_dir: str = ""
    dry_run: bool = False

    def _resolve_project_dir(self, context: AssetExecutionContext) -> Path:
        """Resolve project_dir from resource config or asset metadata."""
        if self.project_dir:
            return Path(self.project_dir)

        # Auto-retrieve from @drt_assets metadata.
        specs = context.assets_def.specs_by_key
        for spec in specs.values():
            meta_dir = (spec.metadata or {}).get(META_KEY_PROJECT_DIR)
            if meta_dir:
                return Path(meta_dir)

        raise ValueError(
            "project_dir must be set on DagsterDrtResource or embedded in @drt_assets metadata."
        )

    def run(
        self,
        context: AssetExecutionContext,
        dry_run: bool | None = None,
    ) -> Iterator[MaterializeResult]:
        """Execute drt syncs and yield ``MaterializeResult`` per sync.

        Automatically filters to ``context.selected_asset_keys`` when
        used inside a ``@multi_asset`` with ``can_subset=True``.

        Args:
            context: Dagster asset execution context.
            dry_run: Override dry-run mode for this run. If None, uses
                the resource-level default.
        """
        from drt.cli.main import _get_destination, _get_source
        from drt.config.credentials import load_profile
        from drt.config.parser import load_project, load_syncs
        from drt.engine.sync import run_sync
        from drt.state.manager import StateManager

        effective_dry_run = dry_run if dry_run is not None else self.dry_run
        project_path = self._resolve_project_dir(context)

        project = load_project(project_path)
        profile = load_profile(project.profile)
        source = _get_source(profile)
        state_mgr = StateManager(project_path)

        # Build a mapping from sync_name -> SyncConfig.
        all_syncs = {s.name: s for s in load_syncs(project_path)}

        # Determine which syncs to run from selected asset keys.
        selected_keys = context.selected_asset_keys
        specs_by_key = context.assets_def.specs_by_key

        for key in selected_keys:
            spec = specs_by_key.get(key)
            if spec is None:
                continue
            sync_name = (spec.metadata or {}).get(META_KEY_SYNC_NAME)
            if sync_name is None or sync_name not in all_syncs:
                context.log.warning(f"No drt sync found for asset key {key}. Skipping.")
                continue

            sync_config = all_syncs[sync_name]
            destination = _get_destination(sync_config)

            result = run_sync(
                sync_config,
                source,
                destination,
                profile,
                project_path,
                dry_run=effective_dry_run,
                state_manager=state_mgr,
            )

            context.log.info(
                f"drt sync '{sync_name}': "
                f"{result.success} synced, {result.failed} failed, "
                f"{result.skipped} skipped (dry_run={effective_dry_run})"
            )
            for row_error in result.row_errors:
                context.log.warning(f"Row error in '{sync_name}': {row_error}")

            yield MaterializeResult(
                asset_key=key,
                metadata={
                    "sync_name": MetadataValue.text(sync_name),
                    "rows_synced": MetadataValue.int(result.success),
                    "rows_failed": MetadataValue.int(result.failed),
                    "rows_skipped": MetadataValue.int(result.skipped),
                    "dry_run": MetadataValue.bool(effective_dry_run),
                    "row_errors_count": MetadataValue.int(len(result.row_errors)),
                },
            )
