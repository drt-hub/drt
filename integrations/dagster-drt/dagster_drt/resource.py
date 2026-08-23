"""DagsterDrtResource — Dagster resource for executing drt syncs.

Follows the same pattern as dagster-dlt's ``DagsterDltResource``.
Encapsulates execution logic so that ``@drt_assets`` function bodies
remain thin and the execution strategy can be swapped in the future.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dagster import (
    AssetExecutionContext,
    AssetKey,
    AssetMaterialization,
    ConfigurableResource,
    MaterializeResult,
    MetadataValue,
    OpExecutionContext,
)

from dagster_drt.event_iterator import DrtEventIterator, DrtEventType
from dagster_drt.specs import META_KEY_PROJECT_DIR, META_KEY_SYNC_NAME
from dagster_drt.translator import DagsterDrtTranslator, DrtTranslatorData


@dataclass(frozen=True)
class _SourceRowCountInput:
    source: Any
    profile: Any
    sync_config: Any
    project_path: Path
    query_tagging: Any
    cursor_value_used: str | None


class DagsterDrtResource(ConfigurableResource["DagsterDrtResource"]):
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

    def _resolve_project_dir(
        self, context: AssetExecutionContext | OpExecutionContext
    ) -> Path:
        """Resolve project_dir from resource config or asset metadata."""
        if self.project_dir:
            return Path(self.project_dir)

        if isinstance(context, AssetExecutionContext):
            # Auto-retrieve from @drt_assets metadata.
            specs = context.assets_def.specs_by_key
            for spec in specs.values():
                meta_dir = (spec.metadata or {}).get(META_KEY_PROJECT_DIR)
                if meta_dir:
                    return Path(meta_dir)

        raise ValueError(
            "project_dir must be set on DagsterDrtResource or embedded in "
            "@drt_assets metadata (asset contexts only)."
        )

    def run(
        self,
        context: AssetExecutionContext | OpExecutionContext,
        dry_run: bool | None = None,
        sync_names: Sequence[str] | None = None,
    ) -> DrtEventIterator[DrtEventType]:
        """Execute drt syncs and return a chainable event iterator.

        Automatically filters to ``context.selected_asset_keys`` when
        used inside a ``@multi_asset`` with ``can_subset=True``. In an
        ``@op``, ``sync_names`` is required and each sync emits an
        ``AssetMaterialization`` instead of a ``MaterializeResult``.

        Args:
            context: Dagster asset execution context.
            dry_run: Override dry-run mode for this run. If None, uses
                the resource-level default.
            sync_names: Explicit sync selection, required for op contexts.
        """
        row_count_inputs: dict[AssetKey, _SourceRowCountInput] = {}

        def _fetch_row_count(event: DrtEventType) -> int:
            asset_key = event.asset_key
            if asset_key is None:
                raise ValueError("A drt MaterializeResult must include an asset_key.")
            row_count_input = row_count_inputs[asset_key]
            from drt.config.query_tags import (
                build_query_tags,
                new_run_id,
                render_comment_header,
            )
            from drt.engine.resolver import resolve_model_ref

            query = resolve_model_ref(
                row_count_input.sync_config.model,
                row_count_input.project_path,
                row_count_input.profile,
                last_cursor_value=row_count_input.cursor_value_used,
            )
            query_tags: dict[str, str] | None = None
            if (
                row_count_input.query_tagging is None
                or row_count_input.query_tagging.enabled
            ):
                extra = (
                    row_count_input.query_tagging.extra
                    if row_count_input.query_tagging
                    else {}
                )
                query_tags = build_query_tags(
                    row_count_input.sync_config.name,
                    new_run_id(),
                    extra,
                )
                query = f"{render_comment_header(query_tags)}\n{query}"
            return sum(
                1
                for _ in row_count_input.source.extract(
                    query,
                    row_count_input.profile,
                    query_tags=query_tags,
                )
            )

        return DrtEventIterator(
            self._run(
                context=context,
                dry_run=dry_run,
                sync_names=sync_names,
                row_count_inputs=row_count_inputs,
            ),
            context=context,
            row_count_fetcher=_fetch_row_count,
        )

    def _run(
        self,
        context: AssetExecutionContext | OpExecutionContext,
        dry_run: bool | None,
        sync_names: Sequence[str] | None,
        row_count_inputs: dict[AssetKey, _SourceRowCountInput],
    ) -> Iterator[DrtEventType]:
        from drt.cli.main import _get_destination, _get_source, _get_watermark_storage
        from drt.config.credentials import load_profile
        from drt.config.parser import load_project, load_syncs
        from drt.engine.observer import (
            CompositeObserver,
            DlqObserver,
            StatePersistingObserver,
            SyncObserver,
        )
        from drt.engine.sync import run_sync
        from drt.state.factory import build_state_bundle

        effective_dry_run = dry_run if dry_run is not None else self.dry_run
        project_path = self._resolve_project_dir(context)

        project = load_project(project_path)
        profile = load_profile(project.profile)
        source = _get_source(profile)
        bundle = build_state_bundle(project, project_path)
        state_mgr = bundle.state

        # Build a mapping from sync_name -> SyncConfig.
        all_syncs = {s.name: s for s in load_syncs(project_path)}

        selected_syncs: list[tuple[AssetKey, Any]] = []
        is_asset_context = isinstance(context, AssetExecutionContext)
        if is_asset_context:
            # Asset execution resolves syncs from the selected AssetSpecs,
            # preserving custom translator keys and subset behavior.
            specs_by_key = context.assets_def.specs_by_key
            for key in context.selected_asset_keys:
                spec = specs_by_key.get(key)
                if spec is None:
                    continue
                sync_name = (spec.metadata or {}).get(META_KEY_SYNC_NAME)
                if sync_name is None or sync_name not in all_syncs:
                    context.log.warning(
                        f"No drt sync found for asset key {key}. Skipping."
                    )
                    continue
                selected_syncs.append((key, all_syncs[sync_name]))
        elif isinstance(context, OpExecutionContext):
            # A plain op has no AssetsDefinition metadata to select from.
            if not sync_names:
                raise ValueError(
                    "sync_names is required when DagsterDrtResource.run() is "
                    "called from an OpExecutionContext."
                )
            missing_sync_names = [name for name in sync_names if name not in all_syncs]
            if missing_sync_names:
                raise ValueError(
                    "Unknown drt sync name(s): " + ", ".join(missing_sync_names)
                )
            translator = DagsterDrtTranslator()
            for sync_name in sync_names:
                sync_config = all_syncs[sync_name]
                key = translator.get_asset_spec(
                    DrtTranslatorData(
                        sync_config=sync_config,
                        project_dir=str(project_path),
                    )
                ).key
                selected_syncs.append((key, sync_config))
        else:
            raise TypeError(
                "context must be an AssetExecutionContext or OpExecutionContext."
            )

        for key, sync_config in selected_syncs:
            sync_name = sync_config.name
            destination = _get_destination(sync_config)
            wm_storage = _get_watermark_storage(sync_config, project_path)

            # The engine only persists state/watermark/DLQ through an
            # observer (AGENTS.md: "state persistence... MUST flow through
            # SyncObserver") — passing state_manager= alone gets cursor
            # *reads* but no post-run save, matching drt/cli/commands/run.py's
            # _build_observer(). No LoggingObserver here: context.log already
            # gives Dagster-native structured logging for this run.
            observers: list[SyncObserver] = [StatePersistingObserver(state_mgr, wm_storage)]
            if (
                not effective_dry_run
                and sync_config.sync.dlq is not None
                and sync_config.sync.dlq.enabled
            ):
                observers.append(
                    DlqObserver(bundle.dlq, max_records=sync_config.sync.dlq.max_records)
                )

            result = run_sync(
                sync_config,
                source,
                destination,
                profile,
                project_path,
                dry_run=effective_dry_run,
                state_manager=state_mgr,
                watermark_storage=wm_storage,
                observer=CompositeObserver(observers),
                history_manager=bundle.history if project.history.enabled else None,
                history_retention_days=project.history.retention_days,
                query_tagging=project.query_tagging,
            )

            context.log.info(
                f"drt sync '{sync_name}': "
                f"{result.rows_extracted} extracted, "
                f"{result.success} synced, {result.failed} failed, "
                f"{result.skipped} skipped (dry_run={effective_dry_run})"
            )
            for row_error in result.row_errors:
                context.log.warning(f"Row error in '{sync_name}': {row_error}")

            metadata = {
                "sync_name": MetadataValue.text(sync_name),
                "rows_extracted": MetadataValue.int(result.rows_extracted),
                "rows_synced": MetadataValue.int(result.success),
                "rows_failed": MetadataValue.int(result.failed),
                "rows_skipped": MetadataValue.int(result.skipped),
                "duration_seconds": MetadataValue.float(
                    result.duration_seconds or 0.0,
                ),
                "dry_run": MetadataValue.bool(effective_dry_run),
                "row_errors_count": MetadataValue.int(len(result.row_errors)),
            }
            row_count_inputs[key] = _SourceRowCountInput(
                source=source,
                profile=profile,
                sync_config=sync_config,
                project_path=project_path,
                query_tagging=project.query_tagging,
                cursor_value_used=result.cursor_value_used,
            )
            if is_asset_context:
                yield MaterializeResult(asset_key=key, metadata=metadata)
            else:
                yield AssetMaterialization(asset_key=key, metadata=metadata)
