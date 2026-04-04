"""Pure spec generation — decoupled from execution logic.

``build_drt_asset_specs()`` returns a list of ``AssetSpec`` objects that
describe drt syncs as Dagster assets, without attaching any compute
function.  This enables Pipes-based remote execution and other custom
patterns.
"""

from __future__ import annotations

from pathlib import Path

from dagster import AssetSpec

from dagster_drt.translator import DagsterDrtTranslator, DrtTranslatorData

# Metadata keys used to store drt-specific info in AssetSpec metadata.
META_KEY_SYNC_NAME = "dagster_drt/sync_name"
META_KEY_PROJECT_DIR = "dagster_drt/project_dir"


def build_drt_asset_specs(
    project_dir: str | Path,
    sync_names: list[str] | None = None,
    dagster_drt_translator: DagsterDrtTranslator | None = None,
) -> list[AssetSpec]:
    """Generate Dagster ``AssetSpec`` objects from drt sync definitions.

    This function only generates specs — no execution logic is attached.
    Use the returned specs with ``@multi_asset`` or ``@drt_assets`` to
    add execution.

    Args:
        project_dir: Path to drt project root.
        sync_names: Optional filter. If None, discovers all syncs.
        dagster_drt_translator: Translator for customising asset specs.

    Returns:
        List of ``AssetSpec`` objects, one per sync.
    """
    from drt.config.parser import load_syncs

    translator = dagster_drt_translator or DagsterDrtTranslator()
    project_path = Path(project_dir)
    syncs = load_syncs(project_path)

    if sync_names:
        syncs = [s for s in syncs if s.name in sync_names]

    specs: list[AssetSpec] = []
    for sync_config in syncs:
        data = DrtTranslatorData(
            sync_config=sync_config,
            project_dir=str(project_path),
        )
        spec = translator.get_asset_spec(data)

        # Merge internal metadata keys for downstream use by
        # DagsterDrtResource and @drt_assets.
        internal_metadata = {
            META_KEY_SYNC_NAME: sync_config.name,
            META_KEY_PROJECT_DIR: str(project_path),
        }
        existing_metadata = dict(spec.metadata or {})
        existing_metadata.update(internal_metadata)
        spec = spec.replace_attributes(metadata=existing_metadata)

        specs.append(spec)

    return specs
