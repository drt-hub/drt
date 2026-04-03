"""DagsterDrtTranslator — controls how drt syncs map to Dagster assets.

Follows the same Translator pattern as dagster-dbt and dagster-dlt.
Subclass and override methods to customise asset keys, groups, deps, etc.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dagster import AssetKey

from drt.config.models import SyncConfig


class DagsterDrtTranslator:
    """Default translator from drt SyncConfig to Dagster asset properties.

    Override any method in a subclass to customise behavior::

        class MyTranslator(DagsterDrtTranslator):
            def get_group_name(self, sync_config):
                return "reverse_etl"

        drt_assets(project_dir="...", dagster_drt_translator=MyTranslator())
    """

    def get_asset_key(self, sync_config: SyncConfig) -> AssetKey:
        """Return the Dagster AssetKey for a sync."""
        return AssetKey(f"drt_{sync_config.name}")

    def get_group_name(self, sync_config: SyncConfig) -> str | None:
        """Return the group name for a sync asset, or None for default."""
        return None

    def get_description(self, sync_config: SyncConfig) -> str | None:
        """Return the asset description."""
        return sync_config.description

    def get_deps(self, sync_config: SyncConfig) -> Sequence[AssetKey]:
        """Return upstream asset dependencies for a sync."""
        return []

    def get_metadata(self, sync_config: SyncConfig) -> Mapping[str, Any]:
        """Return static metadata to attach to the asset definition."""
        return {}
