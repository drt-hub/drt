"""DagsterDrtTranslator — controls how drt syncs map to Dagster assets.

Follows the same Translator pattern as dagster-dbt and dagster-dlt.
The primary override point is ``get_asset_spec()``. Legacy per-attribute
methods (``get_asset_key``, ``get_group_name``, etc.) still work but are
deprecated in favour of ``get_asset_spec()``.
"""

from __future__ import annotations

import warnings
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dagster import AssetKey, AssetSpec

from drt.config.models import SyncConfig


@dataclass(frozen=True)
class DrtTranslatorData:
    """Data passed to :meth:`DagsterDrtTranslator.get_asset_spec`.

    Attributes:
        sync_config: The parsed drt sync configuration.
        project_dir: Path to the drt project root (as string).
    """

    sync_config: SyncConfig
    project_dir: str


# Internal sentinel to detect whether a legacy method was overridden.
_SENTINEL = object()


class DagsterDrtTranslator:
    """Default translator from drt SyncConfig to Dagster asset properties.

    The recommended override point is ``get_asset_spec()``::

        class MyTranslator(DagsterDrtTranslator):
            def get_asset_spec(self, data):
                default = super().get_asset_spec(data)
                return default.replace_attributes(group_name="reverse_etl")

    Legacy per-attribute methods still work but emit deprecation warnings.
    """

    # ------------------------------------------------------------------
    # Primary API (new)
    # ------------------------------------------------------------------

    def get_asset_spec(self, data: DrtTranslatorData) -> AssetSpec:
        """Return a complete ``AssetSpec`` for a drt sync.

        Override this method to customise how syncs map to Dagster assets.
        """
        sync_config = data.sync_config

        # Check if any legacy methods were overridden in a subclass.
        # If so, delegate to them for backward compatibility.
        if self._has_legacy_overrides():
            return self._build_spec_from_legacy(sync_config)

        dest = sync_config.destination
        dest_type = dest.type if hasattr(dest, "type") else "unknown"

        kwargs: dict[str, Any] = {
            "key": AssetKey(f"drt_{sync_config.name}"),
            "kinds": {"drt", dest_type},
        }
        if sync_config.description:
            kwargs["description"] = sync_config.description

        return AssetSpec(**kwargs)

    # ------------------------------------------------------------------
    # Legacy API (deprecated — kept for backward compat)
    # ------------------------------------------------------------------

    def get_asset_key(self, sync_config: SyncConfig) -> AssetKey:
        """Return the Dagster AssetKey for a sync.

        .. deprecated::
            Override ``get_asset_spec()`` instead.
        """
        return AssetKey(f"drt_{sync_config.name}")

    def get_group_name(self, sync_config: SyncConfig) -> str | None:
        """Return the group name for a sync asset, or None for default.

        .. deprecated::
            Override ``get_asset_spec()`` instead.
        """
        return None

    def get_description(self, sync_config: SyncConfig) -> str | None:
        """Return the asset description.

        .. deprecated::
            Override ``get_asset_spec()`` instead.
        """
        return sync_config.description

    def get_deps(self, sync_config: SyncConfig) -> Sequence[AssetKey]:
        """Return upstream asset dependencies for a sync.

        .. deprecated::
            Override ``get_asset_spec()`` instead.
        """
        return []

    def get_metadata(self, sync_config: SyncConfig) -> Mapping[str, Any]:
        """Return static metadata to attach to the asset definition.

        .. deprecated::
            Override ``get_asset_spec()`` instead.
        """
        return {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    _LEGACY_METHODS = (
        "get_asset_key",
        "get_group_name",
        "get_description",
        "get_deps",
        "get_metadata",
    )

    def _has_legacy_overrides(self) -> bool:
        """Return True if any legacy per-attribute method was overridden."""
        for name in self._LEGACY_METHODS:
            if getattr(type(self), name) is not getattr(DagsterDrtTranslator, name):
                return True
        return False

    def _build_spec_from_legacy(self, sync_config: SyncConfig) -> AssetSpec:
        """Build an AssetSpec by calling legacy per-attribute methods."""
        warnings.warn(
            "Overriding individual translator methods (get_asset_key, "
            "get_group_name, etc.) is deprecated. Override get_asset_spec() "
            "instead.",
            DeprecationWarning,
            stacklevel=4,
        )
        key = self.get_asset_key(sync_config)
        group_name = self.get_group_name(sync_config)
        description = self.get_description(sync_config)
        deps = self.get_deps(sync_config)
        metadata = self.get_metadata(sync_config)

        dest = sync_config.destination
        dest_type = dest.type if hasattr(dest, "type") else "unknown"

        kwargs: dict[str, Any] = {
            "key": key,
            "kinds": {"drt", dest_type},
        }
        if group_name is not None:
            kwargs["group_name"] = group_name
        if description:
            kwargs["description"] = description
        if deps:
            kwargs["deps"] = deps
        if metadata:
            kwargs["metadata"] = metadata

        return AssetSpec(**kwargs)
