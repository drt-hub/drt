"""Shared CLI helpers — factories + profile resolution.

Extracted from ``drt/cli/main.py`` in Phase 2 of #546 so that per-command
modules under ``drt/cli/commands/`` can share these utilities without
pulling the whole main module (and its many import dependencies) into
their import graph.

Lives at the same level as ``_app.py`` and ``_connector_detail.py`` —
"underscore-prefixed CLI internals." Not part of the public Python API;
the only stable surface is the ``drt`` CLI itself.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from drt.config.credentials import ProfileConfig
    from drt.config.models import SyncConfig
    from drt.destinations.base import Destination
    from drt.sources.base import Source


def resolve_profile_name(cli_flag: str | None, project_profile: str) -> str:
    """Resolve which profile to use.

    Precedence: ``--profile`` flag > ``DRT_PROFILE`` env var > drt_project.yml
    """
    if cli_flag:
        return cli_flag
    env = os.environ.get("DRT_PROFILE")
    if env:
        return env
    return project_profile


def get_source(profile: ProfileConfig) -> Source:
    """Return a Source instance for the given profile configuration.

    Uses the connector registry for automatic connector discovery and
    instantiation.
    """
    from drt.connectors import get_source as _registry_get_source

    return _registry_get_source(profile)


def get_destination(sync: SyncConfig) -> Destination:
    """Return a Destination instance for the given sync configuration.

    Uses the connector registry for automatic connector discovery and
    instantiation.
    """
    from drt.connectors import get_destination as _registry_get_destination

    return _registry_get_destination(sync.destination)


def get_watermark_storage(sync: SyncConfig, project_dir: Path) -> Any:
    """Build watermark storage from sync config, or ``None`` if not configured."""
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
        return BigQueryWatermarkStorage(
            project=wm.project,
            dataset=wm.dataset,
        )
    return None
