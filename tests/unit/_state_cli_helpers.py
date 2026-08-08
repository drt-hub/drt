"""Test helpers for CLI state-selection integration."""

from __future__ import annotations

import json
from pathlib import Path

from drt.config.fingerprint import sync_fingerprints
from drt.config.parser import load_syncs
from drt.docs.manifest import SCHEMA_VERSION, Manifest, Sync


def write_state_baseline(
    project_dir: Path,
    *,
    schema_version: int = SCHEMA_VERSION,
    filename: str = "baseline-manifest.json",
) -> Path:
    """Write a real manifest baseline for the project's current sync files."""
    fingerprints = sync_fingerprints(project_dir)
    manifest = Manifest(
        schema_version=schema_version,
        drt_version="test",
        syncs=[
            Sync(
                name=sync.name,
                source="default",
                destination=sync.destination.type,
                mode=sync.sync.mode,
                config_hash=fingerprints.get(sync.name),
            )
            for sync in load_syncs(project_dir)
        ],
    )
    path = project_dir / filename
    path.write_text(json.dumps(manifest.to_dict()), encoding="utf-8")
    return path
