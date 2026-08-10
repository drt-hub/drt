"""Baseline manifest comparison for ``state:`` sync selectors (#772)."""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from drt.cli._selection import SelectionError
from drt.config.fingerprint import sync_fingerprints
from drt.docs.manifest import Manifest

if TYPE_CHECKING:
    from drt.config.models import SyncConfig

logger = logging.getLogger(__name__)

# The manifest schema version config_hash was introduced at (ADR 0006, #772).
# Deliberately NOT drt.docs.manifest.SCHEMA_VERSION: that constant moves
# forward for any future additive change, but a baseline only needs to be at
# least this old version to be comparable — pinning to the live SCHEMA_VERSION
# would start rejecting perfectly good v3 baselines the day schema v4 ships
# for something unrelated to config_hash.
_MIN_SCHEMA_VERSION_WITH_CONFIG_HASH = 3


@dataclass(frozen=True)
class StateDiff:
    """Current sync names added or changed relative to a baseline manifest."""

    new: frozenset[str]
    modified: frozenset[str]


def load_state_diff(
    baseline_path: Path, current_syncs: Sequence[SyncConfig], project_dir: Path
) -> StateDiff:
    """Load one baseline manifest and compare it with the current sync tree."""
    current_names = frozenset(sync.name for sync in current_syncs)
    try:
        data: Any = json.loads(baseline_path.read_text(encoding="utf-8"))
        manifest = Manifest.from_dict(data)
    except Exception as exc:  # noqa: BLE001 — a missing/bad first baseline is normal
        logger.warning(
            "Could not load baseline manifest %s (%s); treating every current sync as new.",
            baseline_path,
            exc,
        )
        return StateDiff(new=current_names, modified=current_names)

    # Manifest.from_dict() is a plain dataclass loader with no type
    # validation -- a hand-edited or corrupted baseline can carry a
    # non-numeric schema_version (a string, null) that would otherwise raise
    # a raw, uncaught TypeError from the comparison below. Treat that the
    # same as any other malformed baseline rather than crashing.
    if not isinstance(manifest.schema_version, int):
        logger.warning(
            "Baseline manifest %s has a malformed schema_version (%r); "
            "treating every current sync as new.",
            baseline_path,
            manifest.schema_version,
        )
        return StateDiff(new=current_names, modified=current_names)

    if manifest.schema_version < _MIN_SCHEMA_VERSION_WITH_CONFIG_HASH:
        raise SelectionError(
            f"Baseline manifest schema version {manifest.schema_version} predates config_hash "
            f"support (added in schema version {_MIN_SCHEMA_VERSION_WITH_CONFIG_HASH}); "
            "regenerate the baseline with a current drt version."
        )

    baseline_hashes = {sync.name: sync.config_hash for sync in manifest.syncs}
    current_hashes = sync_fingerprints(project_dir)
    new: set[str] = set()
    modified: set[str] = set()

    # Iterate only current syncs. A baseline-only name represents a deleted sync,
    # which cannot be selected because there is no current definition left to run.
    for name in current_names:
        if name not in baseline_hashes:
            new.add(name)
            modified.add(name)
            continue

        baseline_hash = baseline_hashes[name]
        current_hash = current_hashes.get(name)
        # Missing hashes are uncertainty, never evidence of equality: selecting
        # an extra sync is safer than silently skipping a possibly changed one.
        if baseline_hash is None or current_hash is None or baseline_hash != current_hash:
            modified.add(name)

    return StateDiff(new=frozenset(new), modified=frozenset(modified))
