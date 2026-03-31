"""dbt integration — resolve ref() from dbt manifest.json.

When a dbt project is co-located with a drt project, drt can read
target/manifest.json to resolve ref('model_name') to the fully-qualified
table name that dbt materialized.

Usage:
    from drt.integrations.dbt import resolve_ref_from_manifest
    table = resolve_ref_from_manifest("my_model", project_dir)
    # Returns: '"analytics"."public"."my_model"' or None
"""
from __future__ import annotations

import json
from pathlib import Path


def resolve_ref_from_manifest(
    model_name: str,
    project_dir: Path,
    manifest_path: Path | None = None,
) -> str | None:
    """Resolve a model name to a fully-qualified table using dbt manifest.

    Looks for target/manifest.json in the project directory.
    Returns the relation_name if found, None otherwise.
    """
    mpath = manifest_path or (project_dir / "target" / "manifest.json")
    if not mpath.exists():
        return None

    manifest = json.loads(mpath.read_text())
    nodes = manifest.get("nodes", {})

    for node in nodes.values():
        if node.get("name") == model_name:
            return node.get("relation_name")

    return None
