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
from dataclasses import dataclass
from pathlib import Path


@dataclass
class DbtModel:
    """A model extracted from dbt manifest.json."""

    name: str
    relation_name: str | None
    description: str
    resource_type: str


def list_models_from_manifest(
    manifest_path: Path,
) -> list[DbtModel]:
    """List all models from a dbt manifest.json.

    Returns a list of DbtModel with name, relation_name, and description.
    """
    if not manifest_path.exists():
        raise FileNotFoundError(f"Manifest not found: {manifest_path}")

    manifest = json.loads(manifest_path.read_text())
    nodes = manifest.get("nodes", {})

    models: list[DbtModel] = []
    for node in nodes.values():
        if node.get("resource_type") != "model":
            continue
        models.append(
            DbtModel(
                name=node.get("name", ""),
                relation_name=node.get("relation_name"),
                description=node.get("description", ""),
                resource_type=node.get("resource_type", "model"),
            )
        )

    return sorted(models, key=lambda m: m.name)


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
            rel: str | None = node.get("relation_name")
            return rel

    return None
