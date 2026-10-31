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
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal


@dataclass
class DbtModel:
    """A model extracted from dbt manifest.json."""

    name: str
    relation_name: str | None
    description: str
    resource_type: str


PortableType = Literal["string", "number", "integer", "boolean", "timestamp", "date", "json"]


@dataclass(frozen=True)
class DbtContractColumn:
    """One dbt-declared model column normalized for drt's portable contract."""

    name: str
    type: PortableType
    description: str | None = None


@dataclass(frozen=True)
class DbtContractModel:
    """The single dbt model selected for ``contract.source: dbt``."""

    name: str
    package_name: str
    relation_name: str | None
    columns: tuple[DbtContractColumn, ...]


_DBT_TYPE_ALIASES: dict[str, PortableType] = {
    "varchar": "string",
    "text": "string",
    "string": "string",
    "int": "integer",
    "bigint": "integer",
    "smallint": "integer",
    "numeric": "number",
    "decimal": "number",
    "float": "number",
    "double": "number",
    "double precision": "number",
    "real": "number",
    "bool": "boolean",
    "boolean": "boolean",
    "datetime": "timestamp",
    "date": "date",
    "json": "json",
    "jsonb": "json",
    "variant": "json",
    "object": "json",
    "array": "json",
}


def portable_type_from_dbt(data_type: str) -> PortableType | None:
    """Map a dbt ``data_type`` to drt's portable vocabulary.

    dbt preserves adapter-specific spelling. Matching is case-insensitive and
    ignores precision/length suffixes such as ``numeric(10, 2)`` and
    ``varchar(255)``. All timestamp adapter variants intentionally collapse to
    ``timestamp``.
    """
    normalized = re.sub(r"\([^)]*\)", "", data_type).strip().lower()
    normalized = " ".join(normalized.split())
    if normalized.startswith("timestamp"):
        return "timestamp"
    return _DBT_TYPE_ALIASES.get(normalized)


def _read_manifest(manifest_path: Path) -> dict[str, Any]:
    if not manifest_path.exists():
        raise FileNotFoundError(f"dbt manifest not found: {manifest_path}")
    try:
        value = json.loads(manifest_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not read dbt manifest {manifest_path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"dbt manifest {manifest_path} must contain a JSON object.")
    return value


def _package_name(unique_id: str, node: dict[str, Any]) -> str:
    declared = node.get("package_name")
    if declared:
        return str(declared)
    parts = unique_id.split(".", 2)
    return parts[1] if len(parts) == 3 else "<unknown>"


def resolve_contract_from_manifest(model_name: str, manifest_path: Path) -> DbtContractModel:
    """Resolve and normalize the dbt model used by a dbt-sourced contract.

    Unqualified ``ref('name')`` is deliberately rejected when more than one
    package contributes that model name. drt does not guess which package dbt
    would select from project dependency context.
    """
    manifest = _read_manifest(manifest_path)
    nodes = manifest.get("nodes", {})
    if not isinstance(nodes, dict):
        raise ValueError(f"dbt manifest {manifest_path} has an invalid 'nodes' object.")

    matches: list[tuple[str, dict[str, Any]]] = []
    for unique_id, candidate in nodes.items():
        if not isinstance(candidate, dict):
            continue
        if candidate.get("resource_type") != "model" or candidate.get("name") != model_name:
            continue
        matches.append((str(unique_id), candidate))

    if not matches:
        raise ValueError(
            f"dbt model {model_name!r} from ref({model_name!r}) was not found in {manifest_path}."
        )
    if len(matches) > 1:
        packages = sorted({_package_name(unique_id, node) for unique_id, node in matches})
        raise ValueError(
            f"dbt ref({model_name!r}) is ambiguous in {manifest_path}; model "
            f"{model_name!r} exists in packages: {', '.join(packages)}."
        )

    unique_id, node = matches[0]
    package_name = _package_name(unique_id, node)
    declared = node.get("columns")
    if not isinstance(declared, dict) or not declared:
        raise ValueError(f"dbt model {model_name!r} has no declared columns in {manifest_path}.")

    columns: list[DbtContractColumn] = []
    for key, value in declared.items():
        if not isinstance(value, dict):
            raise ValueError(f"dbt model {model_name!r} column {str(key)!r} is invalid.")
        column_name = str(value.get("name") or key)
        raw_type = value.get("data_type")
        if not isinstance(raw_type, str) or not raw_type.strip():
            raise ValueError(
                f"dbt model {model_name!r} column {column_name!r} has no declared data_type."
            )
        portable_type = portable_type_from_dbt(raw_type)
        if portable_type is None:
            raise ValueError(
                f"dbt model {model_name!r} column {column_name!r} has unmappable "
                f"data_type {raw_type!r}."
            )
        raw_description = value.get("description")
        description = str(raw_description) if raw_description not in (None, "") else None
        columns.append(DbtContractColumn(column_name, portable_type, description))

    return DbtContractModel(
        name=model_name,
        package_name=package_name,
        relation_name=node.get("relation_name"),
        columns=tuple(columns),
    )


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
