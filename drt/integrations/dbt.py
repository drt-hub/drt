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
    "char": "string",
    "character": "string",
    "character varying": "string",
    "nvarchar": "string",
    "int": "integer",
    "integer": "integer",
    "int64": "integer",
    "bigint": "integer",
    "smallint": "integer",
    "tinyint": "integer",
    "numeric": "number",
    "number": "number",
    "decimal": "number",
    "bignumeric": "number",
    "float": "number",
    "float64": "number",
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
    "struct": "json",
    "record": "json",
    "map": "json",
}


def portable_type_from_dbt(data_type: str) -> PortableType | None:
    """Map a dbt ``data_type`` to drt's portable vocabulary.

    dbt preserves adapter-specific spelling. Matching is case-insensitive and
    ignores precision/length suffixes such as ``numeric(10, 2)`` and
    ``varchar(255)``. All timestamp adapter variants intentionally collapse to
    ``timestamp``.
    """
    normalized = re.sub(r"\([^)]*\)", "", data_type)
    # BigQuery spells composites ARRAY<STRING> / STRUCT<a INT64>.
    normalized = re.sub(r"<.*>", "", normalized).strip().lower()
    normalized = " ".join(normalized.split())
    if normalized.startswith("timestamp"):
        return "timestamp"
    return _DBT_TYPE_ALIASES.get(normalized)


def _safe(value: object, limit: int = 80) -> str:
    """Make manifest-sourced text safe to put in a message.

    A manifest is an artifact that can be edited or generated elsewhere: strip control
    characters and square brackets (Rich markup), and bound the length.
    """
    text = "".join(ch if ch.isprintable() and ch not in "[]" else "?" for ch in str(value))
    return text if len(text) <= limit else text[: limit - 1] + "…"


# One parse per manifest per process: every dbt-backed sync, and ``plan --all`` reloading
# the syncs once per name, would otherwise re-parse the whole (often tens of MB) manifest.
_MANIFEST_CACHE: dict[tuple[str, int, int], dict[str, Any]] = {}


def _read_manifest(manifest_path: Path) -> dict[str, Any]:
    if not manifest_path.exists():
        raise FileNotFoundError(f"dbt manifest not found: {manifest_path}")
    try:
        stat = manifest_path.stat()
        key = (str(manifest_path.resolve()), stat.st_mtime_ns, stat.st_size)
        cached = _MANIFEST_CACHE.get(key)
        if cached is not None:
            return cached
        value = json.loads(manifest_path.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Could not read dbt manifest {manifest_path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ValueError(f"dbt manifest {manifest_path} must contain a JSON object.")
    _MANIFEST_CACHE.clear()  # keep at most the latest manifest in memory
    _MANIFEST_CACHE[key] = value
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
        packages = sorted({_safe(_package_name(unique_id, node)) for unique_id, node in matches})
        if any(node.get("version") is not None for _, node in matches):
            raise ValueError(
                f"dbt model {model_name!r} has several versions in {manifest_path}; drt does "
                "not choose a dbt model version. Write the columns: block by hand instead."
            )
        raise ValueError(
            f"dbt ref({model_name!r}) is ambiguous in {manifest_path}; model "
            f"{model_name!r} exists in packages: {', '.join(packages)}. "
            "Package-qualified ref() is not supported; write the columns: block by hand."
        )

    unique_id, node = matches[0]
    package_name = _package_name(unique_id, node)
    config = node.get("config")
    contract = config.get("contract") if isinstance(config, dict) else None
    if not (isinstance(contract, dict) and contract.get("enforced") is True):
        raise ValueError(
            f"dbt model {model_name!r} has no enforced dbt contract "
            "(config.contract.enforced: true) in the manifest; "
            "contract.source: dbt derives columns only from an enforced dbt contract."
        )
    declared = node.get("columns")
    if not isinstance(declared, dict) or not declared:
        raise ValueError(f"dbt model {model_name!r} has no declared columns in {manifest_path}.")

    columns: list[DbtContractColumn] = []
    for key, value in declared.items():
        if not isinstance(value, dict):
            raise ValueError(f"dbt model {model_name!r} column {_safe(key)!r} is invalid.")
        column_name = str(value.get("name") or key)
        raw_type = value.get("data_type")
        if not isinstance(raw_type, str) or not raw_type.strip():
            raise ValueError(
                f"dbt model {model_name!r} column {_safe(column_name)!r} has no declared data_type."
            )
        portable_type = portable_type_from_dbt(raw_type)
        if portable_type is None:
            raise ValueError(
                f"dbt model {model_name!r} column {_safe(column_name)!r} has unmappable "
                f"data_type {_safe(raw_type)!r}."
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
