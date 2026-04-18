"""JSON Schema generation from Pydantic models.

Used for:
- YAML editor autocomplete (drt validate --emit-schema)
- LLM-readable API reference (docs/llm/)
"""

import json
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft7Validator

from drt.config.models import ProjectConfig, SyncConfig

JSON_SCHEMA_DRAFT_07 = "http://json-schema.org/draft-07/schema#"


def generate_project_schema() -> dict[str, Any]:
    schema = ProjectConfig.model_json_schema()
    schema["$schema"] = JSON_SCHEMA_DRAFT_07
    return schema


def generate_sync_schema() -> dict[str, Any]:
    schema = SyncConfig.model_json_schema()
    schema["$schema"] = JSON_SCHEMA_DRAFT_07
    return schema


def write_schemas(output_dir: Path) -> list[Path]:
    """Write drt_project.schema.json and sync.schema.json to output_dir.

    Returns list of written file paths.
    """
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    project_path = output_dir / "drt_project.schema.json"
    project_path.write_text(json.dumps(generate_project_schema(), indent=2))
    written.append(project_path)

    sync_path = output_dir / "sync.schema.json"
    sync_path.write_text(json.dumps(generate_sync_schema(), indent=2))
    written.append(sync_path)

    return written


def validate_yaml_against_schema(yaml_path: Path, schema: dict[str, Any]) -> list[str]:
    """Validate a YAML file against a JSON schema.

    Args:
        yaml_path: Path to the YAML file to validate.
        schema: The JSON schema to validate against.

    Returns:
        List of error messages. Empty list if validation passes.
    """
    with open(yaml_path) as f:
        data = yaml.safe_load(f)

    errors: list[str] = []
    validator = Draft7Validator(schema)
    for error in validator.iter_errors(data):
        # Format: path → key: message (e.g., "destination → type: 'rest_api' is not one of...")
        path = " → ".join(str(part) for part in error.path) if error.path else "(root)"
        errors.append(f"{path}: {error.message}")

    return errors
