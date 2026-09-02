"""JSON Schema generation from Pydantic models.

Used for:
- YAML editor autocomplete (drt validate --emit-schema)
- LLM-readable API reference (docs/llm/)
"""

import json
from pathlib import Path
from typing import Any

import yaml
from jsonschema import validators
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaValue
from pydantic_core import core_schema

from drt.config.base import _registered_plugin_destination_type_schema
from drt.config.models import ProjectConfig, SyncConfig

JSON_SCHEMA_DRAFT_07 = "http://json-schema.org/draft-07/schema#"


class DeterministicGenerateJsonSchema(GenerateJsonSchema):
    """Normalize schema shapes that vary across supported pydantic 2.x releases."""

    def literal_schema(self, schema: core_schema.LiteralSchema) -> JsonSchemaValue:
        json_schema = super().literal_schema(schema)

        # Pydantic 2.5--2.9 variously omitted ``type`` or duplicated a
        # single-value Literal as both ``const`` and ``enum``. Match 2.13.5:
        # one ``const`` plus the JSON type.
        if len(schema["expected"]) == 1:
            json_schema.pop("enum", None)
            json_type = {
                str: "string",
                int: "integer",
                float: "number",
                bool: "boolean",
                list: "array",
                type(None): "null",
            }.get(type(json_schema.get("const")))
            if json_type is not None:
                json_schema["type"] = json_type

        return json_schema

    def dict_schema(self, schema: core_schema.DictSchema) -> JsonSchemaValue:
        json_schema = super().dict_schema(schema)

        # Pydantic 2.11 made the permissive value semantics of dict[str, Any]
        # explicit. Preserve that 2.13.5 shape on every supported version.
        if "additionalProperties" not in json_schema and "patternProperties" not in json_schema:
            json_schema["additionalProperties"] = True

        return json_schema


def generate_project_schema() -> dict[str, Any]:
    schema = ProjectConfig.model_json_schema(schema_generator=DeterministicGenerateJsonSchema)
    schema["$schema"] = JSON_SCHEMA_DRAFT_07
    return schema


def generate_sync_schema() -> dict[str, Any]:
    schema = SyncConfig.model_json_schema(schema_generator=DeterministicGenerateJsonSchema)
    # Pydantic 2.5--2.10 drops GenericDestinationConfig's JSON-schema hook
    # when composing it with the model's wrap validator (#1081). Reapply the
    # shared schema unconditionally so live plugin reflection never depends on
    # that pydantic implementation detail.
    schema["$defs"]["GenericDestinationConfig"]["properties"]["type"] = (
        _registered_plugin_destination_type_schema()
    )
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
    try:
        with open(yaml_path) as f:
            data = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        return [f"(root): {exc}"]

    errors: list[str] = []
    validator_cls = validators.validator_for(schema)
    validator = validator_cls(schema)

    for error in sorted(
        validator.iter_errors(data),
        key=lambda err: (
            tuple(str(part) for part in err.path),
            tuple(str(part) for part in err.schema_path),
            err.message,
        ),
    ):
        # Format: path → key: message (e.g., "destination → type: 'rest_api' is not one of...")
        path = " → ".join(str(part) for part in error.path) if error.path else "(root)"
        errors.append(f"{path}: {error.message}")

    return errors
