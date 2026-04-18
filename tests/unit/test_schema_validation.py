"""Tests for JSON Schema generation and validation."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml

from drt.config.schema import (
    generate_project_schema,
    generate_sync_schema,
    validate_yaml_against_schema,
    write_schemas,
)


class TestGenerateSchemas:
    """Test schema generation functions."""

    def test_generate_project_schema_includes_schema_key(self) -> None:
        """Project schema should include $schema key."""
        schema = generate_project_schema()
        assert "$schema" in schema
        assert schema["$schema"] == "http://json-schema.org/draft-07/schema#"

    def test_generate_project_schema_has_required_properties(self) -> None:
        """Project schema should have properties for ProjectConfig."""
        schema = generate_project_schema()
        assert "properties" in schema
        assert "name" in schema["properties"]
        assert "version" in schema["properties"]
        assert "profile" in schema["properties"]

    def test_generate_sync_schema_includes_schema_key(self) -> None:
        """Sync schema should include $schema key."""
        schema = generate_sync_schema()
        assert "$schema" in schema
        assert schema["$schema"] == "http://json-schema.org/draft-07/schema#"

    def test_generate_sync_schema_has_required_properties(self) -> None:
        """Sync schema should have properties for SyncConfig."""
        schema = generate_sync_schema()
        assert "properties" in schema
        assert "name" in schema["properties"]
        assert "model" in schema["properties"]
        assert "destination" in schema["properties"]

    def test_schemas_are_valid_json(self) -> None:
        """Generated schemas should be valid JSON."""
        project_schema = generate_project_schema()
        sync_schema = generate_sync_schema()

        # Should serialize/deserialize without error
        project_json = json.dumps(project_schema)
        sync_json = json.dumps(sync_schema)
        assert json.loads(project_json) == project_schema
        assert json.loads(sync_json) == sync_schema


class TestWriteSchemas:
    """Test schema file writing."""

    def test_write_schemas_creates_directory(self, tmp_path: Path) -> None:
        """write_schemas should create output directory if it doesn't exist."""
        schema_dir = tmp_path / "schemas"
        assert not schema_dir.exists()

        write_schemas(schema_dir)

        assert schema_dir.exists()
        assert schema_dir.is_dir()

    def test_write_schemas_creates_both_files(self, tmp_path: Path) -> None:
        """write_schemas should create both project and sync schema files."""
        written = write_schemas(tmp_path)

        assert len(written) == 2
        assert written[0].name == "drt_project.schema.json"
        assert written[1].name == "sync.schema.json"
        assert written[0].exists()
        assert written[1].exists()

    def test_write_schemas_files_contain_schema_key(self, tmp_path: Path) -> None:
        """Written schema files should contain $schema key."""
        write_schemas(tmp_path)

        with open(tmp_path / "sync.schema.json") as f:
            sync_schema = json.load(f)
        with open(tmp_path / "drt_project.schema.json") as f:
            project_schema = json.load(f)

        assert sync_schema["$schema"] == "http://json-schema.org/draft-07/schema#"
        assert project_schema["$schema"] == "http://json-schema.org/draft-07/schema#"

    def test_write_schemas_returns_paths(self, tmp_path: Path) -> None:
        """write_schemas should return list of written paths."""
        written = write_schemas(tmp_path)

        assert isinstance(written, list)
        assert all(isinstance(p, Path) for p in written)
        assert all(p.exists() for p in written)


class TestValidateYamlAgainstSchema:
    """Test YAML schema validation."""

    def test_valid_sync_yaml_passes(self, tmp_path: Path) -> None:
        """Valid sync YAML should pass validation."""
        yaml_file = tmp_path / "sync.yml"
        yaml_file.write_text("""
name: test_sync
description: "Test sync"
model: ref('test')
destination:
  type: slack
  webhook_url: https://hooks.slack.com/services/XXX/YYY/ZZZ
sync:
  mode: full
  batch_size: 100
""")
        schema = generate_sync_schema()
        errors = validate_yaml_against_schema(yaml_file, schema)

        assert errors == []

    def test_missing_required_field_fails(self, tmp_path: Path) -> None:
        """YAML missing required fields should fail validation."""
        yaml_file = tmp_path / "sync.yml"
        yaml_file.write_text("""
name: test_sync
# missing model and destination
sync:
  mode: full
""")
        schema = generate_sync_schema()
        errors = validate_yaml_against_schema(yaml_file, schema)

        assert len(errors) > 0
        assert any("model" in err or "destination" in err for err in errors)

    def test_invalid_enum_value_fails(self, tmp_path: Path) -> None:
        """Invalid enum value should fail validation."""
        yaml_file = tmp_path / "sync.yml"
        yaml_file.write_text("""
name: test_sync
model: ref('test')
destination:
  type: slack
  webhook_url: https://hooks.slack.com/services/XXX/YYY/ZZZ
sync:
  mode: invalid_mode
  batch_size: 100
""")
        schema = generate_sync_schema()
        errors = validate_yaml_against_schema(yaml_file, schema)

        assert len(errors) > 0
        assert any("invalid_mode" in err or "mode" in err for err in errors)

    def test_invalid_yaml_syntax_returns_error(self, tmp_path: Path) -> None:
        """Invalid YAML syntax should return graceful error message."""
        yaml_file = tmp_path / "sync.yml"
        yaml_file.write_text("""
name: test_sync
invalid: yaml: syntax: here:
""")
        schema = generate_sync_schema()
        errors = validate_yaml_against_schema(yaml_file, schema)

        assert len(errors) > 0
        assert errors[0].startswith("(root):")

    def test_error_messages_formatted_clearly(self, tmp_path: Path) -> None:
        """Error messages should be formatted with path context."""
        yaml_file = tmp_path / "sync.yml"
        yaml_file.write_text("""
name: test_sync
model: ref('test')
destination:
  type: invalid_type
sync:
  mode: full
""")
        schema = generate_sync_schema()
        errors = validate_yaml_against_schema(yaml_file, schema)

        # Should have errors and they should mention the path
        assert len(errors) > 0
        # Error format should include path indicator (→ symbol)
        assert any("→" in err or "destination" in err for err in errors)

    def test_valid_rest_api_destination(self, tmp_path: Path) -> None:
        """Valid REST API destination config should pass."""
        yaml_file = tmp_path / "sync.yml"
        yaml_file.write_text("""
name: post_data
description: "POST data to REST API"
model: ref('users')
destination:
  type: rest_api
  url: https://api.example.com/webhook
  method: POST
  headers:
    Authorization: Bearer token
sync:
  mode: full
  batch_size: 50
""")
        schema = generate_sync_schema()
        errors = validate_yaml_against_schema(yaml_file, schema)

        assert errors == []

    def test_valid_project_config_passes(self, tmp_path: Path) -> None:
        """Valid project config YAML should pass validation."""
        yaml_file = tmp_path / "project.yml"
        yaml_file.write_text("""
name: my_project
version: "1.0"
profile: default
""")
        schema = generate_project_schema()
        errors = validate_yaml_against_schema(yaml_file, schema)

        assert errors == []

    def test_errors_sorted_consistently(self, tmp_path: Path) -> None:
        """Validation errors should be sorted consistently."""
        yaml_file = tmp_path / "sync.yml"
        yaml_file.write_text("""
invalid: yaml: syntax:
""")
        schema = generate_sync_schema()
        errors = validate_yaml_against_schema(yaml_file, schema)

        # Run validation again
        errors2 = validate_yaml_against_schema(yaml_file, schema)

        # Errors should be in same order both times
        assert errors == errors2
