"""Exact schema-shape compatibility tests across supported pydantic releases.

These tests enforce the invariants provided by
``DeterministicGenerateJsonSchema`` and must be rerun whenever the pydantic
version pinned by the ``schema-gen`` extra changes. They should also be
exercised under representative 2.5.x, 2.9.x, and 2.13.x environments (a
manual matrix is sufficient; the repository has no multi-pydantic CI matrix).
"""

from __future__ import annotations

from unittest.mock import patch

from pydantic.json_schema import GenerateJsonSchema
from pydantic_core import core_schema
from pytest import MonkeyPatch

from drt.config.schema import (
    DeterministicGenerateJsonSchema,
    generate_project_schema,
    generate_sync_schema,
)
from drt.connectors import registry


def test_sync_schema_reflects_registered_plugin_types(monkeypatch: MonkeyPatch) -> None:
    """The top-level generator must not depend on pydantic running the model hook."""
    plugin_type = "schema_compat_plugin"
    monkeypatch.setitem(registry._destination_registry, plugin_type, (object, object))

    type_schema = generate_sync_schema()["$defs"]["GenericDestinationConfig"]["properties"]["type"]

    assert plugin_type in type_schema["enum"]
    assert type_schema["type"] == "string"
    assert type_schema["description"] == (
        "Connector type registered by a third-party package. Built-in types are "
        "validated against their own schema instead."
    )


def test_single_value_literal_has_canonical_shape() -> None:
    type_schema = generate_sync_schema()["$defs"]["AirtableDestinationConfig"]["properties"]["type"]

    assert type_schema == {
        "const": "airtable",
        "title": "Type",
        "type": "string",
    }


def test_unit_test_bare_dict_items_have_canonical_shape() -> None:
    properties = generate_sync_schema()["$defs"]["UnitTest"]["properties"]

    assert properties["given"] == {
        "items": {"additionalProperties": True, "type": "object"},
        "minItems": 1,
        "title": "Given",
        "type": "array",
    }
    assert properties["expect"] == {
        "items": {"additionalProperties": True, "type": "object"},
        "minItems": 1,
        "title": "Expect",
        "type": "array",
    }


def test_project_vars_bare_dict_has_canonical_shape() -> None:
    assert generate_project_schema()["properties"]["vars"] == {
        "additionalProperties": True,
        "title": "Vars",
        "type": "object",
    }


def test_dict_schema_normalizes_pre_2_11_shape_directly() -> None:
    """Exercise the normalization branch regardless of the installed pydantic version.

    Pydantic >=2.11 already includes ``additionalProperties`` by default, so
    this branch is otherwise only reachable by actually installing an older
    pydantic (see this module's docstring). Patching the superclass call
    simulates that pre-2.11 output directly, so the normalization itself is
    covered without a second pydantic version in this environment.
    """
    schema = core_schema.dict_schema()
    with patch.object(GenerateJsonSchema, "dict_schema", return_value={"type": "object"}):
        result = DeterministicGenerateJsonSchema().dict_schema(schema)

    assert result == {"type": "object", "additionalProperties": True}


def test_dict_schema_does_not_override_pattern_properties() -> None:
    """A dict_schema shape that already constrains values must be left alone."""
    schema = core_schema.dict_schema()
    already_shaped = {"type": "object", "patternProperties": {"^x-": {"type": "string"}}}
    with patch.object(GenerateJsonSchema, "dict_schema", return_value=dict(already_shaped)):
        result = DeterministicGenerateJsonSchema().dict_schema(schema)

    assert result == already_shaped
