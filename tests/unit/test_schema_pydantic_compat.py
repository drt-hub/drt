"""Exact schema-shape compatibility tests across supported pydantic releases.

These tests enforce the invariants provided by
``DeterministicGenerateJsonSchema`` and must be rerun whenever the pydantic
version pinned by the ``schema-gen`` extra changes. They should also be
exercised under representative 2.5.x, 2.9.x, and 2.13.x environments (a
manual matrix is sufficient; the repository has no multi-pydantic CI matrix).
"""

from __future__ import annotations

from pytest import MonkeyPatch

from drt.config.schema import generate_project_schema, generate_sync_schema
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
