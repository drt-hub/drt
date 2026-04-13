"""Tests for dbt init integration — list_models_from_manifest."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from drt.integrations.dbt import list_models_from_manifest


def _manifest(models: list[dict]) -> dict:
    """Build a minimal dbt manifest.json."""
    nodes = {}
    for m in models:
        key = f"model.project.{m['name']}"
        nodes[key] = {
            "resource_type": "model",
            "name": m["name"],
            "relation_name": m.get("relation_name"),
            "description": m.get("description", ""),
        }
    return {"nodes": nodes}


def test_list_models(tmp_path: Path) -> None:
    manifest = _manifest([
        {"name": "users", "relation_name": '"analytics"."users"'},
        {"name": "orders", "relation_name": '"analytics"."orders"'},
    ])
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))

    models = list_models_from_manifest(path)
    assert len(models) == 2
    assert models[0].name == "orders"  # sorted
    assert models[1].name == "users"
    assert models[1].relation_name == '"analytics"."users"'


def test_list_models_skips_non_models(tmp_path: Path) -> None:
    manifest = {
        "nodes": {
            "model.p.users": {
                "resource_type": "model",
                "name": "users",
                "relation_name": '"users"',
                "description": "",
            },
            "test.p.test_users": {
                "resource_type": "test",
                "name": "test_users",
                "description": "",
            },
            "seed.p.seed_data": {
                "resource_type": "seed",
                "name": "seed_data",
                "description": "",
            },
        }
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))

    models = list_models_from_manifest(path)
    assert len(models) == 1
    assert models[0].name == "users"


def test_list_models_empty_manifest(tmp_path: Path) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"nodes": {}}))

    models = list_models_from_manifest(path)
    assert models == []


def test_list_models_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        list_models_from_manifest(tmp_path / "nonexistent.json")


def test_list_models_with_description(tmp_path: Path) -> None:
    manifest = _manifest([
        {
            "name": "users",
            "relation_name": '"users"',
            "description": "All active users",
        },
    ])
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(manifest))

    models = list_models_from_manifest(path)
    assert models[0].description == "All active users"
