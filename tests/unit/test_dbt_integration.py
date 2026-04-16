"""Tests for dbt manifest reader."""

from __future__ import annotations

import json
from pathlib import Path


def test_resolve_ref_from_dbt_manifest(tmp_path: Path) -> None:
    from drt.integrations.dbt import resolve_ref_from_manifest

    manifest = {
        "nodes": {
            "model.my_project.my_model": {
                "relation_name": '"analytics"."public"."my_model"',
                "name": "my_model",
            }
        }
    }
    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(manifest))

    result = resolve_ref_from_manifest("my_model", tmp_path)
    assert result == '"analytics"."public"."my_model"'


def test_resolve_ref_not_found_returns_none(tmp_path: Path) -> None:
    from drt.integrations.dbt import resolve_ref_from_manifest

    manifest = {"nodes": {}}
    manifest_path = tmp_path / "target" / "manifest.json"
    manifest_path.parent.mkdir(parents=True)
    manifest_path.write_text(json.dumps(manifest))

    result = resolve_ref_from_manifest("nonexistent", tmp_path)
    assert result is None


def test_no_manifest_returns_none(tmp_path: Path) -> None:
    from drt.integrations.dbt import resolve_ref_from_manifest

    result = resolve_ref_from_manifest("my_model", tmp_path)
    assert result is None


def test_custom_manifest_path(tmp_path: Path) -> None:
    from drt.integrations.dbt import resolve_ref_from_manifest

    manifest = {
        "nodes": {
            "model.proj.users": {
                "relation_name": '"warehouse"."dbt"."users"',
                "name": "users",
            }
        }
    }
    custom_path = tmp_path / "custom" / "manifest.json"
    custom_path.parent.mkdir(parents=True)
    custom_path.write_text(json.dumps(manifest))

    result = resolve_ref_from_manifest("users", tmp_path, manifest_path=custom_path)
    assert result == '"warehouse"."dbt"."users"'
