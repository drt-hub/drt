from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import yaml
from dagster import AssetKey, AssetSpec, Definitions, MaterializeResult, materialize
from dagster.components.scaffold.scaffold import ScaffoldRequest
from dagster.components.testing import create_defs_folder_sandbox
from dagster.components.testing.utils import component_defs
from dagster_drt import DrtSyncComponent
from dagster_drt.components import (
    DrtSyncComponentScaffolder,
    DrtSyncScaffolderParams,
)

from .test_assets import _setup_project


def test_component_scaffolder_writes_requested_defs_yaml(tmp_path: Path) -> None:
    target_path = tmp_path / "drt_syncs"
    request = ScaffoldRequest(
        type_name="dagster_drt.DrtSyncComponent",
        target_path=target_path,
        scaffold_format="yaml",
        project_root=tmp_path,
        params=DrtSyncScaffolderParams(
            project_dir="../../drt-project",
            sync_names=["test_sync"],
            group_name="reverse_etl",
        ),
    )

    DrtSyncComponentScaffolder().scaffold(request)

    contents = yaml.safe_load((target_path / "defs.yaml").read_text())
    assert contents == {
        "type": "dagster_drt.DrtSyncComponent",
        "attributes": {
            "project_dir": "../../drt-project",
            "sync_names": ["test_sync"],
            "translation": {"group_name": "reverse_etl"},
        },
    }


def test_component_loads_from_declarative_yaml(tmp_path: Path) -> None:
    project = _setup_project(tmp_path)

    with create_defs_folder_sandbox() as sandbox:
        defs_path = sandbox.defs_folder_path / "drt_syncs"
        defs_path.mkdir()
        (defs_path / "defs.yaml").write_text(
            "\n".join(
                [
                    "type: dagster_drt.DrtSyncComponent",
                    "attributes:",
                    f"  project_dir: {project}",
                    "  sync_names: [test_sync]",
                    "  translation:",
                    "    group_name: reverse_etl",
                ]
            )
        )

        with sandbox.load_component_and_build_defs(defs_path) as (component, defs):
            assert isinstance(component, DrtSyncComponent)
            assets_def = next(iter(defs.assets))
            assert assets_def.keys == {AssetKey("drt_test_sync")}
            assert (
                assets_def.group_names_by_key[AssetKey("drt_test_sync")]
                == "reverse_etl"
            )


def test_multiple_components_merge_without_op_name_collision(tmp_path: Path) -> None:
    first_project_dir = tmp_path / "first_project"
    second_project_dir = tmp_path / "second_project"
    first_project_dir.mkdir()
    second_project_dir.mkdir()
    first_project = _setup_project(
        first_project_dir,
        {
            "first.yml": "name: first\nmodel: ref('first')\ndestination:\n"
            "  type: rest_api\n  url: http://example.com/first\n"
        },
    )
    second_project = _setup_project(
        second_project_dir,
        {
            "second.yml": "name: second\nmodel: ref('second')\ndestination:\n"
            "  type: rest_api\n  url: http://example.com/second\n"
        },
    )

    with create_defs_folder_sandbox() as sandbox:
        first_defs_path = sandbox.defs_folder_path / "first_syncs"
        second_defs_path = sandbox.defs_folder_path / "second_syncs"
        first_defs_path.mkdir()
        second_defs_path.mkdir()
        (first_defs_path / "defs.yaml").write_text(
            "\n".join(
                [
                    "type: dagster_drt.DrtSyncComponent",
                    "attributes:",
                    f"  project_dir: {first_project}",
                    "  sync_names: [first]",
                ]
            )
        )
        (second_defs_path / "defs.yaml").write_text(
            "\n".join(
                [
                    "type: dagster_drt.DrtSyncComponent",
                    "attributes:",
                    f"  project_dir: {second_project}",
                    "  sync_names: [second]",
                ]
            )
        )

        with (
            sandbox.load_component_and_build_defs(first_defs_path) as (_, first_defs),
            sandbox.load_component_and_build_defs(second_defs_path) as (_, second_defs),
        ):
            merged_defs = Definitions.merge(first_defs, second_defs)
            Definitions.validate_loadable(merged_defs)

            op_names = {assets_def.op.name for assets_def in merged_defs.assets}
            assert len(op_names) == 2


def test_component_get_asset_spec_and_execute_are_overridable(tmp_path: Path) -> None:
    project = _setup_project(tmp_path)
    executed: list[bool] = []

    class CustomDrtSyncComponent(DrtSyncComponent):
        def get_asset_spec(self, data) -> AssetSpec:
            return super().get_asset_spec(data).replace_attributes(group_name="custom")

        def execute(self, context, drt_resource) -> Iterator:
            del drt_resource
            executed.append(True)
            for asset_key in context.selected_asset_keys:
                yield MaterializeResult(asset_key=asset_key)

    component = CustomDrtSyncComponent(
        project_dir=str(project),
        sync_names=["test_sync"],
    )
    defs = component_defs(component=component)
    assets_def = next(iter(defs.assets))

    assert assets_def.group_names_by_key[AssetKey("drt_test_sync")] == "custom"
    result = materialize([assets_def])
    assert result.success
    assert executed == [True]
