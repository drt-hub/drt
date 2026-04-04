import warnings
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from dagster import (
    AssetKey,
    AssetsDefinition,
    AssetSpec,
    BackfillPolicy,
    DailyPartitionsDefinition,
    MaterializeResult,
)

SYNC_YAML = "name: test_sync\nmodel: ref('users')\ndestination:\n  type: rest_api\n  url: http://example.com\n"
PROJECT_YAML = "name: test\nversion: '0.1'\nprofile: local\n"


def _setup_project(tmp_path: Path, syncs: dict[str, str] | None = None) -> Path:
    """Create a minimal drt project structure for testing."""
    (tmp_path / "drt_project.yml").write_text(PROJECT_YAML)
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    if syncs is None:
        syncs = {"test_sync.yml": SYNC_YAML}
    for filename, content in syncs.items():
        (syncs_dir / filename).write_text(content)
    return tmp_path


TWO_SYNCS = {
    "a.yml": "name: a\nmodel: ref('t')\ndestination:\n  type: rest_api\n  url: http://x.com\n",
    "b.yml": "name: b\nmodel: ref('t')\ndestination:\n  type: rest_api\n  url: http://x.com\n",
}


# ===================================================================
# build_drt_asset_specs()
# ===================================================================


class TestBuildDrtAssetSpecs:
    def test_returns_asset_specs(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.specs import build_drt_asset_specs

        specs = build_drt_asset_specs(project_dir=project)
        assert len(specs) == 1
        assert isinstance(specs[0], AssetSpec)

    def test_default_key(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.specs import build_drt_asset_specs

        specs = build_drt_asset_specs(project_dir=project)
        assert specs[0].key == AssetKey("drt_test_sync")

    def test_filter_by_name(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path, TWO_SYNCS)
        from dagster_drt.specs import build_drt_asset_specs

        specs = build_drt_asset_specs(project_dir=project, sync_names=["a"])
        assert len(specs) == 1
        assert specs[0].key == AssetKey("drt_a")

    def test_all_syncs(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path, TWO_SYNCS)
        from dagster_drt.specs import build_drt_asset_specs

        specs = build_drt_asset_specs(project_dir=project)
        assert len(specs) == 2

    def test_internal_metadata(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.specs import (
            META_KEY_PROJECT_DIR,
            META_KEY_SYNC_NAME,
            build_drt_asset_specs,
        )

        specs = build_drt_asset_specs(project_dir=project)
        meta = specs[0].metadata
        assert meta[META_KEY_SYNC_NAME] == "test_sync"
        assert meta[META_KEY_PROJECT_DIR] == str(project)

    def test_kinds_metadata(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.specs import build_drt_asset_specs

        specs = build_drt_asset_specs(project_dir=project)
        assert "drt" in specs[0].kinds


# ===================================================================
# DagsterDrtTranslator — get_asset_spec() (new API)
# ===================================================================


class TestTranslatorNewAPI:
    def test_get_asset_spec_default(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.specs import build_drt_asset_specs

        specs = build_drt_asset_specs(project_dir=project)
        assert specs[0].key == AssetKey("drt_test_sync")

    def test_get_asset_spec_override(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.specs import build_drt_asset_specs
        from dagster_drt.translator import DagsterDrtTranslator

        class MyTranslator(DagsterDrtTranslator):
            def get_asset_spec(self, data):
                default = super().get_asset_spec(data)
                return default.replace_attributes(
                    key=AssetKey(["custom", data.sync_config.name]),
                    group_name="reverse_etl",
                )

        specs = build_drt_asset_specs(
            project_dir=project,
            dagster_drt_translator=MyTranslator(),
        )
        assert specs[0].key == AssetKey(["custom", "test_sync"])
        assert specs[0].group_name == "reverse_etl"


# ===================================================================
# DagsterDrtTranslator — legacy per-attribute methods (backward compat)
# ===================================================================


class TestTranslatorLegacy:
    def test_legacy_group_name(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.specs import build_drt_asset_specs
        from dagster_drt.translator import DagsterDrtTranslator

        class MyTranslator(DagsterDrtTranslator):
            def get_group_name(self, sync_config):
                return "reverse_etl"

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            specs = build_drt_asset_specs(
                project_dir=project,
                dagster_drt_translator=MyTranslator(),
            )
            assert any("deprecated" in str(warning.message).lower() for warning in w)
        assert specs[0].group_name == "reverse_etl"

    def test_legacy_asset_key(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.specs import build_drt_asset_specs
        from dagster_drt.translator import DagsterDrtTranslator

        class MyTranslator(DagsterDrtTranslator):
            def get_asset_key(self, sync_config):
                return AssetKey(["my_prefix", sync_config.name])

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            specs = build_drt_asset_specs(
                project_dir=project,
                dagster_drt_translator=MyTranslator(),
            )
        assert specs[0].key == AssetKey(["my_prefix", "test_sync"])

    def test_legacy_deps(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.specs import build_drt_asset_specs
        from dagster_drt.translator import DagsterDrtTranslator

        class MyTranslator(DagsterDrtTranslator):
            def get_deps(self, sync_config):
                return [AssetKey("upstream_model")]

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            specs = build_drt_asset_specs(
                project_dir=project,
                dagster_drt_translator=MyTranslator(),
            )
        dep_keys = {dep.asset_key for dep in specs[0].deps}
        assert AssetKey("upstream_model") in dep_keys


# ===================================================================
# @drt_assets decorator
# ===================================================================


class TestDrtAssetsDecorator:
    def test_creates_multi_asset(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource

        @drt_assets(project_dir=project)
        def my_syncs(context, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        assert isinstance(my_syncs, AssetsDefinition)
        assert AssetKey("drt_test_sync") in my_syncs.keys

    def test_multi_asset_can_subset(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path, TWO_SYNCS)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource

        @drt_assets(project_dir=project)
        def my_syncs(context, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        assert my_syncs.can_subset

    def test_filter_by_name(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path, TWO_SYNCS)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource

        @drt_assets(project_dir=project, sync_names=["a"])
        def my_syncs(context, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        assert len(my_syncs.keys) == 1
        assert AssetKey("drt_a") in my_syncs.keys

    def test_group_name_override(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource

        @drt_assets(project_dir=project, group_name="reverse_etl")
        def my_syncs(context, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        assert my_syncs.group_names_by_key[AssetKey("drt_test_sync")] == "reverse_etl"

    def test_custom_translator(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource
        from dagster_drt.translator import DagsterDrtTranslator

        class MyTranslator(DagsterDrtTranslator):
            def get_asset_spec(self, data):
                default = super().get_asset_spec(data)
                return default.replace_attributes(key=AssetKey(["custom", data.sync_config.name]))

        @drt_assets(project_dir=project, dagster_drt_translator=MyTranslator())
        def my_syncs(context, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        assert AssetKey(["custom", "test_sync"]) in my_syncs.keys


# ===================================================================
# Legacy drt_assets_legacy() function
# ===================================================================


class TestLegacyDrtAssets:
    def test_creates_asset_list(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.assets import drt_assets_legacy

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            assets = drt_assets_legacy(project_dir=project)
        assert len(assets) == 1

    def test_emits_deprecation_warning(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.assets import drt_assets_legacy

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            drt_assets_legacy(project_dir=project)
            assert any("deprecated" in str(warning.message).lower() for warning in w)

    def test_filter_by_name(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path, TWO_SYNCS)
        from dagster_drt.assets import drt_assets_legacy

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            assets = drt_assets_legacy(project_dir=project, sync_names=["a"])
        assert len(assets) == 1


# ===================================================================
# DrtConfig
# ===================================================================


class TestDrtConfig:
    def test_defaults(self) -> None:
        from dagster_drt.assets import DrtConfig

        config = DrtConfig()
        assert config.dry_run is False

    def test_dry_run(self) -> None:
        from dagster_drt.assets import DrtConfig

        config = DrtConfig(dry_run=True)
        assert config.dry_run is True


# ===================================================================
# Public exports
# ===================================================================


# ===================================================================
# @drt_assets — partitions_def, backfill_policy, pool
# ===================================================================


class TestDrtAssetsAdvanced:
    def test_partitions_def(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource

        partitions = DailyPartitionsDefinition(start_date="2024-01-01")

        @drt_assets(project_dir=project, partitions_def=partitions)
        def my_syncs(context, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        assert my_syncs.partitions_def == partitions

    def test_partitions_def_auto_backfill_policy(self, tmp_path: Path) -> None:
        """When partitions_def is set without backfill_policy, defaults to single_run."""
        project = _setup_project(tmp_path)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource

        partitions = DailyPartitionsDefinition(start_date="2024-01-01")

        @drt_assets(project_dir=project, partitions_def=partitions)
        def my_syncs(context, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        assert my_syncs.backfill_policy == BackfillPolicy.single_run()

    def test_explicit_backfill_policy(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource

        policy = BackfillPolicy.multi_run(max_partitions_per_run=5)

        @drt_assets(
            project_dir=project,
            partitions_def=DailyPartitionsDefinition(start_date="2024-01-01"),
            backfill_policy=policy,
        )
        def my_syncs(context, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        assert my_syncs.backfill_policy == policy

    def test_pool_parameter(self, tmp_path: Path) -> None:
        project = _setup_project(tmp_path)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource

        @drt_assets(project_dir=project, pool="drt_pool")
        def my_syncs(context, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        assert isinstance(my_syncs, AssetsDefinition)

    def test_group_name_conflict_raises(self, tmp_path: Path) -> None:
        """Raise ValueError when translator sets group and decorator also sets group."""
        project = _setup_project(tmp_path)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource
        from dagster_drt.translator import DagsterDrtTranslator

        class MyTranslator(DagsterDrtTranslator):
            def get_asset_spec(self, data):
                default = super().get_asset_spec(data)
                return default.replace_attributes(group_name="from_translator")

        with pytest.raises(ValueError, match="conflicts with translator-set"):

            @drt_assets(
                project_dir=project,
                group_name="from_decorator",
                dagster_drt_translator=MyTranslator(),
            )
            def my_syncs(context, drt: DagsterDrtResource):
                yield from drt.run(context=context)


# ===================================================================
# Legacy translator — kinds preserved
# ===================================================================


class TestTranslatorLegacyKinds:
    def test_legacy_path_preserves_kinds(self, tmp_path: Path) -> None:
        """Legacy translator overrides should still produce kinds metadata."""
        project = _setup_project(tmp_path)
        from dagster_drt.specs import build_drt_asset_specs
        from dagster_drt.translator import DagsterDrtTranslator

        class MyTranslator(DagsterDrtTranslator):
            def get_group_name(self, sync_config):
                return "custom_group"

        with warnings.catch_warnings(record=True):
            warnings.simplefilter("always")
            specs = build_drt_asset_specs(
                project_dir=project,
                dagster_drt_translator=MyTranslator(),
            )
        assert "drt" in specs[0].kinds
        assert "rest_api" in specs[0].kinds


# ===================================================================
# DagsterDrtResource — integration tests with mocked run_sync
# ===================================================================


@dataclass
class _FakeSyncResult:
    """Minimal SyncResult stand-in for testing."""

    success: int = 10
    failed: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)
    row_errors: list = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.success + self.failed + self.skipped


def _make_mock_context(
    assets_def: AssetsDefinition,
    selected_keys: set[AssetKey] | None = None,
) -> MagicMock:
    """Create a mock AssetExecutionContext for resource tests."""
    ctx = MagicMock(spec=["assets_def", "selected_asset_keys", "log"])
    ctx.assets_def = assets_def
    ctx.selected_asset_keys = selected_keys or assets_def.keys
    ctx.log = MagicMock()
    return ctx


# Patch targets for lazy imports inside DagsterDrtResource.run().
_P_LOAD_PROJECT = "drt.config.parser.load_project"
_P_LOAD_PROFILE = "drt.config.credentials.load_profile"
_P_GET_SOURCE = "drt.cli.main._get_source"
_P_GET_DEST = "drt.cli.main._get_destination"
_P_RUN_SYNC = "drt.engine.sync.run_sync"
_P_STATE_MGR = "drt.state.manager.StateManager"
_P_LOAD_SYNCS = "drt.config.parser.load_syncs"


class TestDagsterDrtResourceRun:
    def test_run_yields_materialize_result(self, tmp_path: Path) -> None:
        """Resource.run() should yield MaterializeResult per sync."""
        project = _setup_project(tmp_path)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource

        @drt_assets(project_dir=project)
        def my_syncs(context, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        ctx = _make_mock_context(my_syncs)
        resource = DagsterDrtResource(project_dir=str(project))

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE),
            patch(_P_GET_SOURCE),
            patch(_P_GET_DEST),
            patch(_P_RUN_SYNC) as mock_run,
            patch(_P_STATE_MGR),
            patch(_P_LOAD_SYNCS) as mock_load_syncs,
        ):
            mock_proj.return_value = MagicMock(profile="local")
            mock_sync = MagicMock()
            mock_sync.name = "test_sync"
            mock_load_syncs.return_value = [mock_sync]
            mock_run.return_value = _FakeSyncResult(success=42, failed=1)

            results = list(resource.run(context=ctx))

        assert len(results) == 1
        assert isinstance(results[0], MaterializeResult)
        assert results[0].metadata["rows_synced"].value == 42
        assert results[0].metadata["rows_failed"].value == 1

    def test_run_subset_execution(self, tmp_path: Path) -> None:
        """Resource.run() should only execute syncs for selected asset keys."""
        project = _setup_project(tmp_path, TWO_SYNCS)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource

        @drt_assets(project_dir=project)
        def my_syncs(context, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        # Only select asset "a", not "b".
        ctx = _make_mock_context(my_syncs, selected_keys={AssetKey("drt_a")})
        resource = DagsterDrtResource(project_dir=str(project))

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE),
            patch(_P_GET_SOURCE),
            patch(_P_GET_DEST),
            patch(_P_RUN_SYNC) as mock_run,
            patch(_P_STATE_MGR),
            patch(_P_LOAD_SYNCS) as mock_load_syncs,
        ):
            mock_proj.return_value = MagicMock(profile="local")
            mock_a = MagicMock()
            mock_a.name = "a"
            mock_b = MagicMock()
            mock_b.name = "b"
            mock_load_syncs.return_value = [mock_a, mock_b]
            mock_run.return_value = _FakeSyncResult()

            results = list(resource.run(context=ctx))

        assert len(results) == 1
        assert results[0].asset_key == AssetKey("drt_a")
        # run_sync should only be called once (for "a", not "b")
        mock_run.assert_called_once()

    def test_run_dry_run_override(self, tmp_path: Path) -> None:
        """dry_run parameter passed to run() should override resource default."""
        project = _setup_project(tmp_path)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource

        @drt_assets(project_dir=project)
        def my_syncs(context, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        ctx = _make_mock_context(my_syncs)
        resource = DagsterDrtResource(project_dir=str(project), dry_run=False)

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE),
            patch(_P_GET_SOURCE),
            patch(_P_GET_DEST),
            patch(_P_RUN_SYNC) as mock_run,
            patch(_P_STATE_MGR),
            patch(_P_LOAD_SYNCS) as mock_load_syncs,
        ):
            mock_proj.return_value = MagicMock(profile="local")
            mock_sync = MagicMock()
            mock_sync.name = "test_sync"
            mock_load_syncs.return_value = [mock_sync]
            mock_run.return_value = _FakeSyncResult()

            list(resource.run(context=ctx, dry_run=True))

        # Verify dry_run=True was passed to run_sync
        call_kwargs = mock_run.call_args
        assert call_kwargs.kwargs.get("dry_run") is True

    def test_run_missing_sync_warns(self, tmp_path: Path) -> None:
        """Resource.run() should warn when sync not found for an asset key."""
        project = _setup_project(tmp_path)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource

        @drt_assets(project_dir=project)
        def my_syncs(context, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        ctx = _make_mock_context(my_syncs)
        resource = DagsterDrtResource(project_dir=str(project))

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE),
            patch(_P_GET_SOURCE),
            patch(_P_STATE_MGR),
            patch(_P_LOAD_SYNCS) as mock_load_syncs,
        ):
            mock_proj.return_value = MagicMock(profile="local")
            # Return empty sync list — sync won't be found.
            mock_load_syncs.return_value = []

            results = list(resource.run(context=ctx))

        assert len(results) == 0
        ctx.log.warning.assert_called()

    def test_run_auto_resolve_project_dir(self, tmp_path: Path) -> None:
        """Resource should auto-resolve project_dir from asset metadata."""
        project = _setup_project(tmp_path)
        from dagster_drt.assets import drt_assets
        from dagster_drt.resource import DagsterDrtResource

        @drt_assets(project_dir=project)
        def my_syncs(context, drt: DagsterDrtResource):
            yield from drt.run(context=context)

        ctx = _make_mock_context(my_syncs)
        # No project_dir set on resource — should auto-resolve from metadata.
        resource = DagsterDrtResource()

        with (
            patch(_P_LOAD_PROJECT) as mock_proj,
            patch(_P_LOAD_PROFILE),
            patch(_P_GET_SOURCE),
            patch(_P_GET_DEST),
            patch(_P_RUN_SYNC) as mock_run,
            patch(_P_STATE_MGR),
            patch(_P_LOAD_SYNCS) as mock_load_syncs,
        ):
            mock_proj.return_value = MagicMock(profile="local")
            mock_sync = MagicMock()
            mock_sync.name = "test_sync"
            mock_load_syncs.return_value = [mock_sync]
            mock_run.return_value = _FakeSyncResult()

            results = list(resource.run(context=ctx))

        assert len(results) == 1

    def test_run_no_project_dir_raises(self, tmp_path: Path) -> None:
        """Resource should raise if project_dir can't be resolved."""
        from dagster_drt.resource import DagsterDrtResource

        resource = DagsterDrtResource()

        # Mock a context with no metadata.
        ctx = MagicMock()
        ctx.assets_def.specs_by_key = {AssetKey("x"): AssetSpec(key=AssetKey("x"))}
        ctx.selected_asset_keys = {AssetKey("x")}

        with pytest.raises(ValueError, match="project_dir must be set"):
            list(resource.run(context=ctx))


# ===================================================================
# Public exports
# ===================================================================


def test_public_exports() -> None:
    import dagster_drt

    assert hasattr(dagster_drt, "drt_assets")
    assert hasattr(dagster_drt, "drt_assets_legacy")
    assert hasattr(dagster_drt, "DrtConfig")
    assert hasattr(dagster_drt, "DagsterDrtTranslator")
    assert hasattr(dagster_drt, "DrtTranslatorData")
    assert hasattr(dagster_drt, "DagsterDrtResource")
    assert hasattr(dagster_drt, "build_drt_asset_specs")
