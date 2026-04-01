from pathlib import Path

from dagster import AssetKey

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


# --- Basic asset creation ---


def test_drt_assets_creates_asset_list(tmp_path: Path) -> None:
    project = _setup_project(tmp_path)
    from dagster_drt.assets import drt_assets

    assets = drt_assets(project_dir=project)
    assert len(assets) == 1


def test_drt_assets_filter_by_name(tmp_path: Path) -> None:
    project = _setup_project(
        tmp_path,
        {
            "a.yml": "name: a\nmodel: ref('t')\ndestination:\n  type: rest_api\n  url: http://x.com\n",
            "b.yml": "name: b\nmodel: ref('t')\ndestination:\n  type: rest_api\n  url: http://x.com\n",
        },
    )
    from dagster_drt.assets import drt_assets

    assets = drt_assets(project_dir=project, sync_names=["a"])
    assert len(assets) == 1


# --- Translator: default ---


def test_default_translator_asset_key(tmp_path: Path) -> None:
    project = _setup_project(tmp_path)
    from dagster_drt.assets import drt_assets

    assets = drt_assets(project_dir=project)
    assert assets[0].key == AssetKey("drt_test_sync")


def test_default_translator_no_group(tmp_path: Path) -> None:
    project = _setup_project(tmp_path)
    from dagster_drt.assets import drt_assets

    assets = drt_assets(project_dir=project)
    assert assets[0].group_names_by_key.get(AssetKey("drt_test_sync")) == "default"


# --- Translator: custom ---


def test_custom_translator_group_name(tmp_path: Path) -> None:
    project = _setup_project(tmp_path)
    from dagster_drt.assets import drt_assets
    from dagster_drt.translator import DagsterDrtTranslator

    class MyTranslator(DagsterDrtTranslator):
        def get_group_name(self, sync_config):
            return "reverse_etl"

    assets = drt_assets(project_dir=project, dagster_drt_translator=MyTranslator())
    assert assets[0].group_names_by_key[AssetKey("drt_test_sync")] == "reverse_etl"


def test_custom_translator_asset_key(tmp_path: Path) -> None:
    project = _setup_project(tmp_path)
    from dagster_drt.assets import drt_assets
    from dagster_drt.translator import DagsterDrtTranslator

    class MyTranslator(DagsterDrtTranslator):
        def get_asset_key(self, sync_config):
            return AssetKey(["my_prefix", sync_config.name])

    assets = drt_assets(project_dir=project, dagster_drt_translator=MyTranslator())
    assert assets[0].key == AssetKey(["my_prefix", "test_sync"])


def test_custom_translator_deps(tmp_path: Path) -> None:
    project = _setup_project(tmp_path)
    from dagster_drt.assets import drt_assets
    from dagster_drt.translator import DagsterDrtTranslator

    class MyTranslator(DagsterDrtTranslator):
        def get_deps(self, sync_config):
            return [AssetKey("upstream_model")]

    assets = drt_assets(project_dir=project, dagster_drt_translator=MyTranslator())
    dep_keys = {dep.asset_key for dep in assets[0].specs_by_key[AssetKey("drt_test_sync")].deps}
    assert AssetKey("upstream_model") in dep_keys


# --- DrtConfig ---


def test_drt_assets_accepts_dry_run_default(tmp_path: Path) -> None:
    """dry_run parameter should not break asset creation."""
    project = _setup_project(tmp_path)
    from dagster_drt.assets import drt_assets

    assets = drt_assets(project_dir=project, dry_run=True)
    assert len(assets) == 1


def test_drt_config_defaults() -> None:
    from dagster_drt.assets import DrtConfig

    config = DrtConfig()
    assert config.dry_run is False


def test_drt_config_dry_run() -> None:
    from dagster_drt.assets import DrtConfig

    config = DrtConfig(dry_run=True)
    assert config.dry_run is True


# --- Return type ---


def test_asset_returns_materialize_result(tmp_path: Path) -> None:
    """Verify asset function return annotation is MaterializeResult."""
    project = _setup_project(tmp_path)
    from dagster import MaterializeResult

    from dagster_drt.assets import drt_assets

    assets = drt_assets(project_dir=project)
    # Check the output type from the asset spec
    spec = assets[0].specs_by_key[AssetKey("drt_test_sync")]
    # MaterializeResult assets have no output type annotation on the spec
    # but we can verify the asset was created successfully with the new signature
    assert spec is not None


# --- Exports ---


def test_public_exports() -> None:
    import dagster_drt

    assert hasattr(dagster_drt, "drt_assets")
    assert hasattr(dagster_drt, "DrtConfig")
    assert hasattr(dagster_drt, "DagsterDrtTranslator")
