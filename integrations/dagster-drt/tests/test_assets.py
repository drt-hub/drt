from pathlib import Path

def test_drt_assets_creates_asset_list(tmp_path: Path) -> None:
    (tmp_path / "drt_project.yml").write_text("name: test\nversion: '0.1'\nprofile: local\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "test_sync.yml").write_text(
        "name: test_sync\nmodel: ref('users')\n"
        "destination:\n  type: rest_api\n  url: http://example.com\n"
    )
    from dagster_drt.assets import drt_assets
    assets = drt_assets(project_dir=tmp_path)
    assert len(assets) == 1

def test_drt_assets_filter_by_name(tmp_path: Path) -> None:
    (tmp_path / "drt_project.yml").write_text("name: test\nversion: '0.1'\nprofile: local\n")
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "a.yml").write_text("name: a\nmodel: ref('t')\ndestination:\n  type: rest_api\n  url: http://x.com\n")
    (syncs_dir / "b.yml").write_text("name: b\nmodel: ref('t')\ndestination:\n  type: rest_api\n  url: http://x.com\n")
    from dagster_drt.assets import drt_assets
    assets = drt_assets(project_dir=tmp_path, sync_names=["a"])
    assert len(assets) == 1
