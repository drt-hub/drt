"""Every dagster-drt execution boundary resolves secrets against the drt project (#1372)."""

import warnings
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from dagster import build_asset_context

from drt.config.credentials import resolve_env

from .test_assets import (
    _P_BUILD_STATE_BUNDLE,
    _P_GET_DEST,
    _P_GET_SOURCE,
    _P_LOAD_PROFILE,
    _P_LOAD_PROJECT,
    _P_LOAD_SYNCS,
    _P_RUN_SYNC,
    _FakeSyncResult,
    _make_mock_context,
    _setup_project,
)

KEY = "DAGSTER_DRT_SCOPE_TEST_SECRET"


@pytest.fixture
def scoped_project(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    """A drt project holding a secret, and a different cwd holding a decoy."""
    monkeypatch.delenv(KEY, raising=False)
    (tmp_path / "project").mkdir()
    project = _setup_project(tmp_path / "project")
    (project / ".drt").mkdir()
    (project / ".drt" / "secrets.toml").write_text(f'{KEY} = "project-secret"\n')
    cwd = tmp_path / "elsewhere"
    (cwd / ".drt").mkdir(parents=True)
    (cwd / ".drt" / "secrets.toml").write_text(f'{KEY} = "cwd-decoy"\n')
    monkeypatch.chdir(cwd)
    return project, cwd


def _recorder(seen: list[str | None]) -> Any:
    def _record(*args: Any, **kwargs: Any) -> Any:
        seen.append(resolve_env(None, KEY))
        return MagicMock()

    return _record


def test_resource_run_resolves_project_secrets(scoped_project: tuple[Path, Path]) -> None:
    project, _ = scoped_project
    from dagster_drt.assets import drt_assets
    from dagster_drt.resource import DagsterDrtResource

    @drt_assets(project_dir=project)
    def my_syncs(context, drt: DagsterDrtResource):  # type: ignore[no-untyped-def]
        yield from drt.run(context=context)

    seen: dict[str, list[str | None]] = {"profile": [], "dest": [], "run": []}
    run_result = _FakeSyncResult(success=1)

    def _run(*args: Any, **kwargs: Any) -> Any:
        seen["run"].append(resolve_env(None, KEY))
        return run_result

    with (
        patch(_P_LOAD_PROJECT) as mock_proj,
        patch(_P_LOAD_PROFILE, side_effect=_recorder(seen["profile"])),
        patch(_P_GET_SOURCE),
        patch(_P_GET_DEST, side_effect=_recorder(seen["dest"])),
        patch(_P_RUN_SYNC, side_effect=_run),
        patch(_P_BUILD_STATE_BUNDLE),
        patch(_P_LOAD_SYNCS) as mock_load_syncs,
    ):
        mock_proj.return_value = MagicMock(profile="local")
        sync = MagicMock()
        sync.name = "test_sync"
        sync.sync.dlq = None
        mock_load_syncs.return_value = [sync]
        ctx = _make_mock_context(my_syncs)
        list(DagsterDrtResource(project_dir=str(project)).run(context=ctx))

    assert seen == {
        "profile": ["project-secret"],
        "dest": ["project-secret"],
        "run": ["project-secret"],
    }
    # The scope ends with the operation: the cwd file is used again outside it.
    assert resolve_env(None, KEY) == "cwd-decoy"


def test_row_count_fetch_resolves_project_secrets(scoped_project: tuple[Path, Path]) -> None:
    project, _ = scoped_project
    from dagster_drt.resource import DagsterDrtResource, _SourceRowCountInput

    from drt.config.query_tags import build_query_tags  # noqa: F401  (import must resolve)

    seen: list[str | None] = []

    class Source:
        def extract(self, query: str, profile: Any, query_tags: Any = None) -> Any:
            seen.append(resolve_env(None, KEY))
            return iter([{"id": 1}, {"id": 2}])

    sync = MagicMock()
    sync.name = "test_sync"
    sync.model = "select 1"
    row_input = _SourceRowCountInput(
        source=Source(),
        profile=MagicMock(),
        sync_config=sync,
        project_path=project,
        query_tagging=MagicMock(enabled=False),
        cursor_value_used=None,
    )
    captured: dict[str, Any] = {}

    def _fake_run(self: Any, context: Any, dry_run: Any, sync_names: Any, row_count_inputs: Any):
        captured["inputs"] = row_count_inputs
        return iter(())

    with (
        patch.object(DagsterDrtResource, "_run", _fake_run),
        patch("drt.engine.resolver.resolve_model_ref", return_value="select 1"),
    ):
        iterator = DagsterDrtResource(project_dir=str(project)).run(context=MagicMock())
        captured["inputs"]["k"] = row_input
        event = MagicMock(asset_key="k")
        assert iterator._row_count_fetcher(event) == 2  # noqa: SLF001

    assert seen == ["project-secret"]


def test_legacy_asset_resolves_project_secrets(scoped_project: tuple[Path, Path]) -> None:
    project, _ = scoped_project
    from dagster_drt.assets import DrtConfig, drt_assets_legacy

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        assets = drt_assets_legacy(project_dir=project)

    seen: dict[str, list[str | None]] = {"profile": [], "run": []}

    def _run(*args: Any, **kwargs: Any) -> Any:
        seen["run"].append(resolve_env(None, KEY))
        return _FakeSyncResult(success=1)

    with (
        patch(_P_LOAD_PROJECT) as mock_proj,
        patch(_P_LOAD_PROFILE, side_effect=_recorder(seen["profile"])),
        patch(_P_GET_SOURCE),
        patch(_P_GET_DEST),
        patch(_P_RUN_SYNC, side_effect=_run),
        patch(_P_BUILD_STATE_BUNDLE),
    ):
        mock_proj.return_value = MagicMock(profile="local")
        assets[0](context=build_asset_context(), config=DrtConfig(dry_run=True))

    assert seen == {"profile": ["project-secret"], "run": ["project-secret"]}


def test_sensor_signal_resolves_project_secrets(scoped_project: tuple[Path, Path]) -> None:
    project, _ = scoped_project
    from dagster_drt.sensors import _current_signal

    from drt.config.credentials import DeltaLakeProfile

    seen: list[str | None] = []

    def _profile(*args: Any, **kwargs: Any) -> Any:
        seen.append(resolve_env(None, KEY))
        return DeltaLakeProfile(type="deltalake", location="s3://b/t")

    deltalake = MagicMock()
    deltalake.DeltaTable.return_value.version.return_value = 3
    with (
        patch(_P_LOAD_PROJECT) as mock_proj,
        patch(_P_LOAD_PROFILE, side_effect=_profile),
        patch.dict("sys.modules", {"deltalake": deltalake}),
    ):
        mock_proj.return_value = MagicMock(profile="lake")
        assert _current_signal(project) == "3"

    assert seen == ["project-secret"]


def test_scope_degrades_to_cwd_lookup_on_older_drt_core(
    scoped_project: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch
) -> None:
    """An older drt-core without ``secrets_project_dir`` keeps the previous behaviour."""
    import drt.config.credentials as credentials

    monkeypatch.delattr(credentials, "secrets_project_dir")
    from dagster_drt._secrets import project_secrets_scope

    project, _ = scoped_project
    with project_secrets_scope(project):
        assert resolve_env(None, KEY) == "cwd-decoy"
