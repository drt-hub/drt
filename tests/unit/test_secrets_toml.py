"""Tests for secrets.toml support in resolve_env()."""

from __future__ import annotations

import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from drt.config.credentials import _load_secrets, resolve_env, secrets_project_dir


def _write_secret(project_dir: Path, value: str) -> None:
    secrets_dir = project_dir / ".drt"
    secrets_dir.mkdir()
    (secrets_dir / "secrets.toml").write_text(
        f'[destinations]\nSCOPED_SECRET = "{value}"\n'
    )

# ---------------------------------------------------------------------------
# _load_secrets
# ---------------------------------------------------------------------------


def test_load_secrets_missing_file(tmp_path: Path) -> None:
    assert _load_secrets(tmp_path) == {}


def test_load_secrets_valid(tmp_path: Path) -> None:
    secrets_dir = tmp_path / ".drt"
    secrets_dir.mkdir()
    (secrets_dir / "secrets.toml").write_text(
        '[destinations.mysql]\nMYSQL_PASSWORD = "secret123"\n'
    )
    data = _load_secrets(tmp_path)
    assert data["destinations"]["mysql"]["MYSQL_PASSWORD"] == "secret123"


# ---------------------------------------------------------------------------
# resolve_env with secrets.toml
# ---------------------------------------------------------------------------


def test_resolve_env_explicit_value() -> None:
    assert resolve_env("explicit", "SOME_VAR") == "explicit"


def test_resolve_env_from_env_var(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_SECRET", "from_env")
    assert resolve_env(None, "MY_SECRET") == "from_env"


def test_resolve_env_from_secrets_toml(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("TOML_SECRET", raising=False)
    monkeypatch.chdir(tmp_path)

    secrets_dir = tmp_path / ".drt"
    secrets_dir.mkdir()
    (secrets_dir / "secrets.toml").write_text('[destinations]\nTOML_SECRET = "from_toml"\n')

    result = resolve_env(None, "TOML_SECRET")
    assert result == "from_toml"


def test_resolve_env_env_var_beats_secrets_toml(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("MY_VAR", "from_env")
    monkeypatch.chdir(tmp_path)

    secrets_dir = tmp_path / ".drt"
    secrets_dir.mkdir()
    (secrets_dir / "secrets.toml").write_text('[destinations]\nMY_VAR = "from_toml"\n')

    result = resolve_env(None, "MY_VAR")
    assert result == "from_env"


def test_resolve_env_none_when_not_found(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NONEXISTENT", raising=False)
    assert resolve_env(None, "NONEXISTENT") is None


def test_resolve_env_none_when_no_env_var() -> None:
    assert resolve_env(None, None) is None


def test_secrets_project_dir_never_falls_back_to_cwd(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cwd = tmp_path / "cwd"
    project = tmp_path / "project"
    missing = tmp_path / "missing"
    for path in (cwd, project, missing):
        path.mkdir()
    _write_secret(cwd, "cwd-value")
    _write_secret(project, "project-value")
    monkeypatch.chdir(cwd)
    monkeypatch.delenv("SCOPED_SECRET", raising=False)

    assert resolve_env(None, "SCOPED_SECRET") == "cwd-value"
    with secrets_project_dir(project):
        assert resolve_env(None, "SCOPED_SECRET") == "project-value"
    with secrets_project_dir(missing):
        assert resolve_env(None, "SCOPED_SECRET") is None
    assert resolve_env(None, "SCOPED_SECRET") == "cwd-value"


def test_secrets_project_dir_restores_nested_scope_after_exception(tmp_path: Path) -> None:
    outer = tmp_path / "outer"
    inner = tmp_path / "inner"
    outer.mkdir()
    inner.mkdir()
    _write_secret(outer, "outer-value")
    _write_secret(inner, "inner-value")

    with secrets_project_dir(outer):
        with pytest.raises(RuntimeError, match="boom"):
            with secrets_project_dir(inner):
                assert resolve_env(None, "SCOPED_SECRET") == "inner-value"
                raise RuntimeError("boom")
        assert resolve_env(None, "SCOPED_SECRET") == "outer-value"


def test_secrets_project_dir_isolated_between_threads(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    _write_secret(first, "first-value")
    _write_secret(second, "second-value")
    barrier = threading.Barrier(2)

    def lookup(project_dir: Path) -> str | None:
        with secrets_project_dir(project_dir):
            barrier.wait()
            return resolve_env(None, "SCOPED_SECRET")

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lookup, (first, second)))

    assert results == ["first-value", "second-value"]


@pytest.mark.asyncio
async def test_secrets_project_dir_isolated_between_async_tasks(tmp_path: Path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    _write_secret(first, "first-value")
    _write_secret(second, "second-value")

    async def lookup(project_dir: Path) -> str | None:
        with secrets_project_dir(project_dir):
            await asyncio.sleep(0)
            return resolve_env(None, "SCOPED_SECRET")

    assert await asyncio.gather(lookup(first), lookup(second)) == [
        "first-value",
        "second-value",
    ]
