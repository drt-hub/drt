"""Tests for secrets.toml support in resolve_env()."""

from __future__ import annotations

from pathlib import Path

import pytest

from drt.config.credentials import _load_secrets, resolve_env

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
