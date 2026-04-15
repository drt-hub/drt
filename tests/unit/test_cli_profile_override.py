"""Tests for --profile CLI override and DRT_PROFILE env var."""

from __future__ import annotations

from drt.cli.main import _resolve_profile_name


def test_cli_flag_takes_precedence() -> None:
    assert _resolve_profile_name("prd", "default") == "prd"


def test_env_var_overrides_project(monkeypatch: object) -> None:
    import pytest

    mp = pytest.MonkeyPatch()
    mp.setenv("DRT_PROFILE", "staging")
    assert _resolve_profile_name(None, "default") == "staging"
    mp.undo()


def test_project_profile_is_fallback(monkeypatch: object) -> None:
    import pytest

    mp = pytest.MonkeyPatch()
    mp.delenv("DRT_PROFILE", raising=False)
    assert _resolve_profile_name(None, "default") == "default"
    mp.undo()


def test_cli_flag_beats_env_var(monkeypatch: object) -> None:
    import pytest

    mp = pytest.MonkeyPatch()
    mp.setenv("DRT_PROFILE", "staging")
    assert _resolve_profile_name("prd", "default") == "prd"
    mp.undo()
