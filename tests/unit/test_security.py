"""Tests for the RBAC extension point (#298, ADR 0008)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from drt.security.base import (
    AllowAllPermissionChecker,
    PermissionAction,
    PermissionChecker,
    PermissionDeniedError,
    _reset_permission_checker,
    get_permission_checker,
    register_permission_checker,
)


@pytest.fixture(autouse=True)
def _reset_registry() -> Iterator[None]:
    yield
    _reset_permission_checker()


def test_default_checker_is_allow_all() -> None:
    assert isinstance(get_permission_checker(), AllowAllPermissionChecker)


def test_allow_all_checker_permits_every_action_and_principal() -> None:
    checker = AllowAllPermissionChecker()
    for action in PermissionAction:
        checker.check(action, "some_sync")
        checker.check(action, None)
        checker.check(action, "some_sync", principal="alice")


def test_allow_all_checker_isinstance_permission_checker() -> None:
    assert isinstance(AllowAllPermissionChecker(), PermissionChecker)


def test_register_permission_checker_replaces_active_checker() -> None:
    class _DenyAll:
        def check(
            self,
            action: PermissionAction,
            sync_name: str | None,
            *,
            principal: str | None = None,
        ) -> None:
            raise PermissionDeniedError(f"{principal} may not {action} {sync_name}")

    register_permission_checker(_DenyAll())
    assert isinstance(get_permission_checker(), _DenyAll)
    with pytest.raises(PermissionDeniedError):
        get_permission_checker().check(PermissionAction.RUN, "s")


def test_register_permission_checker_second_call_replaces_first() -> None:
    """Single active policy per process — no duplicate-registration error,
    unlike SecretProvider's per-scheme registry."""

    class _First:
        def check(
            self,
            action: PermissionAction,
            sync_name: str | None,
            *,
            principal: str | None = None,
        ) -> None:
            raise PermissionDeniedError("first")

    class _Second:
        def check(
            self,
            action: PermissionAction,
            sync_name: str | None,
            *,
            principal: str | None = None,
        ) -> None:
            return None

    register_permission_checker(_First())
    register_permission_checker(_Second())
    assert isinstance(get_permission_checker(), _Second)
    get_permission_checker().check(PermissionAction.VIEW, None)  # does not raise


def test_permission_action_values_match_issue_298() -> None:
    """#298: "who can run/edit/view which syncs" — exactly these three verbs."""
    assert {a.value for a in PermissionAction} == {"run", "edit", "view"}


# ---------------------------------------------------------------------------
# CLI hook-point wiring (ADR 0008 Decision 3) — proves a registered checker
# actually gates `drt status`, not just that PermissionChecker.check() works
# in isolation.
# ---------------------------------------------------------------------------


def _write_empty_project(project_dir: Path) -> None:
    import yaml

    (project_dir / "drt_project.yml").write_text(
        yaml.dump({"name": "t", "version": "0.1", "profile": "default"})
    )


def test_drt_status_denied_by_registered_checker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from typer.testing import CliRunner

    from drt.cli.main import app

    class _DenyView:
        def check(
            self,
            action: PermissionAction,
            sync_name: str | None,
            *,
            principal: str | None = None,
        ) -> None:
            if action == PermissionAction.VIEW:
                raise PermissionDeniedError("view denied")

    monkeypatch.chdir(tmp_path)
    _write_empty_project(tmp_path)
    register_permission_checker(_DenyView())

    result = CliRunner().invoke(app, ["status"])

    assert result.exit_code != 0
    assert isinstance(result.exception, PermissionDeniedError)


def test_drt_status_allowed_by_default_checker(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Same command, OSS default checker — no behavior change (control case
    for the deny test above)."""
    from typer.testing import CliRunner

    from drt.cli.main import app

    monkeypatch.chdir(tmp_path)
    _write_empty_project(tmp_path)

    result = CliRunner().invoke(app, ["status"])

    assert result.exit_code == 0
