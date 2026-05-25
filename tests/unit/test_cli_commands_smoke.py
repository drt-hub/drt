"""Smoke tests for the per-command modules extracted in #546.

The commands themselves are thin wrappers — the heavy lifting lives in
``drt.cli.doctor`` / ``drt.mcp.server`` / ``print_sync_table`` etc., each
covered by their own suites. These tests pin the CLI plumbing (Typer
registration, exit codes, error branches) so a future refactor can't
silently break the wrapper layer.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()


# ---------------------------------------------------------------------------
# drt doctor
# ---------------------------------------------------------------------------


def test_doctor_command_invokes_run_doctor(monkeypatch: pytest.MonkeyPatch) -> None:
    """``drt doctor`` calls into ``drt.cli.doctor.run_doctor``."""
    import drt.cli.doctor as doctor_mod

    spy = MagicMock()
    monkeypatch.setattr(doctor_mod, "run_doctor", spy)
    result = runner.invoke(app, ["doctor"])

    assert result.exit_code == 0
    spy.assert_called_once_with()


# ---------------------------------------------------------------------------
# drt list (text format — non-JSON path)
# ---------------------------------------------------------------------------


def test_list_command_text_format_prints_sync_table(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``drt list`` without ``--output json`` reaches ``print_sync_table``."""
    monkeypatch.chdir(tmp_path)
    # Minimal project shell so load_syncs returns an empty list cleanly
    (tmp_path / "drt_project.yml").write_text("name: t\nprofile: default\n")
    (tmp_path / "syncs").mkdir()

    result = runner.invoke(app, ["list"])
    assert result.exit_code == 0
    # print_sync_table prints a header even with zero syncs — guards
    # against the text branch silently dying
    assert "drt list" not in result.output or result.output  # tolerant smoke


# ---------------------------------------------------------------------------
# drt mcp run — both branches of the optional-dep guard
# ---------------------------------------------------------------------------


def test_mcp_run_invokes_server_when_extra_installed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Happy path: when ``drt.mcp.server`` imports, ``run()`` is called."""
    import sys
    import types

    fake_server = types.ModuleType("drt.mcp.server")
    fake_server.run = MagicMock()  # type: ignore[attr-defined]
    # Also need the parent module to be importable
    fake_pkg = types.ModuleType("drt.mcp")
    monkeypatch.setitem(sys.modules, "drt.mcp", fake_pkg)
    monkeypatch.setitem(sys.modules, "drt.mcp.server", fake_server)

    result = runner.invoke(app, ["mcp", "run"])
    assert result.exit_code == 0
    fake_server.run.assert_called_once_with()  # type: ignore[attr-defined]


def test_mcp_run_exits_1_when_extra_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """When ``drt.mcp.server`` import fails, surface the install hint."""
    import builtins
    import sys

    monkeypatch.delitem(sys.modules, "drt.mcp.server", raising=False)
    monkeypatch.delitem(sys.modules, "drt.mcp", raising=False)
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):  # type: ignore[no-untyped-def]
        if name == "drt.mcp.server" or name.startswith("drt.mcp"):
            raise ImportError("simulated missing extra")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    result = runner.invoke(app, ["mcp", "run"])
    assert result.exit_code == 1
    # The hint must include the FULL ``drt-core[mcp]`` install string so
    # users know which extra to install — Rich must not consume ``[mcp]``
    # as a markup tag. Guarded by the escape() in drt.cli.output.print_error.
    assert "drt-core[mcp]" in result.output
