"""Tests for `drt --version` enriched output (issue #515).

The first line must stay ``drt version X.Y.Z`` so existing scripts grepping
that pattern keep working. The follow-up lines add Python, install path,
and platform diagnostics that save a round-trip on bug reports.
"""

from __future__ import annotations

import platform
import sys
from pathlib import Path

from typer.testing import CliRunner

import drt as drt_pkg
from drt import __version__
from drt.cli.main import app

runner = CliRunner()


class TestVersionFlag:
    def test_first_line_is_backwards_compatible(self) -> None:
        """`drt version X.Y.Z` on line 1 -- locks BC for scripts."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        first_line = result.stdout.splitlines()[0]
        assert first_line == f"drt version {__version__}"

    def test_short_flag_works(self) -> None:
        result = runner.invoke(app, ["-v"])
        assert result.exit_code == 0
        assert f"drt version {__version__}" in result.stdout

    def test_output_includes_python_line(self) -> None:
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        py = sys.version_info
        impl = platform.python_implementation()
        assert f"Python {py.major}.{py.minor}.{py.micro} ({impl})" in result.stdout

    def test_output_includes_install_path(self) -> None:
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        install_path = str(Path(drt_pkg.__file__).resolve().parent)
        assert "Install:" in result.stdout
        assert install_path in result.stdout

    def test_output_includes_platform_line(self) -> None:
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        assert "Platform:" in result.stdout
        # System name (Linux / Darwin / Windows) should appear somewhere on the line.
        assert platform.system() in result.stdout

    def test_output_has_four_lines(self) -> None:
        """version + python + install + platform = exactly 4 lines."""
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        assert len(lines) == 4, f"Expected 4 lines, got {len(lines)}: {lines!r}"

    def test_exit_code_is_zero(self) -> None:
        result = runner.invoke(app, ["--version"])
        assert result.exit_code == 0
