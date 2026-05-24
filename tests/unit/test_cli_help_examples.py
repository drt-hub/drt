"""Tests for command help examples."""

from __future__ import annotations

import re

from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _plain(output: str) -> str:
    """Strip Rich ANSI styling and wrap whitespace from CLI help output."""
    return re.sub(r"\s+", " ", _ANSI_RE.sub("", output))


def test_run_help_shows_examples() -> None:
    result = runner.invoke(app, ["run", "--help"])
    output = _plain(result.output)

    assert result.exit_code == 0
    assert "Examples:" in output
    assert "drt run --select post_users" in output
    assert "drt run --select tag:crm --threads 4" in output
    assert "drt run --dry-run --diff" in output


def test_list_help_shows_examples() -> None:
    result = runner.invoke(app, ["list", "--help"])
    output = _plain(result.output)

    assert result.exit_code == 0
    assert "Examples:" in output
    assert "drt list" in output
    assert "drt list --output json" in output


def test_validate_help_shows_examples() -> None:
    result = runner.invoke(app, ["validate", "--help"])
    output = _plain(result.output)

    assert result.exit_code == 0
    assert "Examples:" in output
    assert "drt validate --select post_users" in output
    assert "drt validate --emit-schema" in output
    assert "drt validate --strict" in output
