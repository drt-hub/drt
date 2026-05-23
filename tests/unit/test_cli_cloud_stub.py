"""Tests for drt cloud (stub) commands."""

from __future__ import annotations

from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()


def test_cloud_push_prints_coming_soon() -> None:
    """drt cloud push prints the coming soon message."""
    result = runner.invoke(app, ["cloud", "push"])
    assert result.exit_code == 0
    assert "coming soon" in result.output.lower()
    assert "drt-hub/drt" in result.output


def test_cloud_status_prints_coming_soon() -> None:
    """drt cloud status prints the coming soon message."""
    result = runner.invoke(app, ["cloud", "status"])
    assert result.exit_code == 0
    assert "coming soon" in result.output.lower()
    assert "drt-hub/drt" in result.output


def test_cloud_help_shows_subcommands() -> None:
    """drt cloud --help lists push and status subcommands."""
    result = runner.invoke(app, ["cloud", "--help"])
    assert result.exit_code == 0
    assert "push" in result.output
    assert "status" in result.output


def test_top_level_help_includes_cloud() -> None:
    """drt --help includes cloud in the command list."""
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "cloud" in result.output
