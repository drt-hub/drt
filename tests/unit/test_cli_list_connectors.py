"""Tests for drt sources and drt destinations commands."""

from __future__ import annotations

from typer.testing import CliRunner

from drt.cli.main import app
from drt.config.connectors import DESTINATIONS, SOURCES

runner = CliRunner()


# ---------------------------------------------------------------------------
# drt sources
# ---------------------------------------------------------------------------


def test_sources_command_succeeds() -> None:
    """drt sources should exit with code 0 and contain header."""
    result = runner.invoke(app, ["sources"])
    assert result.exit_code == 0
    assert "Available sources:" in result.output


def test_sources_command_contains_all_connectors() -> None:
    """drt sources should list all available source connectors."""
    result = runner.invoke(app, ["sources"])
    assert result.exit_code == 0
    for source_type, description in SOURCES:
        assert source_type in result.output
        assert description in result.output


# ---------------------------------------------------------------------------
# drt destinations
# ---------------------------------------------------------------------------


def test_destinations_command_succeeds() -> None:
    """drt destinations should exit with code 0 and contain header."""
    result = runner.invoke(app, ["destinations"])
    assert result.exit_code == 0
    assert "Available destinations:" in result.output


def test_destinations_command_contains_all_connectors() -> None:
    """drt destinations should list all available destination connectors."""
    result = runner.invoke(app, ["destinations"])
    assert result.exit_code == 0
    for dest_type, description in DESTINATIONS:
        assert dest_type in result.output
        assert description in result.output
