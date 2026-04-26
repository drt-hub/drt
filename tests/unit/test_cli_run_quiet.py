"""Tests for the --quiet flag on the run command."""

from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()


def test_run_quiet_flag_appears_in_help() -> None:
    """The --quiet/-q option is wired into the run command."""
    result = runner.invoke(app, ["run", "--help"])

    assert result.exit_code == 0
    assert "--quiet" in result.stdout
    assert "-q" in result.stdout
    assert "Suppress output except errors" in result.stdout
