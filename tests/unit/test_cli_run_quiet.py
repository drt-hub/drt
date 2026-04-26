"""Tests for the --quiet flag on the run command."""

from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()


def test_run_accepts_quiet_long_form() -> None:
    """The --quiet option is recognised by the run command parser."""
    result = runner.invoke(app, ["run", "--quiet"])

    # exit 2 = Click/Typer "no such option" error.
    # Any other exit code means the flag was parsed (the run itself may fail
    # because no project file is in the cwd, which is fine for this test).
    assert result.exit_code != 2


def test_run_accepts_quiet_short_form() -> None:
    """The -q short alias is recognised by the run command parser."""
    result = runner.invoke(app, ["run", "-q"])

    assert result.exit_code != 2
