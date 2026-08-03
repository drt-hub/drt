"""Tests for the shared destructive-operation confirmation (#776).

Before this, `drt profile remove` called `typer.confirm` directly. That works
in a terminal, but in CI it prints a prompt to a stream nobody is reading and
then aborts with a bare "Aborted." — measured:

    $ drt profile remove x < /dev/null
    Remove profile 'x'? [y/N]: Aborted.
    (exit 1)

It fails, correctly. But the message names neither the cause nor the fix, and
`[y/N]` actively misleads: it suggests piping `y` would work, when the right
answer is `--yes`. Recovering from that costs a `--help` read at best.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
import typer

from drt.cli._helpers import confirm_destructive


class TestNonInteractive:
    """CI: stdin is at EOF, so there is nothing to read.

    Detected via click.Abort rather than `sys.stdin.isatty()`. A pipe carrying
    a real answer is not a TTY either, so an isatty check refuses input that
    was genuinely supplied — which broke `test_remove_confirm_declined`, where
    CliRunner feeds "n" through a non-TTY stdin.
    """

    def test_refuses_with_an_actionable_message(self) -> None:
        import click

        with patch("typer.confirm", side_effect=click.Abort()):
            with pytest.raises(typer.Exit) as exc:
                confirm_destructive("Reset state for 'users'?", yes=False)

        assert exc.value.exit_code == 1

    def test_the_error_names_the_fix(self, capsys: pytest.CaptureFixture[str]) -> None:
        """A CI user must learn what to do from the error itself."""
        import click

        with patch("typer.confirm", side_effect=click.Abort()):
            with pytest.raises(typer.Exit):
                confirm_destructive("Reset state for 'users'?", yes=False)

        err = capsys.readouterr().err
        assert "--yes" in err, "the error must name the fix"

    def test_yes_never_reaches_the_prompt(self) -> None:
        with patch("typer.confirm") as confirm:
            assert confirm_destructive("Reset?", yes=True) is True
        confirm.assert_not_called()

    def test_a_piped_answer_is_honoured_not_refused(self) -> None:
        """The regression this design exists to avoid: piped input is not a
        TTY, but it *is* an answer."""
        with patch("typer.confirm", return_value=False) as confirm:
            assert confirm_destructive("Reset?", yes=False) is False
        confirm.assert_called_once()


class TestInteractive:
    def test_prompts_and_honours_a_yes(self) -> None:
        with patch("sys.stdin.isatty", return_value=True):
            with patch("typer.confirm", return_value=True) as confirm:
                assert confirm_destructive("Reset?", yes=False) is True
        confirm.assert_called_once()

    def test_prompts_and_honours_a_no(self) -> None:
        with patch("sys.stdin.isatty", return_value=True):
            with patch("typer.confirm", return_value=False):
                assert confirm_destructive("Reset?", yes=False) is False

    def test_yes_skips_the_prompt_even_on_a_tty(self) -> None:
        """--yes means "don't ask me", not "ask me anyway"."""
        with patch("sys.stdin.isatty", return_value=True):
            with patch("typer.confirm") as confirm:
                assert confirm_destructive("Reset?", yes=True) is True
        confirm.assert_not_called()
