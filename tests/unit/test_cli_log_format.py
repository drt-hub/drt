"""Tests for the --log-format option on the run command.

Covers the ``LogFormat(str, Enum)`` path introduced in #577 — both the
JSON branch that switches the root logger to ``_JsonFormatter`` and the
Choice-style validation that rejects unknown values (the latter is what
the typer 0.26 stubs were rejecting at the type level before the Enum
refactor).
"""

from __future__ import annotations

import logging

from typer.testing import CliRunner

from drt.cli.main import app

runner = CliRunner()


def test_run_accepts_log_format_json() -> None:
    """``--log-format json`` is parsed and exercises the JSON-logging branch."""
    original_handlers = logging.root.handlers[:]
    try:
        result = runner.invoke(app, ["run", "--log-format", "json"])
        # exit 2 = Click/Typer "invalid option/value" error.
        # Any other exit code means the value was accepted (the run itself may
        # fail because no project file is in the cwd — that's fine here).
        assert result.exit_code != 2
    finally:
        logging.root.handlers = original_handlers


def test_run_accepts_log_format_text() -> None:
    """``--log-format text`` (the default) is also accepted explicitly."""
    result = runner.invoke(app, ["run", "--log-format", "text"])
    assert result.exit_code != 2


def test_run_rejects_invalid_log_format() -> None:
    """Choice derived from the LogFormat Enum rejects unknown values."""
    result = runner.invoke(app, ["run", "--log-format", "invalid"])
    assert result.exit_code == 2
    assert "'invalid' is not one of 'text', 'json'" in result.output
