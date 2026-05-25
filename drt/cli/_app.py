"""The shared ``typer.Typer`` instance for the drt CLI.

Lives in its own module so command modules under ``drt/cli/commands/`` can
``from drt.cli._app import app`` without triggering a circular import with
``drt/cli/main.py`` (which is the user-facing entry point and imports the
``commands`` package to register all commands).
"""

from __future__ import annotations

import typer

app = typer.Typer(
    name="drt",
    help="Reverse ETL for the code-first data stack.",
    no_args_is_help=True,
)
