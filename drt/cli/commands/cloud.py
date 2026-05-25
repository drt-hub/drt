"""`drt cloud` — stub commands for the future drt Cloud service."""

from __future__ import annotations

import typer

from drt.cli._app import app
from drt.cli.output import console

cloud_app = typer.Typer(name="cloud", help="drt Cloud commands (stub).", no_args_is_help=True)
app.add_typer(cloud_app)


CLOUD_MESSAGE = (
    "\n[bold blue]🚀 drt Cloud[/bold blue]\n"
    "This is a stub for the future drt Cloud service.\n"
    "[dim]Coming soon... Follow https://github.com/drt-hub/drt for updates.[/dim]\n"
)


@cloud_app.command(name="push")
def cloud_push() -> None:
    """Push local project configuration to drt Cloud (stub)."""
    console.print(CLOUD_MESSAGE)


@cloud_app.command(name="status")
def cloud_status() -> None:
    """Check drt Cloud deployment status (stub)."""
    console.print(CLOUD_MESSAGE)
