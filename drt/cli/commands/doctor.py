"""`drt doctor` — environment check command."""

from __future__ import annotations

from drt.cli._app import app


@app.command()
def doctor() -> None:
    """Check environment and report potential issues."""
    from drt.cli.doctor import run_doctor

    run_doctor()
