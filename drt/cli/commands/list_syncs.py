"""`drt list` — list all sync definitions in the project."""

from __future__ import annotations

import json
from pathlib import Path

import typer

from drt.cli._app import app
from drt.cli.output import print_sync_table


@app.command(name="list")
def list_syncs(
    output: str = typer.Option("text", "--output", "-o", help="Output format: text or json."),
) -> None:
    """List all sync definitions in the project.

    Examples:
      drt list
      drt list --output json
    """
    from drt.config.parser import load_syncs

    syncs = load_syncs(Path("."))

    if output == "json":
        print(
            json.dumps(
                {
                    "syncs": [
                        {
                            "name": s.name,
                            "destination_type": s.destination.type,
                            "mode": s.sync.mode,
                            "description": s.description,
                        }
                        for s in syncs
                    ],
                },
                indent=2,
            )
        )
        return

    print_sync_table(syncs)
