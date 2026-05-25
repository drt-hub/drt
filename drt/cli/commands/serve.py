"""`drt serve` — HTTP endpoint that triggers syncs on demand."""

from __future__ import annotations

import os

import typer

from drt.cli._app import app


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", "--host", help="Host to bind."),
    port: int = typer.Option(8080, "--port", "-p", help="Port to bind."),
    token_env: str = typer.Option(
        "DRT_WEBHOOK_TOKEN",
        "--token-env",
        help="Env var holding bearer token for auth. Empty/unset = no auth.",
    ),
) -> None:
    """Start an HTTP endpoint that triggers drt syncs on demand.

    Example:
        drt serve --port 8080 --token-env DRT_WEBHOOK_TOKEN

        curl -X POST http://localhost:8080/sync/my_sync \\
          -H "Authorization: Bearer $DRT_WEBHOOK_TOKEN"
    """
    from drt.cli.server import serve as serve_impl

    token = os.environ.get(token_env) or None
    serve_impl(host=host, port=port, token=token, project_dir=".")
