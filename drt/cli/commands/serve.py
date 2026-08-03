"""`drt serve` — HTTP endpoint that triggers syncs on demand."""

from __future__ import annotations

import os

import typer

from drt.cli._app import app

_AUTH_SCHEMES = ("auto", "none", "bearer", "hmac")


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", "--host", help="Host to bind."),
    port: int = typer.Option(8080, "--port", "-p", help="Port to bind."),
    token_env: str = typer.Option(
        "DRT_WEBHOOK_TOKEN",
        "--token-env",
        help="Env var holding bearer token for auth. Empty/unset = no auth.",
    ),
    auth: str = typer.Option(
        "auto",
        "--auth",
        help=(
            "Auth scheme: auto (bearer if token env set, else none), "
            "none, bearer, or hmac (HMAC-SHA256 body signature)."
        ),
    ),
    hmac_secret_env: str = typer.Option(
        "DRT_WEBHOOK_HMAC_SECRET",
        "--hmac-secret-env",
        help="Env var holding the HMAC signing secret (for --auth hmac).",
    ),
    hmac_header: str = typer.Option(
        "X-Hub-Signature-256",
        "--hmac-header",
        help="Header carrying the HMAC signature (for --auth hmac).",
    ),
) -> None:
    """Start an HTTP endpoint that triggers drt syncs on demand.

    A trigger is answered with 202 and a run id; poll GET /runs/<id> for the
    outcome, or pass ?wait=true to block for the result. Same-sync triggers
    coalesce; different syncs run concurrently.

    Example:
        drt serve --port 8080 --token-env DRT_WEBHOOK_TOKEN

        curl -X POST http://localhost:8080/sync/my_sync \\
          -H "Authorization: Bearer $DRT_WEBHOOK_TOKEN"
    """
    from drt.cli.server import serve as serve_impl

    if auth not in _AUTH_SCHEMES:
        raise typer.BadParameter(
            f"--auth must be one of {', '.join(_AUTH_SCHEMES)}", param_hint="--auth"
        )
    token = os.environ.get(token_env) or None
    hmac_secret = os.environ.get(hmac_secret_env) or None
    if auth == "bearer" and not token:
        raise typer.BadParameter(
            f"--auth bearer requires a token in ${token_env}", param_hint="--auth"
        )
    if auth == "hmac" and not hmac_secret:
        raise typer.BadParameter(
            f"--auth hmac requires a secret in ${hmac_secret_env}", param_hint="--auth"
        )
    serve_impl(
        host=host,
        port=port,
        token=token,
        project_dir=".",
        auth_scheme=auth,
        hmac_secret=hmac_secret,
        hmac_header=hmac_header,
    )
