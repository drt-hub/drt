"""`drt serve` — HTTP endpoint that triggers syncs on demand."""

from __future__ import annotations

import os

import typer

from drt.cli._app import app

_AUTH_SCHEMES = ("auto", "none", "bearer", "hmac", "oidc")
_HMAC_SCHEMES = ("generic", "stripe")

# Each sender names the header differently, so the default follows the scheme
# rather than making every Stripe user pass --hmac-header. An explicit
# --hmac-header still wins.
_DEFAULT_HMAC_HEADER = {"generic": "X-Hub-Signature-256", "stripe": "Stripe-Signature"}


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
            "none, bearer, hmac (HMAC-SHA256 body signature), or oidc "
            "(Google-signed OIDC JWT — Pub/Sub push's own scheme)."
        ),
    ),
    hmac_secret_env: str = typer.Option(
        "DRT_WEBHOOK_HMAC_SECRET",
        "--hmac-secret-env",
        help="Env var holding the HMAC signing secret (for --auth hmac).",
    ),
    hmac_header: str = typer.Option(
        "",
        "--hmac-header",
        help=(
            "Header carrying the HMAC signature (for --auth hmac). "
            "Defaults to X-Hub-Signature-256, or Stripe-Signature "
            "under --hmac-scheme stripe."
        ),
    ),
    hmac_scheme: str = typer.Option(
        "generic",
        "--hmac-scheme",
        help=(
            "Signature shape for --auth hmac: generic (HMAC of the body — "
            "GitHub, Shopify, bare hex) or stripe (timestamped t=...,v1=...)."
        ),
    ),
    hmac_tolerance: int = typer.Option(
        300,
        "--hmac-tolerance",
        help=(
            "Replay window in seconds for --hmac-scheme stripe. "
            "Stripe's own libraries default to 300."
        ),
    ),
    oidc_audience: str = typer.Option(
        "",
        "--oidc-audience",
        help=(
            "Required for --auth oidc: the URL Pub/Sub's push subscription "
            "was configured with (the aud claim Google's JWT must carry)."
        ),
    ),
    oidc_issuer: str = typer.Option(
        "https://accounts.google.com",
        "--oidc-issuer",
        help="Expected iss claim for --auth oidc.",
    ),
    oidc_email: str = typer.Option(
        "",
        "--oidc-email",
        help=(
            "Restrict --auth oidc to one service account's email "
            "(e.g. the Pub/Sub subscription's own push service account). "
            "Empty accepts any Google-signed token for the right audience."
        ),
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
    if auth == "oidc" and not oidc_audience:
        raise typer.BadParameter("--auth oidc requires --oidc-audience", param_hint="--auth")
    if hmac_scheme not in _HMAC_SCHEMES:
        raise typer.BadParameter(
            f"--hmac-scheme must be one of {', '.join(_HMAC_SCHEMES)}",
            param_hint="--hmac-scheme",
        )
    if hmac_tolerance <= 0:
        # Stripe's docs call a tolerance of 0 out specifically: it disables the
        # recency check rather than making it strict.
        raise typer.BadParameter(
            "--hmac-tolerance must be a positive number of seconds",
            param_hint="--hmac-tolerance",
        )
    serve_impl(
        host=host,
        port=port,
        token=token,
        project_dir=".",
        auth_scheme=auth,
        hmac_secret=hmac_secret,
        hmac_header=hmac_header or _DEFAULT_HMAC_HEADER[hmac_scheme],
        hmac_scheme=hmac_scheme,
        hmac_tolerance=hmac_tolerance,
        oidc_audience=oidc_audience or None,
        oidc_issuer=oidc_issuer or None,
        oidc_email=oidc_email or None,
    )
