"""``drt encrypt`` / ``drt decrypt`` for project-local secrets (#303)."""

from __future__ import annotations

from pathlib import Path

import typer

from drt.cli._app import app
from drt.cli.output import console, print_error


@app.command()
def encrypt(
    path: Path = typer.Argument(..., help="Path to a plaintext secrets.toml file."),
    force: bool = typer.Option(False, "--force", help="Replace an existing .enc file."),
) -> None:
    """Encrypt secrets.toml with the age identity in DRT_SECRETS_KEY."""
    from drt.config.encryption import encrypt_secrets_file

    try:
        output = encrypt_secrets_file(path, force=force)
    except (FileNotFoundError, FileExistsError, ImportError, ValueError) as exc:
        print_error(str(exc))
        raise typer.Exit(1) from exc

    console.print(f"[green]Encrypted[/green] {path} -> {output}")
    console.print(
        f"[yellow]Plaintext was not deleted:[/yellow] {path}. "
        "After verifying drt can use the encrypted file, delete the plaintext yourself."
    )


@app.command()
def decrypt(
    path: Path = typer.Argument(..., help="Path to an encrypted secrets.toml.enc file."),
    force: bool = typer.Option(False, "--force", help="Replace an existing plaintext file."),
) -> None:
    """Decrypt secrets.toml.enc with the age identity in DRT_SECRETS_KEY."""
    from drt.config.encryption import decrypt_secrets_file

    try:
        output = decrypt_secrets_file(path, force=force)
    except (FileNotFoundError, FileExistsError, ImportError, ValueError) as exc:
        print_error(str(exc))
        raise typer.Exit(1) from exc

    console.print(f"[green]Decrypted[/green] {path} -> {output}")
    console.print(f"[yellow]Plaintext contains secrets:[/yellow] protect or remove {output}.")
