"""age encryption helpers for the project-local ``secrets.toml`` (#303).

The optional ``pyrage`` dependency is imported lazily so a normal drt install
does not pay for encryption support unless an encrypted secrets file or one of
the encryption CLI commands is used. Plaintext is always encrypted/decrypted
in memory; only the explicit ``drt decrypt`` command writes plaintext to disk.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

SECRETS_KEY_ENV_VAR = "DRT_SECRETS_KEY"
SECRETS_FILENAME = "secrets.toml"
ENCRYPTED_SECRETS_FILENAME = f"{SECRETS_FILENAME}.enc"


class SecretsEncryptionError(ValueError):
    """Base error for encrypted local secrets."""


class MissingSecretsKeyError(SecretsEncryptionError):
    """The age identity environment variable is not available."""


class InvalidSecretsKeyError(SecretsEncryptionError):
    """The configured age identity cannot be parsed."""


class SecretsDecryptionError(SecretsEncryptionError):
    """Ciphertext cannot be decrypted with the configured age identity."""


def _pyrage() -> Any:
    try:
        import pyrage
    except ModuleNotFoundError as exc:
        raise ImportError(
            "Encrypted secrets require the optional encryption extra. "
            "Install it with: pip install 'drt-core[encryption]'"
        ) from exc
    return pyrage


def _identity(key: str | None = None) -> Any:
    value = key if key is not None else os.environ.get(SECRETS_KEY_ENV_VAR)
    if not value:
        raise MissingSecretsKeyError(
            f"Encrypted secrets require an age identity in {SECRETS_KEY_ENV_VAR}. "
            "Set that environment variable to an AGE-SECRET-KEY-... value and retry."
        )

    pyrage = _pyrage()
    try:
        return pyrage.x25519.Identity.from_str(value.strip())
    except pyrage.IdentityError as exc:
        raise InvalidSecretsKeyError(
            f"{SECRETS_KEY_ENV_VAR} is not a valid age X25519 identity "
            "(expected AGE-SECRET-KEY-...)."
        ) from exc


def encrypt_secrets(plaintext: bytes, key: str | None = None) -> bytes:
    """Encrypt ``plaintext`` to the public recipient derived from an age identity."""
    identity = _identity(key)
    pyrage = _pyrage()
    try:
        encrypted: bytes = pyrage.encrypt(plaintext, [identity.to_public()])
    except pyrage.EncryptError as exc:
        raise SecretsEncryptionError("Unable to encrypt secrets.toml with age.") from exc
    return encrypted


def decrypt_secrets(ciphertext: bytes, key: str | None = None) -> bytes:
    """Decrypt age ``ciphertext`` with the configured X25519 identity."""
    identity = _identity(key)
    pyrage = _pyrage()
    try:
        plaintext: bytes = pyrage.decrypt(ciphertext, [identity])
    except pyrage.DecryptError as exc:
        raise SecretsDecryptionError(
            f"Unable to decrypt secrets.toml.enc with {SECRETS_KEY_ENV_VAR}. "
            "Check that the key matches the recipient used to encrypt the file."
        ) from exc
    return plaintext


def _validate_plaintext_path(path: Path) -> None:
    if path.name != SECRETS_FILENAME:
        raise SecretsEncryptionError(
            f"Config encryption is scoped to {SECRETS_FILENAME}; got {path.name!r}."
        )


def _validate_encrypted_path(path: Path) -> None:
    if path.name != ENCRYPTED_SECRETS_FILENAME:
        raise SecretsEncryptionError(
            f"Config decryption expects {ENCRYPTED_SECRETS_FILENAME}; got {path.name!r}."
        )


def _write_private_bytes(path: Path, data: bytes) -> None:
    """Write sensitive config owner-only from creation on POSIX."""
    if os.name != "posix":
        path.write_bytes(data)
        return
    fd = os.open(path, os.O_CREAT | os.O_WRONLY | os.O_TRUNC, 0o600)
    try:
        try:
            os.chmod(fd, 0o600)
        except OSError:
            pass
        with os.fdopen(fd, "wb") as file_obj:
            fd = -1
            file_obj.write(data)
    finally:
        if fd >= 0:
            os.close(fd)


def encrypt_secrets_file(path: Path, *, force: bool = False) -> Path:
    """Encrypt ``secrets.toml`` to the adjacent ``secrets.toml.enc`` file."""
    _validate_plaintext_path(path)
    if not path.is_file():
        raise FileNotFoundError(f"Plaintext secrets file not found: {path}")
    output = path.with_name(ENCRYPTED_SECRETS_FILENAME)
    if output.exists() and not force:
        raise FileExistsError(
            f"Encrypted secrets file already exists: {output}. Use --force to replace it."
        )
    _write_private_bytes(output, encrypt_secrets(path.read_bytes()))
    return output


def decrypt_secrets_file(path: Path, *, force: bool = False) -> Path:
    """Decrypt ``secrets.toml.enc`` to the adjacent ``secrets.toml`` file."""
    _validate_encrypted_path(path)
    if not path.is_file():
        raise FileNotFoundError(f"Encrypted secrets file not found: {path}")
    output = path.with_name(SECRETS_FILENAME)
    if output.exists() and not force:
        raise FileExistsError(
            f"Plaintext secrets file already exists: {output}. Use --force to replace it."
        )
    _write_private_bytes(output, decrypt_secrets(path.read_bytes()))
    return output
