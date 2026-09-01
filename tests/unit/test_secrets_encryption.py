"""age encryption and CLI auto-decryption tests (#303)."""

from __future__ import annotations

import builtins
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.config import encryption
from drt.config.credentials import _load_secrets
from drt.config.encryption import (
    SECRETS_KEY_ENV_VAR,
    InvalidSecretsKeyError,
    MissingSecretsKeyError,
    SecretsDecryptionError,
    decrypt_secrets,
    decrypt_secrets_file,
    encrypt_secrets,
    encrypt_secrets_file,
)

runner = CliRunner()


@pytest.fixture()
def age_key() -> str:
    pyrage = pytest.importorskip("pyrage")
    return str(pyrage.x25519.Identity.generate())


def test_encrypt_decrypt_round_trip(age_key: str) -> None:
    plaintext = b'[destinations]\nAPI_TOKEN = "secret"\n'

    ciphertext = encrypt_secrets(plaintext, age_key)

    assert ciphertext != plaintext
    assert decrypt_secrets(ciphertext, age_key) == plaintext


def test_decrypt_with_wrong_key_fails(age_key: str) -> None:
    pyrage = pytest.importorskip("pyrage")
    wrong_key = str(pyrage.x25519.Identity.generate())
    ciphertext = encrypt_secrets(b"secret", age_key)

    with pytest.raises(SecretsDecryptionError, match="Check that the key matches"):
        decrypt_secrets(ciphertext, wrong_key)


def test_missing_key_fails_with_actionable_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv(SECRETS_KEY_ENV_VAR, raising=False)

    with pytest.raises(MissingSecretsKeyError, match=SECRETS_KEY_ENV_VAR):
        encrypt_secrets(b"secret")


def test_invalid_key_fails_with_actionable_message() -> None:
    with pytest.raises(InvalidSecretsKeyError, match="AGE-SECRET-KEY"):
        encrypt_secrets(b"secret", "not-an-age-key")


def test_missing_pyrage_fails_with_install_message(
    monkeypatch: pytest.MonkeyPatch, age_key: str
) -> None:
    real_import = builtins.__import__

    def import_without_pyrage(name: str, *args: Any, **kwargs: Any) -> Any:
        if name == "pyrage":
            raise ModuleNotFoundError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_pyrage)

    with pytest.raises(ImportError, match=r"pip install 'drt-core\[encryption\]'"):
        encrypt_secrets(b"secret", age_key)


def test_encrypt_library_error_is_wrapped(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeEncryptError(Exception):
        pass

    class FakeIdentity:
        def to_public(self) -> object:
            return object()

    def fail_encrypt(plaintext: bytes, recipients: list[object]) -> bytes:
        raise FakeEncryptError

    fake_pyrage = SimpleNamespace(EncryptError=FakeEncryptError, encrypt=fail_encrypt)
    monkeypatch.setattr(encryption, "_identity", lambda key=None: FakeIdentity())
    monkeypatch.setattr(encryption, "_pyrage", lambda: fake_pyrage)

    with pytest.raises(ValueError, match="Unable to encrypt"):
        encrypt_secrets(b"secret")


@pytest.mark.parametrize(
    ("path_name", "operation", "message"),
    [
        ("other.toml", encrypt_secrets_file, "scoped to secrets.toml"),
        ("other.toml.enc", decrypt_secrets_file, "expects secrets.toml.enc"),
    ],
)
def test_file_operations_reject_out_of_scope_names(
    tmp_path: Path, path_name: str, operation: Any, message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        operation(tmp_path / path_name)


@pytest.mark.parametrize(
    ("path_name", "operation", "message"),
    [
        ("secrets.toml", encrypt_secrets_file, "Plaintext secrets file not found"),
        ("secrets.toml.enc", decrypt_secrets_file, "Encrypted secrets file not found"),
    ],
)
def test_file_operations_report_missing_source(
    tmp_path: Path, path_name: str, operation: Any, message: str
) -> None:
    with pytest.raises(FileNotFoundError, match=message):
        operation(tmp_path / path_name)


def test_encrypt_refuses_existing_destination_unless_forced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plaintext = tmp_path / "secrets.toml"
    encrypted = tmp_path / "secrets.toml.enc"
    plaintext.write_bytes(b"plaintext")
    encrypted.write_bytes(b"existing")
    monkeypatch.setattr(encryption, "encrypt_secrets", lambda data: b"replacement")

    with pytest.raises(FileExistsError, match="Use --force to replace it"):
        encrypt_secrets_file(plaintext)
    assert encrypted.read_bytes() == b"existing"

    assert encrypt_secrets_file(plaintext, force=True) == encrypted
    assert encrypted.read_bytes() == b"replacement"


def test_decrypt_refuses_existing_destination_unless_forced(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    encrypted = tmp_path / "secrets.toml.enc"
    plaintext = tmp_path / "secrets.toml"
    encrypted.write_bytes(b"ciphertext")
    plaintext.write_bytes(b"existing")
    monkeypatch.setattr(encryption, "decrypt_secrets", lambda data: b"replacement")

    with pytest.raises(FileExistsError, match="Use --force to replace it"):
        decrypt_secrets_file(encrypted)
    assert plaintext.read_bytes() == b"existing"

    assert decrypt_secrets_file(encrypted, force=True) == plaintext
    assert plaintext.read_bytes() == b"replacement"


def test_concurrent_encrypt_writers_cannot_both_create_destination(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    plaintext = tmp_path / "secrets.toml"
    plaintext.write_bytes(b"plaintext")
    writers_ready = threading.Barrier(2)

    def synchronized_encrypt(data: bytes) -> bytes:
        writers_ready.wait()
        return b"ciphertext"

    monkeypatch.setattr(encryption, "encrypt_secrets", synchronized_encrypt)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(encrypt_secrets_file, plaintext) for _ in range(2)]

    results: list[Path] = []
    errors: list[FileExistsError] = []
    for future in futures:
        try:
            results.append(future.result())
        except FileExistsError as exc:
            errors.append(exc)

    assert results == [tmp_path / "secrets.toml.enc"]
    assert len(errors) == 1
    assert "Use --force to replace it" in str(errors[0])


def test_private_write_ignores_chmod_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_chmod(fd: int, mode: int) -> None:
        raise OSError("unsupported")

    monkeypatch.setattr(encryption.os, "chmod", fail_chmod)
    output = tmp_path / "secrets.toml"

    encryption._write_private_bytes(output, b"secret")

    assert output.read_bytes() == b"secret"


def test_private_write_closes_descriptor_if_fdopen_fails(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    real_close = encryption.os.close
    closed_fds: list[int] = []

    def fail_fdopen(fd: int, mode: str) -> Any:
        raise OSError("fdopen failed")

    def record_close(fd: int) -> None:
        closed_fds.append(fd)
        real_close(fd)

    monkeypatch.setattr(encryption.os, "fdopen", fail_fdopen)
    monkeypatch.setattr(encryption.os, "close", record_close)

    with pytest.raises(OSError, match="fdopen failed"):
        encryption._write_private_bytes(tmp_path / "secrets.toml", b"secret")

    assert len(closed_fds) == 1


def test_cli_round_trip_preserves_plaintext_by_default(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, age_key: str
) -> None:
    plaintext = tmp_path / "secrets.toml"
    content = '[destinations]\nAPI_TOKEN = "secret"\n'
    plaintext.write_text(content)
    monkeypatch.setenv(SECRETS_KEY_ENV_VAR, age_key)

    encrypted_result = runner.invoke(app, ["encrypt", str(plaintext)])

    encrypted = tmp_path / "secrets.toml.enc"
    assert encrypted_result.exit_code == 0, encrypted_result.output
    assert encrypted.exists()
    assert plaintext.read_text() == content
    assert "Plaintext was not deleted" in encrypted_result.output

    plaintext.unlink()
    decrypted_result = runner.invoke(app, ["decrypt", str(encrypted)])

    assert decrypted_result.exit_code == 0, decrypted_result.output
    assert plaintext.read_text() == content


def test_encrypted_file_takes_precedence_over_plaintext(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, age_key: str
) -> None:
    secrets_dir = tmp_path / ".drt"
    secrets_dir.mkdir()
    plaintext = secrets_dir / "secrets.toml"
    plaintext.write_text('[destinations]\nTOKEN = "encrypted-value"\n')
    monkeypatch.setenv(SECRETS_KEY_ENV_VAR, age_key)
    encrypt_secrets_file(plaintext)
    plaintext.write_text('[destinations]\nTOKEN = "stale-plaintext"\n')

    assert _load_secrets(tmp_path)["destinations"]["TOKEN"] == "encrypted-value"


def test_encrypted_file_without_key_fails_clearly(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, age_key: str
) -> None:
    secrets_dir = tmp_path / ".drt"
    secrets_dir.mkdir()
    plaintext = secrets_dir / "secrets.toml"
    plaintext.write_text('[destinations]\nTOKEN = "secret"\n')
    monkeypatch.setenv(SECRETS_KEY_ENV_VAR, age_key)
    encrypt_secrets_file(plaintext)
    plaintext.unlink()
    monkeypatch.delenv(SECRETS_KEY_ENV_VAR)

    with pytest.raises(MissingSecretsKeyError, match="AGE-SECRET-KEY"):
        _load_secrets(tmp_path)


@pytest.mark.parametrize("encrypted", [False, True])
def test_missing_toml_parser_is_fatal_only_for_encrypted_secrets(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, encrypted: bool
) -> None:
    secrets_dir = tmp_path / ".drt"
    secrets_dir.mkdir()
    suffix = ".enc" if encrypted else ""
    (secrets_dir / f"secrets.toml{suffix}").write_bytes(b"not parsed")
    real_import = builtins.__import__

    def import_without_toml(name: str, *args: Any, **kwargs: Any) -> Any:
        if name in {"tomllib", "tomli"}:
            raise ModuleNotFoundError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_toml)

    if encrypted:
        with pytest.raises(ImportError, match=r"pip install 'drt-core\[encryption\]'"):
            _load_secrets(tmp_path)
    else:
        assert _load_secrets(tmp_path) == {}


@pytest.mark.parametrize("command", ["encrypt", "decrypt"])
@pytest.mark.parametrize(
    ("error_type", "message"),
    [
        (FileNotFoundError, "source is missing"),
        (FileExistsError, "destination exists"),
        (ImportError, "install encryption support"),
        (ValueError, "invalid encryption input"),
    ],
)
def test_cli_reports_caught_errors_and_exits_one(
    monkeypatch: pytest.MonkeyPatch,
    command: str,
    error_type: type[Exception],
    message: str,
) -> None:
    function_name = f"{command}_secrets_file"

    def fail(path: Path, *, force: bool = False) -> Path:
        raise error_type(message)

    monkeypatch.setattr(encryption, function_name, fail)

    result = runner.invoke(app, [command, f"secrets.toml{'.enc' if command == 'decrypt' else ''}"])

    assert result.exit_code == 1
    assert message in result.output


def test_drt_run_auto_decrypts_project_secrets_in_memory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, age_key: str
) -> None:
    """A real CLI run reaches Slack's resolver without plaintext on disk."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv(SECRETS_KEY_ENV_VAR, age_key)
    drt_dir = tmp_path / ".drt"
    drt_dir.mkdir()
    monkeypatch.setattr("drt.config.credentials._config_dir", lambda override=None: drt_dir)

    (tmp_path / "drt_project.yml").write_text(
        yaml.dump({"name": "encrypted", "version": "0.1", "profile": "local"})
    )
    (drt_dir / "profiles.yml").write_text(
        yaml.dump({"profiles": {"local": {"type": "sqlite", "database": ":memory:"}}})
    )
    plaintext = drt_dir / "secrets.toml"
    webhook_url = "https://hooks.slack.test/encrypted"
    plaintext.write_text(f'[destinations.slack]\nSLACK_WEBHOOK_URL = "{webhook_url}"\n')
    encrypt_secrets_file(plaintext)
    plaintext.unlink()

    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "notify.yml").write_text(
        yaml.dump(
            {
                "name": "notify",
                "model": "SELECT 1 AS id",
                "destination": {
                    "type": "slack",
                    "webhook_url_env": "SLACK_WEBHOOK_URL",
                    "message_template": "row {{ row.id }}",
                },
                "sync": {"mode": "full", "batch_size": 1},
            }
        )
    )

    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(200, request=request)

    real_client = httpx.Client
    transport = httpx.MockTransport(handler)

    def mock_client(*args: Any, **kwargs: Any) -> httpx.Client:
        kwargs["transport"] = transport
        return real_client(*args, **kwargs)

    monkeypatch.setattr(httpx, "Client", mock_client)

    result = runner.invoke(app, ["run"])

    assert result.exit_code == 0, result.output
    assert [str(request.url) for request in requests] == [webhook_url]
    assert not plaintext.exists()
