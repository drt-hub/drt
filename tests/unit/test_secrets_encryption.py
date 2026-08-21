"""age encryption and CLI auto-decryption tests (#303)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.config.credentials import _load_secrets
from drt.config.encryption import (
    SECRETS_KEY_ENV_VAR,
    MissingSecretsKeyError,
    SecretsDecryptionError,
    decrypt_secrets,
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
    plaintext.write_text(
        f'[destinations.slack]\nSLACK_WEBHOOK_URL = "{webhook_url}"\n'
    )
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
