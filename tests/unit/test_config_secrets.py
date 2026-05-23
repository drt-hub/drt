"""Tests for hardcoded secret detection helpers."""

from __future__ import annotations

from pathlib import Path

from drt.config.secrets import (
    _looks_high_entropy,
    _secret_reason,
    _shannon_entropy,
    find_hardcoded_secrets,
)


def test_find_hardcoded_secrets_skips_bad_and_non_mapping_yaml(tmp_path: Path) -> None:
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "bad.yml").write_text("name: [")
    (syncs_dir / "scalar.yml").write_text("just-a-string")

    assert find_hardcoded_secrets(tmp_path) == []


def test_find_hardcoded_secrets_recurses_lists_and_uses_filename_fallback(
    tmp_path: Path,
) -> None:
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "nested.yml").write_text(
        "\n".join(
            [
                "destination:",
                "  credentials:",
                "    - api_key: sk-" + "A" * 32,
            ]
        )
    )

    findings = find_hardcoded_secrets(tmp_path)

    assert len(findings) == 1
    assert findings[0].sync_name == "nested"
    assert findings[0].file == "nested.yml"
    assert findings[0].path == "destination.credentials.[0].api_key"
    assert findings[0].reason == "OpenAI-style token"


def test_find_hardcoded_secrets_ignores_env_and_path_suffixes(tmp_path: Path) -> None:
    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "safe.yml").write_text(
        "\n".join(
            [
                "name: safe",
                "destination:",
                "  token_env: sk-" + "B" * 32,
                "  password_path: /run/secrets/db-password",
            ]
        )
    )

    assert find_hardcoded_secrets(tmp_path) == []


def test_secret_reason_uses_entropy_and_ignores_blank_values() -> None:
    assert _secret_reason("   ") is None
    assert _secret_reason("abc123abc123") is None
    assert _secret_reason("abc def ghi jkl mno pqr stu") is None

    reason = _secret_reason("A1b2C3d4E5f6G7h8I9j0K1l2M3n4")

    assert reason is not None
    assert reason.startswith("high entropy")


def test_entropy_helpers_cover_boundary_cases() -> None:
    assert _shannon_entropy("") == 0.0
    assert not _looks_high_entropy("short-token")
    assert not _looks_high_entropy("long token with whitespace value")
