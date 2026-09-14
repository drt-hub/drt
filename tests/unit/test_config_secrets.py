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


def test_secret_reason_exempts_jinja_templates(tmp_path: Path) -> None:
    """#897 (Codex review of PR #1150): a compact, no-whitespace Jinja
    template used as an idempotency key (e.g. `{{row.customer_uuid}}`, 21
    chars, no whitespace) reads as dense mixed-case text -- exactly what
    the entropy heuristic is designed to catch in a real secret -- so
    without this exemption `native_idempotency_key`/`idempotency_key`
    would spuriously fail `drt validate --strict`."""
    assert _secret_reason("{{row.customer_uuid}}") is None
    assert _secret_reason("{{ row.id }}") is None

    syncs_dir = tmp_path / "syncs"
    syncs_dir.mkdir()
    (syncs_dir / "idem.yml").write_text(
        "\n".join(
            [
                "name: idem",
                "destination:",
                "  type: rest_api",
                "  native_idempotency_key: '{{row.customer_uuid}}'",
            ]
        )
    )

    assert find_hardcoded_secrets(tmp_path) == []


def test_secret_reason_still_catches_literal_secret_next_to_a_template() -> None:
    """#897 (Codex review of PR #1150, round 3): the Jinja/env-var exemption
    only suppresses the entropy fallback -- a literal known-pattern secret
    hardcoded alongside a template reference (e.g. someone appending a
    Jinja suffix to a real Stripe key) must still be flagged, not silently
    waved through by an early return."""
    value = "sk_live_" + "a" * 24 + "{{ var('suffix') }}"
    reason = _secret_reason(value)

    assert reason is not None
    assert reason == "Stripe live secret key"


def test_secret_reason_catches_generic_high_entropy_secret_next_to_a_template() -> None:
    """#897 (Codex review of PR #1150, round 4): round 3's fix only ensured
    the six known-provider *patterns* still matched next to a template --
    it left the entropy *fallback* blanket-exempted whenever the value
    contained ``${`` or ``{{`` anywhere, so a generic high-entropy secret
    with no known-provider pattern (most real secrets) followed by a Jinja
    suffix would go undetected. Template expressions are now stripped
    before the entropy check runs, not used to bypass it outright."""
    generic_secret = "A1b2C3d4E5f6G7h8I9j0K1l2M3n4"
    assert _secret_reason(generic_secret) is not None  # sanity: flagged alone

    reason = _secret_reason(generic_secret + "{{ var('suffix') }}")

    assert reason is not None
    assert reason.startswith("high entropy")


def test_entropy_helpers_cover_boundary_cases() -> None:
    assert _shannon_entropy("") == 0.0
    assert not _looks_high_entropy("short-token")
    assert not _looks_high_entropy("long token with whitespace value")
