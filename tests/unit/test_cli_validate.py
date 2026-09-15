"""Tests for drt validate CLI error cases."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
import yaml
from typer.testing import CliRunner

from drt.cli.main import app
from drt.config.parser import _format_validation_errors, load_syncs_safe

runner = CliRunner()

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

VALID_SYNC = {
    "name": "test-sync",
    "model": "SELECT 1",
    "destination": {
        "type": "rest_api",
        "url": "https://example.com/api",
        "method": "POST",
    },
}


def _write_sync(syncs_dir: Path, name: str, data: dict | str) -> None:
    """Write a sync YAML file. Accepts dict (auto-dumps) or raw string."""
    syncs_dir.mkdir(parents=True, exist_ok=True)
    path = syncs_dir / f"{name}.yml"
    if isinstance(data, str):
        path.write_text(data)
    else:
        with path.open("w") as f:
            yaml.dump(data, f)


# ---------------------------------------------------------------------------
# load_syncs_safe — collects errors instead of raising
# ---------------------------------------------------------------------------


def test_load_syncs_safe_valid(tmp_path: Path) -> None:
    _write_sync(tmp_path / "syncs", "ok", VALID_SYNC)
    result = load_syncs_safe(tmp_path)
    assert len(result.syncs) == 1
    assert not result.errors


def test_load_syncs_safe_missing_fields(tmp_path: Path) -> None:
    _write_sync(tmp_path / "syncs", "bad", {"name": "incomplete"})
    result = load_syncs_safe(tmp_path)
    assert not result.syncs
    assert "bad" in result.errors
    assert any("model" in e for e in result.errors["bad"])
    assert any("destination" in e for e in result.errors["bad"])


def test_load_syncs_safe_invalid_destination_type(tmp_path: Path) -> None:
    sync = {**VALID_SYNC, "destination": {"type": "nonexistent", "url": "x"}}
    _write_sync(tmp_path / "syncs", "bad-type", sync)
    result = load_syncs_safe(tmp_path)
    assert not result.syncs
    assert "bad-type" in result.errors
    assert any("nonexistent" in e for e in result.errors["bad-type"])


def test_load_syncs_safe_mixed_valid_and_invalid(tmp_path: Path) -> None:
    _write_sync(tmp_path / "syncs", "a_good", VALID_SYNC)
    _write_sync(tmp_path / "syncs", "b_bad", {"name": "broken"})
    result = load_syncs_safe(tmp_path)
    assert len(result.syncs) == 1
    assert result.syncs[0].name == "test-sync"
    assert "b_bad" in result.errors


def test_load_syncs_safe_no_syncs_dir(tmp_path: Path) -> None:
    result = load_syncs_safe(tmp_path)
    assert not result.syncs
    assert not result.errors


def test_load_syncs_safe_incremental_missing_cursor(tmp_path: Path) -> None:
    sync = {
        **VALID_SYNC,
        "sync": {"mode": "incremental"},
    }
    _write_sync(tmp_path / "syncs", "no-cursor", sync)
    result = load_syncs_safe(tmp_path)
    assert not result.syncs
    assert "no-cursor" in result.errors
    assert any("cursor_field" in e for e in result.errors["no-cursor"])


def test_load_syncs_safe_expands_env_vars(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SAFE_URL", "https://safe.example.com")
    _write_sync(
        tmp_path / "syncs",
        "env",
        {**VALID_SYNC, "destination": {**VALID_SYNC["destination"], "url": "${SAFE_URL}"}},
    )
    result = load_syncs_safe(tmp_path)
    assert len(result.syncs) == 1
    assert result.syncs[0].destination.url == "https://safe.example.com"  # type: ignore[union-attr]
    assert not result.errors


def test_load_syncs_safe_missing_env_var_collected(tmp_path: Path) -> None:
    _write_sync(
        tmp_path / "syncs",
        "bad-env",
        {**VALID_SYNC, "destination": {**VALID_SYNC["destination"], "url": "${MISSING_VAR}"}},
    )
    result = load_syncs_safe(tmp_path)
    assert not result.syncs
    assert "bad-env" in result.errors
    assert any("MISSING_VAR" in e for e in result.errors["bad-env"])


# ---------------------------------------------------------------------------
# _format_validation_errors
# ---------------------------------------------------------------------------


def test_format_validation_errors_shows_path() -> None:
    from pydantic import ValidationError

    from drt.config.models import SyncConfig

    with pytest.raises(ValidationError) as exc_info:
        SyncConfig.model_validate({"name": "test"})
    messages = _format_validation_errors(exc_info.value)
    assert len(messages) >= 2
    # Should contain path-like location info
    assert any("model" in m for m in messages)
    assert any("destination" in m for m in messages)


# ---------------------------------------------------------------------------
# CLI validate — error output
# ---------------------------------------------------------------------------


def test_cli_validate_no_syncs(tmp_path: Path) -> None:
    result = runner.invoke(app, ["validate"], catch_exceptions=False)
    # Without syncs dir it should show "No syncs found"
    assert "No syncs found" in result.output


def test_cli_validate_valid_sync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_sync(tmp_path / "syncs", "good", VALID_SYNC)
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["validate"], catch_exceptions=False)
    assert "✓" in result.output
    assert result.exit_code == 0


def test_cli_validate_invalid_sync_shows_errors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_sync(tmp_path / "syncs", "broken", {"name": "broken"})
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["validate"])
    assert "✗" in result.output
    assert "broken" in result.output
    assert result.exit_code == 1


def test_cli_validate_mixed_shows_both(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _write_sync(tmp_path / "syncs", "a_good", VALID_SYNC)
    _write_sync(tmp_path / "syncs", "b_bad", {"name": "bad"})
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["validate"])
    assert "✓" in result.output
    assert "✗" in result.output
    assert result.exit_code == 1


def test_cli_validate_error_shows_field_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sync = {**VALID_SYNC, "destination": {"type": "nonexistent"}}
    _write_sync(tmp_path / "syncs", "bad-dest", sync)
    monkeypatch.chdir(tmp_path)
    result = runner.invoke(app, ["validate"])
    assert "destination" in result.output
    assert result.exit_code == 1


def test_cli_validate_warns_on_hardcoded_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sync = {
        **VALID_SYNC,
        "destination": {
            **VALID_SYNC["destination"],
            "auth": {"type": "bearer", "token": "sk-" + "a" * 32},
        },
    }
    _write_sync(tmp_path / "syncs", "secret", sync)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["validate"])

    assert result.exit_code == 0
    assert "WARNING" in result.output
    assert "destination.auth.token" in result.output
    assert "hardcoded secret" in result.output


def test_cli_validate_does_not_warn_on_env_secret_reference(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("API_TOKEN", "sk-" + "b" * 32)
    sync = {
        **VALID_SYNC,
        "destination": {
            **VALID_SYNC["destination"],
            "auth": {"type": "bearer", "token": "${API_TOKEN}"},
        },
    }
    _write_sync(tmp_path / "syncs", "env-secret", sync)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["validate"])

    assert result.exit_code == 0
    assert "WARNING" not in result.output
    assert "hardcoded secret" not in result.output


def test_cli_validate_strict_promotes_secret_warning(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sync = {
        **VALID_SYNC,
        "destination": {
            **VALID_SYNC["destination"],
            "auth": {"type": "bearer", "token": "sk-" + "c" * 32},
        },
    }
    _write_sync(tmp_path / "syncs", "secret", sync)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["validate", "--strict"])

    assert result.exit_code == 1
    assert "✗" in result.output
    assert "destination.auth.token" in result.output
    assert "hardcoded secret" in result.output


def test_cli_validate_json_includes_secret_warnings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sync = {
        **VALID_SYNC,
        "destination": {
            **VALID_SYNC["destination"],
            "auth": {"type": "bearer", "token": "sk-" + "d" * 32},
        },
    }
    _write_sync(tmp_path / "syncs", "secret", sync)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["validate", "--output", "json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    warning = payload["results"][0]["warnings"][0]
    assert warning["path"] == "destination.auth.token"
    assert "hardcoded secret" in warning["message"]


# ---------------------------------------------------------------------------
# native_idempotency_key — no-op warning (#897)
# ---------------------------------------------------------------------------


def test_cli_validate_warns_on_ineffective_native_idempotency_key(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#897: SlackDestination has no native idempotency mechanism to wire
    this to (Incoming Webhooks are fire-and-forget with no dedup header),
    so setting native_idempotency_key on it must warn rather than silently
    do nothing. Uses `slack`, not `rest_api` -- rest_api's own wiring
    landed in the follow-up PR that added this test's sibling assertion
    below (test_cli_validate_does_not_warn_for_a_wired_destination_type)."""
    sync = {
        "name": "test-sync",
        "model": "SELECT 1",
        "destination": {"type": "slack", "native_idempotency_key": "{{ row.id }}"},
    }
    _write_sync(tmp_path / "syncs", "idem", sync)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["validate"])

    assert result.exit_code == 0
    assert "WARNING" in result.output
    assert "native_idempotency_key" in result.output
    assert "'slack'" in result.output
    assert "no effect" in result.output


def test_cli_validate_does_not_warn_when_native_idempotency_key_unset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _write_sync(tmp_path / "syncs", "no-idem", VALID_SYNC)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["validate"])

    assert result.exit_code == 0
    assert "native_idempotency_key" not in result.output


def test_cli_validate_does_not_warn_for_a_wired_destination_type(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """#897: rest_api implements NativeIdempotencyCapable (both body_mode:
    record and body_mode: batch are wired) -- setting native_idempotency_key
    on it must not trigger the no-op warning."""
    sync = {
        **VALID_SYNC,
        "destination": {
            **VALID_SYNC["destination"],
            "native_idempotency_key": "{{ row.id }}",
        },
    }
    _write_sync(tmp_path / "syncs", "idem", sync)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["validate"])

    assert result.exit_code == 0
    assert "native_idempotency_key" not in result.output


def test_cli_validate_native_idempotency_warning_does_not_promote_under_strict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Deliberately lower severity than the secret-scan warning: an
    ineffective idempotency key is a missed opportunity, not a security
    issue, so --strict must not fail the run over it alone."""
    sync = {
        "name": "test-sync",
        "model": "SELECT 1",
        "destination": {"type": "slack", "native_idempotency_key": "{{ row.id }}"},
    }
    _write_sync(tmp_path / "syncs", "idem", sync)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["validate", "--strict"])

    assert result.exit_code == 0
    assert "WARNING" in result.output


def test_cli_validate_json_includes_idempotency_warnings(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sync = {
        "name": "test-sync",
        "model": "SELECT 1",
        "destination": {"type": "slack", "native_idempotency_key": "{{ row.id }}"},
    }
    _write_sync(tmp_path / "syncs", "idem", sync)
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ["validate", "--output", "json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    warning = payload["results"][0]["idempotency_warnings"][0]
    assert warning["destination_type"] == "slack"
    assert "no effect" in warning["message"]


def test_find_ineffective_native_idempotency_keys_skips_capable_destination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A destination implementing NativeIdempotencyCapable and reporting
    True must not be flagged -- this is the plugin opt-out path finding 4
    (Codex review of PR #1150) required in place of a hardcoded core-only
    type allowlist."""
    from types import SimpleNamespace

    from drt.cli.commands.validate import _find_ineffective_native_idempotency_keys

    class _CapableDestination:
        def supports_native_idempotency_key(self, config: object) -> bool:
            return True

    monkeypatch.setattr(
        "drt.connectors.registry.get_destination", lambda config: _CapableDestination()
    )
    sync = SimpleNamespace(
        name="s1",
        destination=SimpleNamespace(type="fake_plugin", native_idempotency_key="{{ row.id }}"),
    )

    assert _find_ineffective_native_idempotency_keys([sync]) == []


def test_find_ineffective_native_idempotency_keys_passes_config_for_conditional_support(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """#897 (Codex review of PR #1150, finding B): the capability probe
    receives the sync's own destination config, not just a bare instance --
    a destination whose wiring is conditional on a config value (e.g.
    rest_api's per-record template being inapplicable in body_mode: batch)
    must be able to answer precisely per sync rather than all-or-nothing."""
    from types import SimpleNamespace

    from drt.cli.commands.validate import _find_ineffective_native_idempotency_keys

    class _ConditionallyCapableDestination:
        def supports_native_idempotency_key(self, config: object) -> bool:
            return getattr(config, "body_mode", None) == "record"

    monkeypatch.setattr(
        "drt.connectors.registry.get_destination",
        lambda config: _ConditionallyCapableDestination(),
    )
    wired_sync = SimpleNamespace(
        name="wired",
        destination=SimpleNamespace(
            type="fake_plugin", native_idempotency_key="{{ row.id }}", body_mode="record"
        ),
    )
    unwired_sync = SimpleNamespace(
        name="unwired",
        destination=SimpleNamespace(
            type="fake_plugin", native_idempotency_key="{{ row.id }}", body_mode="batch"
        ),
    )

    findings = _find_ineffective_native_idempotency_keys([wired_sync, unwired_sync])

    assert [f.sync_name for f in findings] == ["unwired"]


def test_find_ineffective_native_idempotency_keys_warns_when_capable_but_unsupported(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Support is a method call, not just Protocol membership -- a
    destination implementing NativeIdempotencyCapable but returning False
    for this instance's config (e.g. conditional wiring) still warns."""
    from types import SimpleNamespace

    from drt.cli.commands.validate import _find_ineffective_native_idempotency_keys

    class _PartiallyCapableDestination:
        def supports_native_idempotency_key(self, config: object) -> bool:
            return False

    monkeypatch.setattr(
        "drt.connectors.registry.get_destination",
        lambda config: _PartiallyCapableDestination(),
    )
    sync = SimpleNamespace(
        name="s1",
        destination=SimpleNamespace(type="fake_plugin", native_idempotency_key="{{ row.id }}"),
    )

    findings = _find_ineffective_native_idempotency_keys([sync])
    assert len(findings) == 1
    assert findings[0].destination_type == "fake_plugin"


def test_find_ineffective_native_idempotency_keys_swallows_construction_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A destination that can't be constructed (missing credentials,
    unresolved env var) must not crash `drt validate` or be flagged --
    wiring can't be determined, so we say nothing rather than guess."""
    from types import SimpleNamespace

    from drt.cli.commands.validate import _find_ineffective_native_idempotency_keys

    def _raise(config: object) -> None:
        raise RuntimeError("missing credentials")

    monkeypatch.setattr("drt.connectors.registry.get_destination", _raise)
    sync = SimpleNamespace(
        name="s1",
        destination=SimpleNamespace(type="rest_api", native_idempotency_key="{{ row.id }}"),
    )

    assert _find_ineffective_native_idempotency_keys([sync]) == []
