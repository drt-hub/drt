"""Unit tests for observability profile config parsing."""

from __future__ import annotations

import yaml

from drt.config.credentials import ObservabilityConfig, load_profile, save_profile


def test_observability_config_parses_otel_block() -> None:
    config = ObservabilityConfig.model_validate(
        {
            "otel": {
                "endpoint": "http://localhost:4317",
                "service_name": "drt",
                "headers": {"Authorization": "Bearer token"},
            }
        }
    )

    assert config.otel.endpoint == "http://localhost:4317"
    assert config.otel.service_name == "drt"
    assert config.otel.headers == {"Authorization": "Bearer token"}


def test_observability_config_defaults_when_otel_block_missing() -> None:
    config = ObservabilityConfig.model_validate({})

    assert config.otel.endpoint is None
    assert config.otel.service_name == "drt"
    assert config.otel.headers == {}


def test_observability_profile_round_trip_expands_env_and_saves_non_default_block(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("OTEL_TOKEN", "secret-token")
    profiles_path = tmp_path / "profiles.yml"
    profiles_path.write_text(
        "dev:\n"
        "  type: duckdb\n"
        "  database: ./warehouse.duckdb\n"
        "  observability:\n"
        "    otel:\n"
        "      endpoint: http://localhost:4317\n"
        "      headers:\n"
        "        Authorization: Bearer ${OTEL_TOKEN}\n"
    )

    loaded = load_profile("dev", config_dir=tmp_path)

    assert loaded.observability.otel.endpoint == "http://localhost:4317"
    assert loaded.observability.otel.service_name == "drt"
    assert loaded.observability.otel.headers == {"Authorization": "Bearer secret-token"}

    save_profile("roundtrip", loaded, config_dir=tmp_path)

    saved = yaml.safe_load(profiles_path.read_text())
    assert saved["roundtrip"]["observability"]["otel"]["endpoint"] == "http://localhost:4317"
    assert saved["roundtrip"]["observability"]["otel"]["headers"] == {
        "Authorization": "Bearer secret-token"
    }
    assert "service_name" not in saved["roundtrip"]["observability"]["otel"]

    roundtrip = load_profile("roundtrip", config_dir=tmp_path)
    assert roundtrip.observability == loaded.observability
