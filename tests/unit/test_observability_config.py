"""Unit tests for observability profile config parsing."""

from __future__ import annotations

import yaml

from drt.config.credentials import (
    ObservabilityConfig,
    load_observability_config,
    load_profile,
    save_profile,
)


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


def test_load_observability_config_parses_top_level_block(tmp_path) -> None:
    profiles_path = tmp_path / "profiles.yml"
    profiles_path.write_text(
        "observability:\n"
        "  otel:\n"
        "    endpoint: http://localhost:4317\n"
        "    service_name: drt\n"
        "    headers:\n"
        "      Authorization: Bearer ${OTEL_TOKEN}\n"
        "profiles:\n"
        "  dev:\n"
        "    type: duckdb\n"
        "    database: ./warehouse.duckdb\n"
    )

    config = load_observability_config(tmp_path)

    assert config.otel.endpoint == "http://localhost:4317"
    assert config.otel.service_name == "drt"
    assert config.otel.headers == {"Authorization": "Bearer ${OTEL_TOKEN}"}


def test_observability_profile_round_trip_keeps_raw_secret_headers(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("OTEL_TOKEN", "secret-token")
    profiles_path = tmp_path / "profiles.yml"
    profiles_path.write_text(
        "observability:\n"
        "  otel:\n"
        "    endpoint: http://localhost:4317\n"
        "    service_name: drt\n"
        "    headers:\n"
        "      Authorization: Bearer ${OTEL_TOKEN}\n"
        "profiles:\n"
        "  dev:\n"
        "    type: duckdb\n"
        "    database: ./warehouse.duckdb\n"
    )

    loaded = load_profile("dev", config_dir=tmp_path)

    assert loaded.database == "./warehouse.duckdb"

    save_profile("roundtrip", loaded, config_dir=tmp_path)

    saved = yaml.safe_load(profiles_path.read_text())
    assert saved["observability"]["otel"]["headers"] == {"Authorization": "Bearer ${OTEL_TOKEN}"}
    assert saved["profiles"]["roundtrip"]["database"] == "./warehouse.duckdb"
    assert "observability" not in saved["profiles"]["roundtrip"]

    roundtrip = load_profile("roundtrip", config_dir=tmp_path)
    assert roundtrip.database == loaded.database
