"""Unit tests for observability profile config parsing."""

from __future__ import annotations

from drt.config.credentials import ObservabilityConfig


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
