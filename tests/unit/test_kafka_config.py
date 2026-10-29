"""Kafka config validation, parsing, and public exports (no Kafka SDK needed)."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from drt.config import models
from drt.config.destinations_saas import KafkaAuthConfig, KafkaDestinationConfig


def test_kafka_config_parses_in_sync_without_resolving_credentials() -> None:
    sync = models.SyncConfig.model_validate(
        {
            "name": "customer_scores",
            "model": "customer_scores",
            "destination": {
                "type": "kafka",
                "bootstrap_servers_env": "UNRESOLVED_KAFKA_BROKERS",
                "topic": "customer_scores",
                "auth": {
                    "type": "sasl_scram",
                    "username_env": "aws-sm://missing/kafka#username",
                    "password_env": "UNRESOLVED_KAFKA_PASSWORD",
                    "ca_env": "UNRESOLVED_KAFKA_CA_PATH",
                },
            },
        }
    )
    config = sync.destination
    assert isinstance(config, models.KafkaDestinationConfig)
    assert isinstance(config.auth, models.KafkaAuthConfig)
    assert config.key_field is None
    assert config.delivery_timeout_s == 30
    assert config.enable_idempotence is True
    assert config.auth.scram_mechanism == "SCRAM-SHA-512"
    assert config.auth.username_env == "aws-sm://missing/kafka#username"
    assert config.describe_safe() == "kafka"


def test_kafka_config_preserves_explicit_overrides() -> None:
    config = KafkaDestinationConfig.model_validate(
        {
            "type": "kafka",
            "bootstrap_servers_env": "KAFKA_BROKERS",
            "topic": "customer_scores",
            "key_field": "customer_id",
            "delivery_timeout_s": 2.5,
            "enable_idempotence": False,
            "headers": {"source": "drt"},
            "retry": {"max_attempts": 2},
            "rate_limit": {"requests_per_second": 20, "burst": 3},
            "auth": {
                "type": "sasl_scram",
                "username_env": "KAFKA_USER",
                "password_env": "KAFKA_PASSWORD",
                "scram_mechanism": "SCRAM-SHA-256",
            },
        }
    )
    assert config.key_field == "customer_id"
    assert config.delivery_timeout_s == 2.5
    assert config.enable_idempotence is False
    assert config.headers == {"source": "drt"}
    assert config.retry is not None and config.retry.max_attempts == 2
    assert config.rate_limit is not None and config.rate_limit.burst == 3
    assert config.auth.scram_mechanism == "SCRAM-SHA-256"
    assert config.describe() == "kafka (customer_scores)"


@pytest.mark.parametrize(
    "auth",
    [
        {"type": "none"},
        {"type": "ssl"},
        {"type": "ssl", "ca_env": "KAFKA_CA"},
        {"type": "ssl", "cert_env": "KAFKA_CERT", "key_env": "KAFKA_KEY"},
        {"type": "sasl_plain", "username_env": "KAFKA_USER", "password_env": "KAFKA_PASS"},
        {"type": "sasl_scram", "username_env": "KAFKA_USER", "password_env": "KAFKA_PASS"},
        {
            "type": "sasl_scram",
            "username_env": "KAFKA_USER",
            "password_env": "KAFKA_PASS",
            "cert_env": "KAFKA_CERT",
            "key_env": "KAFKA_KEY",
        },
    ],
)
def test_kafka_auth_accepts_consistent_declarations(auth: dict[str, Any]) -> None:
    config = KafkaAuthConfig.model_validate(auth)
    assert config.type == auth["type"]


@pytest.mark.parametrize("auth", [{}, {"type": "SASL_PLAINTEXT"}, {"type": "unknown"}])
def test_kafka_auth_requires_an_explicit_supported_type(auth: dict[str, Any]) -> None:
    with pytest.raises(ValidationError, match="type"):
        KafkaAuthConfig.model_validate(auth)


@pytest.mark.parametrize("mode", ["sasl_plain", "sasl_scram"])
@pytest.mark.parametrize(
    "credentials",
    [{}, {"username_env": "KAFKA_USER"}, {"password_env": "KAFKA_PASS"}],
)
def test_kafka_sasl_requires_both_credential_references(
    mode: str, credentials: dict[str, str]
) -> None:
    with pytest.raises(ValidationError, match="username_env|password_env"):
        KafkaAuthConfig.model_validate({"type": mode, **credentials})


@pytest.mark.parametrize("mode", ["none", "ssl"])
@pytest.mark.parametrize("field", ["username_env", "password_env"])
def test_kafka_non_sasl_modes_reject_unused_credentials(mode: str, field: str) -> None:
    with pytest.raises(ValidationError, match=field):
        KafkaAuthConfig.model_validate({"type": mode, field: "KAFKA_CREDENTIAL"})


@pytest.mark.parametrize(
    "tls",
    [
        {"ca_env": "KAFKA_CA"},
        {"cert_env": "KAFKA_CERT", "key_env": "KAFKA_KEY"},
    ],
)
def test_kafka_no_auth_rejects_unused_tls_references(tls: dict[str, str]) -> None:
    with pytest.raises(ValidationError, match="ca_env|cert_env|key_env"):
        KafkaAuthConfig.model_validate({"type": "none", **tls})


@pytest.mark.parametrize("mode", ["ssl", "sasl_plain", "sasl_scram"])
@pytest.mark.parametrize("field", ["cert_env", "key_env"])
def test_kafka_client_certificate_and_key_must_be_paired(mode: str, field: str) -> None:
    auth = {"type": mode, field: "KAFKA_TLS_PATH"}
    if mode != "ssl":
        auth.update(username_env="KAFKA_USER", password_env="KAFKA_PASS")
    with pytest.raises(ValidationError, match="cert_env|key_env"):
        KafkaAuthConfig.model_validate(auth)


@pytest.mark.parametrize("field", ["username_env", "password_env", "ca_env", "cert_env", "key_env"])
@pytest.mark.parametrize("value", ["", " \t"])
def test_kafka_auth_rejects_blank_references(field: str, value: str) -> None:
    auth = {
        "type": "sasl_scram",
        "username_env": "KAFKA_USER",
        "password_env": "KAFKA_PASS",
        "ca_env": "KAFKA_CA",
        "cert_env": "KAFKA_CERT",
        "key_env": "KAFKA_KEY",
    }
    auth[field] = value
    with pytest.raises(ValidationError, match=field):
        KafkaAuthConfig.model_validate(auth)


@pytest.mark.parametrize("field", ["password", "username", "security_protocol"])
def test_kafka_auth_rejects_inline_credentials_and_protocol_overrides(field: str) -> None:
    with pytest.raises(ValidationError, match=field):
        KafkaAuthConfig.model_validate({"type": "none", field: "not-allowed"})


@pytest.mark.parametrize("auth", [None, {}])
def test_kafka_destination_requires_explicit_auth(auth: dict[str, Any] | None) -> None:
    values: dict[str, Any] = {
        "type": "kafka",
        "bootstrap_servers_env": "KAFKA_BROKERS",
        "topic": "customer_scores",
        "key_field": "customer_id",
    }
    if auth is not None:
        values["auth"] = auth
    with pytest.raises(ValidationError, match="auth"):
        KafkaDestinationConfig.model_validate(values)


@pytest.mark.parametrize("field", ["bootstrap_servers_env", "topic", "key_field"])
@pytest.mark.parametrize("value", ["", " \t"])
def test_kafka_destination_rejects_blank_names(field: str, value: str) -> None:
    with pytest.raises(ValidationError, match=field):
        KafkaDestinationConfig.model_validate(
            {
                "type": "kafka",
                "bootstrap_servers_env": "KAFKA_BROKERS",
                "topic": "customer_scores",
                "key_field": "customer_id",
                "auth": {"type": "none"},
                field: value,
            }
        )


@pytest.mark.parametrize(
    "timeout", [0, -1, 0.0001, float("inf"), float("-inf"), float("nan"), 2_147_483.648]
)
def test_kafka_timeout_must_fit_finite_librdkafka_milliseconds(timeout: float) -> None:
    with pytest.raises(ValidationError, match="delivery_timeout_s"):
        KafkaDestinationConfig.model_validate(
            {
                "type": "kafka",
                "bootstrap_servers_env": "KAFKA_BROKERS",
                "topic": "customer_scores",
                "key_field": "customer_id",
                "auth": {"type": "none"},
                "delivery_timeout_s": timeout,
            }
        )


@pytest.mark.parametrize("timeout", [0.001, 2_147_483.647])
def test_kafka_timeout_accepts_librdkafka_boundaries(timeout: float) -> None:
    config = KafkaDestinationConfig.model_validate(
        {
            "type": "kafka",
            "bootstrap_servers_env": "KAFKA_BROKERS",
            "topic": "customer_scores",
            "key_field": "customer_id",
            "auth": {"type": "none"},
            "delivery_timeout_s": timeout,
        }
    )
    assert config.delivery_timeout_s == timeout
