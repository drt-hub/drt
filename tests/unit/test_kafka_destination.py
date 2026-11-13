"""Kafka delivery contracts with a fake SDK; no brokers or credentials required."""

from __future__ import annotations

import json
import sys
from datetime import date
from decimal import Decimal
from typing import Any
from uuid import UUID

import pytest

from drt.config.models import KafkaDestinationConfig, SyncOptions
from drt.destinations.kafka import KafkaDestination
from drt.state.idempotency import successful_indices


class FakeProducer:
    def __init__(self) -> None:
        self.settings: dict[str, Any] = {}
        self.messages: list[dict[str, Any]] = []
        self.pending: set[int] = set()
        self.errors: dict[int, Exception] = {}
        self.held: set[int] = set()
        self.report_on_poll = False
        self.reverse_reports = False
        self.full_attempts = 0
        self.attempts = 0
        self.send_error: Exception | None = None
        self.flush_error: Exception | None = None
        self.flush_timeouts: list[float] = []
        self.closed = False
        self.purged = False

    def __len__(self) -> int:
        return len(self.pending)

    def produce(self, **message: Any) -> None:
        assert not self.closed
        self.attempts += 1
        if self.full_attempts:
            self.full_attempts -= 1
            raise BufferError("queue full")
        if self.send_error is not None:
            raise self.send_error
        index = len(self.messages)
        self.messages.append(message)
        self.pending.add(index)

    def complete(self, index: int, error: Exception | None = None) -> None:
        self.pending.remove(index)
        self.messages[index]["on_delivery"](error, None)

    def poll(self, timeout: float) -> int:
        if self.report_on_poll:
            ready = sorted(self.pending - self.held)
            for index in ready:
                self.complete(index, self.errors.get(index))
            return len(ready)
        return 0

    def flush(self, timeout: float) -> int:
        self.flush_timeouts.append(timeout)
        if self.flush_error is not None:
            raise self.flush_error
        for index in sorted(self.pending - self.held, reverse=self.reverse_reports):
            self.complete(index, self.errors.get(index))
        return len(self.pending)

    def purge(self) -> None:
        self.purged = True
        for index in list(self.pending):
            self.complete(index, RuntimeError("purged"))

    def close(self) -> bool:
        assert not self.pending, "close must not perform an unbounded delivery wait"
        self.closed = True
        return True


class FakeSDK:
    def __init__(self) -> None:
        self.producers: list[FakeProducer] = []
        self.next_producer = FakeProducer()

    def Producer(self, settings: dict[str, Any]) -> FakeProducer:
        producer = self.next_producer
        producer.settings = settings
        self.producers.append(producer)
        self.next_producer = FakeProducer()
        return producer


@pytest.fixture
def sdk(monkeypatch: pytest.MonkeyPatch) -> FakeSDK:
    monkeypatch.setenv("KAFKA_BROKERS", " broker-b:9092,broker-a:9092,broker-b:9092 ")
    fake = FakeSDK()
    monkeypatch.setitem(sys.modules, "confluent_kafka", fake)
    return fake


def config(**overrides: Any) -> KafkaDestinationConfig:
    return KafkaDestinationConfig.model_validate(
        {
            "type": "kafka",
            "bootstrap_servers_env": "KAFKA_BROKERS",
            "topic": "events",
            "auth": {"type": "none"},
            **overrides,
        }
    )


def options(**overrides: Any) -> SyncOptions:
    return SyncOptions.model_validate(
        {
            "on_error": "skip",
            "rate_limit": {"requests_per_second": 0},
            "retry": {"max_attempts": 3, "initial_backoff": 0},
            **overrides,
        }
    )


def test_empty_load_and_hooks_do_not_resolve_env_or_import_sdk(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setitem(sys.modules, "confluent_kafka", None)

    def unexpected(*args: Any, **kwargs: Any) -> None:
        pytest.fail("empty load must not resolve secrets")

    monkeypatch.setattr("drt.destinations.kafka.resolve_env", unexpected)
    destination = KafkaDestination()
    result = destination.load([], config(), options())
    destination.finalize_sync(config(), options())
    destination.reset_write_state(config(), options())
    assert result.total == 0


def test_reuses_producer_across_batches_then_closes_and_starts_fresh(sdk: FakeSDK) -> None:
    destination = KafkaDestination()
    first = destination.load([{"id": 1}], config(), options())
    producer = sdk.producers[0]
    second = destination.load([{"id": 2}], config(), options())
    assert (first.success, second.success) == (1, 1)
    assert len(sdk.producers) == 1
    assert not producer.closed
    assert len(producer.messages) == 2
    destination.finalize_sync(config(), options())
    destination.reset_write_state(config(), options())
    assert producer.closed
    third = destination.load([{"id": 3}], config(), options())
    assert third.success == 1
    assert len(sdk.producers) == 2
    destination.reset_write_state(config(), options())
    assert sdk.producers[1].closed


def test_counts_only_delivery_reports_and_correlates_reordered_callbacks(sdk: FakeSDK) -> None:
    producer = sdk.next_producer
    producer.reverse_reports = True
    producer.errors[0] = RuntimeError("broker rejected record")
    records = [{"id": "a"}, {"id": "b"}, {"id": "c"}]
    destination = KafkaDestination()
    result = destination.load(records, config(), options())
    assert (result.success, result.failed, result.skipped) == (2, 1, 0)
    assert [error.batch_index for error in result.row_errors] == [0]
    assert "broker rejected" in result.row_errors[0].error_message
    assert successful_indices(records, result) == {1, 2}
    assert producer.attempts == 3  # A failed delivery report is never resent.
    destination.finalize_sync(config(), options())


def test_batch_indices_remain_correct_after_validation_and_delivery_failures(sdk: FakeSDK) -> None:
    producer = sdk.next_producer
    producer.errors[0] = RuntimeError("first produced message failed")
    producer.reverse_reports = True
    records: list[dict[str, Any]] = [{"id": None}, {"id": "b"}, {"id": "c"}]
    destination = KafkaDestination()
    result = destination.load(records, config(key_field="id"), options())
    assert (result.success, result.failed, result.total) == (1, 2, 3)
    assert [error.batch_index for error in result.row_errors] == [0, 1]
    assert successful_indices(records, result) == {2}
    destination.finalize_sync(config(), options())


@pytest.mark.parametrize("record", [{}, {"id": None}, {"id": []}, {"id": {}}, {"id": (1,)}])
def test_invalid_key_is_a_row_error_and_is_never_enqueued(
    sdk: FakeSDK, record: dict[str, Any]
) -> None:
    destination = KafkaDestination()
    result = destination.load([record, {"id": "valid"}], config(key_field="id"), options())
    assert (result.success, result.failed) == (1, 1)
    assert result.row_errors[0].batch_index == 0
    assert len(sdk.producers[0].messages) == 1
    destination.finalize_sync(config(), options())


@pytest.mark.parametrize(
    "key,encoded",
    [
        ("é", b"\xc3\xa9"),
        (12, b"12"),
        (1.5, b"1.5"),
        (Decimal("1.20"), b"1.20"),
        (UUID(int=0), b"00000000-0000-0000-0000-000000000000"),
    ],
)
def test_scalar_keys_are_utf8_strings(sdk: FakeSDK, key: Any, encoded: bytes) -> None:
    destination = KafkaDestination()
    result = destination.load([{"id": key}], config(key_field="id"), options())
    assert result.success == 1
    assert sdk.producers[0].messages[0]["key"] == encoded
    destination.finalize_sync(config(), options())


def test_json_serialization_headers_and_optional_key(sdk: FakeSDK) -> None:
    destination = KafkaDestination()
    record = {"when": date(2026, 1, 2), "amount": Decimal("1.20"), "id": UUID(int=0)}
    result = destination.load([record], config(headers={"source": "drt"}), options())
    assert result.success == 1
    message = sdk.producers[0].messages[0]
    assert message["topic"] == "events"
    assert message.get("key") is None
    assert message["headers"] == {"source": "drt"}
    assert json.loads(message["value"]) == {
        "when": "2026-01-02",
        "amount": "1.20",
        "id": "00000000-0000-0000-0000-000000000000",
    }
    destination.finalize_sync(config(), options())


def test_serialization_failure_is_a_row_error(sdk: FakeSDK) -> None:
    record: dict[str, Any] = {}
    record["circular"] = record
    destination = KafkaDestination()
    result = destination.load([record, {"ok": True}], config(), options())
    assert (result.failed, result.success) == (1, 1)
    assert result.row_errors[0].batch_index == 0
    assert len(sdk.producers[0].messages) == 1
    destination.finalize_sync(config(), options())


def test_fail_policy_drains_enqueued_rows_and_skips_unattempted_rows(sdk: FakeSDK) -> None:
    destination = KafkaDestination()
    result = destination.load(
        [{"id": "a"}, {"id": None}, {"id": "c"}],
        config(key_field="id"),
        options(on_error="fail"),
    )
    assert (result.success, result.failed, result.skipped, result.total) == (1, 1, 1, 3)
    assert result.row_errors[0].batch_index == 1
    assert len(sdk.producers[0].messages) == 1
    destination.finalize_sync(config(), options())


def test_fail_policy_stops_when_a_delivery_failure_is_observed_while_polling(sdk: FakeSDK) -> None:
    producer = sdk.next_producer
    producer.report_on_poll = True
    producer.errors[0] = RuntimeError("delivery failed")
    destination = KafkaDestination()
    result = destination.load([{"id": 1}, {"id": 2}], config(), options(on_error="fail"))
    assert (result.success, result.failed, result.skipped) == (0, 1, 1)
    assert producer.attempts == 1
    destination.finalize_sync(config(), options())


def test_timeout_marks_only_unconfirmed_rows_and_late_callbacks_do_not_mutate_result(
    sdk: FakeSDK,
) -> None:
    producer = sdk.next_producer
    producer.held = {1}
    destination = KafkaDestination()
    result = destination.load([{"id": 1}, {"id": 2}], config(delivery_timeout_s=0.2), options())
    assert (result.success, result.failed, result.skipped) == (1, 1, 0)
    assert result.row_errors[0].batch_index == 1
    assert "tim" in result.row_errors[0].error_message.lower()
    assert producer.flush_timeouts == [0.2]
    producer.complete(1)  # Could have reached the broker despite the deadline.
    assert (result.success, result.failed) == (1, 1)
    next_result = destination.load([{"id": 3}], config(delivery_timeout_s=0.2), options())
    assert next_result.success == 1
    assert len(sdk.producers) == 1
    destination.finalize_sync(config(delivery_timeout_s=0.2), options())


@pytest.mark.parametrize("hook", ["finalize_sync", "reset_write_state"])
def test_safety_drain_closes_but_fails_loudly_if_anything_was_outstanding(
    sdk: FakeSDK,
    hook: str,
) -> None:
    destination = KafkaDestination()
    destination.load([{"id": 1}], config(), options())
    producer = sdk.producers[0]
    producer.produce(topic="events", value=b"unexpected", on_delivery=lambda *_: None)
    with pytest.raises(RuntimeError, match="undelivered|outstanding|unconfirmed"):
        getattr(destination, hook)(config(), options())
    assert producer.closed
    destination.reset_write_state(config(), options())


def test_safety_drain_purges_before_close_if_bounded_flush_times_out(sdk: FakeSDK) -> None:
    producer = sdk.next_producer
    producer.held = {0}
    destination = KafkaDestination()
    result = destination.load([{"id": 1}], config(delivery_timeout_s=0.1), options())
    assert result.failed == 1
    with pytest.raises(RuntimeError, match="undelivered|outstanding|unconfirmed"):
        destination.finalize_sync(config(delivery_timeout_s=0.1), options())
    assert producer.purged
    assert producer.closed
    assert result.failed == 1 and result.success == 0


def test_reset_closes_producer_after_a_batch_level_exception(sdk: FakeSDK) -> None:
    producer = sdk.next_producer
    producer.flush_error = RuntimeError("poll loop failed")
    destination = KafkaDestination()
    with pytest.raises(RuntimeError, match="poll loop failed"):
        destination.load([{"id": 1}], config(), options())
    producer.flush_error = None
    with pytest.raises(RuntimeError, match="undelivered|outstanding|unconfirmed"):
        destination.reset_write_state(config(), options())
    assert producer.closed


def test_immediate_send_errors_are_attributed_without_application_resends(sdk: FakeSDK) -> None:
    producer = sdk.next_producer
    producer.send_error = RuntimeError("producer fatal")
    destination = KafkaDestination()
    result = destination.load([{"id": 1}, {"id": 2}], config(), options(on_error="fail"))
    assert (result.failed, result.skipped) == (1, 1)
    assert result.row_errors[0].batch_index == 0
    assert producer.attempts == 1
    destination.finalize_sync(config(), options())


def test_only_queue_full_is_retried_and_each_message_is_paced_once(
    sdk: FakeSDK,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer = sdk.next_producer
    producer.full_attempts = 2
    acquisitions: list[None] = []

    class Limiter:
        def acquire(self) -> None:
            acquisitions.append(None)

    monkeypatch.setattr("drt.destinations.kafka.resolve_rate_limiter", lambda *a, **kw: Limiter())
    destination = KafkaDestination()
    result = destination.load([{"id": 1}], config(), options())
    assert result.success == 1
    assert producer.attempts == 3
    assert len(producer.messages) == 1
    assert len(acquisitions) == 1
    destination.finalize_sync(config(), options())


@pytest.mark.parametrize(
    "override,attempts", [(None, 3), ({"max_attempts": 2, "initial_backoff": 0}, 2)]
)
def test_queue_full_exhaustion_becomes_a_row_error(
    sdk: FakeSDK,
    override: dict[str, Any] | None,
    attempts: int,
) -> None:
    producer = sdk.next_producer
    producer.full_attempts = 20
    destination = KafkaDestination()
    result = destination.load([{"id": 1}], config(retry=override), options())
    assert (result.success, result.failed) == (0, 1)
    assert result.row_errors[0].batch_index == 0
    assert producer.attempts == attempts
    assert not producer.messages
    destination.finalize_sync(config(retry=override), options())


def test_rate_limit_identity_uses_normalized_brokers_and_topic(
    sdk: FakeSDK,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[str] = []
    from drt.destinations.rate_limiter import RateLimiter

    def factory(*, key: str, requests_per_second: float, burst: int | None) -> RateLimiter:
        seen.append(key)
        return RateLimiter(key=key, requests_per_second=requests_per_second, burst=burst)

    monkeypatch.setattr("drt.destinations.rate_limiter.get_rate_limiter_backend", lambda: factory)
    destination = KafkaDestination()
    cfg = config(rate_limit={"requests_per_second": 100000})
    destination.load([{"id": 1}], cfg, options())
    assert seen == ["kafka:broker-a:9092,broker-b:9092:events"]
    destination.finalize_sync(cfg, options())


@pytest.mark.parametrize("brokers", [None, "", "   ", ", ,"])
def test_missing_or_empty_broker_list_fails_before_producer_creation(
    sdk: FakeSDK,
    monkeypatch: pytest.MonkeyPatch,
    brokers: str | None,
) -> None:
    monkeypatch.setattr("drt.destinations.kafka.resolve_env", lambda *_: brokers)
    with pytest.raises(ValueError, match="bootstrap_servers_env"):
        KafkaDestination().load([{"id": 1}], config(), options())
    assert not sdk.producers


@pytest.mark.parametrize(
    "auth,expected",
    [
        ({"type": "none"}, {"security.protocol": "PLAINTEXT"}),
        ({"type": "ssl"}, {"security.protocol": "SSL"}),
        (
            {"type": "sasl_plain", "username_env": "KAFKA_USER", "password_env": "KAFKA_PASS"},
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "PLAIN",
                "sasl.username": "user",
                "sasl.password": "pass",
            },
        ),
        (
            {"type": "sasl_scram", "username_env": "KAFKA_USER", "password_env": "KAFKA_PASS"},
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "SCRAM-SHA-512",
                "sasl.username": "user",
                "sasl.password": "pass",
            },
        ),
        (
            {
                "type": "sasl_scram",
                "username_env": "KAFKA_USER",
                "password_env": "KAFKA_PASS",
                "scram_mechanism": "SCRAM-SHA-256",
            },
            {
                "security.protocol": "SASL_SSL",
                "sasl.mechanism": "SCRAM-SHA-256",
                "sasl.username": "user",
                "sasl.password": "pass",
            },
        ),
        (
            {"type": "ssl", "ca_env": "KAFKA_CA", "cert_env": "KAFKA_CERT", "key_env": "KAFKA_KEY"},
            {
                "security.protocol": "SSL",
                "ssl.ca.location": "/ca.pem",
                "ssl.certificate.location": "/cert.pem",
                "ssl.key.location": "/key.pem",
            },
        ),
    ],
)
def test_native_producer_settings_and_auth_resolution(
    sdk: FakeSDK,
    monkeypatch: pytest.MonkeyPatch,
    auth: dict[str, Any],
    expected: dict[str, Any],
) -> None:
    for name, value in {
        "KAFKA_USER": "user",
        "KAFKA_PASS": "pass",
        "KAFKA_CA": "/ca.pem",
        "KAFKA_CERT": "/cert.pem",
        "KAFKA_KEY": "/key.pem",
    }.items():
        monkeypatch.setenv(name, value)
    destination = KafkaDestination()
    cfg = config(auth=auth, delivery_timeout_s=2.5)
    result = destination.load([{"id": 1}], cfg, options())
    assert result.success == 1
    settings = sdk.producers[0].settings
    assert settings == {
        "bootstrap.servers": "broker-a:9092,broker-b:9092",
        "enable.idempotence": True,
        "acks": "all",
        "message.timeout.ms": 2500,
        **expected,
    }
    destination.finalize_sync(cfg, options())


def test_idempotence_opt_out_still_keeps_retries_ordered(sdk: FakeSDK) -> None:
    destination = KafkaDestination()
    cfg = config(enable_idempotence=False)
    destination.load([{"id": 1}], cfg, options())
    settings = sdk.producers[0].settings
    assert settings["enable.idempotence"] is False
    assert settings["max.in.flight.requests.per.connection"] == 1
    assert settings["acks"] == "all"
    destination.finalize_sync(cfg, options())


@pytest.mark.parametrize("field", ["username_env", "password_env", "ca_env", "cert_env", "key_env"])
def test_unresolved_auth_reference_fails_without_exposing_other_credentials(
    sdk: FakeSDK,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
) -> None:
    auth = {
        "type": "sasl_scram",
        "username_env": "KAFKA_USER",
        "password_env": "KAFKA_PASS",
        "ca_env": "KAFKA_CA",
        "cert_env": "KAFKA_CERT",
        "key_env": "KAFKA_KEY",
    }
    values = {name: "sensitive-value" for name in auth.values()}
    values[auth[field]] = "   "
    values["KAFKA_BROKERS"] = "broker:9092"
    monkeypatch.setattr("drt.destinations.kafka.resolve_env", lambda _v, name: values.get(name))
    with pytest.raises(ValueError, match=field) as error:
        KafkaDestination().load([{"id": 1}], config(auth=auth), options())
    assert "sensitive-value" not in str(error.value)
    assert not sdk.producers


def test_missing_sdk_has_actionable_extra_install_message(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BROKERS", "broker:9092")
    monkeypatch.setitem(sys.modules, "confluent_kafka", None)
    with pytest.raises(ImportError, match=r"drt-core\[kafka\]"):
        KafkaDestination().load([{"id": 1}], config(), options())


def test_queue_full_poll_observing_failure_keeps_unsent_row_skipped(
    sdk: FakeSDK,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    producer = sdk.next_producer
    producer.report_on_poll = True
    producer.held = {0}
    producer.errors[0] = RuntimeError("earlier delivery failed")
    produce = producer.produce

    def queue_full_on_second_row(**message: Any) -> None:
        if producer.messages:
            producer.held.clear()
            raise BufferError("queue full")
        produce(**message)

    monkeypatch.setattr(producer, "produce", queue_full_on_second_row)
    destination = KafkaDestination()
    result = destination.load(
        [{"id": 1}, {"id": 2}],
        config(),
        options(on_error="fail", retry={"max_attempts": 1}),
    )
    assert (result.success, result.failed, result.skipped) == (0, 1, 1)
    assert [error.batch_index for error in result.row_errors] == [0]
    destination.finalize_sync(config(), options())
