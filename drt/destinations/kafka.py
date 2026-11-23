"""Append-only Kafka destination with confirmed, per-batch delivery outcomes.

One producer is reused until finalize_sync/reset_write_state closes the run.
Producer idempotence protects librdkafka retries, not application-level replays.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from functools import partial
from hashlib import sha256
from json import dumps
from reprlib import repr as bounded_repr
from typing import Any
from uuid import UUID

from drt.config.credentials import resolve_env
from drt.config.destinations_saas import KafkaDestinationConfig
from drt.config.models import DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import resolve_rate_limiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import record_preview, record_row_error


class KafkaDestination:
    """Append-only destination using the existing load and lifecycle hooks."""

    def __init__(self) -> None:
        """Initialize per-run producer state without importing the SDK."""
        self._client: Any = None
        self._rate_limit_key = ""

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Settle delivery reports before returning the batch result.

        RowError.batch_index must retain the index into this records list,
        including after validation failures, reordered callbacks, and retries.
        The engine and DLQ replay mark ledger entries after load() returns;
        queued messages alone must never be reported as delivered.
        """
        assert isinstance(config, KafkaDestinationConfig)
        if not records:
            return SyncResult()

        # Client def for the first time.
        if self._client is None:
            servers = resolve_env(None, config.bootstrap_servers_env)
            brokers = sorted(
                {broker.strip() for broker in (servers or "").split(",") if broker.strip()}
            )
            if not brokers:
                raise ValueError(
                    "Kafka bootstrap_servers_env must resolve to a non-empty broker list. (e.g. "
                    "bootstrap_servers_env="
                    "broker1.example.com:9092,"
                    "broker2.example.com:9092)"
                )
            bootstrap_servers = ",".join(brokers)
            identity = dumps([bootstrap_servers, config.topic]).encode("utf-8")
            self._rate_limit_key = f"kafka:{sha256(identity).hexdigest()}"
            self._client = self._initialize_producer(config, bootstrap_servers)
        # Reuse the client connection here
        producer = self._client
        limiter = resolve_rate_limiter(
            config,
            sync_options,
            key_override=self._rate_limit_key,
        )
        retry = resolve_retry(config.retry, sync_options)
        result = SyncResult()
        pending: set[int] = set()
        discarding = False

        def fail(index: int, exc: Exception) -> None:
            try:
                preview = record_preview(records[index])
            except (TypeError, ValueError):
                # A JSON serialization failure can also make the shared preview fail.
                preview = repr(records[index])[:200]
            record_row_error(result, index, preview, exc)
            result.errors.append(str(exc))

        def on_delivery(index: int, error: Any, message: Any) -> None:
            """
            Update pending, result counters.
            Use discarding to ensure
            """
            if discarding or index not in pending:
                return
            if error is not None:
                self._raise_fatal(error)
            pending.remove(index)
            if error is None:
                result.success += 1
            else:
                fail(index, RuntimeError(f"Kafka delivery failed: {error}"))

        def send(body: bytes, key: bytes | None, report: Callable[[Any, Any], None]) -> bool:
            """Call the Kafka's producer API here."""
            # Poll the previous callback before sending
            producer.poll(0)
            if result.failed and sync_options.on_error == "fail":
                return False
            try:
                producer.produce(
                    topic=config.topic,
                    value=body,
                    key=key,
                    headers=config.headers,
                    on_delivery=report,
                )
            except BufferError:
                producer.poll(0)
                if result.failed and sync_options.on_error == "fail":
                    return False
                raise
            return True

        try:
            for index, record in enumerate(records):
                producer.poll(0)
                if result.failed and sync_options.on_error == "fail":
                    break
                try:
                    body, key = self._serialize_record(record, config)
                except (TypeError, ValueError, RecursionError) as exc:
                    fail(index, exc)
                    continue

                limiter.acquire()
                report = partial(on_delivery, index)
                send_record = partial(send, body, key, report)
                try:
                    if not with_retry(
                        send_record, retry, retry_on=lambda exc: isinstance(exc, BufferError)
                    ):
                        break
                    pending.add(index)
                except BufferError as exc:
                    fail(index, exc)

            if pending:
                producer.flush(config.delivery_timeout_s)
            for index in sorted(pending):
                fail(
                    index,
                    TimeoutError(
                        "Kafka delivery timed out before confirmation; the message may have "
                        "been delivered, so replay can duplicate it."
                    ),
                )
            if pending:
                # Settle timeout outcomes before servicing purge callbacks.
                # Purge stops further local sends/retries.
                # Poll ensures producer to process available events.
                pending.clear()
                producer.purge()
                producer.poll(0)
        finally:
            discarding = True
            try:
                if pending:
                    producer.purge()
                    producer.poll(0)
            finally:
                # remove the local record-to-producer mapping.
                pending.clear()

        result.skipped = len(records) - result.total
        result.row_errors.sort(key=lambda error: error.batch_index)
        return result

    @staticmethod
    def _serialize_record(
        record: dict[str, Any], config: KafkaDestinationConfig
    ) -> tuple[bytes, bytes | None]:
        key = None
        if config.key_field is not None:
            value = record.get(config.key_field)
            if (
                not isinstance(value, (str, int, float, Decimal, UUID))
                or isinstance(value, bool)
                or value == ""
            ):
                raise ValueError(
                    f"Kafka key_field {config.key_field!r} must contain a non-empty scalar, "
                    "excluding bool."
                )
            key = str(value).encode("utf-8")
        return dumps(record, default=str, ensure_ascii=False).encode("utf-8"), key

    def _close(self) -> None:
        """Close client connection."""
        producer = self._client
        # Detach even if purge/close fails, so repeated cleanup cannot reuse it.
        self._client = None
        self._rate_limit_key = ""
        if producer is None:
            return
        remaining = len(producer)
        if remaining:
            producer.purge()
            producer.poll(0)
        if len(producer):
            raise RuntimeError("Kafka producer still has undelivered messages after cleanup.")
        if not producer.close():
            raise RuntimeError("Kafka producer close failed.")
        if remaining:
            raise RuntimeError(
                "Kafka safety drain found unconfirmed messages after load(); "
                "the sync must not be reported as successful."
            )

    def finalize_sync(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> None:
        """Safety drain/close only; delivery accounting belongs to load()"""
        self._close()

    def reset_write_state(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> None:
        """Close on raised/interrupted exits too; repeated calls are safe."""
        self._close()

    @staticmethod
    def _raise_fatal(error: Any) -> None:
        """Abort on unrecoverable producer or auth/config errors from either callback."""
        from confluent_kafka import KafkaError  # type: ignore[import-untyped]

        if error.fatal() or error.code() in {
            KafkaError._AUTHENTICATION,
            KafkaError._INVALID_ARG,
            KafkaError._SSL,
            KafkaError.SASL_AUTHENTICATION_FAILED,
            KafkaError.TOPIC_AUTHORIZATION_FAILED,
            KafkaError.CLUSTER_AUTHORIZATION_FAILED,
            KafkaError.INVALID_CONFIG,
            KafkaError.UNSUPPORTED_SASL_MECHANISM,
            KafkaError.ILLEGAL_SASL_STATE,
        }:
            raise RuntimeError(f"Kafka produer fatal error: {error}")

    @staticmethod
    def _initialize_producer(
        config: KafkaDestinationConfig,
        bootstrap_servers: str,
    ) -> Any:
        """Validate declared auth configs before lazily importing the optional SDK."""
        settings: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": config.enable_idempotence,
            "acks": "all",
            "message.timeout.ms": int(config.delivery_timeout_s * 1000),
            "error_cb": KafkaDestination._raise_fatal,
            "security.protocol": {
                "none": "PLAINTEXT",
                "ssl": "SSL",
                "sasl_plain": "SASL_SSL",
                "sasl_scram": "SASL_SSL",
            }[config.auth.type],
        }
        if not config.enable_idempotence:
            # Guarantee message ordering under no idempotence
            settings["max.in.flight.requests.per.connection"] = 1
        if config.auth.type in {"sasl_plain", "sasl_scram"}:
            settings["sasl.mechanism"] = (
                "PLAIN" if config.auth.type == "sasl_plain" else config.auth.scram_mechanism
            )
        for field, setting in (
            ("username_env", "sasl.username"),
            ("password_env", "sasl.password"),
            ("ca_env", "ssl.ca.location"),
            ("cert_env", "ssl.certificate.location"),
            ("key_env", "ssl.key.location"),
        ):
            reference = getattr(config.auth, field)
            if reference is not None:
                value = resolve_env(None, reference)
                if value is None or not value.strip():
                    raise ValueError(f"Kafka auth.{field} must resolve to a non-empty value.")
                settings[setting] = value
        try:
            from confluent_kafka import Producer  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError("Kafka destination requires: pip install drt-core[kafka]") from exc
        return Producer(settings)
