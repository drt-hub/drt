"""Kafka destination interface scaffold for #1180.

Not registered or usable yet. Delivery, authentication, and lifecycle behavior
remain unimplemented pending the owner's design decisions.
"""

from __future__ import annotations

from typing import Any

from drt.config.destinations_saas import KafkaDestinationConfig
from drt.config.models import DestinationConfig, SyncOptions
from drt.config.credentials import resolve_env
from drt.destinations.base import SyncResult


class KafkaDestination:
    """Append-only destination using the existing load and lifecycle hooks."""

    def __init__(self) -> None:
        """Initialize per-run producer state without importing the SDK."""
        self._client: Any = None
        self._bootstrap_servers: str = ""

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
        # Client def for the first time.
        if self._client is None:
            servers = resolve_env(None, config.bootstrap_servers_env)
            brokers = sorted(
                {broker.strip() for broker in (servers or "").split(",") if broker.strip()}
            )
            if not brokers:
                raise ValueError(
                    "Kafka bootstrap_servers_env must resolve to a non-empty broker list. (e.g. " \
                    "bootstrap_servers_env=" \
                    "broker1.example.com:9092," \
                    "broker2.example.com:9092)"
                )
            self._bootstrap_servers = ",".join(brokers)
            self._client = self._initialize_producer(config, self._bootstrap_servers)
        # Reuse the client connection here
        producer = self._client

    def finalize_sync(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> None:
        """TODO: defensively flush without recounting settled batch outcomes.

        This hook is skipped when the engine's batch loop raises. Delivery
        confirmation therefore cannot depend solely on this hook.
        """
        raise NotImplementedError("Kafka finalization is not implemented.")

    def reset_write_state(
        self,
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> None:
        """TODO: clear per-run state on clean, interrupted, and raised exits.

        Use the existing unconditional reset hook without publishing messages
        or changing outcomes previously returned from load().
        """
        raise NotImplementedError("Kafka write-state reset is not implemented.")

    @staticmethod
    def _initialize_producer(
        config: KafkaDestinationConfig,
        bootstrap_servers: str,
    ) -> Any:
        """Validate declared auth configs before lazily importing the optional SDK.
        """
        settings: dict[str, Any] = {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": config.enable_idempotence,
            "acks": "all",
            "delivery.timeout.ms": int(config.delivery_timeout_s * 1000),
            "security.protocol": {
                "none": "PLAINTEXT",
                "ssl": "SSL",
                "sasl_plain": "SASL_PLAINTEXT",
                "sasl_scram": "SASL_SSL"
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
            ("key_env", "ssl.key.location")
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
