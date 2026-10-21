"""Kafka destination interface scaffold for #1180.

Not registered or usable yet. Delivery, authentication, and lifecycle behavior
remain unimplemented pending the owner's design decisions.
"""

from __future__ import annotations

from typing import Any

from drt.config.destinations_saas import KafkaDestinationConfig
from drt.config.models import DestinationConfig, SyncOptions
from drt.destinations.base import SyncResult


class KafkaDestination:
    """Append-only destination using the existing load and lifecycle hooks."""

    def __init__(self) -> None:
        """TODO: initialize per-run producer state without importing the SDK."""
        raise NotImplementedError("Kafka producer lifecycle is not implemented.")

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """TODO: settle delivery reports before returning the batch result.

        RowError.batch_index must retain the index into this records list,
        including after validation failures, reordered callbacks, and retries.
        The engine and DLQ replay mark ledger entries after load() returns;
        queued messages alone must never be reported as delivered.
        """
        raise NotImplementedError("Kafka message delivery is not implemented.")

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
    def _producer(
        config: KafkaDestinationConfig,
        bootstrap_servers: str,
    ) -> Any:
        """TODO: construct the chosen SDK producer with resolved settings.

        Keep the import lazy; the SDK and its concrete return type are pending
        owner confirmation. Any follows the SQS client's current boundary.
        """
        raise NotImplementedError("Kafka client selection is pending.")
