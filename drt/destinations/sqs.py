"""Amazon SQS destination.

For FIFO queues, drt sends at most one entry per MessageGroupId per
batch. A later entry in a group is held back until its predecessor is
confirmed successful. If the predecessor permanently fails, later entries
in that group become RowErrors for the current run.

Please refer to sqs.md for more information.
"""

from __future__ import annotations

from decimal import Decimal
from json import dumps
from string import ascii_letters, digits, punctuation
from typing import Any
from uuid import UUID

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, RetryConfig, SQSDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiterBackend, resolve_rate_limiter
from drt.destinations.retry import resolve_retry, with_retry
from drt.destinations.row_errors import record_preview, record_row_error

_MAX_ENTRIES = 10
_MAX_PAYLOAD_BYTES = 1_048_576
_ID_CHARACTERS = ascii_letters + digits + punctuation


class _TransientBatchError(Exception):
    """Signal that the pending entries need another bounded attempt."""


class SQSDestination:
    def __init__(self) -> None:
        """
        _blocked_groups contains FIFO grouops whose earlier records failed during the current run.
        The engine clears this state by calling reset_write_state() after the run.
        """
        self._blocked_groups: set[tuple[str, str]] = set()

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Serialize records and deliver them to Amazon SQS.

        Steps:
        - Each record is validated and converted into a SQS entry.
        - Entries are grouped into batch requests of at most 10 entries and 1MiB of message bodies.
        - For FIFO queues, a batch contains at most one entry per MessageGroupId,
          so the next record in a group is held until its predecessor's batch has completed.
        - Batch will be sent to SQS.

        Returns:
            The delivery result, including successful, failed,
            and skipped rows as well as row errors.

        """
        assert isinstance(config, SQSDestinationConfig)
        if not records:
            return SyncResult()

        queue_url = resolve_env(None, config.queue_url_env)
        if not queue_url:
            raise ValueError(
                "SQS destination: queue_url_env must resolve to a non-empty queue URL "
                f"(set {config.queue_url_env!r})."
            )
        is_fifo = queue_url.endswith(".fifo")
        if is_fifo:
            if not config.message_group_id_field:
                raise ValueError(
                    "SQS message_group_id_field is required for a FIFO queue "
                    "(resolved queue URL ends in .fifo)."
                )
        else:
            for field in ("message_group_id_field", "deduplication_id_field"):
                if getattr(config, field) is not None:
                    raise ValueError(
                        f"SQS {field} is invalid for a standard queue; "
                        "this field requires a FIFO queue URL ending in .fifo."
                    )

        client = self._client(config)
        limiter = resolve_rate_limiter(config, sync_options, key_override=f"sqs:{queue_url}")
        retry_config = resolve_retry(config.retry, sync_options)
        result = SyncResult()
        entries: list[dict[str, str]] = []
        payload_bytes = 0
        for index, record in enumerate(records):
            group_value = (
                record.get(config.message_group_id_field)
                if config.message_group_id_field is not None
                else None
            )
            group_key = (
                (queue_url, str(group_value))
                if isinstance(group_value, (str, int, float, Decimal, UUID))
                else None
            )
            try:
                entry = self._entry(index, record, config)
                size = len(entry["MessageBody"].encode("utf-8"))
                if size > _MAX_PAYLOAD_BYTES:
                    raise ValueError("SQS MessageBody exceeds the 1 MiB message limit.")
            except (KeyError, TypeError, ValueError) as exc:
                if group_key is not None:
                    self._blocked_groups.add(group_key)
                record_row_error(result, index, record_preview(record), exc)
                result.errors.append(str(exc))
                if sync_options.on_error == "fail":
                    result.skipped = len(records) - result.total
                    return result
                continue

            if entries and (
                len(entries) == _MAX_ENTRIES
                or payload_bytes + size > _MAX_PAYLOAD_BYTES
                or (
                    group_key is not None
                    and any(item["MessageGroupId"] == group_key[1] for item in entries)
                )
            ):
                self._send_batch(client, queue_url, entries, records, result, retry_config, limiter)
                if result.failed and sync_options.on_error == "fail":
                    # Unsent rows must not be inferred as delivered by the engine.
                    result.skipped = len(records) - result.total
                    return result
                entries = []
                payload_bytes = 0
            if group_key in self._blocked_groups:
                blocked_group_name = group_key[1]
                message = (
                    "SQS FIFO group "
                    f"{blocked_group_name!r} is blocked by an earlier failed record "
                    "in this run."
                )
                record_row_error(result, index, record_preview(record), ValueError(message))
                result.errors.append(message)
                if sync_options.on_error == "fail":
                    result.skipped = len(records) - result.total
                    return result
                continue
            entries.append(entry)
            payload_bytes += size

        if entries:
            self._send_batch(client, queue_url, entries, records, result, retry_config, limiter)
        if is_fifo:
            # Validation can fail before an earlier buffered send completes.
            # Preserve source order when the engine stores these rows in the DLQ.
            result.row_errors.sort(key=lambda error: error.batch_index)
        return result

    def reset_write_state(
        self, config: DestinationConfig | None, sync_options: SyncOptions | None
    ) -> None:
        """The engine calls this after every run, including raised exits."""
        self._blocked_groups.clear()

    @staticmethod
    def _client(config: SQSDestinationConfig) -> Any:
        """Create an SQS client using boto3's standard credential chain."""
        try:
            import boto3  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError("SQS destination requires: pip install drt-core[sqs]") from exc
        session = boto3.session.Session(**({"region_name": config.region} if config.region else {}))
        return session.client("sqs")

    @staticmethod
    def _entry(index: int, record: dict[str, Any], config: SQSDestinationConfig) -> dict[str, str]:
        """Create an entry: an input data format for SQS."""

        entry = {
            "Id": str(index),
            "MessageBody": dumps(record, default=str, ensure_ascii=False),
        }
        for field, parameter in (
            (config.message_group_id_field, "MessageGroupId"),
            (config.deduplication_id_field, "MessageDeduplicationId"),
        ):
            if field is not None:
                value = record.get(field)
                if not isinstance(value, (str, int, float, Decimal, UUID)):
                    raise ValueError(
                        f"SQS {parameter}: record field {field!r} must have a scalar ID."
                    )
                text = str(value)
                if not text or len(text) > 128:
                    raise ValueError(
                        f"SQS {parameter}: record field {field!r} must contain 1–128 characters."
                    )
                if any(character not in _ID_CHARACTERS for character in text):
                    raise ValueError(
                        f"SQS {parameter}: record field {field!r} must contain only "
                        "ASCII letters, digits, and punctuation "
                        "(no whitespace or control characters)."
                    )
                entry[parameter] = text
        return entry

    def _send_batch(
        self,
        client: Any,
        queue_url: str,
        entries: list[dict[str, str]],
        records: list[dict[str, Any]],
        result: SyncResult,
        retry_config: RetryConfig,
        limiter: RateLimiterBackend,
    ) -> None:
        """
        Deliver one SQS batch and retry transient entry failures.

        Successful entries are removed from the pending set. Permanent
        sender faults become RowErrors immediately. Transient entry failures
        remain pending and are retried according to the configured retry
        policy. Entries still pending after retry exhaustion become RowErrors
        and block their FIFO groups for the remainder of the run.

        An incomplete or inconsistent SQS response raises RuntimeError.
        Transport exceptions from the SQS client are allowed to propagate.
        """
        pending = {entry["Id"]: entry for entry in entries}
        last_errors: dict[str, str] = {}

        def fail_entry(id_: str, message: str) -> None:
            index = int(id_)
            record_row_error(result, index, record_preview(records[index]), ValueError(message))
            result.errors.append(message)
            group = pending[id_].get("MessageGroupId")
            if group is not None:
                self._blocked_groups.add((queue_url, group))
            del pending[id_]

        def send_pending() -> None:
            limiter.acquire()
            response = client.send_message_batch(QueueUrl=queue_url, Entries=list(pending.values()))
            successes = response.get("Successful", [])
            failures = response.get("Failed", [])
            reported = [entry["Id"] for entry in successes + failures]
            if len(reported) != len(pending) or set(reported) != set(pending):
                raise RuntimeError("SQS batch response did not report each requested entry once.")
            for success in successes:
                del pending[success["Id"]]
                result.success += 1
            for failure in failures:
                id_ = failure["Id"]
                message = f"SQS {failure['Code']}: {failure.get('Message', '')}"
                if failure.get("SenderFault") is True:
                    fail_entry(id_, message)
                elif failure.get("SenderFault") is False:
                    last_errors[id_] = message
                else:
                    raise RuntimeError("SQS batch response is missing a boolean SenderFault.")
            if pending:
                raise _TransientBatchError("SQS batch has transiently failed entries.")

        try:
            with_retry(
                send_pending,
                retry_config,
                retry_on=lambda exc: isinstance(exc, _TransientBatchError),
            )
        except _TransientBatchError:
            for id_ in list(pending):
                fail_entry(id_, last_errors[id_])
