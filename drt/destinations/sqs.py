"""Amazon SQS destination scaffolding; message delivery is not implemented yet."""

from __future__ import annotations

from decimal import Decimal
from json import dumps
from typing import Any
from uuid import UUID

from drt.config.credentials import resolve_env
from drt.config.models import DestinationConfig, SQSDestinationConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.destinations.rate_limiter import RateLimiterBackend, resolve_rate_limiter
from drt.destinations.retry import with_retry
from drt.destinations.row_errors import record_preview, record_row_error

_MAX_ENTRIES = 10
_MAX_PAYLOAD_BYTES = 1_048_576


class _TransientBatchError(Exception):
    """Signal that the pending entries need another bounded attempt."""


class SQSDestination:
    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        assert isinstance(config, SQSDestinationConfig)
        if not records:
            return SyncResult()

        queue_url = resolve_env(None, config.queue_url_env)
        if not queue_url:
            raise ValueError(
                "SQS destination: queue_url_env must resolve to a non-empty queue URL "
                f"(set {config.queue_url_env!r})."
            )
        if queue_url.endswith(".fifo"):
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
        limiter = resolve_rate_limiter(config, sync_options)
        result = SyncResult()
        entries: list[dict[str, str]] = []
        payload_bytes = 0
        for index, record in enumerate(records):
            try:
                entry = self._entry(index, record, config)
                size = len(entry["MessageBody"].encode("utf-8"))
                if size > _MAX_PAYLOAD_BYTES:
                    raise ValueError("SQS MessageBody exceeds the 1 MiB message limit.")
            except (KeyError, TypeError, ValueError) as exc:
                record_row_error(result, index, record_preview(record), exc)
                result.errors.append(str(exc))
                if sync_options.on_error == "fail":
                    result.skipped = len(records) - result.total
                    return result
                continue

            if entries and (
                len(entries) == _MAX_ENTRIES or payload_bytes + size > _MAX_PAYLOAD_BYTES
            ):
                self._send_batch(client, queue_url, entries, records, result, sync_options, limiter)
                if result.failed and sync_options.on_error == "fail":
                    # Unsent rows must not be inferred as delivered by the engine.
                    result.skipped = len(records) - result.total
                    return result
                entries = []
                payload_bytes = 0
            entries.append(entry)
            payload_bytes += size

        if entries:
            self._send_batch(client, queue_url, entries, records, result, sync_options, limiter)
        return result

    @staticmethod
    def _client(config: SQSDestinationConfig) -> Any:
        try:
            import boto3  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError("SQS destination requires: pip install drt-core[sqs]") from exc
        session = boto3.session.Session(**({"region_name": config.region} if config.region else {}))
        return session.client("sqs")

    @staticmethod
    def _entry(index: int, record: dict[str, Any], config: SQSDestinationConfig) -> dict[str, str]:
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
                entry[parameter] = text
        return entry

    @staticmethod
    def _send_batch(
        client: Any,
        queue_url: str,
        entries: list[dict[str, str]],
        records: list[dict[str, Any]],
        result: SyncResult,
        sync_options: SyncOptions,
        limiter: RateLimiterBackend,
    ) -> None:
        pending = {entry["Id"]: entry for entry in entries}
        last_errors: dict[str, str] = {}

        def fail_entry(id_: str, message: str) -> None:
            index = int(id_)
            record_row_error(result, index, record_preview(records[index]), ValueError(message))
            result.errors.append(message)
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
                sync_options.retry,
                retry_on=lambda exc: isinstance(exc, _TransientBatchError),
            )
        except _TransientBatchError:
            for id_ in list(pending):
                fail_entry(id_, last_errors[id_])
