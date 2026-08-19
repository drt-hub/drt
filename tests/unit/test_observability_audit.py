"""Tests for the config_changed/secret_accessed audit-log extension point
(#299, ADR 0008)."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from drt.observability.audit import (
    AuditEvent,
    AuditLogger,
    NoOpAuditLogger,
    _reset_audit_logger,
    get_audit_logger,
    register_audit_logger,
)


@pytest.fixture(autouse=True)
def _reset_registry() -> Iterator[None]:
    yield
    _reset_audit_logger()


def test_default_logger_is_noop() -> None:
    assert isinstance(get_audit_logger(), NoOpAuditLogger)


def test_noop_logger_discards_events_without_error() -> None:
    NoOpAuditLogger().log_event(
        AuditEvent(event_type="config_changed", timestamp="2026-08-19T00:00:00Z")
    )


def test_noop_logger_isinstance_audit_logger() -> None:
    assert isinstance(NoOpAuditLogger(), AuditLogger)


def test_register_audit_logger_replaces_active_logger() -> None:
    captured: list[AuditEvent] = []

    class _Capturing:
        def log_event(self, event: AuditEvent) -> None:
            captured.append(event)

    register_audit_logger(_Capturing())
    event = AuditEvent(
        event_type="secret_accessed",
        timestamp="2026-08-19T00:00:00Z",
        details={"scheme": "aws-sm", "path": "prod/drt/snowflake"},
    )
    get_audit_logger().log_event(event)

    assert captured == [event]


def test_audit_event_type_matches_the_two_non_sync_lifecycle_events() -> None:
    """The other three #299 event types (sync_started/sync_completed/
    sync_failed) go through SyncObserver instead — see ADR 0008 Decision 1."""
    import typing

    from drt.observability.audit import AuditEventType

    assert typing.get_args(AuditEventType) == ("config_changed", "secret_accessed")
