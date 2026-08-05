"""Empty-batch contract for special-transport destinations.

Sibling to:

- ``test_destination_empty_batch.py`` (HTTP webhooks via
  ``pytest_httpserver``)
- ``test_destination_file_empty_batch.py`` (filesystem)
- ``test_destination_sql_empty_batch.py`` (SQL drivers, lazy import)
- ``test_destination_api_empty_batch.py`` (HTTP API via ``httpx``)

This module covers destinations that use neither ``httpx`` nor the
filesystem:

- **email_smtp** — ``smtplib`` (stdlib). Assertion uses an
  ``smtplib.SMTP.__init__`` / ``smtplib.SMTP_SSL.__init__`` tripwire
  that records every connection attempt into a captured list. The
  ``SMTP`` constructor opens the TCP connection synchronously, so any
  attempt would show up here.
- **google_sheets** — ``googleapiclient.discovery`` (optional
  ``[sheets]`` extra). Assertion is implicit by way of the lazy
  import: the destination's ``_build_sheets_service`` (which performs
  ``from googleapiclient.discovery import build``) is only called
  AFTER the empty-records short-circuit. CI's minimal install does not
  include the ``[sheets]`` extra, so a regression that lost the
  short-circuit would surface as ``ModuleNotFoundError`` immediately —
  same implicit "no driver was imported" pattern as the SQL contract
  module (#595).

Out of scope for this module: ``salesforce_bulk`` and
``staged_upload`` use the ``StagedDestination`` Protocol (``stage()``
+ a separate trigger step rather than ``load()``) — different
contract shape, separate follow-up.
"""

from __future__ import annotations

from typing import Any

import pytest

from drt.config.models import (
    EmailSmtpDestinationConfig,
    GoogleSheetsDestinationConfig,
    RateLimitConfig,
    SyncOptions,
)
from drt.destinations.base import Destination, SyncResult
from drt.destinations.email_smtp import EmailSmtpDestination
from drt.destinations.google_sheets import GoogleSheetsDestination


@pytest.fixture
def empty_sync_options() -> SyncOptions:
    """Minimal SyncOptions with no rate limit — keeps the test fast."""
    return SyncOptions(
        mode="full",
        batch_size=100,
        on_error="skip",
        rate_limit=RateLimitConfig(requests_per_second=0),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _email_smtp_config() -> EmailSmtpDestinationConfig:
    return EmailSmtpDestinationConfig(
        type="email_smtp",
        host="smtp.example.com",
        port=587,
        sender="noreply@example.com",
        recipients=["admin@example.com"],
        subject_template="{{ row.subject }}",
        body_template="{{ row.body }}",
        username="dummy",
        password="dummy",
    )


def _google_sheets_config() -> GoogleSheetsDestinationConfig:
    return GoogleSheetsDestinationConfig(
        type="google_sheets",
        spreadsheet_id="1BxiMVs_dummy",
        sheet="Sheet1",
        mode="overwrite",
        # credentials_path / credentials_env intentionally left unset:
        # _build_sheets_service must never be reached, so the lack of
        # credentials is fine — and would only surface if the contract
        # broke.
    )


# ---------------------------------------------------------------------------
# email_smtp
# ---------------------------------------------------------------------------


def test_email_smtp_satisfies_destination_protocol() -> None:
    """``EmailSmtpDestination`` satisfies the ``Destination`` Protocol."""
    assert isinstance(EmailSmtpDestination(), Destination)


def test_email_smtp_empty_batch_returns_empty_sync_result(
    empty_sync_options: SyncOptions,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``load([])`` on email_smtp returns ``SyncResult(0, 0, 0)``."""
    # Block SMTP / SMTP_SSL constructors so any connection attempt is a
    # test failure rather than a silent real-network call. The tripwire
    # records calls into a list rather than raising — some destinations
    # have broad except handlers; recording-then-asserting at test level
    # sidesteps the swallow path.
    captured: list[tuple[Any, ...]] = []

    def _record(self: Any, *args: Any, **kwargs: Any) -> None:
        captured.append((args, kwargs))
        # Don't actually initialise — there's no real socket to set up.

    monkeypatch.setattr("smtplib.SMTP.__init__", _record)
    monkeypatch.setattr("smtplib.SMTP_SSL.__init__", _record)

    result = EmailSmtpDestination().load(
        [], _email_smtp_config(), empty_sync_options
    )

    assert isinstance(result, SyncResult)
    assert result.success == 0
    assert result.failed == 0
    assert result.skipped == 0
    assert len(captured) == 0, (
        f"EmailSmtpDestination opened {len(captured)} SMTP connection(s) "
        "on empty batch; destinations must short-circuit when there's "
        "nothing to send."
    )


def test_email_smtp_empty_batch_makes_no_smtp_connection(
    empty_sync_options: SyncOptions,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``load([])`` on email_smtp never calls ``smtplib.SMTP.__init__``.

    Standalone assertion in case the SyncResult check above passes
    vacuously — the SMTP tripwire is the load-bearing check here.
    """
    captured: list[tuple[Any, ...]] = []

    def _record(self: Any, *args: Any, **kwargs: Any) -> None:
        captured.append((args, kwargs))

    monkeypatch.setattr("smtplib.SMTP.__init__", _record)
    monkeypatch.setattr("smtplib.SMTP_SSL.__init__", _record)

    EmailSmtpDestination().load([], _email_smtp_config(), empty_sync_options)

    assert len(captured) == 0


# ---------------------------------------------------------------------------
# google_sheets
# ---------------------------------------------------------------------------


def test_google_sheets_satisfies_destination_protocol() -> None:
    """``GoogleSheetsDestination`` satisfies the ``Destination`` Protocol."""
    assert isinstance(GoogleSheetsDestination(), Destination)


def test_google_sheets_empty_batch_returns_empty_sync_result(
    empty_sync_options: SyncOptions,
) -> None:
    """``load([])`` on google_sheets returns ``SyncResult(0, 0, 0)``.

    Implicit "no driver was imported" contract — same as the SQL
    contract module (#595). CI's minimal install does not include the
    ``[sheets]`` extra, so ``googleapiclient.discovery`` is not
    available. If this test reaches ``_build_sheets_service`` it
    crashes with ``ModuleNotFoundError`` — surfacing the regression.
    """
    result = GoogleSheetsDestination().load(
        [], _google_sheets_config(), empty_sync_options
    )

    assert isinstance(result, SyncResult)
    assert result.success == 0
    assert result.failed == 0
    assert result.skipped == 0


def test_google_sheets_empty_batch_does_not_call_build_service(
    empty_sync_options: SyncOptions,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``load([])`` on google_sheets never enters ``_build_sheets_service``.

    Explicit tripwire on the service builder, complementing the
    implicit "no driver was imported" check above. Records any call
    into a list rather than raising so broad-except handlers in the
    destination can't swallow the bug.
    """
    captured: list[tuple[Any, ...]] = []

    def _record(*args: Any, **kwargs: Any) -> Any:
        captured.append((args, kwargs))
        return None

    monkeypatch.setattr(
        "drt.destinations.google_sheets._build_sheets_service", _record
    )

    GoogleSheetsDestination().load(
        [], _google_sheets_config(), empty_sync_options
    )

    assert len(captured) == 0, (
        f"GoogleSheetsDestination called _build_sheets_service "
        f"{len(captured)} time(s) on empty batch; destinations must "
        "short-circuit before any auth / discovery work."
    )
