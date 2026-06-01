"""Empty-batch contract for the ``StagedDestination`` Protocol.

Sibling to:

- ``test_destination_empty_batch.py`` (HTTP webhooks via
  ``pytest_httpserver``)
- ``test_destination_file_empty_batch.py`` (filesystem)
- ``test_destination_sql_empty_batch.py`` (SQL drivers, lazy import)
- ``test_destination_api_empty_batch.py`` (HTTP API via ``httpx``)
- ``test_destination_special_empty_batch.py`` (smtplib + googleapiclient)

The ``StagedDestination`` Protocol
(:class:`drt.destinations.base.StagedDestination`) is the
"accumulate + upload" shape used by destinations that talk to async
bulk-upload APIs:

- ``salesforce_bulk`` — Salesforce Bulk API 2.0 (auth → create job →
  upload → close → poll → fetch errors)
- ``staged_upload`` — generic three-phase upload (stage upload →
  trigger job → poll for completion)

The shape is:

- ``stage(records, config, opts) -> None`` — accumulate records, no
  HTTP allowed
- ``finalize(config, opts) -> SyncResult`` — upload + trigger + poll

Contracts under test:

1. The destination satisfies the ``StagedDestination`` Protocol.
2. After only empty ``stage([])`` call(s), ``finalize()`` returns
   ``SyncResult(success=0, failed=0, skipped=0)``.
3. After only empty ``stage([])`` call(s), ``finalize()`` never calls
   ``httpx.Client.send`` — the empty-source short-circuit must live
   in ``finalize()`` itself, because the engine calls ``finalize()``
   regardless of whether any batch accumulated records.

Why the third contract is load-bearing: a StagedDestination that
attempted the auth → upload → trigger → poll dance on empty input
would burn API quota and create a zero-row job (e.g. a Salesforce
Bulk job whose ``numberRecordsProcessed`` is zero), wasting an OAuth
token refresh and the job-id allocation. The contract pins the
short-circuit behaviour as required, not optional.

Tripwire mechanism is the same record-then-assert pattern as #604:
the patch captures attempted calls into a list rather than raising,
so broad ``except Exception`` row-error handlers in destinations
can't swallow the AssertionError and mask the bug the contract is
meant to catch.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import pytest

from drt.config.models import (
    SalesforceBulkDestinationConfig,
    StagedUploadDestinationConfig,
    StagedUploadPhaseConfig,
    SyncOptions,
)
from drt.destinations.base import StagedDestination, SyncResult
from drt.destinations.salesforce_bulk import SalesforceBulkDestination
from drt.destinations.staged_upload import StagedUploadDestination

# Each entry: (destination class, factory → valid config).
STAGED_DESTINATIONS: list[Any] = [
    pytest.param(
        StagedUploadDestination,
        lambda: StagedUploadDestinationConfig(
            type="staged_upload",
            format="csv",
            stage=StagedUploadPhaseConfig(
                url="https://upload.example.com/files",
                method="POST",
            ),
            trigger=StagedUploadPhaseConfig(
                url="https://api.example.com/jobs",
                method="POST",
            ),
            poll=None,
        ),
        id="staged_upload",
    ),
    pytest.param(
        SalesforceBulkDestination,
        lambda: SalesforceBulkDestinationConfig(
            type="salesforce_bulk",
            instance_url="https://example.my.salesforce.com",
            object_name="Contact",
            operation="upsert",
            external_id_field="External_Id__c",
            client_id_env="SF_CLIENT_ID_TEST_UNSET",
            client_secret_env="SF_CLIENT_SECRET_TEST_UNSET",
            username_env="SF_USERNAME_TEST_UNSET",
            password_env="SF_PASSWORD_TEST_UNSET",
        ),
        id="salesforce_bulk",
    ),
]


@pytest.fixture
def block_httpx(monkeypatch: pytest.MonkeyPatch) -> list[tuple[Any, ...]]:
    """Patch ``httpx.Client.send`` (sync + async) to a tripwire that records
    any attempted HTTP call as a captured tuple instead of raising.

    See module docstring for why we record-then-assert at test level
    rather than raising inside the patch.
    """
    captured: list[tuple[Any, ...]] = []

    def _record(self: Any, request: Any, *args: Any, **kwargs: Any) -> Any:
        captured.append((self, request, args, kwargs))
        return None

    monkeypatch.setattr("httpx.Client.send", _record)
    monkeypatch.setattr("httpx.AsyncClient.send", _record)
    return captured


@pytest.fixture
def empty_sync_options() -> SyncOptions:
    """Minimal SyncOptions — staged destinations don't read rate-limit."""
    return SyncOptions(mode="full", batch_size=100, on_error="skip")


@pytest.mark.parametrize("destination_class, config_factory", STAGED_DESTINATIONS)
def test_satisfies_staged_destination_protocol(
    destination_class: type,
    config_factory: Callable[[], Any],
) -> None:
    """Every staged destination satisfies the ``StagedDestination`` Protocol."""
    dest = destination_class()
    assert isinstance(dest, StagedDestination)


@pytest.mark.parametrize("destination_class, config_factory", STAGED_DESTINATIONS)
def test_empty_stage_then_finalize_returns_empty_sync_result(
    destination_class: type,
    config_factory: Callable[[], Any],
    empty_sync_options: SyncOptions,
    block_httpx: list[tuple[Any, ...]],
) -> None:
    """``stage([])`` then ``finalize()`` returns ``SyncResult(0, 0, 0)``.

    Multiple empty stage calls are exercised because the engine may
    invoke stage() many times if the source produced empty batches
    across windows — the contract holds across the whole shape, not
    just a single call.
    """
    dest = destination_class()
    config = config_factory()

    dest.stage([], config, empty_sync_options)
    dest.stage([], config, empty_sync_options)
    result = dest.finalize(config, empty_sync_options)

    assert isinstance(result, SyncResult)
    assert result.success == 0
    assert result.failed == 0
    assert result.skipped == 0


@pytest.mark.parametrize("destination_class, config_factory", STAGED_DESTINATIONS)
def test_empty_stage_then_finalize_makes_no_http_request(
    destination_class: type,
    config_factory: Callable[[], Any],
    empty_sync_options: SyncOptions,
    block_httpx: list[tuple[Any, ...]],
) -> None:
    """``finalize()`` after only empty ``stage([])`` never calls
    ``httpx.Client.send``.

    The load-bearing contract: a StagedDestination must short-circuit
    in ``finalize()`` when nothing was staged, rather than running the
    full auth → upload → trigger → poll lifecycle on an empty payload.
    """
    dest = destination_class()
    config = config_factory()

    dest.stage([], config, empty_sync_options)
    dest.finalize(config, empty_sync_options)

    assert len(block_httpx) == 0, (
        f"{destination_class.__name__} made {len(block_httpx)} HTTP "
        "request(s) during finalize() after only empty stage([]) call(s); "
        "StagedDestinations must short-circuit when nothing was staged."
    )
