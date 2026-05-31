"""Empty-batch contract for file destinations (Step 2a of #364 follow-up).

Mirror of ``test_destination_empty_batch.py`` (HTTP destinations) for
destinations that write to disk: ``file`` (CSV / JSON / JSONL) and
``parquet`` when its optional extras are installed.

The three invariants are the same:

1. Satisfies the ``Destination`` Protocol.
2. ``load([])`` returns ``SyncResult(success=0, failed=0, skipped=0)``.
3. ``load([])`` leaves the filesystem untouched — no new files, not
   even a 0-byte placeholder. A 0-byte output would still indicate a
   premature file handle was opened.

The HTTP suite asserts ``httpserver.log`` length; here we snapshot the
directory before/after and assert no new entries.

Adding a new file destination: append a ``pytest.param(...)`` entry to
``FILE_DESTINATIONS``. The parquet entry shows the conditional pattern
for destinations that depend on optional ``[extras]``.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from drt.config.models import (
    FileDestinationConfig,
    RateLimitConfig,
    SyncOptions,
)
from drt.destinations.base import Destination, SyncResult
from drt.destinations.file import FileDestination

# Each entry: (destination class, factory taking a tmp Path → valid config).
FILE_DESTINATIONS: list[Any] = [
    pytest.param(
        FileDestination,
        lambda p: FileDestinationConfig(type="file", path=str(p / "out.csv"), format="csv"),
        id="file-csv",
    ),
    pytest.param(
        FileDestination,
        lambda p: FileDestinationConfig(type="file", path=str(p / "out.json"), format="json"),
        id="file-json",
    ),
    pytest.param(
        FileDestination,
        lambda p: FileDestinationConfig(type="file", path=str(p / "out.jsonl"), format="jsonl"),
        id="file-jsonl",
    ),
]


# Parquet destination is gated on `pandas` + `pyarrow` (the `[parquet]`
# extras). Import here under try/except so the suite still collects on
# minimal installs; the parquet parameter is simply skipped in that case.
try:
    import pandas as _pd  # noqa: F401
    import pyarrow as _pa  # noqa: F401

    from drt.config.models import ParquetDestinationConfig
    from drt.destinations.parquet import ParquetDestination

    FILE_DESTINATIONS.append(
        pytest.param(
            ParquetDestination,
            lambda p: ParquetDestinationConfig(
                type="parquet",
                path=str(p / "out.parquet"),
            ),
            id="parquet",
        ),
    )
except ImportError:
    pass


@pytest.fixture
def empty_sync_options() -> SyncOptions:
    """Minimal SyncOptions — no rate limit, full mode, on_error=skip."""
    return SyncOptions(
        mode="full",
        batch_size=100,
        on_error="skip",
        rate_limit=RateLimitConfig(requests_per_second=0),
    )


@pytest.mark.parametrize("destination_class, config_factory", FILE_DESTINATIONS)
def test_satisfies_destination_protocol(
    destination_class: type,
    config_factory: Callable[[Path], Any],
    tmp_path: Path,
) -> None:
    """Every file destination satisfies the ``Destination`` Protocol."""
    dest = destination_class()
    assert isinstance(dest, Destination)


@pytest.mark.parametrize("destination_class, config_factory", FILE_DESTINATIONS)
def test_empty_batch_returns_empty_sync_result(
    destination_class: type,
    config_factory: Callable[[Path], Any],
    tmp_path: Path,
    empty_sync_options: SyncOptions,
) -> None:
    """``load([])`` returns ``SyncResult(success=0, failed=0, skipped=0)``."""
    dest = destination_class()
    config = config_factory(tmp_path)

    result = dest.load([], config, empty_sync_options)

    assert isinstance(result, SyncResult)
    assert result.success == 0
    assert result.failed == 0
    assert result.skipped == 0


@pytest.mark.parametrize("destination_class, config_factory", FILE_DESTINATIONS)
def test_empty_batch_writes_no_file(
    destination_class: type,
    config_factory: Callable[[Path], Any],
    tmp_path: Path,
    empty_sync_options: SyncOptions,
) -> None:
    """``load([])`` leaves the filesystem untouched.

    A 0-byte output file would still indicate the destination opened
    a file handle before checking the record count — destinations
    must short-circuit before any I/O.
    """
    dest = destination_class()
    config = config_factory(tmp_path)

    pre_state = set(tmp_path.iterdir())
    dest.load([], config, empty_sync_options)
    post_state = set(tmp_path.iterdir())

    new_paths = post_state - pre_state
    assert new_paths == set(), (
        f"{destination_class.__name__} created {len(new_paths)} file(s) "
        f"on empty batch: {sorted(p.name for p in new_paths)}; "
        "destinations must short-circuit when there's nothing to write."
    )
