"""Fail-fast capability coverage for advanced ``sync.mode`` values (#1042)."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from drt.config.credentials import BigQueryProfile, ProfileConfig
from drt.config.models import DestinationConfig, SyncConfig, SyncOptions
from drt.destinations.base import ModeCapable, SyncResult
from drt.destinations.bigquery import BigQueryDestination
from drt.destinations.clickhouse import ClickHouseDestination
from drt.destinations.databricks import DatabricksDestination
from drt.destinations.mysql import MySQLDestination
from drt.destinations.postgres import PostgresDestination
from drt.destinations.snowflake import SnowflakeDestination
from drt.destinations.sql_base import BaseSqlDestination
from drt.engine.sync import _check_mode_supported, run_sync


class _IncapableDestination:
    """A normal destination with no replace/mirror machinery."""

    def __init__(self) -> None:
        self.calls: list[list[dict[str, Any]]] = []

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        self.calls.append(records)
        return SyncResult(success=len(records))


class _CountingSource:
    def __init__(self) -> None:
        self.extract_called = False

    def extract(
        self,
        query: str,
        config: ProfileConfig,
        *,
        query_tags: dict[str, str] | None = None,
    ) -> Iterator[dict[str, Any]]:
        self.extract_called = True
        yield {"id": 1}


class _MirrorOnlyDestination(_IncapableDestination):
    def supported_modes(self) -> frozenset[str]:
        return frozenset({"mirror"})


def _sync(mode: str) -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": f"unsupported_{mode}",
            "model": "ref('records')",
            "destination": {"type": "rest_api", "url": "https://example.com"},
            "sync": {"mode": mode},
        }
    )


def _profile() -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="p", dataset="d")


@pytest.mark.parametrize("mode", ["mirror", "replace"])
def test_run_sync_rejects_advanced_mode_before_any_io(mode: str, tmp_path: Path) -> None:
    source = _CountingSource()
    destination = _IncapableDestination()

    with pytest.raises(
        ValueError,
        match=rf"sync\.mode: {mode} is not supported by _IncapableDestination",
    ):
        run_sync(_sync(mode), source, destination, _profile(), tmp_path)

    assert source.extract_called is False
    assert destination.calls == []


@pytest.mark.parametrize("mode", ["full", "incremental", "upsert"])
def test_always_safe_modes_need_no_capability(mode: str) -> None:
    _check_mode_supported(mode, _IncapableDestination())


def test_capable_destination_rejects_an_undeclared_advanced_mode() -> None:
    with pytest.raises(
        ValueError,
        match=r"sync\.mode: replace.*Supported here: mirror",
    ):
        _check_mode_supported("replace", _MirrorOnlyDestination())


@pytest.mark.parametrize(
    "destination",
    [
        PostgresDestination(),
        MySQLDestination(),
        SnowflakeDestination(),
        DatabricksDestination(),
        ClickHouseDestination(),
    ],
    ids=["postgres", "mysql", "snowflake", "databricks", "clickhouse"],
)
@pytest.mark.parametrize("mode", ["replace", "mirror"])
def test_existing_advanced_mode_destinations_remain_supported(
    destination: ModeCapable, mode: str
) -> None:
    assert isinstance(destination, ModeCapable)
    assert destination.supported_modes() == frozenset({"replace", "mirror"})
    _check_mode_supported(mode, destination)


def test_bigquery_destination_does_not_claim_sync_mode_capability() -> None:
    assert not isinstance(BigQueryDestination(), ModeCapable)
    with pytest.raises(ValueError, match="not supported by BigQueryDestination"):
        _check_mode_supported("mirror", BigQueryDestination())


def test_incomplete_base_sql_destination_subclass_does_not_inherit_the_capability() -> None:
    """``supported_modes()`` must be declared per concrete dialect, not on
    ``BaseSqlDestination`` itself (caught in review, #1042): the base class's
    replace/mirror hooks (``_load_replace_swap``, ``_build_mirror_delete``,
    etc.) are abstract ``NotImplementedError`` stubs, so a subclass that only
    implements the hooks it needs (e.g. plain upsert) must not silently
    inherit a capability it can't actually serve — that would let the engine
    wave a `mirror`/`replace` sync past the fail-fast guard and crash later,
    mid-run, possibly after some records were already written.
    """

    class _IncompleteSqlDestination(BaseSqlDestination):
        """Only implements what a plain-upsert dialect needs."""

    dest = _IncompleteSqlDestination()
    assert not isinstance(dest, ModeCapable)
    for mode in ("replace", "mirror"):
        with pytest.raises(
            ValueError, match=rf"sync\.mode: {mode} is not supported"
        ):
            _check_mode_supported(mode, dest)
