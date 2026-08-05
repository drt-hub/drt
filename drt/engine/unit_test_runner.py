"""Offline transform-pipeline test runner (#780).

Runs a sync's ``unit_tests:`` fixtures through the *real* engine pipeline —
``computed_fields`` -> ``field_mappings`` -> ``mask`` (and anything else
``run_sync()`` applies) — without a network call or a credential in sight.

The trick is that it needs none: :func:`drt.engine.sync.run_sync` already
accepts any ``Source``/``Destination`` Protocol implementation, and
``FakeSource`` (#364) plus the ``CaptureDestination`` this module adds are
exactly that. Every stateful side effect in ``run_sync()`` — state
persistence, watermark storage, history, DLQ — is opt-in via a parameter
that defaults to ``None``/off, so calling it with only a source and a
destination is already side-effect-free. No engine change was needed to
make this possible; this module is a *consumer* of the public API, not a
modification of it.

Two things a full sync can do that a unit test deliberately can't:

* **``lookups``** resolve FK values by querying the real destination
  (``build_lookup_map``) — there is no fake for that yet (tracked as v2 in
  #780's own scope), so a sync with ``destination.lookups`` configured is
  rejected outright rather than silently skipping the lookup step.
* **``alerts``** fire real Slack/webhook HTTP calls from inside
  ``run_sync()``'s own ``finally`` block whenever a run reports a failure —
  not observer-mediated, so passing a real ``SyncConfig`` through as-is
  risks firing them the moment a test deliberately exercises a failure
  path. The sync is run with ``alerts`` stripped (a shallow
  ``model_copy(update=...)``; the caller's own config object is untouched).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from drt.config.credentials import DuckDBProfile
from drt.config.models import DestinationConfig, SyncConfig, SyncOptions, UnitTest
from drt.destinations.base import SyncResult
from drt.engine.sync import run_sync
from drt.sources.fake import FakeSource


class UnitTestLookupsUnsupportedError(ValueError):
    """Raised when a unit-tested sync's destination has ``lookups:`` configured.

    Lookups resolve against the real destination (``build_lookup_map``), which
    a unit test has none of. Rejected rather than silently run without them,
    since a passing test would then be asserting against output the real sync
    never produces.
    """


@dataclass
class CaptureDestination:
    """``Destination`` stand-in that records what it would have sent.

    Accumulates across every ``load()`` call — batching must never change
    which rows a unit test compares against, only how many calls it takes to
    deliver them.
    """

    records: list[dict[str, Any]] = field(default_factory=list)

    def load(
        self,
        records: list[dict[str, Any]],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        self.records.extend(records)
        return SyncResult(success=len(records))


@dataclass
class UnitTestResult:
    """Outcome of one :class:`UnitTest`."""

    name: str
    passed: bool
    actual: list[dict[str, Any]]
    # Human-readable mismatch descriptions; empty when passed. A raised
    # exception from run_sync() (a template error, an unsupported
    # match_policy, ...) becomes a single-entry list rather than propagating —
    # the point of a unit test is to report a broken config safely, not to
    # crash the `drt test --unit` invocation on the first one.
    mismatches: list[str] = field(default_factory=list)


def _row_matches(expected: dict[str, Any], actual: dict[str, Any]) -> bool:
    """``expected``'s keys must be present in ``actual`` with equal values.

    Subset match, not exact-record match: a sync's source columns grow over
    time, and requiring every unit test to enumerate every column it doesn't
    care about would make each one a maintenance burden against unrelated
    schema growth. Row *count* is still checked exactly by the caller — a
    transform that drops or duplicates rows is exactly what a unit test
    exists to catch.
    """
    return all(key in actual and actual[key] == value for key, value in expected.items())


def run_unit_test(
    sync: SyncConfig, test: UnitTest, project_dir: Path = Path(".")
) -> UnitTestResult:
    """Run one ``UnitTest`` fixture through ``sync``'s real transform pipeline.

    Raises:
        UnitTestLookupsUnsupportedError: ``sync.destination`` has
            ``lookups:`` configured. Not a per-test result — this is a
            config-shape problem the same for every fixture on this sync, so
            the caller should surface it once rather than call this in a loop
            and get the same rejection N times.
    """
    if getattr(sync.destination, "lookups", None):
        raise UnitTestLookupsUnsupportedError(
            f"sync '{sync.name}': unit_tests do not support destination.lookups yet "
            "(no fake lookup table — see #780). Remove lookups to unit-test this "
            "sync, or test the lookup-resolved fields via a real --dry-run instead."
        )

    # Alerts fire real HTTP from inside run_sync()'s own finally block on any
    # reported failure — stripped so a fixture that deliberately exercises an
    # on_error path can't page anyone. model_copy is a shallow copy; `sync`
    # itself (the caller's object) is never mutated.
    test_sync = sync.model_copy(update={"alerts": None})

    capture = CaptureDestination()
    try:
        run_sync(
            test_sync,
            FakeSource(rows=test.given),
            capture,
            DuckDBProfile(type="duckdb"),
            project_dir,
        )
    except Exception as exc:  # noqa: BLE001 — reported as a failed test, not a crash
        return UnitTestResult(
            name=test.name,
            passed=False,
            actual=capture.records,
            mismatches=[f"{type(exc).__name__}: {exc}"],
        )

    mismatches: list[str] = []
    if len(capture.records) != len(test.expect):
        mismatches.append(
            f"expected {len(test.expect)} row(s), got {len(capture.records)} "
            f"— given {len(test.given)}"
        )
    else:
        for i, (expected, actual) in enumerate(zip(test.expect, capture.records)):
            if not _row_matches(expected, actual):
                mismatches.append(f"row {i}: expected {expected!r} to be a subset of {actual!r}")

    return UnitTestResult(
        name=test.name, passed=not mismatches, actual=capture.records, mismatches=mismatches
    )
