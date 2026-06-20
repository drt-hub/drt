"""Tests for OpenTelemetry span instrumentation in the sync engine (Phase 3, #619).

The engine wraps a sync run in a ``drt.sync.run`` span with one
``drt.sync.extract`` child and one ``drt.sync.load`` child per batch. These
tests patch ``drt.engine.sync.get_tracer`` with a recording double and assert
the spans + attributes the engine emits, then confirm the zero-cost no-op path
still produces a clean ``SyncResult`` when ``[otel]`` isn't installed.

Span boundaries live at call sites in ``engine/sync.py`` (per #619) using the
Phase 2 ``get_tracer()`` utility; ``get_tracer()`` returns a no-op tracer when
OTel is unconfigured, so the engine never branches on whether OTel is enabled.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, Literal

import pytest

import drt.engine.sync as sync_mod
from drt.config.credentials import ProfileConfig
from drt.destinations.base import SyncResult
from drt.engine.sync import run_sync
from tests.unit.test_engine import (
    FakeDestination,
    FakeSource,
    _make_profile,
    _make_sync,
)

# ---------------------------------------------------------------------------
# Recording tracer double
# ---------------------------------------------------------------------------


class _RecSpan:
    """Records everything the engine does to a span."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.attributes: dict[str, Any] = {}
        self.status: Any = "UNSET"
        self.exceptions: list[BaseException] = []
        self.ended = False

    # context-manager form (start_as_current_span)
    def __enter__(self) -> _RecSpan:
        return self

    def __exit__(self, *_exc: object) -> Literal[False]:
        self.ended = True
        return False

    def set_attribute(self, key: str, value: Any) -> None:
        self.attributes[key] = value

    def record_exception(self, exc: BaseException, *_a: object, **_k: object) -> None:
        self.exceptions.append(exc)

    def set_status(self, status: Any, *_a: object, **_k: object) -> None:
        self.status = status

    def add_event(self, *_a: object, **_k: object) -> None:
        pass

    # explicit-lifetime form (start_span)
    def end(self, *_a: object, **_k: object) -> None:
        self.ended = True


class _RecTracer:
    def __init__(self) -> None:
        self.spans: list[_RecSpan] = []

    def start_as_current_span(self, name: str, *_a: object, **_k: object) -> _RecSpan:
        span = _RecSpan(name)
        self.spans.append(span)
        return span

    def start_span(self, name: str, *_a: object, **_k: object) -> _RecSpan:
        span = _RecSpan(name)
        self.spans.append(span)
        return span

    def by_name(self, name: str) -> list[_RecSpan]:
        return [s for s in self.spans if s.name == name]

    def one(self, name: str) -> _RecSpan:
        matches = self.by_name(name)
        assert len(matches) == 1, f"expected exactly one {name!r} span, got {len(matches)}"
        return matches[0]


@pytest.fixture
def rec_tracer(monkeypatch: pytest.MonkeyPatch) -> _RecTracer:
    """Patch the engine's tracer + status builder with recording doubles.

    ``build_status`` is patched to return readable sentinels (``"OK"`` /
    ``"ERROR"``) so status assertions don't depend on whether the real OTel
    API is importable in the test environment.
    """
    tracer = _RecTracer()
    monkeypatch.setattr(sync_mod, "get_tracer", lambda: tracer)
    monkeypatch.setattr(
        sync_mod,
        "build_status",
        lambda *, ok, description="": "OK" if ok else "ERROR",
    )
    return tracer


class _RaisingSource:
    """Source whose extraction raises partway through iteration."""

    def __init__(self, rows_before_error: int = 1) -> None:
        self._n = rows_before_error

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict[str, Any]]:
        for i in range(self._n):
            yield {"id": i}
        raise RuntimeError("source blew up mid-stream")

    def test_connection(self, config: ProfileConfig) -> bool:  # pragma: no cover
        return True


# ---------------------------------------------------------------------------
# Span tree + attributes
# ---------------------------------------------------------------------------


def test_run_span_attributes_and_ok_status(rec_tracer: _RecTracer, tmp_path: Path) -> None:
    run_sync(
        _make_sync(batch_size=10),
        FakeSource([{"id": 1}, {"id": 2}]),
        FakeDestination(),
        _make_profile(),
        tmp_path,
        observer=None,
    )

    run = rec_tracer.one("drt.sync.run")
    assert run.attributes == {
        "sync.name": "test_sync",
        "source.type": "bigquery",
        "destination.type": "rest_api",
        "sync.mode": "full",
        "batch_size": 10,
    }
    assert run.status == "OK"
    assert run.exceptions == []
    assert run.ended is True


def test_extract_span_records_rows_extracted(rec_tracer: _RecTracer, tmp_path: Path) -> None:
    run_sync(
        _make_sync(batch_size=2),
        FakeSource([{"id": i} for i in range(5)]),
        FakeDestination(),
        _make_profile(),
        tmp_path,
    )

    extract = rec_tracer.one("drt.sync.extract")
    assert extract.attributes["extract.rows_extracted"] == 5
    assert extract.ended is True


def test_one_load_span_per_batch_with_indices(rec_tracer: _RecTracer, tmp_path: Path) -> None:
    # 5 rows, batch_size 2 -> batches of [2, 2, 1] -> 3 load spans.
    run_sync(
        _make_sync(batch_size=2),
        FakeSource([{"id": i} for i in range(5)]),
        FakeDestination(),
        _make_profile(),
        tmp_path,
    )

    loads = rec_tracer.by_name("drt.sync.load")
    assert [s.attributes["batch_index"] for s in loads] == [0, 1, 2]
    assert [s.attributes["batch_size"] for s in loads] == [2, 2, 1]
    # Clean run -> all rows succeed, none failed/skipped.
    assert [s.attributes["load.success"] for s in loads] == [2, 2, 1]
    assert all(s.attributes["load.failed"] == 0 for s in loads)
    assert all(s.attributes["load.skipped"] == 0 for s in loads)
    assert all(s.ended for s in loads)


def test_load_span_reports_failed_count(rec_tracer: _RecTracer, tmp_path: Path) -> None:
    # Fail the 2nd global record; single batch, on_error=skip so the run completes.
    run_sync(
        _make_sync(batch_size=10, on_error="skip"),
        FakeSource([{"id": 0}, {"id": 1}, {"id": 2}]),
        FakeDestination(fail_indices={1}),
        _make_profile(),
        tmp_path,
    )

    load = rec_tracer.one("drt.sync.load")
    assert load.attributes["load.success"] == 2
    assert load.attributes["load.failed"] == 1


def test_dry_run_emits_no_load_spans(rec_tracer: _RecTracer, tmp_path: Path) -> None:
    """dry_run never calls destination.load(), so no load spans are created."""
    run_sync(
        _make_sync(batch_size=2),
        FakeSource([{"id": i} for i in range(3)]),
        FakeDestination(),
        _make_profile(),
        tmp_path,
        dry_run=True,
    )

    assert rec_tracer.by_name("drt.sync.load") == []
    # run + extract spans still present, extract still records rows.
    assert rec_tracer.one("drt.sync.extract").attributes["extract.rows_extracted"] == 3
    assert rec_tracer.one("drt.sync.run").status == "OK"


def test_run_span_records_exception_and_error_status(
    rec_tracer: _RecTracer, tmp_path: Path
) -> None:
    with pytest.raises(RuntimeError, match="source blew up"):
        run_sync(
            _make_sync(batch_size=10),
            _RaisingSource(rows_before_error=1),
            FakeDestination(),
            _make_profile(),
            tmp_path,
        )

    run = rec_tracer.one("drt.sync.run")
    assert run.status == "ERROR"
    assert len(run.exceptions) == 1
    assert isinstance(run.exceptions[0], RuntimeError)
    assert run.ended is True
    # The extract span is still ended even though extraction raised.
    assert rec_tracer.one("drt.sync.extract").ended is True


# ---------------------------------------------------------------------------
# Zero-cost no-op path (verification plan item 2)
# ---------------------------------------------------------------------------


def test_noop_tracer_when_otel_absent_produces_clean_result(tmp_path: Path) -> None:
    """With OTel not installed, get_tracer() is the no-op fallback and the sync
    still returns a clean SyncResult (the unconditional span calls are free)."""
    from drt.observability import get_tracer
    from drt.observability.otel import _FallbackNoOpTracer

    try:
        import opentelemetry  # noqa: F401

        otel_installed = True
    except ImportError:
        otel_installed = False

    result = run_sync(
        _make_sync(batch_size=2),
        FakeSource([{"id": i} for i in range(3)]),
        FakeDestination(),
        _make_profile(),
        tmp_path,
    )

    assert isinstance(result, SyncResult)
    assert result.success == 3
    assert result.failed == 0
    assert result.rows_extracted == 3

    if not otel_installed:
        assert isinstance(get_tracer(), _FallbackNoOpTracer)