"""Coverage-completing tests for OTel Phase 3 (#619).

Three groups:

1. ``build_status`` — the real-``Status`` branch (only taken when the OTel API
   is importable) is exercised via import-mocking so it runs in CI's default
   no-``[otel]`` install, plus a real ``importorskip`` variant.
2. Engine paths that the span instrumentation re-indented (staged destinations,
   dry-run diff, lookups) — driven through ``run_sync`` so the spans coexist
   with those branches.
3. A real OpenTelemetry SDK end-to-end check (in-memory exporter), skipped when
   ``opentelemetry`` isn't installed.
"""

from __future__ import annotations

import importlib
import types
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock

import pytest

import drt.engine.sync as sync_mod
import drt.observability.otel as otel
from drt.config.models import SyncConfig
from drt.destinations.base import SyncResult
from drt.engine.sync import run_sync
from tests.unit.test_engine import (
    FakeDestination,
    FakeSource,
    _make_profile,
    _make_sync,
)
from tests.unit.test_engine_tracing import _RecTracer

# ---------------------------------------------------------------------------
# build_status
# ---------------------------------------------------------------------------


def test_build_status_returns_real_status_when_api_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When ``opentelemetry.trace.status`` imports, build_status returns a real
    ``Status``. Exercised with a fake status module so it runs without OTel."""
    captured: dict[str, Any] = {}

    class _Code:
        OK = "OK"
        ERROR = "ERROR"

    class _Status:
        def __init__(self, code: object, desc: object = None) -> None:
            captured["code"] = code
            captured["desc"] = desc

    fake_status_mod = types.SimpleNamespace(StatusCode=_Code, Status=_Status)
    real_import = importlib.import_module

    def fake_import(name: str, package: str | None = None) -> Any:
        if name == "opentelemetry.trace.status":
            return fake_status_mod
        return real_import(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import)

    ok = otel.build_status(ok=True)
    assert isinstance(ok, _Status)
    assert captured["code"] == "OK"
    assert captured["desc"] is None

    err = otel.build_status(ok=False, description="boom")
    assert isinstance(err, _Status)
    assert captured["code"] == "ERROR"
    assert captured["desc"] == "boom"


def test_build_status_returns_none_when_api_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_import = importlib.import_module

    def fake_import(name: str, package: str | None = None) -> Any:
        if name == "opentelemetry.trace.status":
            raise ImportError("opentelemetry not installed")
        return real_import(name, package)

    monkeypatch.setattr(importlib, "import_module", fake_import)
    assert otel.build_status(ok=True) is None


# ---------------------------------------------------------------------------
# Engine paths driven through run_sync (so spans coexist with each branch)
# ---------------------------------------------------------------------------


class _FakeStagedDestination:
    """Minimal StagedDestination: accumulates via stage(), commits in finalize()."""

    def __init__(self) -> None:
        self.staged: list[dict[str, Any]] = []
        self.finalized = False

    def stage(self, records: list[dict[str, Any]], config: Any, sync_options: Any) -> None:
        self.staged.extend(records)

    def finalize(self, config: Any, sync_options: Any) -> SyncResult:
        self.finalized = True
        return SyncResult(success=len(self.staged))


def test_staged_destination_runs_through_engine_with_spans(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Staged destinations go through stage()/finalize(); the engine emits run +
    extract spans but no per-batch load span (load spans wrap destination.load only)."""
    tracer = _RecTracer()
    monkeypatch.setattr(sync_mod, "get_tracer", lambda: tracer)

    dest = _FakeStagedDestination()
    result = run_sync(
        _make_sync(batch_size=2),
        FakeSource([{"id": i} for i in range(3)]),
        dest,
        _make_profile(),
        tmp_path,
    )

    assert dest.finalized is True
    assert dest.staged == [{"id": 0}, {"id": 1}, {"id": 2}]
    assert result.success == 3
    assert tracer.by_name("drt.sync.run")
    assert tracer.one("drt.sync.extract").attributes["extract.rows_extracted"] == 3
    assert tracer.by_name("drt.sync.load") == []


def test_dry_run_with_compute_diff_paths(tmp_path: Path) -> None:
    """dry_run + compute_diff accumulates records and populates SyncResult.diff."""
    result = run_sync(
        _make_sync(batch_size=2),
        FakeSource([{"id": i} for i in range(3)]),
        FakeDestination(),
        _make_profile(),
        tmp_path,
        dry_run=True,
        compute_diff=True,
    )

    assert result.diff is not None
    assert result.success == 3


def _make_sync_with_lookups() -> SyncConfig:
    return SyncConfig.model_validate(
        {
            "name": "lk_sync",
            "model": "ref('t')",
            "destination": {
                "type": "clickhouse",
                "connection_string_env": "CH_DSN",
                "table": "t",
                "lookups": {
                    "cust_id": {"table": "dim", "match": {"k": "k"}, "select": "id"},
                },
            },
            "sync": {"batch_size": 10, "on_error": "skip"},
        }
    )


def test_lookup_ambiguity_warning_and_fully_filtered_batch(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """When the destination declares lookups, the engine emits ambiguity warnings
    and skips a batch that lookups filter down to empty (load is never called)."""
    monkeypatch.setattr(
        sync_mod,
        "detect_ambiguous_lookup_ordering",
        lambda lookups: ["ambiguous lookup ordering on 'k'"],
    )
    monkeypatch.setattr(sync_mod, "build_lookup_map", lambda dest, cfg: {})
    # Filter every row out -> record_batch becomes empty -> the engine `continue`s.
    monkeypatch.setattr(sync_mod, "apply_lookups", lambda batch, maps, on_err: ([], []))

    observer = MagicMock()
    dest = FakeDestination()
    run_sync(
        _make_sync_with_lookups(),
        FakeSource([{"id": 1}, {"id": 2}]),
        dest,
        _make_profile(),
        tmp_path,
        observer=observer,
    )

    warnings = [call.args[1] for call in observer.on_warning.call_args_list]
    assert any("ambiguous" in w for w in warnings)
    # Batch was filtered to empty before reaching the destination.
    assert dest.calls == []


# ---------------------------------------------------------------------------
# Real OpenTelemetry SDK end-to-end (skipped when opentelemetry isn't installed)
# ---------------------------------------------------------------------------


def test_real_otel_in_memory_export(tmp_path: Path) -> None:
    pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    # Point drt at this provider's tracer without touching global provider state.
    saved = (otel._STATE.initialized, otel._STATE.tracer, otel._STATE.meter)
    otel._STATE.initialized = True
    otel._STATE.tracer = provider.get_tracer("drt.observability")
    otel._STATE.meter = None
    try:
        run_sync(
            _make_sync(batch_size=2, on_error="skip"),
            FakeSource([{"id": i} for i in range(3)]),
            FakeDestination(fail_indices={1}),
            _make_profile(),
            tmp_path,
        )
    finally:
        otel._STATE.initialized, otel._STATE.tracer, otel._STATE.meter = saved

    by_name: dict[str, list[Any]] = {}
    for span in exporter.get_finished_spans():
        by_name.setdefault(span.name, []).append(span)

    run = by_name["drt.sync.run"][0]
    extract = by_name["drt.sync.extract"][0]
    loads = by_name["drt.sync.load"]

    assert run.status.status_code == trace.StatusCode.OK
    assert run.attributes["sync.name"] == "test_sync"
    assert extract.attributes["extract.rows_extracted"] == 3
    assert extract.parent.span_id == run.context.span_id
    # 3 rows, batch_size 2 -> batches [2, 1] -> 2 load spans, both children of run.
    assert len(loads) == 2
    assert {s.attributes["batch_index"] for s in loads} == {0, 1}
    for s in loads:
        assert s.parent.span_id == run.context.span_id


def test_real_otel_records_exception_on_failure(tmp_path: Path) -> None:
    pytest.importorskip("opentelemetry.sdk.trace")
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
        InMemorySpanExporter,
    )

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))

    class _Boom:
        def extract(self, query: str, config: Any) -> Any:
            yield {"id": 0}
            raise RuntimeError("kaboom")

        def test_connection(self, config: Any) -> bool:  # pragma: no cover
            return True

    saved = (otel._STATE.initialized, otel._STATE.tracer, otel._STATE.meter)
    otel._STATE.initialized = True
    otel._STATE.tracer = provider.get_tracer("drt.observability")
    otel._STATE.meter = None
    try:
        with pytest.raises(RuntimeError, match="kaboom"):
            run_sync(
                _make_sync(batch_size=10),
                _Boom(),
                FakeDestination(),
                _make_profile(),
                tmp_path,
            )
    finally:
        otel._STATE.initialized, otel._STATE.tracer, otel._STATE.meter = saved

    run = next(s for s in exporter.get_finished_spans() if s.name == "drt.sync.run")
    assert run.status.status_code == trace.StatusCode.ERROR
    # Recorded exactly once (auto-recording on __exit__ is disabled).
    exception_events = [e for e in run.events if e.name == "exception"]
    assert len(exception_events) == 1