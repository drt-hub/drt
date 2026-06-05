"""Tests for lazy OpenTelemetry provider setup."""

from __future__ import annotations

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from drt.observability import otel


@dataclass
class _FakeResource:
    attributes: dict[str, Any]


class _FakeResourceModule:
    class Resource:
        @staticmethod
        def create(attributes: dict[str, Any]) -> _FakeResource:
            return _FakeResource(attributes=attributes)


class _FakeSpanProcessor:
    def __init__(self, exporter: Any) -> None:
        self.exporter = exporter


class _FakeTracerProvider:
    def __init__(self, *, resource: _FakeResource) -> None:
        self.resource = resource
        self.span_processors: list[Any] = []

    def add_span_processor(self, processor: Any) -> None:
        self.span_processors.append(processor)

    def get_tracer(self, scope: str) -> Any:
        return SimpleNamespace(scope=scope, provider=self)


class _FakeOTLPSpanExporter:
    def __init__(self, *, endpoint: str, headers: dict[str, str] | None, insecure: bool) -> None:
        self.endpoint = endpoint
        self.headers = headers
        self.insecure = insecure


class _FakeMetricReader:
    def __init__(self, exporter: Any) -> None:
        self.exporter = exporter


class _FakeMeterProvider:
    def __init__(self, *, resource: _FakeResource, metric_readers: list[Any]) -> None:
        self.resource = resource
        self.metric_readers = metric_readers

    def get_meter(self, scope: str) -> Any:
        return SimpleNamespace(scope=scope, provider=self)


class _FakeOTLPMetricExporter:
    def __init__(self, *, endpoint: str, headers: dict[str, str] | None, insecure: bool) -> None:
        self.endpoint = endpoint
        self.headers = headers
        self.insecure = insecure


class _FakeTraceApi:
    def __init__(self) -> None:
        self.providers: list[Any] = []

    def set_tracer_provider(self, provider: Any) -> None:
        self.providers.append(provider)

    def get_tracer(self, scope: str) -> Any:
        if self.providers:
            return self.providers[-1].get_tracer(scope)
        return SimpleNamespace(scope=scope)


class _FakeMetricsApi:
    def __init__(self) -> None:
        self.providers: list[Any] = []

    def set_meter_provider(self, provider: Any) -> None:
        self.providers.append(provider)

    def get_meter(self, scope: str) -> Any:
        if self.providers:
            return self.providers[-1].get_meter(scope)
        return SimpleNamespace(scope=scope)


@pytest.fixture(autouse=True)
def _reset_state(monkeypatch: pytest.MonkeyPatch) -> None:
    otel._STATE = otel._ProviderState()
    monkeypatch.setattr(otel, "_load_observability_block", lambda _config_dir=None: None)


def test_get_tracer_returns_noop_when_otel_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        otel,
        "_load_observability_block",
        lambda _config_dir=None: {"otel": {"endpoint": "http://localhost:4317"}},
    )

    class FakeNoOpTracer:
        pass

    class FakeNoOpMeter:
        pass

    class FakeNoOpTracerProvider:
        def get_tracer(self, _scope: str) -> FakeNoOpTracer:
            return FakeNoOpTracer()

    class FakeNoOpMeterProvider:
        def get_meter(self, _scope: str) -> FakeNoOpMeter:
            return FakeNoOpMeter()

    def fail_import(name: str) -> Any:
        if name.startswith("opentelemetry.sdk") or name.startswith("opentelemetry.exporter"):
            raise ImportError(name)
        return SimpleNamespace(
            get_tracer=lambda _scope: FakeNoOpTracer(),
            get_meter=lambda _scope: FakeNoOpMeter(),
            NoOpTracerProvider=FakeNoOpTracerProvider,
            NoOpMeterProvider=FakeNoOpMeterProvider,
        )

    monkeypatch.setattr(otel.importlib, "import_module", fail_import)

    tracer = otel.get_tracer()

    assert tracer.__class__.__name__ in {"FakeNoOpTracer", "_FallbackNoOpTracer", "NoOpTracer"}


def test_get_tracer_uses_noop_when_otel_block_missing() -> None:
    tracer = otel.get_tracer()
    meter = otel.get_meter()

    assert tracer is otel.get_tracer()
    assert meter is otel.get_meter()


def test_get_tracer_initializes_real_provider_and_expands_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_TOKEN", "secret")
    monkeypatch.setattr(
        otel,
        "_load_observability_block",
        lambda _config_dir=None: {
            "otel": {
                "endpoint": "http://localhost:4317",
                "service_name": "custom-service",
                "headers": {"Authorization": "Bearer ${OTEL_TOKEN}"},
            }
        },
    )

    trace_api = _FakeTraceApi()
    metrics_api = _FakeMetricsApi()
    fake_resource_mod = _FakeResourceModule()

    def fake_import(name: str) -> Any:
        mapping = {
            "opentelemetry.trace": trace_api,
            "opentelemetry.metrics": metrics_api,
            "opentelemetry.sdk.resources": fake_resource_mod,
            "opentelemetry.sdk.trace": SimpleNamespace(
                TracerProvider=_FakeTracerProvider,
            ),
            "opentelemetry.sdk.trace.export": SimpleNamespace(
                SimpleSpanProcessor=_FakeSpanProcessor,
            ),
            "opentelemetry.exporter.otlp.proto.grpc.trace_exporter": SimpleNamespace(
                OTLPSpanExporter=_FakeOTLPSpanExporter,
            ),
            "opentelemetry.sdk.metrics": SimpleNamespace(MeterProvider=_FakeMeterProvider),
            "opentelemetry.sdk.metrics.export": SimpleNamespace(
                PeriodicExportingMetricReader=_FakeMetricReader,
            ),
            "opentelemetry.exporter.otlp.proto.grpc.metric_exporter": SimpleNamespace(
                OTLPMetricExporter=_FakeOTLPMetricExporter,
            ),
        }
        return mapping[name]

    monkeypatch.setattr(otel.importlib, "import_module", fake_import)

    tracer = otel.get_tracer()
    meter = otel.get_meter()

    assert tracer.scope == "drt.observability"
    assert meter.scope == "drt.observability"
    assert isinstance(tracer.provider, _FakeTracerProvider)
    assert isinstance(meter.provider, _FakeMeterProvider)
    assert tracer.provider.resource.attributes == {"service.name": "custom-service"}

    tracer_exporter = tracer.provider.span_processors[0].exporter
    metric_exporter = meter.provider.metric_readers[0].exporter
    assert tracer_exporter.endpoint == "localhost:4317"
    assert tracer_exporter.insecure is True
    assert tracer_exporter.headers == {"Authorization": "Bearer secret"}
    assert metric_exporter.endpoint == "localhost:4317"
    assert metric_exporter.insecure is True
    assert metric_exporter.headers == {"Authorization": "Bearer secret"}


def test_get_tracer_uses_env_fallback_when_observability_block_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", "Authorization=Bearer secret")

    trace_api = _FakeTraceApi()
    metrics_api = _FakeMetricsApi()
    fake_resource_mod = _FakeResourceModule()

    def fake_import(name: str) -> Any:
        mapping = {
            "opentelemetry.trace": trace_api,
            "opentelemetry.metrics": metrics_api,
            "opentelemetry.sdk.resources": fake_resource_mod,
            "opentelemetry.sdk.trace": SimpleNamespace(
                TracerProvider=_FakeTracerProvider,
            ),
            "opentelemetry.sdk.trace.export": SimpleNamespace(
                SimpleSpanProcessor=_FakeSpanProcessor,
            ),
            "opentelemetry.exporter.otlp.proto.grpc.trace_exporter": SimpleNamespace(
                OTLPSpanExporter=_FakeOTLPSpanExporter,
            ),
            "opentelemetry.sdk.metrics": SimpleNamespace(MeterProvider=_FakeMeterProvider),
            "opentelemetry.sdk.metrics.export": SimpleNamespace(
                PeriodicExportingMetricReader=_FakeMetricReader,
            ),
            "opentelemetry.exporter.otlp.proto.grpc.metric_exporter": SimpleNamespace(
                OTLPMetricExporter=_FakeOTLPMetricExporter,
            ),
        }
        return mapping[name]

    monkeypatch.setattr(otel.importlib, "import_module", fake_import)

    tracer = otel.get_tracer()

    assert isinstance(tracer.provider, _FakeTracerProvider)
    assert tracer.provider.resource.attributes == {"service.name": "drt"}
    exporter = tracer.provider.span_processors[0].exporter
    assert exporter.endpoint == "localhost:4317"
    assert exporter.headers == {"Authorization": "Bearer secret"}


def test_get_tracer_falls_back_when_exporter_init_fails(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(
        otel,
        "_load_observability_block",
        lambda _config_dir=None: {"otel": {"endpoint": "http://localhost:4317"}},
    )

    class BrokenSpanExporter:
        def __init__(self, **_kwargs: object) -> None:
            raise RuntimeError("boom")

    def fake_import(name: str) -> Any:
        mapping = {
            "opentelemetry.trace": SimpleNamespace(
                set_tracer_provider=lambda _provider: None,
                get_tracer=lambda _scope: SimpleNamespace(scope=_scope),
            ),
            "opentelemetry.metrics": SimpleNamespace(
                set_meter_provider=lambda _provider: None,
                get_meter=lambda _scope: SimpleNamespace(scope=_scope),
            ),
            "opentelemetry.sdk.resources": _FakeResourceModule(),
            "opentelemetry.sdk.trace": SimpleNamespace(
                TracerProvider=_FakeTracerProvider,
            ),
            "opentelemetry.sdk.trace.export": SimpleNamespace(
                SimpleSpanProcessor=_FakeSpanProcessor,
            ),
            "opentelemetry.exporter.otlp.proto.grpc.trace_exporter": SimpleNamespace(
                OTLPSpanExporter=BrokenSpanExporter,
            ),
            "opentelemetry.sdk.metrics": SimpleNamespace(MeterProvider=_FakeMeterProvider),
            "opentelemetry.sdk.metrics.export": SimpleNamespace(
                PeriodicExportingMetricReader=_FakeMetricReader,
            ),
            "opentelemetry.exporter.otlp.proto.grpc.metric_exporter": SimpleNamespace(
                OTLPMetricExporter=_FakeOTLPMetricExporter,
            ),
        }
        return mapping[name]

    monkeypatch.setattr(
        otel,
        "_load_noop_tracer_and_meter",
        lambda: (otel._FallbackNoOpTracer(), otel._FallbackNoOpMeter()),
    )
    monkeypatch.setattr(otel.importlib, "import_module", fake_import)

    with caplog.at_level("WARNING"):
        tracer = otel.get_tracer()

    with tracer.start_as_current_span("test-span"):
        pass

    assert isinstance(tracer, otel._FallbackNoOpTracer)
    assert any("Failed to initialize OTEL exporters" in record.message for record in caplog.records)


def test_get_meter_reuses_initialized_state(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        otel,
        "_load_observability_block",
        lambda _config_dir=None: {"otel": {"endpoint": "http://localhost:4317"}},
    )

    tracer = otel.get_tracer()
    meter = otel.get_meter()

    assert tracer is otel.get_tracer()
    assert meter is otel.get_meter()