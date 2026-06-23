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


class _FakeBatchSpanProcessor:
    def __init__(self, exporter: Any) -> None:
        self.exporter = exporter


class _FakeTracerProvider:
    def __init__(self, *, resource: _FakeResource) -> None:
        self.resource = resource
        self.span_processors: list[Any] = []
        self.shutdown_calls = 0

    def add_span_processor(self, processor: Any) -> None:
        self.span_processors.append(processor)

    def get_tracer(self, scope: str) -> Any:
        return SimpleNamespace(scope=scope, provider=self)

    def shutdown(self) -> None:
        self.shutdown_calls += 1


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
        self.shutdown_calls = 0

    def get_meter(self, scope: str) -> Any:
        return SimpleNamespace(scope=scope, provider=self)

    def shutdown(self) -> None:
        self.shutdown_calls += 1


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

    def fail_import(name: str) -> Any:
        # All opentelemetry imports fail — simulates package not installed.
        # This forces _load_noop_tracer_and_meter() to return _FallbackNoOpTracer.
        raise ImportError(name)

    monkeypatch.setattr(otel.importlib, "import_module", fail_import)

    tracer = otel.get_tracer()

    assert isinstance(tracer, otel._FallbackNoOpTracer)


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
                BatchSpanProcessor=_FakeBatchSpanProcessor,
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
                BatchSpanProcessor=_FakeBatchSpanProcessor,
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
                BatchSpanProcessor=_FakeBatchSpanProcessor,
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


# ---------------------------------------------------------------------------
# New tests to cover previously uncovered defensive paths
# ---------------------------------------------------------------------------


def test_parse_otlp_headers_env_raises_on_missing_equals() -> None:
    """_parse_otlp_headers_env raises ValueError when an entry has no '='."""
    with pytest.raises(ValueError, match="key=value"):
        otel._parse_otlp_headers_env("Authorization")


def test_parse_otlp_headers_env_raises_on_empty_key() -> None:
    """_parse_otlp_headers_env raises ValueError when the key is empty."""
    with pytest.raises(ValueError, match="key=value"):
        otel._parse_otlp_headers_env("=Bearer secret")


def test_expand_headers_raises_on_non_dict() -> None:
    """_expand_headers raises ValueError when expand_env_vars returns a non-dict."""
    import drt.observability.otel as otel_mod

    original = otel_mod.expand_env_vars

    def fake_expand(_headers: object) -> object:
        return "not-a-dict"

    otel_mod.expand_env_vars = fake_expand  # type: ignore[assignment]
    try:
        with pytest.raises(ValueError, match="mapping"):
            otel._expand_headers({"key": "value"})
    finally:
        otel_mod.expand_env_vars = original


def test_expand_headers_raises_on_non_string_value() -> None:
    """_expand_headers raises ValueError when a value is not a string."""
    import drt.observability.otel as otel_mod

    original = otel_mod.expand_env_vars

    def fake_expand(_headers: object) -> object:
        return {"key": 123}

    otel_mod.expand_env_vars = fake_expand  # type: ignore[assignment]
    try:
        with pytest.raises(ValueError, match="string keys and values"):
            otel._expand_headers({"key": "value"})
    finally:
        otel_mod.expand_env_vars = original


def test_fallback_noop_instrument_add_and_record() -> None:
    """_FallbackNoOpInstrument.add and .record do not raise."""
    instrument = otel._FallbackNoOpInstrument()
    instrument.add(1, {"attr": "val"})
    instrument.record(1.5, {"attr": "val"})


def test_fallback_noop_meter_creates_instruments() -> None:
    """_FallbackNoOpMeter creates _FallbackNoOpInstrument instances for all methods."""
    meter = otel._FallbackNoOpMeter()
    assert isinstance(meter.create_counter("c"), otel._FallbackNoOpInstrument)
    assert isinstance(meter.create_up_down_counter("ud"), otel._FallbackNoOpInstrument)
    assert isinstance(meter.create_histogram("h"), otel._FallbackNoOpInstrument)
    assert isinstance(meter.create_gauge("g"), otel._FallbackNoOpInstrument)

def _build_fake_import(
    trace_api: Any,
    metrics_api: Any,
    fake_resource_mod: Any,
) -> Any:
    """Shared fake importlib.import_module for the BatchSpanProcessor tests (#658)."""

    def fake_import(name: str) -> Any:
        mapping = {
            "opentelemetry.trace": trace_api,
            "opentelemetry.metrics": metrics_api,
            "opentelemetry.sdk.resources": fake_resource_mod,
            "opentelemetry.sdk.trace": SimpleNamespace(TracerProvider=_FakeTracerProvider),
            "opentelemetry.sdk.trace.export": SimpleNamespace(
                SimpleSpanProcessor=_FakeSpanProcessor,
                BatchSpanProcessor=_FakeBatchSpanProcessor,
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

    return fake_import


def test_default_uses_batch_span_processor(monkeypatch: pytest.MonkeyPatch) -> None:
    """With an endpoint configured and no span_processor override, the tracer uses
    BatchSpanProcessor (off-critical-path export), not the blocking SimpleSpanProcessor."""
    monkeypatch.setattr(
        otel,
        "_load_observability_block",
        lambda _config_dir=None: {"otel": {"endpoint": "localhost:4317"}},
    )
    monkeypatch.setattr(
        otel.importlib,
        "import_module",
        _build_fake_import(_FakeTraceApi(), _FakeMetricsApi(), _FakeResourceModule()),
    )

    tracer = otel.get_tracer()

    processor = tracer.provider.span_processors[0]
    assert isinstance(processor, _FakeBatchSpanProcessor)
    assert not isinstance(processor, _FakeSpanProcessor)
    assert processor.exporter.endpoint == "localhost:4317"


def test_simple_span_processor_when_configured(monkeypatch: pytest.MonkeyPatch) -> None:
    """span_processor: simple restores synchronous SimpleSpanProcessor for local debugging."""
    monkeypatch.setattr(
        otel,
        "_load_observability_block",
        lambda _config_dir=None: {
            "otel": {"endpoint": "localhost:4317", "span_processor": "simple"}
        },
    )
    monkeypatch.setattr(
        otel.importlib,
        "import_module",
        _build_fake_import(_FakeTraceApi(), _FakeMetricsApi(), _FakeResourceModule()),
    )

    tracer = otel.get_tracer()

    processor = tracer.provider.span_processors[0]
    assert isinstance(processor, _FakeSpanProcessor)
    assert not isinstance(processor, _FakeBatchSpanProcessor)


def test_shutdown_telemetry_flushes_providers_and_is_idempotent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """shutdown_telemetry() shuts down both providers once; a second call is a no-op."""
    monkeypatch.setattr(
        otel,
        "_load_observability_block",
        lambda _config_dir=None: {"otel": {"endpoint": "localhost:4317"}},
    )
    monkeypatch.setattr(
        otel.importlib,
        "import_module",
        _build_fake_import(_FakeTraceApi(), _FakeMetricsApi(), _FakeResourceModule()),
    )

    otel.get_tracer()
    trace_provider = otel._STATE.trace_provider
    meter_provider = otel._STATE.meter_provider
    assert trace_provider is not None and meter_provider is not None

    otel.shutdown_telemetry()
    assert trace_provider.shutdown_calls == 1
    assert meter_provider.shutdown_calls == 1
    assert otel._STATE.trace_provider is None
    assert otel._STATE.meter_provider is None

    # Second call must not shut down again (idempotent).
    otel.shutdown_telemetry()
    assert trace_provider.shutdown_calls == 1
    assert meter_provider.shutdown_calls == 1


def test_shutdown_telemetry_noop_when_otel_inactive(monkeypatch: pytest.MonkeyPatch) -> None:
    """shutdown_telemetry() is a safe no-op when OTel was never activated (no endpoint)."""
    monkeypatch.setattr(otel, "_load_observability_block", lambda _config_dir=None: None)
    monkeypatch.delenv("OTEL_EXPORTER_OTLP_ENDPOINT", raising=False)

    otel.get_tracer()  # no-op path
    assert otel._STATE.trace_provider is None

    otel.shutdown_telemetry()  # must not raise
