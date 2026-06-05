"""OpenTelemetry provider helpers.

This module owns lazy initialization of the global tracer and meter
providers. When OTEL is not configured or unavailable, the public getters
return no-op objects and never raise.
"""

from __future__ import annotations

import importlib
import logging
import os
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal
from urllib.parse import urlparse

from drt.config.credentials import ObservabilityConfig, _load_profiles_yaml
from drt.config.parser import expand_env_vars

if TYPE_CHECKING:
    from opentelemetry.metrics import Meter
    from opentelemetry.trace import Tracer
else:
    Tracer = Any
    Meter = Any

logger = logging.getLogger("drt.observability.otel")

_DEFAULT_SERVICE_NAME = "drt"
_TRACER_SCOPE = "drt.observability"
_METER_SCOPE = "drt.observability"


@dataclass
class _ProviderState:
    initialized: bool = False
    tracer: Tracer | None = None
    meter: Meter | None = None
    warned: bool = False


_STATE = _ProviderState()
_LOCK = threading.Lock()


class _FallbackNoOpSpan:
    def __enter__(self) -> _FallbackNoOpSpan:
        return self

    def __exit__(
        self,
        exc_type: object,
        exc: object,
        tb: object,
    ) -> Literal[False]:
        return False

    def set_attribute(self, *_args: object, **_kwargs: object) -> None:
        return None

    def add_event(self, *_args: object, **_kwargs: object) -> None:
        return None

    def record_exception(self, *_args: object, **_kwargs: object) -> None:
        return None

    def set_status(self, *_args: object, **_kwargs: object) -> None:
        return None


class _FallbackNoOpTracer:
    def start_as_current_span(self, *_args: object, **_kwargs: object) -> _FallbackNoOpSpan:
        return _FallbackNoOpSpan()

    def start_span(self, *_args: object, **_kwargs: object) -> _FallbackNoOpSpan:
        return _FallbackNoOpSpan()


class _FallbackNoOpInstrument:
    def add(self, *_args: object, **_kwargs: object) -> None:
        return None

    def record(self, *_args: object, **_kwargs: object) -> None:
        return None


class _FallbackNoOpMeter:
    def create_counter(self, *_args: object, **_kwargs: object) -> object:
        return _FallbackNoOpInstrument()

    def create_up_down_counter(self, *_args: object, **_kwargs: object) -> object:
        return _FallbackNoOpInstrument()

    def create_histogram(self, *_args: object, **_kwargs: object) -> object:
        return _FallbackNoOpInstrument()

    def create_gauge(self, *_args: object, **_kwargs: object) -> object:
        return _FallbackNoOpInstrument()


def _load_observability_block(config_dir: Path | None = None) -> dict[str, Any] | None:
    try:
        data = _load_profiles_yaml(config_dir)
    except FileNotFoundError:
        return None

    observability = data.get("observability")
    if isinstance(observability, dict):
        return observability
    return None


def _parse_otlp_headers_env(raw: str | None) -> dict[str, str]:
    if not raw:
        return {}

    headers: dict[str, str] = {}
    for part in raw.split(","):
        piece = part.strip()
        if not piece:
            continue
        if "=" not in piece:
            raise ValueError("OTEL_EXPORTER_OTLP_HEADERS entries must be key=value pairs")
        key, value = piece.split("=", 1)
        key = key.strip()
        value = value.strip()
        if not key:
            raise ValueError("OTEL_EXPORTER_OTLP_HEADERS entries must be key=value pairs")
        headers[key] = value
    return headers


def _resolve_otel_settings(
    observability_raw: dict[str, Any] | None,
) -> tuple[str | None, str, dict[str, str]]:
    if observability_raw is not None:
        config = ObservabilityConfig.model_validate(observability_raw)
        return (
            config.otel.endpoint,
            config.otel.service_name or _DEFAULT_SERVICE_NAME,
            dict(config.otel.headers),
        )

    endpoint = os.environ.get("OTEL_EXPORTER_OTLP_ENDPOINT")
    headers = _parse_otlp_headers_env(os.environ.get("OTEL_EXPORTER_OTLP_HEADERS"))
    return endpoint, _DEFAULT_SERVICE_NAME, headers


def _expand_headers(headers: dict[str, str]) -> dict[str, str]:
    expanded = expand_env_vars(headers)
    if not isinstance(expanded, dict):
        raise ValueError("observability.otel.headers must be a mapping")

    result: dict[str, str] = {}
    for key, value in expanded.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise ValueError("observability.otel.headers must contain string keys and values")
        result[key] = value
    return result


def _normalize_endpoint(endpoint: str) -> tuple[str, bool]:
    parsed = urlparse(endpoint)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        normalized = parsed.netloc + parsed.path
        return normalized, parsed.scheme == "http"
    return endpoint, False


def _load_noop_tracer_and_meter() -> tuple[Tracer, Meter]:
    try:
        trace_api = importlib.import_module("opentelemetry.trace")
        metrics_api = importlib.import_module("opentelemetry.metrics")
    except ImportError:
        return _FallbackNoOpTracer(), _FallbackNoOpMeter()

    return trace_api.get_tracer(_TRACER_SCOPE), metrics_api.get_meter(_METER_SCOPE)


def _warn_once(message: str) -> None:
    if not _STATE.warned:
        logger.warning(message)
        _STATE.warned = True


def _initialize_if_needed() -> None:
    if _STATE.initialized:
        return

    with _LOCK:
        if _STATE.initialized:
            return

        observability_raw = _load_observability_block()
        endpoint, service_name, headers = _resolve_otel_settings(observability_raw)
        if not endpoint:
            _STATE.tracer, _STATE.meter = _load_noop_tracer_and_meter()
            _STATE.initialized = True
            return

        try:
            trace_api = importlib.import_module("opentelemetry.trace")
            metrics_api = importlib.import_module("opentelemetry.metrics")
            resources_mod = importlib.import_module("opentelemetry.sdk.resources")
            trace_sdk = importlib.import_module("opentelemetry.sdk.trace")
            trace_export_mod = importlib.import_module("opentelemetry.sdk.trace.export")
            trace_exporter_mod = importlib.import_module(
                "opentelemetry.exporter.otlp.proto.grpc.trace_exporter"
            )
            metrics_sdk = importlib.import_module("opentelemetry.sdk.metrics")
            metric_export_mod = importlib.import_module("opentelemetry.sdk.metrics.export")
            metric_exporter_mod = importlib.import_module(
                "opentelemetry.exporter.otlp.proto.grpc.metric_exporter"
            )

            headers = _expand_headers(headers)
            endpoint, insecure = _normalize_endpoint(endpoint)
            resource = resources_mod.Resource.create({"service.name": service_name})

            tracer_exporter = trace_exporter_mod.OTLPSpanExporter(
                endpoint=endpoint,
                headers=headers or None,
                insecure=insecure,
            )
            trace_provider = trace_sdk.TracerProvider(resource=resource)
            trace_provider.add_span_processor(
                trace_export_mod.SimpleSpanProcessor(tracer_exporter)
            )

            metric_exporter = metric_exporter_mod.OTLPMetricExporter(
                endpoint=endpoint,
                headers=headers or None,
                insecure=insecure,
            )
            metric_reader = metric_export_mod.PeriodicExportingMetricReader(metric_exporter)
            meter_provider = metrics_sdk.MeterProvider(
                resource=resource,
                metric_readers=[metric_reader],
            )

            trace_api.set_tracer_provider(trace_provider)
            metrics_api.set_meter_provider(meter_provider)

            _STATE.tracer = trace_api.get_tracer(_TRACER_SCOPE)
            _STATE.meter = metrics_api.get_meter(_METER_SCOPE)
        except ImportError:
            _warn_once("OTEL extras are unavailable; falling back to no-op tracing.")
            _STATE.tracer, _STATE.meter = _load_noop_tracer_and_meter()
        except Exception:
            _warn_once("Failed to initialize OTEL exporters; falling back to no-op tracing.")
            _STATE.tracer, _STATE.meter = _load_noop_tracer_and_meter()

        _STATE.initialized = True


def get_tracer() -> Tracer:
    """Return the shared tracer, initializing OTEL lazily on first use."""

    _initialize_if_needed()
    if _STATE.tracer is not None:
        return _STATE.tracer
    tracer, meter = _load_noop_tracer_and_meter()
    _STATE.tracer = tracer
    if _STATE.meter is None:
        _STATE.meter = meter
    return tracer


def get_meter() -> Meter:
    """Return the shared meter, initializing OTEL lazily on first use."""

    _initialize_if_needed()
    if _STATE.meter is not None:
        return _STATE.meter
    tracer, meter = _load_noop_tracer_and_meter()
    if _STATE.tracer is None:
        _STATE.tracer = tracer
    _STATE.meter = meter
    return meter