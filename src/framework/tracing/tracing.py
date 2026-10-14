"""Explicit provider ownership; no SDK/exporter required when disabled.

`init_tracing` installs one process provider exporting over OTLP gRPC. Every export, its
retries included, is bounded by `export_timeout_s`, so `shutdown_tracing` returns within
about that time even when the collector is unreachable.
"""

import os
from collections.abc import Iterable, Mapping
from typing import Any

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.propagate import inject, extract

#: Upper bound of one export, retries included (the OTLP exporter's default is 10 s).
DEFAULT_EXPORT_TIMEOUT_S = 3.0

_provider: Any = None
_tracer = trace.NoOpTracerProvider().get_tracer("framework")
_NOOP_TRACER = _tracer


def init_tracing(
    service_name: str,
    otlp_endpoint: str | None = None,
    insecure: bool = False,
    *,
    enabled: bool | None = None,
    app: Any = None,
    resource_attributes: Mapping[str, str] | None = None,
    export_timeout_s: float = DEFAULT_EXPORT_TIMEOUT_S,
) -> Any:
    """Install the process tracer provider once (``None`` when tracing is disabled).

    Args:
        service_name: The resource's ``service.name``.
        otlp_endpoint: The OTLP gRPC collector.
        insecure: Plain-text gRPC.
        enabled: Whether to trace (default: ``QSINT_ENABLE_TRACING`` / ``ENABLE_TRACING``).
        app: A FastAPI application to instrument.
        resource_attributes: Extra resource attributes (e.g. ``deployment.environment``);
            ``service.name`` always comes from `service_name`.
        export_timeout_s: Upper bound of one export, retries included.
    """
    global _provider, _tracer
    if enabled is None:
        enabled = _tracing_enabled()
    if not enabled:
        return None
    if _provider is None:
        _provider = _build_provider(
            service_name, otlp_endpoint, insecure, resource_attributes, export_timeout_s
        )
        trace.set_tracer_provider(_provider)
        _tracer = _provider.get_tracer("framework")
    if app is not None:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(
            app, tracer_provider=_provider, excluded_urls="health/live,health/ready,metrics"
        )
    return _provider


def _build_provider(
    service_name: str,
    otlp_endpoint: str | None,
    insecure: bool,
    resource_attributes: Mapping[str, str] | None,
    export_timeout_s: float,
) -> Any:
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

    resource = Resource.create({**(resource_attributes or {}), "service.name": service_name})
    exporter = OTLPSpanExporter(endpoint=otlp_endpoint, insecure=insecure, timeout=export_timeout_s)
    provider = TracerProvider(resource=resource)
    provider.add_span_processor(
        BatchSpanProcessor(exporter, export_timeout_millis=export_timeout_s * 1000)
    )
    return provider


def get_tracer() -> trace.Tracer:
    return _tracer


def shutdown_tracing() -> None:
    """Export what is queued and close the provider (bounded by its export timeout).

    The framework tracer is a no-op afterwards; a later `init_tracing` builds a new provider.
    """
    global _provider, _tracer
    if _provider:
        provider, _provider, _tracer = _provider, None, _NOOP_TRACER
        provider.shutdown()


def inject_trace_headers(
    existing: Mapping[str, Any] | Iterable[tuple[str, Any]] | None = None,
) -> list[tuple[str, Any]]:
    carrier: dict[str, str] = {}
    inject(carrier)
    items = list((existing or {}).items()) if isinstance(existing, dict) else list(existing or [])
    items = [(k, v) for k, v in items if k not in carrier]
    return items + [(k, v.encode()) for k, v in carrier.items()]


def extract_context_from_headers(
    headers: Mapping[str, Any] | Iterable[tuple[str, Any]] | None,
) -> Context:
    items = headers.items() if isinstance(headers, dict) else headers or []
    carrier = {
        k: v.decode("utf-8", errors="replace") if isinstance(v, bytes) else str(v)
        for k, v in items
        if v is not None
    }
    return extract(carrier)


def _tracing_enabled() -> bool:
    return os.getenv("QSINT_ENABLE_TRACING", os.getenv("ENABLE_TRACING", "false")).lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


class NoOpSpan(trace.NonRecordingSpan):
    def __init__(self):
        super().__init__(trace.INVALID_SPAN_CONTEXT)


NoOpTracer = trace.NoOpTracer


class _TracerProxy:
    def __getattr__(self, name):
        return getattr(_tracer, name)


tracer = _TracerProxy()
