"""Explicit provider ownership; no SDK/exporter required when disabled."""

import os

from opentelemetry import trace
from opentelemetry.propagate import inject, extract

_provider = None
_tracer = trace.NoOpTracerProvider().get_tracer("framework")


def init_tracing(service_name, otlp_endpoint=None, insecure=False, *, enabled=None, app=None):
    global _provider, _tracer
    if enabled is None:
        enabled = _tracing_enabled()
    if not enabled:
        return None
    if _provider is None:
        from opentelemetry.sdk.resources import Resource
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import BatchSpanProcessor
        from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

        _provider = TracerProvider(resource=Resource.create({"service.name": service_name}))
        _provider.add_span_processor(
            BatchSpanProcessor(OTLPSpanExporter(endpoint=otlp_endpoint, insecure=insecure))
        )
        trace.set_tracer_provider(_provider)
        _tracer = _provider.get_tracer("framework")
    if app is not None:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor

        FastAPIInstrumentor.instrument_app(
            app, tracer_provider=_provider, excluded_urls="health/live,health/ready,metrics"
        )
    return _provider


def get_tracer():
    return _tracer


def shutdown_tracing():
    # Provider is process-owned: flush here; process exit owns final shutdown.
    if _provider:
        _provider.force_flush(timeout_millis=5000)


def inject_trace_headers(existing=None):
    carrier = {}
    inject(carrier)
    items = list((existing or {}).items()) if isinstance(existing, dict) else list(existing or [])
    items = [(k, v) for k, v in items if k not in carrier]
    return items + [(k, v.encode()) for k, v in carrier.items()]


def extract_context_from_headers(headers):
    items = headers.items() if isinstance(headers, dict) else headers or []
    carrier = {
        k: v.decode("utf-8", errors="replace") if isinstance(v, bytes) else str(v)
        for k, v in items
        if v is not None
    }
    return extract(carrier)


def _tracing_enabled():
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
