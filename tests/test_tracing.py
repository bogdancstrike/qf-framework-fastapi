from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from framework.tracing import (
    NoOpSpan,
    NoOpTracer,
    inject_trace_headers,
    extract_context_from_headers,
)
from framework.tracing import tracing
from framework.etl.processing import Processor
from framework.observability import Metrics
from conftest import Producer, record


def test_noop_methods():
    span = NoOpSpan()
    span.set_attribute("x", 1)
    assert not span.is_recording()
    with NoOpTracer().start_as_current_span("noop") as span:
        assert not span.is_recording()


async def test_kafka_trace_continues_parent_and_injects_output(settings, spec, monkeypatch):
    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(tracing, "_tracer", provider.get_tracer("test"))
    producer = Producer()
    traceparent = b"00-12345678901234567890123456789012-1234567890123456-01"
    await Processor(settings, producer, Metrics()).handle(
        spec, [record(headers=[("traceparent", traceparent)])], lambda: True
    )
    spans = exporter.get_finished_spans()
    assert len(spans) == 1
    assert spans[0].parent.span_id == int("1234567890123456", 16)
    outgoing = dict(producer.sent[0][2]["headers"])["traceparent"].decode()
    assert outgoing.split("-")[1] == "12345678901234567890123456789012"
    assert outgoing != traceparent.decode()
    provider.shutdown()


def test_trace_headers_replace_existing_parent(monkeypatch):
    monkeypatch.setattr(tracing, "inject", lambda carrier: carrier.update(traceparent="new"))
    headers = inject_trace_headers([("traceparent", b"old"), ("custom", b"value")])
    assert dict(headers) == {"custom": b"value", "traceparent": b"new"}
    assert extract_context_from_headers(None) is not None


async def test_bulk_keeps_each_message_trace_id(settings, spec, monkeypatch):
    import json
    from dataclasses import replace

    provider = TracerProvider()
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    monkeypatch.setattr(tracing, "_tracer", provider.get_tracer("test"))
    producer = Producer()
    trace_ids = ["12345678901234567890123456789012", "22345678901234567890123456789012"]
    records = [
        record(
            i,
            json.dumps({"id": str(i)}).encode(),
            headers=[("traceparent", f"00-{trace_id}-1234567890123456-01".encode())],
        )
        for i, trace_id in enumerate(trace_ids)
    ]
    await Processor(settings, producer, Metrics()).handle(
        replace(spec, bulk_mode=True), records, lambda: True
    )
    actual = [
        dict(item[2]["headers"])["traceparent"].decode().split("-")[1] for item in producer.sent
    ]
    assert actual == trace_ids
    provider.shutdown()


async def test_bulk_dlq_keeps_each_message_trace_id(settings, spec, monkeypatch):
    from dataclasses import replace

    async def broken(*args):
        raise ValueError("invalid")

    provider = TracerProvider()
    monkeypatch.setattr(tracing, "_tracer", provider.get_tracer("test"))
    producer = Producer()
    trace_ids = ["12345678901234567890123456789012", "22345678901234567890123456789012"]
    records = [
        record(i, headers=[("traceparent", f"00-{tid}-1234567890123456-01".encode())])
        for i, tid in enumerate(trace_ids)
    ]
    await Processor(settings, producer, Metrics()).handle(
        replace(spec, fn=broken, bulk_mode=True), records, lambda: True
    )
    actual = [
        dict(item[2]["headers"])["traceparent"].decode().split("-")[1] for item in producer.sent
    ]
    assert actual == trace_ids
    provider.shutdown()


def _closed_port() -> int:
    import socket

    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_resource_attributes_join_the_service_name():
    provider = tracing.init_tracing(
        "svc",
        f"127.0.0.1:{_closed_port()}",
        insecure=True,
        enabled=True,
        resource_attributes={"deployment.environment": "qsint", "service.name": "ignored"},
    )
    try:
        attributes = provider.resource.attributes
        assert attributes["service.name"] == "svc"
        assert attributes["deployment.environment"] == "qsint"
    finally:
        tracing.shutdown_tracing()


def test_shutdown_is_bounded_when_the_collector_is_unreachable():
    import time

    tracing.init_tracing(
        "svc", f"127.0.0.1:{_closed_port()}", insecure=True, enabled=True, export_timeout_s=1.0
    )
    for number in range(100):
        with tracing.get_tracer().start_as_current_span("unexported", attributes={"n": number}):
            pass
    started = time.monotonic()
    tracing.shutdown_tracing()
    assert time.monotonic() - started < 3
    assert not tracing.get_tracer().start_span("after").is_recording()
