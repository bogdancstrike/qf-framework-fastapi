"""In-memory span capture for tests.

The framework tracer is a no-op until `init_tracing`. `capture_spans()` swaps in a real SDK
tracer exporting to memory, so a test can assert span names, attributes and parent/child
links exactly as Jaeger would receive them. Needs ``opentelemetry-sdk`` (the ``tracing`` or
``dev`` extra).

Example (a pytest fixture)::

    @pytest.fixture
    def spans():
        with capture_spans() as captured:
            yield captured

    def test_order_span(spans):
        ...
        assert spans.one("orders.save").attributes["order.id"] == 42
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from opentelemetry.sdk.trace import ReadableSpan, TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from ..tracing import tracing as _tracing


class CapturedSpans:
    """The finished spans of a `capture_spans` block, with lookup helpers."""

    def __init__(self, exporter: InMemorySpanExporter) -> None:
        self.exporter = exporter

    @property
    def all(self) -> list[ReadableSpan]:
        """Every finished span, in end order."""
        return list(self.exporter.get_finished_spans())

    def named(self, name: str) -> list[ReadableSpan]:
        """The finished spans called `name`."""
        return [s for s in self.all if s.name == name]

    def one(self, name: str) -> ReadableSpan:
        """The only finished span called `name` (`AssertionError` otherwise)."""
        found = self.named(name)
        assert len(found) == 1, f"expected one span {name!r}, got {[s.name for s in self.all]}"
        return found[0]

    def children_of(self, parent: ReadableSpan) -> list[ReadableSpan]:
        """The finished direct children of `parent`."""
        return [
            s
            for s in self.all
            if s.parent is not None and s.parent.span_id == parent.context.span_id
        ]

    def text(self) -> str:
        """Every span name and attribute set, for "never contains X" assertions."""
        return "\n".join(f"{s.name} {dict(s.attributes or {})}" for s in self.all)

    def clear(self) -> None:
        """Forget the spans captured so far."""
        self.exporter.clear()


@contextmanager
def capture_spans() -> Iterator[CapturedSpans]:
    """Route the framework tracer to memory for the duration of the block."""
    exporter = InMemorySpanExporter()
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    previous = _tracing._tracer
    _tracing._tracer = provider.get_tracer("tests")
    try:
        yield CapturedSpans(exporter)
    finally:
        _tracing._tracer = previous
        provider.shutdown()
