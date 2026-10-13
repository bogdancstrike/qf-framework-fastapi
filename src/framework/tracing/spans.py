"""Span helpers on the framework tracer: `traced_span`, `@traced`, header propagation.

The tracer is the process tracer `init_tracing` installs (a no-op until then), read at call
time, so the helpers work before and after tracing is configured. Attribute values are made
safe with `framework.commons.events.redact` (redaction keys, long strings, DSN passwords):
a span never carries what a log line may not.

Example::

    from framework.tracing.spans import traced, traced_span

    with traced_span("orders.load", **{"order.id": 42}):
        ...

    @traced("orders.save")
    async def save(order): ...
"""

from __future__ import annotations

import functools
import inspect
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from typing import Any, TypeVar

from opentelemetry import trace
from opentelemetry.trace import Span, Status, StatusCode

from ..commons.events import redact
from . import tracing as _tracing

F = TypeVar("F", bound=Callable[..., Any])

_ATTRIBUTE_TYPES = (str, bool, int, float)


def current_tracer() -> trace.Tracer:
    """The process tracer, read at call time (`init_tracing` replaces it)."""
    return _tracing.get_tracer()


def span_attributes(attributes: Mapping[str, Any]) -> dict[str, Any]:
    """OpenTelemetry-compatible, redacted attributes; `None` values are dropped."""
    out: dict[str, Any] = {}
    for key, value in attributes.items():
        if value is None:
            continue
        value = redact(value, key)
        if isinstance(value, _ATTRIBUTE_TYPES):
            out[key] = value
        elif isinstance(value, (list, tuple)) and all(
            isinstance(v, _ATTRIBUTE_TYPES) for v in value
        ):
            out[key] = list(value)
        else:
            out[key] = str(value)
    return out


@contextmanager
def traced_span(name: str, **attributes: Any) -> Iterator[Span]:
    """Run a block inside a span; an exception is recorded, marks the span failed and is re-raised."""
    with current_tracer().start_as_current_span(
        name, attributes=span_attributes(attributes)
    ) as span:
        try:
            yield span
        except BaseException as exc:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR, type(exc).__name__))
            raise


def traced(name: str | None = None, **attributes: Any) -> Callable[[F], F]:
    """Decorator form of `traced_span` for sync and async callables.

    The span name defaults to ``<module>.<function>`` (the last module component).
    """

    def decorate(fn: F) -> F:
        span_name = name or f"{fn.__module__.rsplit('.', 1)[-1]}.{fn.__name__}"
        if inspect.iscoroutinefunction(fn):

            @functools.wraps(fn)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                with traced_span(span_name, **attributes):
                    return await fn(*args, **kwargs)

            return async_wrapper  # type: ignore[return-value]

        @functools.wraps(fn)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with traced_span(span_name, **attributes):
                return fn(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorate


def set_span_attributes(**attributes: Any) -> None:
    """Add redacted attributes to the current span (ignored when tracing is off)."""
    trace.get_current_span().set_attributes(span_attributes(attributes))


def inject_http_headers(headers: Mapping[str, str] | None = None) -> dict[str, str]:
    """HTTP headers (as `str`) carrying the current trace context, for outgoing calls."""
    out = dict(headers or {})
    for key, value in _tracing.inject_trace_headers():
        out[key] = value.decode() if isinstance(value, bytes) else str(value)
    return out


def current_trace_id() -> str | None:
    """The current trace id (32 hex digits), or `None` outside a recording span."""
    context = trace.get_current_span().get_span_context()
    return f"{context.trace_id:032x}" if context.is_valid else None
