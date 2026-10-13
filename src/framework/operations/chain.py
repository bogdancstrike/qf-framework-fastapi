"""The operation request pipeline (Chain of responsibility), in Werkzeug/flask-restx order.

1. method check     - Starlette routing + the exception handlers (405 / OPTIONS 200)
2. body size        - `BodySizeLimitMiddleware` (413, streaming-safe)
3. `MediaTypeGuard`   - 415 for a non-JSON content type
4. `JsonDecoder`      - Werkzeug's 400 for an empty or malformed body
5. `BodyKindPolicy`   - what a non-object JSON value means for this route
6. `SchemaValidator`  - flask-restx Draft 4 model validation
7. `IdentityResolver` - the caller from the gateway headers (`@with_user` operations need one)
8. `HandlerInvoker`   - the operation, inside a span
9. `ErrorMapper`      - exceptions to their legacy answers

Steps 1-2 run before routing; 3-9 are `OperationChain`. Each link returns a response
(short-circuit) or lets the request continue; links hold no per-request state. After each
request the chain tells its `OperationObserver` (metrics, access logs).
"""

from __future__ import annotations

import json
import time
from collections.abc import Awaitable, Callable, Collection, Sequence
from dataclasses import dataclass, field
from typing import Any

from opentelemetry import trace
from starlette.requests import Request
from starlette.responses import Response

from ..commons.events import EventLogger, get_event_logger
from ..tracing.spans import traced_span
from .catalogue import OperationSpec
from .encoding import respond
from .errors import ErrorMapper
from .identity import GATE_HEADERS, GateHeaders, Identity, resolve_identity
from .validation import OBJECT_ONLY, validation_body
from .werkzeug_answers import BAD_REQUEST, UNSUPPORTED_MEDIA, is_json_mimetype

_NO_BODY_METHODS = ("GET", "HEAD", "OPTIONS")


@dataclass(slots=True)
class OperationCall:
    """What an operation receives: the decoded body, the caller and the application context."""

    body: Any
    identity: Identity
    ctx: Any
    request: Request
    spec: OperationSpec

    @property
    def payload(self) -> dict[str, Any]:
        """The body as a dict (``{}`` for a non-object body)."""
        return self.body if isinstance(self.body, dict) else {}


#: An operation: ``async def op(call: OperationCall) -> body | (body, status[, headers]) | Response``.
Handler = Callable[[OperationCall], Awaitable[Any]]


@dataclass(slots=True)
class ChainState:
    """Per-request state passed down the links (created by `OperationChain.run`, never shared).

    Attributes:
        request: The Starlette request.
        spec: The contract of the matched route.
        ctx: The application context (``app.state.ctx``).
        handler: The operation the chain ends with.
        body: The decoded body.
        identity: The caller, set by `IdentityResolver`.
        extras: Free slot for application links.
    """

    request: Request
    spec: OperationSpec
    ctx: Any
    handler: Handler
    body: Any = None
    identity: Identity | None = None
    extras: dict[str, Any] = field(default_factory=dict)


class ChainLink:
    """One step of the request pipeline."""

    async def handle(self, state: ChainState) -> Response | None:
        """Return a response to stop the chain, or `None` to let the request continue."""
        raise NotImplementedError


def _carries_body(state: ChainState) -> bool:
    return state.spec.reads_body and state.request.method not in _NO_BODY_METHODS


class MediaTypeGuard(ChainLink):
    """Link 3: 415 when a body-reading route gets a non-JSON ``Content-Type``."""

    async def handle(self, state: ChainState) -> Response | None:
        """415 for a non-JSON content type on a body-carrying request."""
        if _carries_body(state) and not is_json_mimetype(state.request.headers.get("content-type")):
            return respond(UNSUPPORTED_MEDIA, 415)
        return None


class JsonDecoder(ChainLink):
    """Link 4: decode the JSON body; an empty or malformed body is Werkzeug's 400."""

    async def handle(self, state: ChainState) -> Response | None:
        """Decode into `state.body`; 400 when the body is empty or not JSON."""
        if not _carries_body(state):
            return None
        raw = await state.request.body()
        if not raw:
            return respond(BAD_REQUEST, 400)
        try:
            state.body = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            return respond(BAD_REQUEST, 400)
        return None


class BodyKindPolicy(ChainLink):
    """Link 5: apply the route's body kind (``validated``/``object``/``raw``/``ignored``/``none``)."""

    async def handle(self, state: ChainState) -> Response | None:
        """Read ignored bodies as ``{}``; 400 (validation body) for a non-object where one is needed."""
        kind = state.spec.body_kind
        if kind in ("ignored", "none") or (kind == "raw" and state.body in ([], None)):
            state.body = {}
            return None
        if not isinstance(state.body, dict):
            return respond(validation_body(OBJECT_ONLY.errors(state.body)), 400)
        return None


class SchemaValidator(ChainLink):
    """Link 6: flask-restx Draft 4 validation against the route's operation schema."""

    async def handle(self, state: ChainState) -> Response | None:
        """400 with the flask-restx ``errors`` body when the model rejects the body."""
        validator = state.spec.validator
        if validator is None or state.spec.body_kind != "validated":
            return None
        errors = validator.errors(state.body)
        return respond(validation_body(errors), 400) if errors else None


class IdentityResolver(ChainLink):
    """Link 7: the caller from the gateway headers (never answers itself).

    Args:
        super_users: ``ctx -> user names`` that are super users (default: none).
        gate: The identity header names.
    """

    def __init__(
        self,
        super_users: Callable[[Any], Collection[str]] = lambda ctx: (),
        gate: GateHeaders = GATE_HEADERS,
    ) -> None:
        self.super_users = super_users
        self.gate = gate

    async def handle(self, state: ChainState) -> Response | None:
        """Set `state.identity`."""
        state.identity = resolve_identity(
            state.request.headers, self.super_users(state.ctx), gate=self.gate
        )
        return None


class HandlerInvoker(ChainLink):
    """Link 8: run the operation inside a ``<span_prefix>.<namespace>.<operation>`` span.

    The operation may return a `Response` (sent as is), a ``(body, status[, headers])`` tuple,
    or a body (200).

    Args:
        span_prefix: First component of the span name and of its ``<prefix>.group`` attribute.
    """

    def __init__(self, span_prefix: str = "operation") -> None:
        self.span_prefix = span_prefix

    async def handle(self, state: ChainState) -> Response | None:
        """Run the operation and turn its result into a response."""
        trace.get_current_span().set_attribute("http.route", state.spec.path)
        call = self._call(state)
        name = f"{self.span_prefix}.{state.spec.prefix}.{state.spec.operation}"
        attributes = {
            f"{self.span_prefix}.group": state.spec.group,
            "enduser.id": call.identity.user or None,
        }
        with traced_span(name, **attributes):
            result = await state.handler(call)
        return _as_response(result)

    @staticmethod
    def _call(state: ChainState) -> OperationCall:
        return OperationCall(
            body=state.body,
            identity=state.identity or Identity(user=""),
            ctx=state.ctx,
            request=state.request,
            spec=state.spec,
        )


def _as_response(result: Any) -> Response:
    if isinstance(result, Response):
        return result
    if isinstance(result, tuple):
        body, status, *rest = result
        return respond(body, int(status), rest[0] if rest else None)
    return respond(result, 200)


class OperationObserver:
    """Observer (OperationObserver): told once per answered request; the default logs it.

    Subclass it for metrics or another log format.

    Args:
        log: Where the default implementation logs (``framework.operations``).
    """

    def __init__(self, log: EventLogger | None = None) -> None:
        self.log = log or get_event_logger("framework.operations")

    def on_response(self, spec: OperationSpec, status: int, duration_s: float) -> None:
        """``operation_request`` at DEBUG, or ``operation_request_failed`` at ERROR for a 5xx."""
        if status >= 500:
            self.log.error("operation_request_failed", route=spec.path, status=status)
        else:
            self.log.debug(
                "operation_request",
                route=spec.path,
                status=status,
                duration_ms=round(duration_s * 1000, 2),
            )


#: The fixed order of the in-route links.
DEFAULT_LINKS: tuple[type[ChainLink], ...] = (
    MediaTypeGuard,
    JsonDecoder,
    BodyKindPolicy,
    SchemaValidator,
    IdentityResolver,
    HandlerInvoker,
)


def default_links(
    *,
    super_users: Callable[[Any], Collection[str]] = lambda ctx: (),
    gate: GateHeaders = GATE_HEADERS,
    span_prefix: str = "operation",
) -> tuple[ChainLink, ...]:
    """The `DEFAULT_LINKS`, configured."""
    return (
        MediaTypeGuard(),
        JsonDecoder(),
        BodyKindPolicy(),
        SchemaValidator(),
        IdentityResolver(super_users, gate),
        HandlerInvoker(span_prefix),
    )


class OperationChain:
    """Runs the links for one request, maps errors and tells the observer. One per application.

    Args:
        links: The links, in order (default `default_links()`).
        mapper: Turns exceptions into answers.
        observer: Told about every answered request.
    """

    def __init__(
        self,
        links: Sequence[ChainLink] | None = None,
        mapper: ErrorMapper | None = None,
        observer: OperationObserver | None = None,
    ) -> None:
        self.links = tuple(links) if links is not None else default_links()
        self.mapper = mapper or ErrorMapper()
        self.observer = observer or OperationObserver()

    async def run(
        self, request: Request, spec: OperationSpec, ctx: Any, handler: Handler
    ) -> Response:
        """Run the links in order for one request; map any exception; observe the answer."""
        state = ChainState(request=request, spec=spec, ctx=ctx, handler=handler)
        started = time.perf_counter()
        try:
            response = await self._through_links(state)
        except Exception as exc:
            response = self.mapper.map(exc, spec)
        self.observer.on_response(spec, response.status_code, time.perf_counter() - started)
        return response

    async def _through_links(self, state: ChainState) -> Response:
        for link in self.links:
            response = await link.handle(state)
            if response is not None:
                return response
        raise RuntimeError("the chain ended without an answer (HandlerInvoker always answers)")
