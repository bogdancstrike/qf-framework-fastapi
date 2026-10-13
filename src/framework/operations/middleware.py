"""Pure ASGI middleware of the legacy HTTP contract (no buffering, no per-process state).

- `TrailingSlashMiddleware`: the trailing slash is optional on every route (no redirect).
- `BodySizeLimitMiddleware`: Werkzeug's ``MAX_CONTENT_LENGTH``: 413 above the limit, also for
  chunked bodies, without reading past it; a non-JSON body answers 415 first.
- `SecurityHeadersMiddleware`: flask-talisman's defaults (CSP off, HSTS only over HTTPS).
- `EchoOriginCorsMiddleware`: flask-cors behaviour: an allowed ``Origin`` is echoed back.
"""

from __future__ import annotations

from collections.abc import Collection

from starlette.datastructures import Headers, MutableHeaders
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from .encoding import dumps
from .werkzeug_answers import TOO_LARGE, UNSUPPORTED_MEDIA, is_json_mimetype

BODY_METHODS = frozenset({"POST", "PUT", "PATCH", "DELETE"})
SECURITY_HEADERS = (
    ("x-frame-options", "SAMEORIGIN"),
    ("x-content-type-options", "nosniff"),
    ("referrer-policy", "strict-origin-when-cross-origin"),
    ("permissions-policy", "browsing-topics=()"),
)
HSTS = ("strict-transport-security", "max-age=31556926; includeSubDomains")
CORS_METHODS = b"DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT"


async def send_json(send: Send, status: int, body: object) -> None:
    """Send a complete Flask-encoded JSON response from inside a middleware."""
    payload = dumps(body).encode("ascii")
    headers = [
        (b"content-type", b"application/json"),
        (b"content-length", str(len(payload)).encode()),
    ]
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": payload})


class TrailingSlashMiddleware:
    """``/ns/op/`` routes like ``/ns/op``.

    Inner middleware see the route Starlette matched; outer ones do not (the scope is copied).
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and len(scope["path"]) > 1 and scope["path"].endswith("/"):
            scope = dict(scope)
            scope["path"] = scope["path"].rstrip("/") or "/"
            raw = scope.get("raw_path")
            if raw:
                scope["raw_path"] = raw.rstrip(b"/") or b"/"
        await self.app(scope, receive, send)


class _BodyTooLarge(Exception):
    pass


class BodySizeLimitMiddleware:
    """413 above `max_bytes` without reading the body past the limit (415 first for non-JSON)."""

    def __init__(self, app: ASGIApp, max_bytes: int) -> None:
        self.app = app
        self.max_bytes = max_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or scope["method"] not in BODY_METHODS:
            await self.app(scope, receive, send)
            return
        headers = Headers(scope=scope)
        if self._declared_too_large(headers):
            json_body = is_json_mimetype(headers.get("content-type"))
            await send_json(
                send, 413 if json_body else 415, TOO_LARGE if json_body else UNSUPPORTED_MEDIA
            )
            return
        await self._stream(scope, receive, send)

    def _declared_too_large(self, headers: Headers) -> bool:
        length = headers.get("content-length")
        return bool(length and length.isdigit() and int(length) > self.max_bytes)

    async def _stream(self, scope: Scope, receive: Receive, send: Send) -> None:
        received = 0
        started = False

        async def limited_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.max_bytes:
                    raise _BodyTooLarge
            return message

        async def tracking_send(message: Message) -> None:
            nonlocal started
            started = started or message["type"] == "http.response.start"
            await send(message)

        try:
            await self.app(scope, limited_receive, tracking_send)
        except _BodyTooLarge:
            if not started:
                await send_json(send, 413, TOO_LARGE)


class SecurityHeadersMiddleware:
    """flask-talisman's default headers on every response (error pages included)."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        https = scope.get("scheme") == "https"

        async def with_headers(message: Message) -> None:
            if message["type"] == "http.response.start":
                headers = MutableHeaders(scope=message)
                for key, value in SECURITY_HEADERS:
                    headers.setdefault(key, value)
                if https:
                    headers.setdefault(*HSTS)
            await send(message)

        await self.app(scope, receive, with_headers)


class EchoOriginCorsMiddleware:
    """flask-cors: echo an allowed ``Origin`` with ``Vary: Origin``; answer preflights.

    Args:
        allowed_origins: Exact origins, or ``"*"`` for any.
    """

    def __init__(self, app: ASGIApp, allowed_origins: Collection[str]) -> None:
        self.app = app
        self.any_origin = "*" in allowed_origins
        self.allowed = frozenset(allowed_origins)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = Headers(scope=scope)
        origin = headers.get("origin")
        if not origin or not (self.any_origin or origin in self.allowed):
            await self.app(scope, receive, send)
        elif scope["method"] == "OPTIONS" and headers.get("access-control-request-method"):
            await self._preflight(send, origin, headers.get("access-control-request-headers"))
        else:
            await self.app(scope, receive, self._echoing(send, origin))

    @staticmethod
    async def _preflight(send: Send, origin: str, requested: str | None) -> None:
        response_headers = [
            (b"access-control-allow-origin", origin.encode("latin-1")),
            (b"vary", b"Origin"),
            (b"access-control-allow-methods", CORS_METHODS),
            (b"content-length", b"0"),
        ]
        if requested:
            response_headers.append((b"access-control-allow-headers", requested.encode("latin-1")))
        await send({"type": "http.response.start", "status": 200, "headers": response_headers})
        await send({"type": "http.response.body", "body": b""})

    @staticmethod
    def _echoing(send: Send, origin: str) -> Send:
        async def with_cors(message: Message) -> None:
            if message["type"] == "http.response.start":
                out = MutableHeaders(scope=message)
                out["Access-Control-Allow-Origin"] = origin
                out.add_vary_header("Origin")
            await send(message)

        return with_cors
