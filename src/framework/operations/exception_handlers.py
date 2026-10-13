"""Application exception handlers with Werkzeug's and flask-restx's answers.

They replace FastAPI's defaults (``{"detail": ...}`` JSON):

- unknown path, any method: Werkzeug's HTML 404;
- known path, wrong method: JSON 405 with ``Allow``; ``OPTIONS`` answers Flask's automatic
  200 (empty, ``text/html; charset=utf-8``);
- a body the framework's dynamic endpoints could not parse as JSON: 415 for a non-JSON
  content type, otherwise Werkzeug's 400;
- request validation of native FastAPI routes: the flask-restx validation body;
- any unhandled exception: ``{"message": "Internal Server Error"}`` (no traceback, no body).
"""

from __future__ import annotations

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from starlette.requests import Request
from starlette.responses import Response

from ..commons.events import EventLogger, get_event_logger
from .encoding import LegacyJSONResponse
from .validation import validation_body
from .werkzeug_answers import (
    BAD_REQUEST,
    INTERNAL_ERROR,
    METHOD_NOT_ALLOWED,
    NOT_FOUND_HTML,
    UNSUPPORTED_MEDIA,
    is_json_mimetype,
)

#: The detail the framework's dynamic endpoints raise for an unparsable body.
FRAMEWORK_INVALID_JSON = "Invalid JSON body"
_HTML = "text/html; charset=utf-8"


def not_found_response() -> Response:
    """Werkzeug's HTML 404 page."""
    return Response(NOT_FOUND_HTML, status_code=404, media_type=_HTML)


def allow_header(exc: StarletteHTTPException) -> str:
    """The ``Allow`` value Werkzeug sends: the route's methods plus ``HEAD`` (with GET) and ``OPTIONS``."""
    methods = [m.strip() for m in (exc.headers or {}).get("Allow", "").split(",") if m.strip()]
    if "GET" in methods and "HEAD" not in methods:
        methods.append("HEAD")
    if "OPTIONS" not in methods:
        methods.append("OPTIONS")
    return ", ".join(sorted(methods))


def _method_not_allowed(request: Request, exc: StarletteHTTPException) -> Response:
    allow = allow_header(exc)
    if request.method == "OPTIONS":
        return Response(b"", status_code=200, headers={"Allow": allow}, media_type=_HTML)
    return LegacyJSONResponse(METHOD_NOT_ALLOWED, status_code=405, headers={"Allow": allow})


def _unparsable_body(request: Request) -> Response:
    if not is_json_mimetype(request.headers.get("content-type")):
        return LegacyJSONResponse(UNSUPPORTED_MEDIA, status_code=415)
    return LegacyJSONResponse(BAD_REQUEST, status_code=400)


async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> Response:
    """404 → Werkzeug HTML, 405 → JSON with ``Allow`` (OPTIONS → 200), unparsable bodies → 400/415."""
    if exc.status_code == 404:
        return not_found_response()
    if exc.status_code == 405:
        return _method_not_allowed(request, exc)
    if exc.status_code == 400 and exc.detail == FRAMEWORK_INVALID_JSON:
        return _unparsable_body(request)
    return LegacyJSONResponse(
        {"message": str(exc.detail)}, status_code=exc.status_code, headers=exc.headers
    )


async def validation_exception_handler(request: Request, exc: RequestValidationError) -> Response:
    """Native FastAPI routes answer the flask-restx validation body."""
    errors = {
        ".".join(str(p) for p in e.get("loc", ())[1:]): e.get("msg", "invalid")
        for e in exc.errors()
    }
    return LegacyJSONResponse(validation_body(errors), status_code=400)


def unhandled_exception_handler(log: EventLogger):
    """The handler logging an unhandled exception and answering Flask's clean 500."""

    async def handle(request: Request, exc: Exception) -> Response:
        log.error(
            "unhandled_exception", path=request.url.path, error=type(exc).__name__, exc_info=exc
        )
        return LegacyJSONResponse(INTERNAL_ERROR, status_code=500)

    return handle


def install_exception_handlers(app: FastAPI, *, log: EventLogger | None = None) -> None:
    """Replace FastAPI's default exception handlers with the legacy ones."""
    log = log or get_event_logger("framework.operations")
    app.add_exception_handler(StarletteHTTPException, http_exception_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, unhandled_exception_handler(log))
