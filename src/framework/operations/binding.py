"""`@api_operation` and `install_operations`: endpoint-map routes served with the legacy contract.

An operation is a plain ``async def op(call: OperationCall)`` named in an endpoint map by
module and function, and decorated with `@api_operation`. The decorator gives it the native
async signature the framework's dynamic endpoints call (``op(app=..., operation=...,
request=...)``); at call time it finds the route's `OperationSpec` and runs the application's
`OperationChain`, so operations never parse, validate or authenticate anything themselves.

`install_operations` sets the whole contract up on a framework app::

    app = FrameworkApp(settings).build_app()
    install_operations(app, [load_map(Path("maps/orders.json"))], max_body_bytes=16 << 20)
    app.state.ctx = my_context          # what `call.ctx` is

Middleware order (outermost first): security headers, CORS echo, trailing slash, the
`extra_middleware`, body size limit. Extra middleware therefore sees the matched route.
"""

from __future__ import annotations

import functools
from collections.abc import Collection, Sequence
from pathlib import Path
from typing import Any

from fastapi import FastAPI
from starlette.requests import Request
from starlette.responses import Response

from ..api.dynamic import generate_endpoints_from_config
from ..commons.events import EventLogger
from .catalogue import EndpointMap, RouteCatalogue
from .chain import Handler, OperationChain
from .exception_handlers import install_exception_handlers
from .middleware import (
    BodySizeLimitMiddleware,
    EchoOriginCorsMiddleware,
    SecurityHeadersMiddleware,
    TrailingSlashMiddleware,
)
from .profiles import ProfileRegistry

APP_STATE_CATALOGUE = "operation_routes"
APP_STATE_CHAIN = "operation_chain"
APP_STATE_CONTEXT = "ctx"
DEFAULT_MAX_BODY_BYTES = 16 * 1024 * 1024

#: ``(middleware class, keyword arguments)`` for `install_operations(extra_middleware=...)`.
MiddlewareEntry = tuple[type, dict[str, Any]]


def api_operation(handler: Handler) -> Any:
    """Adapt ``handler(call)`` to the framework's native async endpoint signature."""

    @functools.wraps(handler)
    async def endpoint(app: FastAPI, operation: str, request: Request, **_: Any) -> Response:
        route_path = getattr(request.scope.get("route"), "path", "")
        spec = getattr(app.state, APP_STATE_CATALOGUE).get(route_path)
        if spec is None:
            raise RuntimeError(f"Route {route_path or '?'} is not in the operation catalogue")
        chain: OperationChain = getattr(app.state, APP_STATE_CHAIN)
        return await chain.run(request, spec, getattr(app.state, APP_STATE_CONTEXT, None), handler)

    endpoint.__operation_handler__ = handler  # type: ignore[attr-defined]
    return endpoint


def mount_endpoint_maps(
    app: FastAPI,
    maps: Sequence[EndpointMap],
    chain: OperationChain | None = None,
    profiles: ProfileRegistry | None = None,
) -> RouteCatalogue:
    """Compile the maps' routes and install the catalogue and chain on `app`."""
    catalogue = RouteCatalogue.from_maps(maps, profiles)
    setattr(app.state, APP_STATE_CATALOGUE, catalogue)
    setattr(app.state, APP_STATE_CHAIN, chain or OperationChain())
    for endpoint_map in maps:
        generate_endpoints_from_config(app, Path(endpoint_map.path))
    return catalogue


def install_operations(
    app: FastAPI,
    maps: Sequence[EndpointMap],
    *,
    chain: OperationChain | None = None,
    profiles: ProfileRegistry | None = None,
    max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
    cors_origins: Collection[str] = (),
    extra_middleware: Sequence[MiddlewareEntry] = (),
    log: EventLogger | None = None,
) -> RouteCatalogue:
    """Mount `maps` with the legacy contract: routes, exception handlers and middleware.

    Args:
        app: A framework (or plain FastAPI) application.
        maps: The endpoint maps to serve.
        chain: The request chain (default `OperationChain()`).
        profiles: API profiles by URL prefix (default: flask-restx validation body everywhere).
        max_body_bytes: Larger bodies answer 413.
        cors_origins: Origins echoed back (``"*"`` for any); empty disables CORS.
        extra_middleware: Added inside the trailing-slash middleware, so they see the route.
        log: Where unhandled exceptions are logged.
    """
    catalogue = mount_endpoint_maps(app, maps, chain, profiles)
    install_exception_handlers(app, log=log)
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=max_body_bytes)
    for middleware, options in extra_middleware:
        app.add_middleware(middleware, **options)
    app.add_middleware(TrailingSlashMiddleware)
    if cors_origins:
        app.add_middleware(EchoOriginCorsMiddleware, allowed_origins=cors_origins)
    app.add_middleware(SecurityHeadersMiddleware)
    return catalogue
