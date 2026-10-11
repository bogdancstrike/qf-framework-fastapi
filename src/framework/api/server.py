import re
import time
import uuid

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, Response
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from starlette.datastructures import MutableHeaders

from ..commons.logger import logger, request_id
from ..observability import Metrics
from ..tracing import get_tracer, extract_context_from_headers


class ObservabilityMiddleware:
    """Pure ASGI middleware: preserves context and streaming without buffering bodies."""

    def __init__(self, app, owner):
        self.app, self.owner = app, owner

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        headers = dict(scope.get("headers", []))
        candidate = headers.get(b"x-request-id", b"").decode("ascii", errors="ignore")
        rid = candidate if re.fullmatch(r"[A-Za-z0-9._-]{1,64}", candidate) else uuid.uuid4().hex
        scope.setdefault("state", {})["request_id"] = rid
        token = request_id.set(rid)
        started, status = time.perf_counter(), 500

        async def observed(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                MutableHeaders(scope=message)["X-Request-ID"] = rid
            await send(message)

        try:
            carrier = [(k.decode("latin1"), v) for k, v in scope.get("headers", [])]
            with get_tracer().start_as_current_span(
                "http.request", context=extract_context_from_headers(carrier)
            ) as span:
                await self.app(scope, receive, observed)
                route = getattr(scope.get("route"), "path", "unmatched")
                span.update_name(f"{scope['method']} {route}")
                span.set_attribute("http.response.status_code", status)
        finally:
            route = getattr(scope.get("route"), "path", "unmatched")
            method = (
                scope["method"]
                if scope["method"] in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}
                else "OTHER"
            )
            metrics = self.owner.state.metrics
            duration = time.perf_counter() - started
            metrics.requests.labels(method, route, str(status)).inc()
            metrics.http_duration.labels(method, route).observe(duration)
            if self.owner.state.settings.log_endpoints:
                logger.info(
                    "HTTP request",
                    extra={
                        "event": {
                            "method": method,
                            "route": route,
                            "status": status,
                            "duration_ms": round(duration * 1000, 3),
                        }
                    },
                )
            request_id.reset(token)


def create_app(settings=None, *, lifespan=None, metrics=None):
    if settings is None:
        from ..config import FrameworkSettings

        settings = FrameworkSettings(enable_etl=False, enable_dynamic_endpoints=False)
    app = FastAPI(
        title=settings.api_title,
        version=settings.api_version,
        description=settings.api_description,
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.metrics = metrics or Metrics()
    app.state.runtime = None
    app.state.started = False
    app.add_middleware(ObservabilityMiddleware, owner=app)
    if settings.cors_origins:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origins,
            allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
            allow_headers=["Content-Type", "Authorization", "X-Request-ID"],
        )

    @app.get("/health/live", tags=["Operations"])
    async def live():
        return {"status": "alive"}

    @app.get("/health/ready", tags=["Operations"])
    async def ready():
        runtime = app.state.runtime
        ok = app.state.started and (
            not settings.enable_etl or runtime is not None and runtime.ready
        )
        return JSONResponse(
            {"status": "ready" if ok else "not_ready"}, status_code=200 if ok else 503
        )

    @app.get("/metrics", include_in_schema=False)
    async def metrics_endpoint():
        return Response(
            generate_latest(app.state.metrics.registry),
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )

    @app.exception_handler(Exception)
    async def unexpected(request, exc):
        logger.error("HTTP handler failed", exc_info=(type(exc), exc, exc.__traceback__))
        rid = getattr(request.state, "request_id", None)
        return JSONResponse(
            {"detail": "Internal server error", "request_id": rid},
            status_code=500,
            headers={"X-Request-ID": rid} if rid else {},
        )

    return app


def create_api(app, version="2.0", title="QF Worker API", description=""):
    app.version, app.title, app.description = version, title, description
    return app
