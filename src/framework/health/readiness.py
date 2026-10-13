"""Readiness with per-check detail: ``ready``, ``degraded`` or ``not_ready``.

The framework's own ``/health/ready`` only knows "started". Applications that depend on
several stores usually need more: which store is down, and whether the outage is one the
service survives. This module gives the pieces:

- `run_probes`: run ``{name: awaitable -> truthy}`` probes concurrently, each with a time
  budget; a probe that raises or times out is unhealthy;
- `readiness_status`: ``not_ready`` when the application has not started or a *required*
  check failed (the pod must leave the load balancer), ``degraded`` when only optional checks
  failed (still served: a restart would not help), else ``ready``;
- `install_readiness_routes`: replace ``/health/ready`` (and add ``/health/dependencies``)
  with a route answering the report: 200 for ``ready``/``degraded``, 503 for ``not_ready``.

Example::

    async def report(app):
        results = await run_probes({"postgres": db.ping(), "redis": redis.ping()})
        checks = {name: {"ok": ok} for name, ok in results.items()}
        status = readiness_status(checks, required={"postgres"}, started=app.state.started)
        return {"status": status, "checks": checks}

    install_readiness_routes(app, report)
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Collection, Mapping, Sequence
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse

READY = "ready"
DEGRADED = "degraded"
NOT_READY = "not_ready"
PROBE_TIMEOUT_S = 3.0
DEFAULT_PATHS = ("/health/ready", "/health/dependencies")

#: Builds the readiness report of `app`: at least ``{"status": ..., "checks": {...}}``.
ReportBuilder = Callable[[FastAPI], Awaitable[dict[str, Any]]]


async def _probe(probe: Awaitable[Any], timeout_s: float) -> bool:
    try:
        return bool(await asyncio.wait_for(probe, timeout=timeout_s))
    except Exception:
        return False


async def run_probes(
    probes: Mapping[str, Awaitable[Any]], *, timeout_s: float = PROBE_TIMEOUT_S
) -> dict[str, bool]:
    """Await every probe concurrently; truthy within `timeout_s` means healthy."""
    results = await asyncio.gather(*(_probe(p, timeout_s) for p in probes.values()))
    return dict(zip(probes, results, strict=True))


def readiness_status(
    checks: Mapping[str, Mapping[str, Any]], *, required: Collection[str], started: bool = True
) -> str:
    """``not_ready`` / ``degraded`` / ``ready`` from checks shaped ``{name: {"ok": bool, ...}}``.

    A required name missing from `checks` counts as failed.
    """
    if not started or not all(checks.get(name, {}).get("ok") for name in required):
        return NOT_READY
    if all(check.get("ok") for check in checks.values()):
        return READY
    return DEGRADED


def install_readiness_routes(
    app: FastAPI, report: ReportBuilder, *, paths: Sequence[str] = DEFAULT_PATHS
) -> None:
    """Serve `report` on `paths`, replacing any route already there (the framework's ``/health/ready``)."""
    app.router.routes[:] = [r for r in app.router.routes if getattr(r, "path", None) not in paths]

    async def readiness() -> JSONResponse:
        body = await report(app)
        return JSONResponse(body, status_code=503 if body.get("status") == NOT_READY else 200)

    for path in paths:
        app.add_api_route(path, readiness, methods=["GET"], include_in_schema=False)
