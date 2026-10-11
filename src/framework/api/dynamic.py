"""Compile legacy endpoint.json mappings into typed FastAPI routes once."""

import importlib
import inspect
import json
import re

from fastapi import Body, Request, HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse, Response
from pydantic import ConfigDict, Field, create_model
from starlette.concurrency import run_in_threadpool

TYPES = {
    "string": str,
    "boolean": bool,
    "integer": int,
    "number": float,
    "float": float,
    "dict": dict,
}


class LegacyRequest:
    """Sync handler bridge. Native async handlers receive a Starlette Request."""

    def __init__(self, request, body):
        self._request = request
        self.json = body
        self.args = request.query_params
        self.headers = request.headers
        self.method = request.method
        self.url = str(request.url)
        self.path = request.url.path
        self.form = {}

    def get_json(self, silent=False, force=False):
        return self.json

    def __getattr__(self, name):
        return getattr(self._request, name)


def get_model(api, models, model_name):
    definition = models.get(model_name)
    if not definition:
        return None
    fields = {}
    for name, entry in definition.items():
        kind = entry.get("type", "string")
        if kind == "list":
            subtype = entry.get("super", "string")
            if subtype not in TYPES:
                raise ValueError(f"Unknown list subtype: {subtype}")
            annotation = list[TYPES[subtype]]
        elif kind in TYPES:
            annotation = TYPES[kind]
        else:
            raise ValueError(f"Unknown model type: {kind}")
        args = entry.get("args", {})
        default = ... if args.get("required") else args.get("default", None)
        constraints = {
            "min_length": args.get("min_length"),
            "max_length": args.get("max_length"),
            "ge": args.get("min"),
            "le": args.get("max"),
            "pattern": args.get("pattern"),
        }
        fields[name] = (
            annotation if args.get("required") else annotation | None,
            Field(
                default,
                description=args.get("description"),
                **{k: v for k, v in constraints.items() if v is not None},
            ),
        )
    return create_model(model_name, __config__=ConfigDict(extra="allow"), **fields)


def normalize_path(namespace, operation, api_url):
    prefix = f"/{namespace.strip('/')}"
    path = api_url or f"{prefix}/{operation}"
    if not path.startswith("/"):
        path = "/" + path
    if path != prefix and not path.startswith(prefix + "/"):
        path = prefix + path
    types = {}

    def convert(match):
        converter, name = match.group(1) or "string", match.group(2)
        if converter not in {"int", "float", "string", "path", "uuid"}:
            raise ValueError(f"Unsupported path converter: {converter}")
        import uuid

        types[name] = {"int": int, "float": float, "uuid": uuid.UUID}.get(converter, str)
        return "{" + name + (":path" if converter == "path" else "") + "}"

    path = re.sub(r"<(?:([a-z]+):)?([A-Za-z_]\w*)>", convert, path)
    for name in re.findall(r"{([A-Za-z_]\w*)(?::path)?}", path):
        types.setdefault(name, str)
    if any(name in {"request", "payload"} for name in types):
        raise ValueError("Path parameter names request and payload are reserved")
    return path, types


def make_endpoint(handler, operation, model, path_types, method):
    async def endpoint(request: Request, **kwargs):
        body = kwargs.pop("payload", None)
        parsed = body.model_dump(exclude_unset=True) if body is not None else None
        if parsed is None and request.method in {"POST", "PUT", "PATCH", "DELETE"}:
            raw = await request.body()
            try:
                parsed = json.loads(raw) if raw else None
            except (ValueError, UnicodeError) as exc:
                raise HTTPException(400, "Invalid JSON body") from exc
        if inspect.iscoroutinefunction(handler):
            result = await handler(app=request.app, operation=operation, request=request, **kwargs)
        else:
            result = await run_in_threadpool(
                handler,
                app=request.app,
                operation=operation,
                request=LegacyRequest(request, parsed),
                **kwargs,
            )
            if inspect.isawaitable(result):
                result = await result
        if isinstance(result, Response):
            return result
        if isinstance(result, tuple):
            content, status, *headers = result
            return JSONResponse(
                jsonable_encoder(content),
                status_code=status,
                headers=headers[0] if headers else None,
            )
        return result

    params = [
        inspect.Parameter("request", inspect.Parameter.POSITIONAL_OR_KEYWORD, annotation=Request)
    ]
    for name, annotation in path_types.items():
        params.append(
            inspect.Parameter(name, inspect.Parameter.KEYWORD_ONLY, annotation=annotation)
        )
    if model is not None and method in {"POST", "PUT", "PATCH", "DELETE"}:
        params.append(
            inspect.Parameter(
                "payload", inspect.Parameter.KEYWORD_ONLY, annotation=model, default=Body(...)
            )
        )
    endpoint.__signature__ = inspect.Signature(params)
    endpoint.__name__ = f"{method.lower()}_{operation}"
    return endpoint


def generate_endpoints_from_config(api, path="maps/endpoint.json", wait=0):
    if wait:
        raise ValueError("Startup sleeps are unsupported; use lifespan readiness")
    with open(path, encoding="utf-8") as source:
        config = json.load(source)
    namespaces = {ns["name"]: ns for ns in config["namespaces"]}
    if len(namespaces) != len(config["namespaces"]):
        raise ValueError("Duplicate namespace")
    models = {
        name: get_model(api, config.get("models", {}), name) for name in config.get("models", {})
    }
    seen = {(r.path, method) for r in api.routes for method in getattr(r, "methods", [])}
    compiled = []
    for entry in config["endpoints"]:
        ns, operation = entry["namespace"], entry["operation_name"]
        if ns not in namespaces:
            raise ValueError(f"Unknown namespace: {ns}")
        model_name = entry.get("model_name")
        if model_name and model_name not in models:
            raise ValueError(f"Unknown model: {model_name}")
        target = entry["exec_method"]
        handler = getattr(importlib.import_module(target["module_name"]), target["method_name"])
        if not callable(handler):
            raise ValueError("Endpoint handler must be callable")
        uri, types = normalize_path(ns, operation, entry.get("api_url"))
        for method in entry["request_method"]:
            method = method.upper()
            if method not in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}:
                raise ValueError(f"Unsupported HTTP method: {method}")
            if (uri, method) in seen:
                raise ValueError(f"Duplicate route: {method} {uri}")
            seen.add((uri, method))
            compiled.append((uri, method, ns, operation, models.get(model_name), types, handler))
    for uri, method, ns, operation, model, types, handler in compiled:
        api.add_api_route(
            uri,
            make_endpoint(handler, operation, model, types, method),
            methods=[method],
            tags=[ns],
            operation_id=f"{ns}_{operation}_{method.lower()}",
        )
