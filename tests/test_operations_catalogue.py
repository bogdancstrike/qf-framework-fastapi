"""Operation layer: catalogue, models, profiles, observer, spans and extension points."""

import json
from typing import Any

import httpx
import pytest

from framework.api.server import create_app
from framework.config import FrameworkSettings
from framework.operations import (
    ApiProfile,
    CatalogueError,
    ClientInputError,
    OperationCall,
    OperationChain,
    OperationObserver,
    ProfileRegistry,
    RouteCatalogue,
    api_operation,
    describe_routes,
    install_operations,
    load_map,
    requires_user,
    user_required,
    with_user,
)
from framework.operations.validation import ModelDefinitionError, compile_model
from framework.testing.spans import capture_spans
from framework.tracing.spans import traced_span

CALLS: list[OperationCall] = []
TRACE_ID = "4bf92f3577b34da6a3ce929d0e0e4736"


@api_operation
async def echo(call: OperationCall) -> dict[str, Any]:
    CALLS.append(call)
    return {"body": call.body}


@api_operation
async def health_get(call: OperationCall) -> dict[str, str]:
    CALLS.append(call)
    return {"status": "ok"}


@api_operation
async def raises_client_input(call: OperationCall) -> None:
    raise ClientInputError("a", "b")


@api_operation
async def traced(call: OperationCall) -> dict[str, bool]:
    with traced_span("handler.work"):
        return {"ok": True}


@api_operation
@user_required({"message": "login first"}, 401)
async def custom_user(call: OperationCall) -> dict[str, str]:
    return {"user": call.identity.user}


class XProfile(ApiProfile):
    """Strategy (ApiProfile) of a test-only API family ``x``."""

    def client_input_error(self, path: str, reason: str) -> tuple[Any, int]:
        return {"problem": path, "why": reason}, 422


def ep(ns, op, handler, contract, methods=("POST",)):
    return {
        "namespace": ns,
        "operation_name": op,
        "model_name": None,
        "request_method": list(methods),
        "exec_method": {"module_name": __name__, "method_name": handler},
        "operation_contract": contract,
    }


API_MAP = {
    "group": "importer",
    "namespaces": [{"name": "api", "description": "test"}],
    "models": None,
    "operation_schemas": {
        "process": {
            "operation": {"type": "string", "args": {"required": True}},
            "message": {"type": "nested", "model": "message", "args": {"required": True}},
            "platform": {
                "type": "string",
                "args": {"enum_ci": ["Facebook", "X"], "enum_ci_aliases": ["twitter"]},
            },
            "parts": {"type": "list", "super": "nested", "model": "message"},
        },
        "message": {"system": {"type": "dict"}, "general": {"type": "dict"}},
    },
    "endpoints": [
        ep("api", "process", "echo", {"model": "process", "body_kind": "validated"}),
        ep("api", "empty", "echo", {"body_kind": "object"}),
        ep("api", "health", "health_get", {"body_kind": "none"}, methods=("GET",)),
        ep("api", "traced", "traced", {"body_kind": "ignored"}),
        ep("api", "custom_user", "custom_user", {"body_kind": "ignored"}),
    ],
}

X_MAP = {
    "group": "search",
    "api_profile": f"{__name__}:XProfile",
    "namespaces": [{"name": "x", "description": "test family"}],
    "models": None,
    "operation_schemas": {},
    "endpoints": [ep("x", "boom", "raises_client_input", {"body_kind": "ignored"})],
}

JSON = {"Content-Type": "application/json"}


def write(tmp_path, document, name):
    path = tmp_path / name
    path.write_text(json.dumps(document))
    return path


class RecordingObserver(OperationObserver):
    def __init__(self):
        super().__init__()
        self.seen = []

    def on_response(self, spec, status, duration_s):
        self.seen.append((spec.path, status))


class RouteSeen:
    """Extra middleware recording the route template Starlette matched."""

    routes: list = []

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        await self.app(scope, receive, send)
        RouteSeen.routes.append(getattr(scope.get("route"), "path", None))


@pytest.fixture
def observer():
    return RecordingObserver()


@pytest.fixture
def app(tmp_path, observer):
    CALLS.clear()
    RouteSeen.routes = []
    maps = [
        load_map(write(tmp_path, API_MAP, "api.json")),
        load_map(write(tmp_path, X_MAP, "x.json")),
    ]
    application = create_app(FrameworkSettings(enable_etl=False, enable_dynamic_endpoints=False))
    install_operations(
        application,
        maps,
        chain=OperationChain(observer=observer),
        extra_middleware=[(RouteSeen, {})],
    )
    return application


@pytest.fixture
def client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test")


async def test_object_kind_route(client):
    resp = await client.post("/api/empty", content=b"[]", headers=JSON)
    assert resp.status_code == 400
    assert resp.json()["errors"] == {"": "[] is not of type 'object'"}
    assert (await client.post("/api/empty")).status_code == 415


async def test_nested_models_reject_null_sections(client):
    body = {"operation": "update", "message": {"system": None}}
    resp = await client.post("/api/process", content=json.dumps(body), headers=JSON)
    assert resp.json()["errors"] == {"message.system": "None is not of type 'object'"}
    ok = {"operation": "update", "message": {"system": {"_id": "a"}}, "parts": [{"general": {}}]}
    assert (await client.post("/api/process", json=ok, headers=JSON)).status_code == 200


async def test_nested_list_items_are_validated(client):
    body = {"operation": "u", "message": {}, "parts": [{"system": 1}]}
    resp = await client.post("/api/process", json=body, headers=JSON)
    assert resp.json()["errors"] == {"parts.0.system": "1 is not of type 'object'"}


@pytest.mark.parametrize("value", ["facebook", " X ", "TWITTER"])
async def test_enum_ci_accepts_spelling_variants(client, value):
    body = {"operation": "u", "message": {}, "platform": value}
    assert (await client.post("/api/process", json=body, headers=JSON)).status_code == 200


async def test_enum_ci_rejects_with_the_canonical_values(client):
    body = {"operation": "u", "message": {}, "platform": "tiktok"}
    resp = await client.post("/api/process", json=body, headers=JSON)
    assert resp.json()["errors"] == {"platform": "'tiktok' is not one of ['Facebook', 'X']"}


async def test_get_route_methods_and_trailing_slash(client):
    for path in ("/api/health/", "/api/health"):
        resp = await client.post(path, json={}, headers=JSON)
        assert resp.status_code == 405
        assert {"GET", "HEAD", "OPTIONS"} <= {m.strip() for m in resp.headers["allow"].split(",")}
    resp = await client.get("/api/health/")
    assert (resp.status_code, resp.json()) == (200, {"status": "ok"})


async def test_a_profile_named_by_its_map(client):
    resp = await client.post("/x/boom", json={}, headers=JSON)
    assert (resp.status_code, resp.json()) == (422, {"problem": "a", "why": "b"})


async def test_the_default_profile_is_the_validation_body(tmp_path):
    profile = ProfileRegistry().for_prefix("anything")
    assert profile.client_input_error("a.b", "bad") == (
        {"errors": {"a.b": "bad"}, "message": "Input payload validation failed"},
        400,
    )
    with pytest.raises(ValueError, match="No API profile"):
        ProfileRegistry(default=None).for_prefix("x")


async def test_custom_user_required_answer(client):
    resp = await client.post("/api/custom_user", json={}, headers=JSON)
    assert (resp.status_code, resp.json()) == (401, {"message": "login first"})
    resp = await client.post("/api/custom_user", json={}, headers={**JSON, "X-User-Name": "ana"})
    assert resp.json() == {"user": "ana"}


async def test_observer_sees_every_answer(client, observer):
    await client.post("/api/empty", json={}, headers=JSON)
    await client.post("/api/empty", content=b"[]", headers=JSON)
    assert observer.seen == [("/api/empty", 200), ("/api/empty", 400)]


async def test_extra_middleware_sees_the_matched_route(client):
    await client.post("/api/empty/", json={}, headers=JSON)
    assert RouteSeen.routes == ["/api/empty"]


async def test_operation_span_continues_the_incoming_trace(client):
    with capture_spans() as spans:
        await client.post(
            "/api/traced", json={}, headers={"traceparent": f"00-{TRACE_ID}-00f067aa0ba902b7-01"}
        )
        work = spans.one("handler.work")
        operation = spans.one("operation.api.traced")
    assert f"{work.context.trace_id:032x}" == TRACE_ID
    assert work.parent.span_id == operation.context.span_id
    assert operation.attributes["operation.group"] == "importer"


def test_catalogue_reads_with_user_from_the_operation(app):
    catalogue = app.state.operation_routes
    assert catalogue.get("/api/custom_user").with_user is True
    assert catalogue.get("/api/empty").with_user is False
    assert requires_user(with_user(lambda call: None)) is True
    assert catalogue.groups == ("importer", "search")
    lines = describe_routes(catalogue, mounted_groups=["importer"])
    assert any("/x/boom" in line and "(group not mounted)" in line for line in lines)


def test_unknown_profile_fails_start_up(tmp_path):
    broken = {**X_MAP, "api_profile": "nowhere_module:Nope"}
    with pytest.raises(CatalogueError, match="unknown api_profile"):
        RouteCatalogue.from_maps([load_map(write(tmp_path, broken, "x.json"))])


def test_unknown_handler_fails_start_up(tmp_path):
    broken = {**X_MAP, "endpoints": [ep("x", "boom", "missing_handler", {"body_kind": "ignored"})]}
    with pytest.raises(CatalogueError, match="unknown handler"):
        RouteCatalogue.from_maps([load_map(write(tmp_path, broken, "x.json"))])


@pytest.mark.parametrize("op", ["search", "config"])
def test_duplicate_routes_fail_start_up_naming_the_path(tmp_path, op):
    def module_map(name):
        return {
            "group": "search",
            "namespaces": [{"name": "qdata", "description": name}],
            "operation_schemas": {},
            "endpoints": [ep("qdata", op, "echo", {"body_kind": "ignored"})],
        }

    maps = [load_map(write(tmp_path, module_map(n), f"{n}.json")) for n in ("one", "two")]
    with pytest.raises(CatalogueError, match=f"duplicate_operation_route: /qdata/{op}"):
        RouteCatalogue.from_maps(maps)


@pytest.mark.parametrize(
    "document, message",
    [
        ({"namespaces": []}, "missing 'group'"),
        ({"group": "g", "models": {"m": {}}}, "operation_schemas"),
    ],
)
def test_invalid_maps_fail_loading(tmp_path, document, message):
    with pytest.raises(CatalogueError, match=message):
        load_map(write(tmp_path, document, "bad.json"))


@pytest.mark.parametrize(
    "contract, message",
    [
        ({"model": "nope"}, "unknown operation schema"),
        ({"body_kind": "json"}, "unknown body_kind"),
    ],
)
def test_invalid_contracts_fail_start_up(tmp_path, contract, message):
    broken = {**X_MAP, "endpoints": [ep("x", "boom", "echo", contract)]}
    with pytest.raises(CatalogueError, match=message):
        RouteCatalogue.from_maps([load_map(write(tmp_path, broken, "x.json"))])


@pytest.mark.parametrize(
    "models, message",
    [
        ({"m": {"a": {"type": "nested", "model": "missing"}}}, "unknown nested model"),
        ({"m": {"a": {"type": "nested", "model": "m"}}}, "recursive nested model"),
        ({"m": {"a": {"type": "list", "super": "tuple"}}}, "unknown list item type"),
        ({"m": {"a": {"type": "date"}}}, "unknown type"),
    ],
)
def test_malformed_models_fail(models, message):
    with pytest.raises(ModelDefinitionError, match=message):
        compile_model("m", models["m"], models)


def test_compiled_schema_matches_flask_restx():
    schema = compile_model(
        "m",
        {
            "a": {"type": "string", "args": {"required": True, "enum": ["x"]}},
            "b": {"type": "float"},
        },
    )
    assert schema == {
        "properties": {"a": {"type": "string", "enum": ["x"]}, "b": {"type": "number"}},
        "type": "object",
        "required": ["a"],
    }
    assert compile_model("none", None) is None
