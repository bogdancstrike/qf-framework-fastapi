"""Operation layer: the Flask / flask-restx / Werkzeug wire contract, through a framework app."""

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from framework.api.server import create_app
from framework.config import FrameworkSettings
from framework.operations import (
    ClientInputError,
    ErrorFieldProfile,
    ErrorMapper,
    IdentityResolver,
    OperationCall,
    OperationChain,
    OperationError,
    ProfileRegistry,
    api_operation,
    default_links,
    install_operations,
    load_map,
    parse_roles,
    resolve_identity,
    with_user,
)
from framework.operations.chain import DEFAULT_LINKS

CALLS: list[OperationCall] = []


class AccessDenied(Exception):
    """An application exception the test app registers on its `ErrorMapper`."""


@api_operation
async def echo(call: OperationCall) -> dict[str, Any]:
    CALLS.append(call)
    return {
        "body": call.body,
        "user": call.identity.user,
        "roles": list(call.identity.roles),
        "super": call.identity.is_super,
    }


@api_operation
@with_user
async def echo_named(call: OperationCall) -> dict[str, Any]:
    return await echo.__operation_handler__(call)


@api_operation
async def raises_client_input(call: OperationCall) -> None:
    raise ClientInputError("tree.children1.0", "not an object")


@api_operation
async def raises_denied(call: OperationCall) -> None:
    raise AccessDenied()


@api_operation
async def raises_operation_error(call: OperationCall) -> None:
    raise OperationError(404, {"error": "Document not found"})


@api_operation
async def raises_boom(call: OperationCall) -> None:
    raise RuntimeError("boom")


@api_operation
async def romanian(call: OperationCall) -> dict[str, str]:
    return {"message": "Platformă necunoscută"}


@api_operation
async def created(call: OperationCall) -> tuple[dict[str, str], int, dict[str, str]]:
    return {"id": "a"}, 201, {"X-Extra": "1"}


def endpoint(op: str, handler: str, contract: dict[str, Any]) -> dict[str, Any]:
    return {
        "namespace": "qdata",
        "operation_name": op,
        "model_name": None,
        "request_method": ["POST"],
        "exec_method": {"module_name": __name__, "method_name": handler},
        "operation_contract": contract,
    }


TEST_MAP = {
    "group": "search",
    "namespaces": [{"name": "qdata", "description": "test"}],
    "models": None,
    "operation_schemas": {
        "save": {
            "alias": {"type": "string", "args": {"required": True}},
            "is_public": {"type": "boolean", "args": {"required": True}},
            "tree": {"type": "dict"},
            "page": {"type": "integer"},
            "files": {"type": "list", "super": "dict"},
            "order": {"type": "string", "args": {"enum": ["asc", "desc"]}},
        }
    },
    "endpoints": [
        endpoint("save_search", "echo_named", {"model": "save", "body_kind": "validated"}),
        endpoint("open", "echo", {"model": "save", "body_kind": "validated"}),
        endpoint("config", "echo", {"body_kind": "ignored"}),
        endpoint("admin", "echo_named", {"body_kind": "raw"}),
        endpoint("client_input", "raises_client_input", {"body_kind": "ignored"}),
        endpoint("denied", "raises_denied", {"body_kind": "ignored"}),
        endpoint("missing", "raises_operation_error", {"body_kind": "ignored"}),
        endpoint("boom", "raises_boom", {"body_kind": "object"}),
        endpoint("romanian", "romanian", {"body_kind": "ignored"}),
        endpoint("created", "created", {"body_kind": "ignored"}),
    ],
}

JSON = {"Content-Type": "application/json"}
USER = {**JSON, "X-User-Name": "ana"}


def build_app(tmp_path, document=TEST_MAP, chain=None, **options):
    path = tmp_path / "test.json"
    path.write_text(json.dumps(document))
    app = create_app(FrameworkSettings(enable_etl=False, enable_dynamic_endpoints=False))
    mapper = ErrorMapper().register(
        AccessDenied, lambda exc, spec: ({"status": "FORBIDDEN", "route": spec.path}, 403)
    )
    chain = chain or OperationChain(
        default_links(super_users=lambda ctx: ctx.super_users, span_prefix="operation"), mapper
    )
    install_operations(
        app,
        [load_map(path)],
        chain=chain,
        profiles=ProfileRegistry({"qdata": ErrorFieldProfile()}),
        **{"max_body_bytes": 1024, "cors_origins": ("https://ui.example",), **options},
    )
    app.state.ctx = SimpleNamespace(super_users=("admin1",))
    app.state.started = True
    return app


def client_for(app, base_url="http://test"):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=base_url)


@pytest.fixture
def client(tmp_path):
    CALLS.clear()
    return client_for(build_app(tmp_path))


async def test_valid_body_reaches_the_operation_unchanged(client):
    resp = await client.post(
        "/qdata/save_search", json={"alias": "a", "is_public": True}, headers=USER
    )
    assert resp.status_code == 200
    assert resp.json() == {
        "body": {"alias": "a", "is_public": True},
        "user": "ana",
        "roles": [],
        "super": False,
    }
    assert len(CALLS) == 1


@pytest.mark.parametrize("content_type", ["text/plain", None])
async def test_non_json_content_type_is_415(client, content_type):
    headers = {"X-User-Name": "ana"} | ({"Content-Type": content_type} if content_type else {})
    resp = await client.post("/qdata/save_search", content=b"{}", headers=headers)
    assert resp.status_code == 415
    assert resp.headers["content-type"] == "application/json"
    assert resp.json() == {
        "message": "Did not attempt to load JSON data because the request Content-Type was not "
        "'application/json'."
    }


@pytest.mark.parametrize(
    "content_type",
    ["application/json; charset=utf-8", "application/vnd.api+json", "APPLICATION/JSON"],
)
async def test_json_media_type_variants_are_accepted(client, content_type):
    resp = await client.post("/qdata/config", content=b"{}", headers={"Content-Type": content_type})
    assert resp.status_code == 200


@pytest.mark.parametrize("raw", [b"", b"{bad", b'{"a":', b"\xff\xfe"])
async def test_empty_or_malformed_body_is_werkzeug_400(client, raw):
    resp = await client.post("/qdata/save_search", content=raw, headers=USER)
    assert resp.status_code == 400
    assert resp.json() == {
        "message": "The browser (or proxy) sent a request that this server could not understand."
    }


async def test_required_properties(client):
    resp = await client.post("/qdata/save_search", json={}, headers=USER)
    assert resp.status_code == 400
    assert resp.json() == {
        "errors": {
            "alias": "'alias' is a required property",
            "is_public": "'is_public' is a required property",
        },
        "message": "Input payload validation failed",
    }


@pytest.mark.parametrize(
    "body, key, message",
    [
        ({"alias": None, "is_public": True}, "alias", "None is not of type 'string'"),
        ({"alias": "a", "is_public": "true"}, "is_public", "'true' is not of type 'boolean'"),
        ({"alias": "a", "is_public": True, "page": 2.0}, "page", "2.0 is not of type 'integer'"),
        (
            {"alias": "a", "is_public": True, "order": "ASC"},
            "order",
            "'ASC' is not one of ['asc', 'desc']",
        ),
    ],
)
async def test_types_are_strict(client, body, key, message):
    resp = await client.post("/qdata/save_search", content=json.dumps(body), headers=USER)
    assert resp.status_code == 400
    assert resp.json()["errors"] == {key: message}


@pytest.mark.parametrize(
    "raw, message",
    [
        ("[]", "[] is not of type 'object'"),
        ('"x"', "'x' is not of type 'object'"),
        ("null", "None is not of type 'object'"),
    ],
)
async def test_non_object_on_a_validated_route(client, raw, message):
    resp = await client.post("/qdata/save_search", content=raw, headers=USER)
    assert resp.status_code == 400
    assert resp.json() == {"errors": {"": message}, "message": "Input payload validation failed"}


@pytest.mark.parametrize("raw", ["[]", "null"])
async def test_raw_route_reads_empty_shapes_as_an_empty_object(client, raw):
    resp = await client.post("/qdata/admin", content=raw, headers=USER)
    assert resp.status_code == 200
    assert resp.json()["body"] == {}


class NullIsBadRequest(ErrorFieldProfile):
    """A family whose legacy service answered a JSON ``null`` with Werkzeug's 400."""

    def non_object_body(self, body: Any) -> tuple[Any, int] | None:
        return ({"message": "bad request"}, 400) if body is None else None


@pytest.mark.parametrize(("raw", "expected"), [("null", {"message": "bad request"}), ("[]", None)])
async def test_a_profile_can_answer_non_object_bodies(tmp_path, raw, expected):
    path = tmp_path / "test.json"
    path.write_text(json.dumps(TEST_MAP))
    app = create_app(FrameworkSettings(enable_etl=False, enable_dynamic_endpoints=False))
    install_operations(
        app, [load_map(path)], profiles=ProfileRegistry({"qdata": NullIsBadRequest()})
    )
    app.state.ctx = SimpleNamespace()
    async with client_for(app) as client:
        resp = await client.post("/qdata/save_search", content=raw, headers=USER)
    assert resp.status_code == 400
    if expected is None:  # the profile declines: the flask-restx validation body
        assert resp.json()["message"] == "Input payload validation failed"
    else:
        assert resp.json() == expected


def test_the_package_ships_its_type_hints():
    import framework

    assert (Path(framework.__file__).parent / "py.typed").is_file()


async def test_raw_route_rejects_other_non_objects(client):
    resp = await client.post("/qdata/admin", content="[1]", headers=USER)
    assert resp.status_code == 400
    assert list(resp.json()["errors"]) == [""]
    assert not CALLS


async def test_ignored_route_accepts_any_json(client):
    resp = await client.post("/qdata/config", content="[1, 2]", headers=JSON)
    assert resp.status_code == 200
    assert resp.json()["body"] == {}


async def test_query_parameters_never_fill_the_body(client):
    resp = await client.post(
        "/qdata/save_search?alias=a&is_public=true", json={"is_public": True}, headers=USER
    )
    assert resp.status_code == 400
    assert resp.json()["errors"] == {"alias": "'alias' is a required property"}


async def test_list_item_errors_are_keyed_by_index(client):
    body = {"alias": "a", "is_public": True, "files": ["http://x"]}
    resp = await client.post("/qdata/save_search", json=body, headers=USER)
    assert resp.json()["errors"] == {"files.0": "'http://x' is not of type 'object'"}


@pytest.mark.parametrize("extra", [{}, {"X-User-Name": ""}])
async def test_with_user_operation_requires_a_user(client, extra):
    resp = await client.post(
        "/qdata/save_search", json={"alias": "a", "is_public": True}, headers={**JSON, **extra}
    )
    assert resp.status_code == 400
    assert resp.json() == {"error": "Missing X-User-Name header"}
    assert not CALLS


async def test_validation_runs_before_the_user_check(client):
    resp = await client.post("/qdata/save_search", json={}, headers=JSON)
    assert resp.status_code == 400
    assert "errors" in resp.json()


async def test_anonymous_caller_on_an_operation_without_with_user(client):
    resp = await client.post("/qdata/open", json={"alias": "a", "is_public": False}, headers=JSON)
    assert resp.status_code == 200
    assert resp.json()["user"] == ""


async def test_roles_and_super_users_come_from_the_gate_headers(client):
    headers = {**JSON, "X-User-Name": "admin1", "X-User-Roles": " PROFIL_N, ,PROFIL_Q"}
    resp = await client.post("/qdata/config", json={}, headers=headers)
    assert resp.json()["roles"] == ["PROFIL_N", "PROFIL_Q"]
    assert resp.json()["super"] is True


def test_roles_are_split_trimmed_and_case_kept():
    assert parse_roles(" PROFIL_N, ,PROFIL_Q") == ("PROFIL_N", "PROFIL_Q")
    assert parse_roles("profil_n") == ("profil_n",)
    assert parse_roles(None) == ()


def test_super_users_match_exactly():
    for user, expected in (("admin1", True), (" admin1", False), ("Admin1", False)):
        assert resolve_identity({"x-user-name": user}, ("admin1",)).is_super is expected
    assert resolve_identity({}, ("admin1",)).is_anonymous


@pytest.mark.parametrize(
    "method, path", [("POST", "/qdata/x"), ("GET", "/qdata/x"), ("PUT", "/api/x/y"), ("GET", "/")]
)
async def test_unknown_paths_answer_werkzeug_html_404(client, method, path):
    resp = await client.request(method, path, content=b"{}", headers=JSON)
    assert resp.status_code == 404
    assert resp.headers["content-type"] == "text/html; charset=utf-8"
    assert resp.text.startswith("<!doctype html>\n<html lang=en>\n<title>404 Not Found</title>")


@pytest.mark.parametrize("path", ["/qdata/save_search", "/qdata/save_search/"])
async def test_wrong_method_is_json_405(client, path):
    resp = await client.get(path)
    assert resp.status_code == 405
    assert resp.json() == {"message": "The method is not allowed for the requested URL."}
    assert {"POST", "OPTIONS"} <= {m.strip() for m in resp.headers["allow"].split(",")}


async def test_options_is_flasks_empty_200(client):
    resp = await client.options("/qdata/save_search")
    assert resp.status_code == 200
    assert resp.content == b""
    assert resp.headers["content-type"] == "text/html; charset=utf-8"


async def test_body_over_the_limit_is_413(client):
    resp = await client.post("/qdata/config", content=b"[" + b"1," * 600 + b"1]", headers=JSON)
    assert resp.status_code == 413
    assert resp.json() == {"message": "The data value transmitted exceeds the capacity limit."}


async def test_chunked_body_over_the_limit_is_413(client):
    async def chunks():
        for _ in range(4):
            yield b"[" + b"1," * 200

    resp = await client.post("/qdata/config", content=chunks(), headers=JSON)
    assert resp.status_code == 413


async def test_media_type_is_checked_before_size(client):
    resp = await client.post(
        "/qdata/config", content=b"x" * 2000, headers={"Content-Type": "text/plain"}
    )
    assert resp.status_code == 415


def test_chain_order():
    assert [c.__name__ for c in DEFAULT_LINKS] == [
        "MediaTypeGuard",
        "JsonDecoder",
        "BodyKindPolicy",
        "SchemaValidator",
        "IdentityResolver",
        "HandlerInvoker",
    ]
    assert [type(link) for link in default_links()] == list(DEFAULT_LINKS)


async def test_client_input_error_uses_the_route_profile(client):
    resp = await client.post("/qdata/client_input", json={}, headers=JSON)
    assert resp.status_code == 400
    assert resp.json() == {"error": "Invalid request: tree.children1.0"}


async def test_registered_exception_answers_its_body(client):
    resp = await client.post("/qdata/denied", json={}, headers=JSON)
    assert resp.status_code == 403
    assert resp.json() == {"status": "FORBIDDEN", "route": "/qdata/denied"}


async def test_operation_error_answers_its_exact_body(client):
    resp = await client.post("/qdata/missing", json={}, headers=JSON)
    assert (resp.status_code, resp.json()) == (404, {"error": "Document not found"})


async def test_unhandled_error_is_a_clean_500_without_the_body(client, caplog):
    resp = await client.post("/qdata/boom", json={"content": "secret text"}, headers=JSON)
    assert resp.status_code == 500
    assert resp.json() == {"message": "Internal Server Error"}
    assert "secret text" not in caplog.text


async def test_tuple_results_carry_status_and_headers(client):
    resp = await client.post("/qdata/created", json={}, headers=JSON)
    assert (resp.status_code, resp.json(), resp.headers["x-extra"]) == (201, {"id": "a"}, "1")


async def test_response_bytes_match_flask(client):
    resp = await client.post("/qdata/romanian", json={}, headers=JSON)
    assert resp.content == b'{"message": "Platform\\u0103 necunoscut\\u0103"}\n'


@pytest.mark.parametrize("path", ["/qdata/config", "/qdata/nope", "/qdata/boom"])
async def test_security_headers_on_every_response(client, path):
    resp = await client.post(path, json={}, headers=JSON)
    assert resp.headers["x-frame-options"] == "SAMEORIGIN"
    assert resp.headers["x-content-type-options"] == "nosniff"
    assert resp.headers["referrer-policy"] == "strict-origin-when-cross-origin"
    assert resp.headers["permissions-policy"] == "browsing-topics=()"
    assert "content-security-policy" not in resp.headers
    assert "strict-transport-security" not in resp.headers


async def test_hsts_only_over_https(tmp_path):
    async with client_for(build_app(tmp_path), "https://test") as https:
        resp = await https.post("/qdata/config", json={}, headers=JSON)
    assert resp.headers["strict-transport-security"] == "max-age=31556926; includeSubDomains"


async def test_cors_echoes_an_allowed_origin_only(client):
    resp = await client.post(
        "/qdata/config", json={}, headers={**JSON, "Origin": "https://ui.example"}
    )
    assert resp.headers["access-control-allow-origin"] == "https://ui.example"
    assert "Origin" in resp.headers["vary"]
    resp = await client.post("/qdata/config", json={}, headers={**JSON, "Origin": "https://evil"})
    assert "access-control-allow-origin" not in resp.headers


async def test_cors_preflight_is_answered(client):
    resp = await client.options(
        "/qdata/config",
        headers={
            "Origin": "https://ui.example",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )
    assert resp.status_code == 200
    assert resp.headers["access-control-allow-origin"] == "https://ui.example"
    assert resp.headers["access-control-allow-headers"] == "content-type"


async def test_cors_is_off_without_origins(tmp_path):
    async with client_for(build_app(tmp_path, cors_origins=())) as client:
        resp = await client.post(
            "/qdata/config", json={}, headers={**JSON, "Origin": "https://ui.example"}
        )
    assert "access-control-allow-origin" not in resp.headers


async def test_identity_resolver_reads_custom_gate_headers(tmp_path):
    from framework.operations import GateHeaders
    from framework.operations.chain import (
        BodyKindPolicy,
        HandlerInvoker,
        JsonDecoder,
        MediaTypeGuard,
        SchemaValidator,
    )

    links = (
        MediaTypeGuard(),
        JsonDecoder(),
        BodyKindPolicy(),
        SchemaValidator(),
        IdentityResolver(gate=GateHeaders(user="x-remote-user", roles="x-remote-groups")),
        HandlerInvoker(),
    )
    app = build_app(tmp_path, chain=OperationChain(links))
    async with client_for(app) as client:
        resp = await client.post(
            "/qdata/config",
            json={},
            headers={**JSON, "X-Remote-User": "bob", "X-Remote-Groups": "a,b"},
        )
    assert (resp.json()["user"], resp.json()["roles"]) == ("bob", ["a", "b"])
