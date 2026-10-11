import json
import sys
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient
from framework.app import FrameworkApp, FrameworkSettings


def make_app(tmp_path, monkeypatch, async_handler=False):
    def handler(app, operation, request, item_id):
        return {
            "id": item_id,
            "name": request.get_json()["name"],
            "query": request.args.get("q"),
        }, 201

    async def native(app, operation, request, item_id):
        return {"id": item_id, "name": (await request.json())["name"]}

    monkeypatch.setitem(
        sys.modules, "api_fixture", SimpleNamespace(handler=native if async_handler else handler)
    )
    mapping = {
        "namespaces": [{"name": "items", "description": "items"}],
        "models": {"Item": {"name": {"type": "string", "args": {"required": True}}}},
        "endpoints": [
            {
                "namespace": "items",
                "operation_name": "save",
                "model_name": "Item",
                "request_method": ["POST"],
                "api_url": "/items/<int:item_id>",
                "exec_method": {"module_name": "api_fixture", "method_name": "handler"},
            }
        ],
    }
    (tmp_path / "endpoint.json").write_text(json.dumps(mapping))
    return FrameworkApp(
        FrameworkSettings(enable_etl=False, endpoint_json_path="endpoint.json"), app_root=tmp_path
    ).build_app()


@pytest.mark.parametrize("native", [False, True])
def test_dynamic_routes_typed_validation_and_sync_adapter(tmp_path, monkeypatch, native):
    app = make_app(tmp_path, monkeypatch, native)
    with TestClient(app) as client:
        response = client.post("/items/42?q=x", json={"name": "alice"})
        assert response.status_code == (200 if native else 201)
        assert response.json()["id"] == 42
        assert client.post("/items/no", json={"name": "alice"}).status_code == 422
        assert client.post("/items/42", json={}).status_code == 422
        assert client.get("/health/ready").status_code == 200
        assert "Item" in client.get("/openapi.json").json()["components"]["schemas"]
        assert client.get("/metrics").status_code == 200
        assert response.headers["x-request-id"]
    assert not app.state.started


def test_metrics_cardinality_and_app_isolation(tmp_path):
    settings = FrameworkSettings(
        enable_etl=False, enable_dynamic_endpoints=False, log_level="ERROR"
    )
    one, two = FrameworkApp(settings).build_app(), FrameworkApp(settings).build_app()
    assert one.state.metrics.registry is not two.state.metrics.registry
    with TestClient(one) as client:
        for i in range(100):
            assert client.get(f"/unknown/{i}").status_code == 404
        metrics = client.get("/metrics").text
        assert 'route="unmatched"' in metrics
        assert "/unknown/" not in metrics


def test_init_hook_failure_propagates():
    def broken(app):
        raise RuntimeError("hook")

    with pytest.raises(RuntimeError, match="hook"):
        FrameworkApp(
            FrameworkSettings(enable_etl=False, enable_dynamic_endpoints=False, init_app=broken)
        ).build_app()


def test_readiness_tracks_runtime_failure():
    class Runtime:
        def __init__(self, *a, **kw):
            self.ready = True

        async def start(self):
            pass

        async def stop(self):
            self.ready = False

    app = FrameworkApp(
        FrameworkSettings(
            worker_modules=["x"],
            kafka_bootstrap_servers="x",
            consumer_name="x",
            enable_dynamic_endpoints=False,
        ),
        runtime_factory=Runtime,
    ).build_app()
    with TestClient(app) as client:
        assert client.get("/health/live").status_code == 200
        assert client.get("/health/ready").status_code == 200
        app.state.runtime.ready = False
        assert client.get("/health/ready").status_code == 503
        assert client.get("/health/live").status_code == 200
        app.state.runtime.error = RuntimeError("terminal transport failure")
        response = client.get("/health/live")
        assert response.status_code == 503
        assert response.json() == {"status": "failed"}
        assert "transport" not in response.text


def test_error_response_keeps_request_id_and_hides_exception():
    app = FrameworkApp(
        FrameworkSettings(enable_etl=False, enable_dynamic_endpoints=False, log_level="ERROR")
    ).build_app()

    @app.get("/broken")
    async def broken():
        raise RuntimeError("internal secret")

    with TestClient(app, raise_server_exceptions=False) as client:
        response = client.get("/broken", headers={"X-Request-ID": "test-id"})
        assert response.status_code == 500
        assert response.headers["x-request-id"] == "test-id"
        assert response.json()["request_id"] == "test-id"
        assert "internal secret" not in response.text


@pytest.mark.parametrize("native", [False, True])
@pytest.mark.parametrize("method", ["GET", "POST", "PUT", "PATCH", "DELETE"])
@pytest.mark.parametrize(
    "variant", ["null_name", "omitted_name", "null_models", "omitted_models", "null_definition"]
)
def test_endpoints_without_model_accept_optional_unvalidated_json(
    tmp_path, monkeypatch, native, method, variant
):
    def sync_handler(app, operation, request):
        return {"payload": request.get_json()}

    async def async_handler(app, operation, request):
        raw = await request.body()
        return {"payload": json.loads(raw) if raw else None}

    monkeypatch.setitem(
        sys.modules,
        "no_model_fixture",
        SimpleNamespace(handler=async_handler if native else sync_handler),
    )
    entry = {
        "namespace": "free",
        "operation_name": "echo",
        "model_name": None,
        "request_method": [method],
        "exec_method": {"module_name": "no_model_fixture", "method_name": "handler"},
    }
    mapping = {"namespaces": [{"name": "free"}], "models": {}, "endpoints": [entry]}
    if variant == "omitted_name":
        entry.pop("model_name")
    elif variant == "null_models":
        mapping["models"] = None
    elif variant == "omitted_models":
        mapping.pop("models")
    elif variant == "null_definition":
        mapping["models"] = {"Unvalidated": None}
        entry["model_name"] = "Unvalidated"
    (tmp_path / "endpoint.json").write_text(json.dumps(mapping))
    app = FrameworkApp(
        FrameworkSettings(enable_etl=False, endpoint_json_path="endpoint.json"), app_root=tmp_path
    ).build_app()
    with TestClient(app) as client:
        assert client.request(method, "/free/echo").json() == {"payload": None}
        schema = client.get("/openapi.json").json()["paths"]["/free/echo"][method.lower()]
        assert "requestBody" not in schema
        if method != "GET":
            for payload in ({"arbitrary": 42}, [1, "two"], "hello", None):
                response = client.request(
                    method,
                    "/free/echo",
                    content=json.dumps(payload),
                    headers={"Content-Type": "application/json"},
                )
                assert response.status_code == 200
                assert response.json() == {"payload": payload}
            assert client.request(method, "/free/echo", content="{").status_code == 400
