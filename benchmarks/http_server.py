import sys

if "--original" in sys.argv:
    from gevent import monkey

    monkey.patch_all()
import argparse
import asyncio
import logging
import time
from pathlib import Path
from types import ModuleType

p = argparse.ArgumentParser()
p.add_argument("--original", action="store_true")
p.add_argument("--original-root", required=True)
p.add_argument("--port", type=int, required=True)
args = p.parse_args()

if args.original:
    sys.path.insert(0, str(Path(args.original_root) / "src"))
    module = ModuleType("config")
    module.Config = type("Config", (), {"SECRET_KEY": "benchmark-only"})
    sys.modules["config"] = module
    from flask import Flask
    from gevent.pywsgi import WSGIServer
    from framework.api.server import create_app, create_api
    from framework.api.dynamic import generate_endpoints_from_config

    app = Flask(__name__)
    create_app(app)
    api = create_api(app, "1", "Bench", "Bench")

    def handler(app, request, operation):
        if operation == "io":
            time.sleep(0.005)
        return {"value": request.get_json()["value"]}
else:
    import uvicorn
    from framework.app import FrameworkApp, FrameworkSettings
    from framework.api.dynamic import generate_endpoints_from_config

    app = FrameworkApp(
        FrameworkSettings(
            enable_etl=False, enable_dynamic_endpoints=False, api_title="Bench", log_level="ERROR"
        )
    ).build_app()
    api = app

    async def handler(app, request, operation):
        if operation == "io":
            await asyncio.sleep(0.005)
        return {"value": (await request.json())["value"]}


module = ModuleType("bench_handlers")
module.handler = handler
sys.modules["bench_handlers"] = module
import json
import tempfile

mapping = {
    "namespaces": [{"name": "bench", "description": "benchmark"}],
    "models": {"Payload": {"value": {"type": "integer", "args": {"required": True}}}},
    "endpoints": [
        {
            "namespace": "bench",
            "operation_name": operation,
            "model_name": "Payload",
            "request_method": ["POST"],
            "exec_method": {"module_name": "bench_handlers", "method_name": "handler"},
        }
        for operation in ["noop", "io"]
    ],
}
with tempfile.TemporaryDirectory() as directory:
    path = Path(directory) / "endpoint.json"
    path.write_text(json.dumps(mapping))
    generate_endpoints_from_config(api, str(path))
from framework.commons.logger import logger

logger.setLevel(logging.ERROR)
if args.original:
    import socket

    class NoDelayServer(WSGIServer):
        def handle(self, sock, address):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            return super().handle(sock, address)

    NoDelayServer(("127.0.0.1", args.port), app, log=None, backlog=2048).serve_forever()
else:
    uvicorn.run(
        app, host="127.0.0.1", port=args.port, log_level="error", loop="asyncio", backlog=2048
    )
