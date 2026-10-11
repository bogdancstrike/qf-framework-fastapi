import asyncio
import signal
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from ..api.dynamic import generate_endpoints_from_config
from ..api.server import create_app
from ..commons.logger import configure_logging
from ..config import load_framework_settings
from ..etl.framework_etl import KafkaRuntime
from ..observability import Metrics
from ..tracing import init_tracing, shutdown_tracing


@dataclass
class FrameworkHandles:
    app: FastAPI | None = None
    api: FastAPI | None = None
    runtime: KafkaRuntime | None = None


class FrameworkApp:
    def __init__(self, settings=None, *, app_root=None, config=None, runtime_factory=KafkaRuntime):
        self.settings = settings or load_framework_settings(config=config)
        self.app_root = Path(app_root or Path.cwd())
        self.runtime_factory = runtime_factory
        self.handles = FrameworkHandles()
        self.metrics = Metrics()

    @classmethod
    def from_config(cls, config, **kwargs):
        return cls(config=config, **kwargs)

    def build_app(self):
        if self.handles.app:
            return self.handles.app

        @asynccontextmanager
        async def lifespan(app):
            configure_logging(self.settings.log_level)
            init_tracing(
                self.settings.service_name or self.settings.consumer_name or "qf-worker",
                self.settings.otlp_endpoint,
                self.settings.otlp_insecure,
                enabled=self.settings.enable_tracing,
            )
            try:
                if self.settings.enable_etl:
                    runtime = self.runtime_factory(self.settings, metrics=self.metrics)
                    self.handles.runtime = app.state.runtime = runtime
                    await runtime.start()
                app.state.started = True
                yield
            finally:
                app.state.started = False
                if self.handles.runtime:
                    await self.handles.runtime.stop()
                await asyncio.to_thread(shutdown_tracing)

        app = create_app(self.settings, lifespan=lifespan, metrics=self.metrics)
        if self.settings.enable_dynamic_endpoints:
            generate_endpoints_from_config(app, self.app_root / self.settings.endpoint_json_path)
        if self.settings.init_app:
            self.settings.init_app(app)
        self.handles.app = self.handles.api = app
        return app

    build_fastapi_app = build_app

    def run(self):
        """Build handles; Uvicorn owns the async lifespan. No detached worker thread."""
        self.build_app()
        return self.handles

    def serve(self):
        if not self.settings.enable_api:
            asyncio.run(self.run_worker())
            return
        uvicorn.run(self.build_app(), host=self.settings.api_host, port=self.settings.api_port)

    async def run_worker(self):
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(signum, stop.set)
        runtime = self.runtime_factory(self.settings, metrics=self.metrics)
        self.handles.runtime = runtime
        configure_logging(self.settings.log_level)
        init_tracing(
            self.settings.service_name or self.settings.consumer_name,
            self.settings.otlp_endpoint,
            self.settings.otlp_insecure,
            enabled=self.settings.enable_tracing,
        )
        try:
            await runtime.start()
            waiter = asyncio.create_task(stop.wait())
            try:
                await asyncio.wait({waiter, runtime.task}, return_when=asyncio.FIRST_COMPLETED)
                if runtime.error:
                    raise RuntimeError("Worker runtime failed") from runtime.error
            finally:
                waiter.cancel()
                await asyncio.gather(waiter, return_exceptions=True)
        finally:
            await runtime.stop()
            await asyncio.to_thread(shutdown_tracing)
            for signum in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(signum)

    async def shutdown(self):
        if self.handles.runtime:
            await self.handles.runtime.stop()
