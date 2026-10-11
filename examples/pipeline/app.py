"""Run: uvicorn examples.pipeline.app:app --port 4000 --workers 1."""

import asyncio
import os
from contextlib import asynccontextmanager
from uuid import uuid4

from fastapi import HTTPException, Request
from opentelemetry import trace
from pydantic import BaseModel, Field

from framework.app import FrameworkApp
from framework.config import FrameworkSettings
from framework.streams.kafka_client import KafkaClient

from . import topics

settings = FrameworkSettings(
    _env_file=None,
    enable_etl=True,
    enable_dynamic_endpoints=False,
    worker_modules=["examples.pipeline.workers"],
    consumer_name=os.getenv("QSINT_CONSUMER_NAME", f"{topics.PREFIX}.workers"),
    kafka_bootstrap_servers=os.getenv("QSINT_KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"),
    redis_url=os.getenv("QSINT_REDIS_URL", "redis://localhost:26379/0"),
    error_topic=topics.DLQ,
)
framework = FrameworkApp(settings)
app = framework.build_app()
framework_lifespan = app.router.lifespan_context


@asynccontextmanager
async def lifespan(application):
    # Compose lifespans: framework owns workers; this app owns HTTP ingress.
    async with framework_lifespan(application):
        async with KafkaClient(settings.kafka_bootstrap_servers) as ingress:
            application.state.ingress = ingress
            yield


app.router.lifespan_context = lifespan


class Job(BaseModel):
    id: str = Field(default_factory=lambda: uuid4().hex, min_length=1, max_length=100)
    text: str = Field(min_length=1, max_length=10000)


@app.post("/jobs", status_code=202)
async def submit(job: Job, request: Request):
    try:
        async with asyncio.timeout(settings.output_ack_timeout_sec):
            metadata = await request.app.state.ingress.put_message(
                topics.INPUT, job.model_dump(), key=job.id
            )
    except Exception as exc:
        # A timeout is ambiguous: Kafka may already have accepted the input.
        raise HTTPException(
            503, "Kafka submission unavailable; acceptance may be ambiguous"
        ) from exc
    context = trace.get_current_span().get_span_context()
    return {
        "id": job.id,
        "accepted": True,
        "topic": metadata.topic,
        "partition": metadata.partition,
        "offset": metadata.offset,
        "trace_id": f"{context.trace_id:032x}" if context.is_valid else None,
    }
