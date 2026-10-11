"""Independent process for the real OTLP HTTP -> fanout -> join tracing test."""

import argparse
import json
from opentelemetry import trace

from framework.app import FrameworkApp, FrameworkSettings
from framework.decorators import kafka_handler, kafka_aggregator
from framework.tracing import get_tracer, inject_trace_headers

p = argparse.ArgumentParser()
p.add_argument("--role", choices=["ingress", "a", "b", "merge"], required=True)
p.add_argument("--prefix", required=True)
p.add_argument("--port", type=int, default=0)
p.add_argument("--bootstrap", default="localhost:29092")
p.add_argument("--otlp", default="http://localhost:24317")
args = p.parse_args()
prefix = args.prefix


async def handler(message, consumer_name, metadatas):
    with get_tracer().start_as_current_span("application." + args.role):
        if args.role in {"a", "b"}:
            message[args.role] = True
        if args.role == "merge":
            message["complete"] = True
        return message


if args.role == "ingress":
    kafka_handler(
        name="fanout", topics_in=[prefix + "-in"], topics_out=[prefix + "-a", prefix + "-b"]
    )(handler)
elif args.role in {"a", "b"}:
    kafka_handler(
        name=args.role,
        topics_in=[prefix + "-" + args.role],
        topics_out=[prefix + "-join-" + args.role],
    )(handler)
else:
    kafka_aggregator(
        name="merge",
        topics_in=[prefix + "-join-a", prefix + "-join-b"],
        topics_out=[prefix + "-out"],
        aggregate_by="id",
    )(handler)
settings = FrameworkSettings(
    worker_modules=["json"],
    kafka_bootstrap_servers=args.bootstrap,
    consumer_name=prefix + "-" + args.role,
    enable_api=args.role == "ingress",
    enable_dynamic_endpoints=False,
    enable_tracing=True,
    otlp_endpoint=args.otlp,
    otlp_insecure=True,
    service_name="qf-otlp-" + args.role,
    log_level="ERROR",
    redis_url="redis://localhost:26379/0",
    api_port=args.port or 4000,
)
framework = FrameworkApp(settings)
if args.role == "ingress":
    app = framework.build_app()

    @app.post("/publish")
    async def publish(payload: dict):
        await app.state.runtime.producer.send_and_wait(
            prefix + "-in", json.dumps(payload).encode(), headers=inject_trace_headers()
        )
        context = trace.get_current_span().get_span_context()
        return {"trace_id": f"{context.trace_id:032x}"}


framework.serve()
